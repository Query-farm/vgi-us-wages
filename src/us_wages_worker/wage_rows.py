"""Reading a cached OEWS release: filters, row-group pruning and derived columns."""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any, cast

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from vgi.table_filter_pushdown import (
    AndFilter,
    ComparisonOp,
    ConstantFilter,
    Filter,
    InFilter,
    IsNotNullFilter,
    IsNullFilter,
)

from .common import AREA_TYPE_CODES, AREA_TYPE_NAMES, NEW_ENGLAND, STATE_ABBR_TO_FIPS, STATE_FIPS_TO_ABBR, pylist
from .meta import column
from .oews import CACHE_SCHEMA, WAGE_COLUMNS, Release

D = pa.float64()

# Output columns shared by wages() and wages_for_zip(), in order, after the area columns.
OCCUPATION_FIELDS: list[pa.Field[Any]] = [
    column("occ_code", pa.string(), "SOC 2018 code 'XX-XXXX' (or an OEWS-specific aggregate code).", nullable=False),
    column("occ_title", pa.string(), "SOC or OEWS occupation title."),
    column("o_group", pa.string(), "Occupation level: total, major, minor, broad or detailed."),
    column("tot_emp", pa.int64(), "Estimated employment, rounded to the nearest 10; excludes the self-employed."),
    column("emp_prse", D, "Relative standard error of tot_emp, in percent."),
    column("jobs_1000", D, "Jobs in this occupation per 1,000 jobs in the area (state, metro and nonmetro rows)."),
    column("loc_quotient", D, "Location quotient: the occupation's share of area employment over its US share."),
    column("h_mean", D, "Mean hourly wage, US dollars."),
    column("a_mean", D, "Mean annual wage, US dollars."),
    column("mean_prse", D, "Relative standard error of the mean wage, in percent."),
    column("h_pct10", D, "Hourly wage, 10th percentile."),
    column("h_pct25", D, "Hourly wage, 25th percentile."),
    column("h_median", D, "Hourly wage, median."),
    column("h_pct75", D, "Hourly wage, 75th percentile."),
    column("h_pct90", D, "Hourly wage, 90th percentile."),
    column("a_pct10", D, "Annual wage, 10th percentile."),
    column("a_pct25", D, "Annual wage, 25th percentile."),
    column("a_median", D, "Annual wage, median."),
    column("a_pct75", D, "Annual wage, 75th percentile."),
    column("a_pct90", D, "Annual wage, 90th percentile."),
    column(
        "annual_only",
        pa.bool_(),
        "BLS publishes only annual wages for this occupation (e.g. teachers).",
        nullable=False,
    ),
    column(
        "hourly_only", pa.bool_(), "BLS publishes only hourly wages for this occupation (e.g. actors).", nullable=False
    ),
    column(
        "suppressed_columns",
        pa.list_(pa.string()),
        "Columns BLS marked not available ('*' wage, '**' employment); they are NULL here, never 0.",
        nullable=False,
    ),
    column(
        "topcoded_columns",
        pa.list_(pa.string()),
        "Wage columns BLS top-coded ('#'): the true value is at least topcode_hourly/topcode_annual; NULL here.",
        nullable=False,
    ),
    column("topcode_hourly", D, "This release's hourly top-code threshold (e.g. 115.0), from the workbook notes."),
    column("topcode_annual", D, "This release's annual top-code threshold (e.g. 239200.0), from the workbook notes."),
]

AREA_FIELDS: list[pa.Field[Any]] = [
    column("release_year", pa.int16(), "May reference year of the OEWS estimates (e.g. 2025).", nullable=False),
    column(
        "area_code",
        pa.string(),
        "OEWS area code: '99' US, 2-digit state FIPS, 5-digit CBSA, 7-digit nonmetro.",
        nullable=False,
    ),
    column("area_title", pa.string(), "Area name as BLS publishes it."),
    column(
        "area_type", pa.int8(), "BLS area type: 1 US, 2 state, 3 territory, 4 metro (MSA), 6 nonmetro.", nullable=False
    ),
    column("area_type_name", pa.string(), "national, state, territory, metro or nonmetro.", nullable=False),
    column("prim_state", pa.string(), "USPS abbreviation of the area's primary state ('US' for the nation)."),
    column("state_fips", pa.string(), "2-digit FIPS code of prim_state (NULL for the US row)."),
    column("cbsa_code", pa.string(), "5-digit OMB CBSA code for metro rows; NULL for NECTAs and non-metro rows."),
    column(
        "is_necta", pa.bool_(), "True for New England NECTA metro areas (releases before May 2024).", nullable=False
    ),
]

INDUSTRY_FIELDS: list[pa.Field[Any]] = [
    column("naics", pa.string(), "NAICS code of the industry ('000000' = cross-industry)."),
    column("naics_title", pa.string(), "Industry title."),
    column("i_group", pa.string(), "Industry level: cross-industry, sector, 3-digit, 4-digit, 5-digit, 6-digit."),
    column("own_code", pa.string(), "Ownership: 1235 all, 5 private, 1/2/3 federal/state/local government, ..."),
]
INDUSTRY_SHARE_FIELDS: list[pa.Field[Any]] = [
    column("pct_total", D, "Percent of industry employment in this occupation (national industry rows only)."),
    column("pct_rpt", D, "Percent of establishments reporting the occupation (national industry rows only)."),
    column("pct_rpt_below_half", pa.bool_(), "BLS marked pct_rpt '~': under 0.5% of establishments.", nullable=False),
]

WAGES_SCHEMA = pa.schema([*AREA_FIELDS, *INDUSTRY_FIELDS, *OCCUPATION_FIELDS, *INDUSTRY_SHARE_FIELDS])

BASE_COLUMNS = set(CACHE_SCHEMA.names)
_PC_OP = {
    ComparisonOp.EQ: pc.equal,
    ComparisonOp.NE: pc.not_equal,
    ComparisonOp.GT: pc.greater,
    ComparisonOp.GE: pc.greater_equal,
    ComparisonOp.LT: pc.less,
    ComparisonOp.LE: pc.less_equal,
}


@dataclass(frozen=True, slots=True)
class Selection:
    """What to read: equality sets for pruning plus a row predicate."""

    area_codes: frozenset[str] | None
    occ_codes: frozenset[str] | None
    predicate: pc.Expression | None


def _and(a: pc.Expression | None, b: pc.Expression | None) -> pc.Expression | None:
    if a is None:
        return b
    if b is None:
        return a
    return a & b


def _translate(f: Filter) -> tuple[pc.Expression | None, dict[str, frozenset[str]]]:
    """Translate one pushed filter into an Arrow expression (None = not translated)."""
    eq: dict[str, frozenset[str]] = {}
    name = f.column_name
    if isinstance(f, AndFilter):
        expr = None
        for child in f.children:
            e, ceq = _translate(child)
            expr = _and(expr, e)
            for k, v in ceq.items():
                eq[k] = eq[k] & v if k in eq else v
        return expr, eq
    target = {"cbsa_code": "area_code"}.get(name, name)
    if target not in BASE_COLUMNS:
        return None, eq
    field = pc.field(target)
    if isinstance(f, ConstantFilter):
        value = f.value.as_py()
        if value is None:
            return None, eq
        if f.op is ComparisonOp.EQ and isinstance(value, str):
            eq[target] = frozenset([value])
        if name == "cbsa_code":
            return (pc.field("area_type") == 4) & (field == value) if f.op is ComparisonOp.EQ else None, eq
        return _PC_OP[f.op](field, pc.scalar(value)), eq
    if isinstance(f, InFilter) and name != "cbsa_code":
        values = [v for v in pylist(f.values) if v is not None]
        if all(isinstance(v, str) for v in values):
            eq[target] = frozenset(values)
        return field.isin(values), eq
    if isinstance(f, IsNullFilter) and name != "cbsa_code":
        return field.is_null(), eq
    if isinstance(f, IsNotNullFilter) and name != "cbsa_code":
        return cast(pc.Expression, field.is_valid()), eq
    return None, eq


def occupation_predicate(occupation: str | None) -> tuple[pc.Expression | None, frozenset[str] | None]:
    """``'15-1252'`` exact; ``'15-'`` / ``'15-12'`` prefix; None/'all' everything."""
    if occupation is None or occupation.strip().lower() in {"", "all", "*"}:
        return None, None
    code = occupation.strip()
    if len(code) == 7:
        return pc.field("occ_code") == code, frozenset([code])
    return pc.starts_with(pc.field("occ_code"), code), None


def build_selection(
    *,
    occupation: str | None,
    area: str | None,
    area_type: str | None,
    state: str | None,
    industry: str | None,
    pushed: Any,
) -> Selection:
    """Combine named arguments and pushed WHERE filters into one `Selection`."""
    expr, occ_codes = occupation_predicate(occupation)
    area_codes: frozenset[str] | None = None
    if area is not None and area.strip().lower() not in {"", "all"}:
        code = area.strip()
        area_codes = frozenset([code])
        expr = _and(expr, pc.field("area_code") == code)
    if area_type is not None and area_type.lower() != "all":
        expr = _and(expr, pc.field("area_type") == AREA_TYPE_CODES[area_type.lower()])
    if state is not None and state.strip():
        s = state.strip().upper()
        abbr = STATE_FIPS_TO_ABBR.get(s.zfill(2), s) if s.isdigit() else s
        if abbr not in STATE_ABBR_TO_FIPS:
            raise ValueError(f"unknown state {state!r}; use a USPS abbreviation ('CA') or 2-digit FIPS ('06')")
        expr = _and(expr, pc.field("prim_state") == abbr)
    ind = (industry or "cross-industry").strip().lower()
    if ind == "cross-industry":
        expr = _and(expr, pc.field("i_group") == "cross-industry")
    elif ind != "all":
        code = ind.ljust(6, "0") if ind.isdigit() else ind
        expr = _and(expr, pc.field("naics") == code)
    for f in pushed.filters if pushed is not None else ():
        e, eq = _translate(f)
        expr = _and(expr, e)
        if "area_code" in eq:
            area_codes = eq["area_code"] if area_codes is None else area_codes & eq["area_code"]
        if "occ_code" in eq:
            occ_codes = eq["occ_code"] if occ_codes is None else occ_codes & eq["occ_code"]
    return Selection(area_codes=area_codes, occ_codes=occ_codes, predicate=expr)


def _may_contain(stats: Any, values: frozenset[str] | None) -> bool:
    if values is None or stats is None or not stats.has_min_max:
        return True
    return any(stats.min <= v <= stats.max for v in values)


def iter_row_groups(release: Release, sel: Selection, start: int = 0) -> Iterator[tuple[int, pa.Table]]:
    """Yield ``(row_group_index, filtered_rows)`` from ``start``, skipping groups stats rule out."""
    pf = pq.ParquetFile(release.parquet)
    names = pf.schema_arrow.names
    a_idx, o_idx = names.index("area_code"), names.index("occ_code")
    for i in range(start, pf.num_row_groups):
        rg = pf.metadata.row_group(i)
        if not _may_contain(rg.column(a_idx).statistics, sel.area_codes) or not _may_contain(
            rg.column(o_idx).statistics, sel.occ_codes
        ):
            yield i, CACHE_SCHEMA.empty_table()
            continue
        t = pf.read_row_group(i)
        if sel.predicate is not None:
            t = t.filter(sel.predicate)
        yield i, t


def read_all(release: Release, sel: Selection) -> pa.Table:
    """Every matching row of a release."""
    parts = [t for _, t in iter_row_groups(release, sel) if t.num_rows]
    return pa.concat_tables(parts) if parts else CACHE_SCHEMA.empty_table()


def enrich(t: pa.Table, release: Release) -> dict[str, pa.Array[Any] | pa.ChunkedArray[Any]]:
    """Compute the derived columns (release, area labels, FIPS, CBSA, top-code thresholds) for cache rows."""
    n = t.num_rows
    area_type = pylist(t["area_type"])
    codes = pylist(t["area_code"])
    prim = pylist(t["prim_state"])
    necta = [
        at == 4 and code[:1] == "7" and len(code) == 5 and (ps in NEW_ENGLAND)
        for at, code, ps in zip(area_type, codes, prim, strict=True)
    ]
    cols: dict[str, Any] = {name: t[name] for name in t.column_names}
    cols["release_year"] = pa.array([release.year] * n, pa.int16())
    cols["area_type_name"] = pa.array([AREA_TYPE_NAMES.get(a, str(a)) for a in area_type], pa.string())
    cols["state_fips"] = pa.array(
        [
            code if at in (2, 3) else STATE_ABBR_TO_FIPS.get(ps or "")
            for at, code, ps in zip(area_type, codes, prim, strict=True)
        ],
        pa.string(),
    )
    cols["cbsa_code"] = pa.array(
        [code if at == 4 and not nc else None for at, code, nc in zip(area_type, codes, necta, strict=True)],
        pa.string(),
    )
    cols["is_necta"] = pa.array(necta, pa.bool_())
    cols["topcode_hourly"] = pa.array([release.topcode_hourly] * n, pa.float64())
    cols["topcode_annual"] = pa.array([release.topcode_annual] * n, pa.float64())
    return cols


def to_batch(t: pa.Table, release: Release, schema: pa.Schema, extra: dict[str, Any] | None = None) -> pa.RecordBatch:
    """Project enriched rows onto ``schema`` (a possibly projected output schema)."""
    cols = enrich(t, release)
    if extra:
        cols.update(extra)
    arrays = []
    for f in schema:
        col = cols[f.name]
        if isinstance(col, pa.ChunkedArray):
            col = col.combine_chunks()
        arrays.append(col.cast(f.type) if col.type != f.type else col)
    return pa.RecordBatch.from_arrays(arrays, schema=schema)


__all__ = ["OCCUPATION_FIELDS", "WAGES_SCHEMA", "WAGE_COLUMNS", "Selection", "build_selection", "iter_row_groups"]
