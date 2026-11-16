"""OEWS table functions: ``wages``, ``wages_for_zip``, ``areas`` and ``area_counties``."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Annotated, Any, ClassVar

import pyarrow as pa
import pyarrow.compute as pc
from vgi.arguments import Arg
from vgi.metadata import FunctionExample
from vgi.table_function import (
    OutputCollector,
    ProcessParams,
    TableFunctionGenerator,
    bind_fixed_schema,
    init_single_worker,
)
from vgi_rpc import ArrowSerializableDataclass

from .common import AREA_TYPE_NAMES, RELEASE_PATTERN, STATE_FIPS_TO_ABBR, options_from, parse_release, pylist
from .meta import column, object_tags, result_columns
from .oews import ensure_area_definitions, ensure_release, load_release
from .reference import bundled
from .wage_rows import (
    AREA_FIELDS,
    OCCUPATION_FIELDS,
    WAGES_SCHEMA,
    build_selection,
    enrich,
    iter_row_groups,
    read_all,
    to_batch,
)

AREA_TYPE_CHOICES = ["all", "national", "state", "territory", "metro", "nonmetro"]
OCC_PATTERN = r"^(all|\d\d-\d{0,4})$"
OCC_DOC = (
    "SOC code 'XX-XXXX' (e.g. '15-1252'), a code prefix such as '15-' or '29-11' for a whole group, or 'all' (default)."
)
RELEASE_DOC = "May reference year as text ('2024'), or 'latest' (default) for the newest BLS release."
METRO_LABEL = "metro-level estimate (not ZIP-level)"
NONMETRO_LABEL = "nonmetropolitan-area estimate (not ZIP-level)"


@dataclass(kw_only=True)
class ScanState(ArrowSerializableDataclass):
    """Cursor: the resolved release and the next Parquet row group to read."""

    year: int = 0
    next_row_group: int = 0
    done: bool = False


# ============================================================================ wages


@dataclass(slots=True, frozen=True, kw_only=True)
class WagesArgs:
    """Arguments for ``wages(...)``; all named, all optional."""

    occupation: Annotated[str, Arg("occupation", default="all", doc=OCC_DOC, pattern=OCC_PATTERN)]
    area: Annotated[
        str,
        Arg(
            "area",
            default="all",
            doc="OEWS area code: '99' (US), state FIPS ('06'), CBSA ('41860') or nonmetro ('0600001'); 'all'.",
        ),
    ]
    area_type: Annotated[
        str,
        Arg(
            "area_type",
            default="all",
            doc="national, state, territory, metro, nonmetro, or all.",
            choices=AREA_TYPE_CHOICES,
        ),
    ]
    state: Annotated[
        str,
        Arg(
            "state",
            default="",
            doc="Only areas whose primary state is this USPS code ('CA') or FIPS ('06'): the state and its metros.",
        ),
    ]
    industry: Annotated[
        str,
        Arg(
            "industry",
            default="cross-industry",
            doc="'cross-industry' (default), 'all' (adds national NAICS rows) or a NAICS code such as '541500'.",
        ),
    ]
    release: Annotated[str, Arg("release", default="latest", doc=RELEASE_DOC, pattern=RELEASE_PATTERN)]


WAGES_DOC_LLM = (
    "Occupational employment and wage estimates from BLS OEWS, one row per area x industry x occupation. "
    "All arguments are named and optional and filter before rows leave the worker: occupation := SOC code "
    "'15-1252' or prefix '15-'; area := OEWS area code ('99' US, '06' California, '41860' San Francisco "
    "metro, '0600001' a nonmetro area); area_type := 'national'|'state'|'territory'|'metro'|'nonmetro'; "
    "state := 'CA' (the state's own row plus metros and nonmetro areas whose primary state it is); "
    "industry := 'cross-industry' (default), 'all', or a NAICS code (industry rows exist for the US only); "
    "release := 'latest' or a year such as '2024'. WHERE filters on area_code, occ_code, o_group, "
    "prim_state, cbsa_code, tot_emp, wages etc. are pushed down as well. Use o_group = 'detailed' to avoid "
    "double counting aggregates. Wages are US dollars; suppressed ('*', '**') and top-coded ('#') values "
    "are NULL and listed in suppressed_columns / topcoded_columns. The first call for a release downloads "
    "the BLS all-data file (~80 MB) into the local cache; later calls are local."
)


@init_single_worker
@bind_fixed_schema
class Wages(TableFunctionGenerator[WagesArgs, ScanState]):
    """Stream OEWS rows of one release, filtered locally."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = WAGES_SCHEMA

    class Meta:
        """Function metadata surfaced to DuckDB, agents and vgi-lint."""

        name = "wages"
        description = "BLS OEWS employment and wages by occupation, area and industry for one May release"
        filter_pushdown = True
        auto_apply_filters = True
        projection_pushdown = True
        tags = {
            **object_tags(
                title="Occupational Wages (OEWS)",
                category="wages",
                keywords="wages, salary, pay, median wage, employment, occupation, metro, state, bls, oews",
                doc_llm=WAGES_DOC_LLM,
                doc_md=(
                    "## wages(occupation := , area := , area_type := , state := , industry := , release := )\n\n"
                    "BLS [OEWS](https://www.bls.gov/oes/) estimates: employment, mean and percentile wages "
                    "for every occupation in the US, each state, metro (CBSA) and nonmetro area.\n\n"
                    "- Filters are named arguments and pushed-down `WHERE` clauses, applied locally.\n"
                    "- `*`/`**`/`#` markers become `NULL` plus `suppressed_columns` / `topcoded_columns`.\n"
                    "- The first query for a release downloads the BLS file (~80 MB) into the cache."
                ),
            ),
            "vgi.source_url": "https://www.bls.gov/oes/tables.htm",
            "vgi.result_columns_schema": result_columns(WAGES_SCHEMA, short=True),
        }
        examples = [
            FunctionExample(
                sql=(
                    "SELECT area_title, a_median, tot_emp FROM us_wages.main.wages(occupation := '29-1141', "
                    "area_type := 'metro') WHERE a_median IS NOT NULL ORDER BY a_median DESC LIMIT 5"
                ),
                description="The five metro areas paying registered nurses the highest median wage",
            ),
            FunctionExample(
                sql=(
                    "SELECT occ_code, occ_title, a_median FROM us_wages.main.wages(area := '06') "
                    "WHERE o_group = 'major' ORDER BY a_median DESC NULLS LAST"
                ),
                description="Median annual pay of each major occupation group in California",
            ),
        ]

    @classmethod
    def initial_state(cls, params: ProcessParams[WagesArgs]) -> ScanState:
        """Resolve the release and make sure it is in the cache (downloads on first use)."""
        opts = options_from(params.attach_options)
        release = load_release(opts, parse_release(params.args.release))
        return ScanState(year=release.year)

    @classmethod
    def process(cls, params: ProcessParams[WagesArgs], state: ScanState, out: OutputCollector) -> None:
        """Emit the matching rows of the next non-empty row group."""
        if state.done:
            out.finish()
            return
        a = params.args
        release = ensure_release(options_from(params.attach_options), state.year)
        sel = build_selection(
            occupation=a.occupation,
            area=a.area,
            area_type=a.area_type,
            state=a.state,
            industry=a.industry,
            pushed=params.current_pushdown_filters,
        )
        for i, rows in iter_row_groups(release, sel, state.next_row_group):
            state.next_row_group = i + 1
            if rows.num_rows:
                out.emit(to_batch(rows, release, params.output_schema))
                return
        state.done = True
        out.finish()


# ==================================================================== wages_for_zip

ZIP_FIELDS: list[pa.Field[Any]] = [
    column("zip", pa.string(), "The ZIP code asked about (matched to the 2020 ZCTA of the same code).", nullable=False),
    column(
        "estimate_geography",
        pa.string(),
        "Always says which geography the figures describe: a metro or nonmetro area, never the ZIP itself.",
        nullable=False,
    ),
    column(
        "zcta_land_share",
        pa.float64(),
        "Share (0-1) of the ZCTA's land area that lies in this OEWS area (2020 Census relationship file).",
        nullable=False,
    ),
    column("is_primary_area", pa.bool_(), "True for the OEWS area holding most of the ZCTA's land.", nullable=False),
    column(
        "counties", pa.list_(pa.string()), "5-digit county FIPS codes of the ZCTA inside this area.", nullable=False
    ),
]
ZIP_SCHEMA = pa.schema([*ZIP_FIELDS, *AREA_FIELDS, *OCCUPATION_FIELDS])


@dataclass(slots=True, frozen=True, kw_only=True)
class ZipArgs:
    """Arguments for ``wages_for_zip(zip, occupation, all_areas := false, release := 'latest')``."""

    zip: Annotated[str, Arg(0, doc="5-digit ZIP code, e.g. '94103'.", pattern=r"^\d{5}$")]
    occupation: Annotated[str, Arg(1, doc=OCC_DOC.replace(" (default)", ""), pattern=OCC_PATTERN)]
    all_areas: Annotated[
        bool,
        Arg(
            "all_areas", default=False, doc="Also return the other OEWS areas a ZIP straddles (default: primary only)."
        ),
    ]
    release: Annotated[
        str, Arg("release", default="latest", doc=RELEASE_DOC + " May 2024 or later.", pattern=RELEASE_PATTERN)
    ]


def zip_areas(zip_code: str, defs: pa.Table) -> list[dict[str, Any]]:
    """OEWS areas overlapping a ZCTA, largest land share first."""
    xw = bundled("zcta_county")
    rows = xw.filter(pc.field("zcta") == zip_code).to_pylist()
    if not rows:
        raise ValueError(
            f"ZIP {zip_code} has no 2020 Census ZCTA (PO-box-only and single-organisation ZIPs have none), "
            "so it cannot be placed in an OEWS area"
        )
    county_area = dict(zip(pylist(defs["county_fips"]), pylist(defs["area_code"]), strict=True))
    titles = dict(zip(pylist(defs["area_code"]), pylist(defs["area_title"]), strict=True))
    by_area: dict[str, dict[str, Any]] = {}
    for r in rows:
        county = r["county_fips"]
        area = county_area.get(county)
        if area is None and county[:2] in ("66", "78"):
            area = county[:2] + "00001"  # Guam / US Virgin Islands: one OEWS nonmetro area each
        if area is None:
            continue
        entry = by_area.setdefault(area, {"area_code": area, "title": titles.get(area), "share": 0.0, "counties": []})
        entry["share"] += r["land_share"]
        entry["counties"].append(county)
    if not by_area:
        raise ValueError(f"ZIP {zip_code} lies in counties that OEWS publishes no area for")
    ranked = sorted(by_area.values(), key=lambda e: (-e["share"], e["area_code"]))
    for i, e in enumerate(ranked):
        e["share"] = round(min(e["share"], 1.0), 6)
        e["primary"] = i == 0
    return ranked


@init_single_worker
@bind_fixed_schema
class WagesForZip(TableFunctionGenerator[ZipArgs, ScanState]):
    """Wages for the OEWS area (metro or nonmetro) that contains a ZIP code."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = ZIP_SCHEMA

    class Meta:
        """Function metadata surfaced to DuckDB, agents and vgi-lint."""

        name = "wages_for_zip"
        description = "Metro-level OEWS wages for the metro or nonmetro area containing a ZIP code (not ZIP-level)"
        tags = {
            **object_tags(
                title="Wages for a ZIP Code's Metro Area",
                category="wages",
                keywords="zip code, zcta, metro area, local wages, salary near me, cbsa, crosswalk",
                doc_llm=(
                    "Look up OEWS wages for the area around a ZIP code. OEWS publishes no ZIP-level data: this "
                    "maps the ZIP to its 2020 Census ZCTA, the ZCTA to counties by land-area overlap, and the "
                    "counties to the release's OEWS metro (CBSA) or nonmetro area, then returns that area's "
                    "rows. Arguments: zip (5 digits, positional), occupation (positional: SOC code "
                    "'29-1141', prefix '29-' or 'all'), all_areas := true to also get other areas a ZIP "
                    "straddles, release := 'latest' or a year >= 2024. Columns say which area was used "
                    "(area_code, area_title, cbsa_code), zcta_land_share, is_primary_area and "
                    "estimate_geography (always 'metro-level ...' or 'nonmetropolitan-area ...'). Errors for "
                    "ZIPs without a ZCTA (PO boxes)."
                ),
                doc_md=(
                    "## wages_for_zip(zip, occupation, all_areas := false, release := 'latest')\n\n"
                    "The **metro (or nonmetro) area's** OEWS figures for a ZIP code, with the area used and "
                    "the share of the ZIP's land in it. ZIP codes are matched to "
                    "[2020 ZCTAs](https://www.census.gov/programs-surveys/geography/guidance/geo-areas/zctas.html), "
                    "which approximate but are not identical to USPS ZIP codes."
                ),
            ),
            "vgi.source_url": "https://www.bls.gov/oes/current/msa_def.htm",
            "vgi.result_columns_schema": result_columns(ZIP_SCHEMA, short=True),
        }
        examples = [
            FunctionExample(
                sql=(
                    "SELECT area_title, cbsa_code, estimate_geography, a_median "
                    "FROM us_wages.main.wages_for_zip('94103', '15-1252') ORDER BY area_code"
                ),
                description="Software developer pay in the metro area that contains ZIP 94103",
            ),
            FunctionExample(
                sql=(
                    "SELECT occ_title, a_median FROM us_wages.main.wages_for_zip('06103', '29-') "
                    "WHERE o_group = 'detailed' ORDER BY a_median DESC NULLS LAST LIMIT 10"
                ),
                description="Best-paid healthcare occupations in the metro area around Hartford ZIP 06103",
            ),
        ]

    @classmethod
    def initial_state(cls, params: ProcessParams[ZipArgs]) -> ScanState:
        """Resolve the release and cache it (downloads on first use)."""
        opts = options_from(params.attach_options)
        release = load_release(opts, parse_release(params.args.release))
        return ScanState(year=release.year)

    @classmethod
    def process(cls, params: ProcessParams[ZipArgs], state: ScanState, out: OutputCollector) -> None:
        """Map the ZIP to areas and emit those areas' rows in one batch."""
        if state.done:
            out.finish()
            return
        state.done = True
        a = params.args
        opts = options_from(params.attach_options)
        release = ensure_release(opts, state.year)
        defs, _ = ensure_area_definitions(opts, release.year)
        areas = zip_areas(a.zip, defs)
        if not a.all_areas:
            areas = areas[:1]
        sel = build_selection(
            occupation=a.occupation,
            area=None,
            area_type=None,
            state=None,
            industry="cross-industry",
            pushed=None,
        )
        wanted = {e["area_code"]: e for e in areas}
        sel = type(sel)(
            area_codes=frozenset(wanted),
            occ_codes=sel.occ_codes,
            predicate=(pc.field("area_code").isin(list(wanted)) & sel.predicate)
            if sel.predicate is not None
            else pc.field("area_code").isin(list(wanted)),
        )
        rows = read_all(release, sel)
        codes = pylist(rows["area_code"])
        types = pylist(rows["area_type"])
        extra = {
            "zip": pa.array([a.zip] * rows.num_rows, pa.string()),
            "estimate_geography": pa.array([METRO_LABEL if t == 4 else NONMETRO_LABEL for t in types], pa.string()),
            "zcta_land_share": pa.array([wanted[c]["share"] for c in codes], pa.float64()),
            "is_primary_area": pa.array([wanted[c]["primary"] for c in codes], pa.bool_()),
            "counties": pa.array([wanted[c]["counties"] for c in codes], pa.list_(pa.string())),
        }
        if rows.num_rows:
            out.emit(to_batch(rows, release, params.output_schema, extra))
        out.finish()


# ============================================================================ areas


@dataclass(slots=True, frozen=True, kw_only=True)
class ReleaseArgs:
    """Arguments: ``release := 'latest'``."""

    release: Annotated[str, Arg("release", default="latest", doc=RELEASE_DOC, pattern=RELEASE_PATTERN)]


AREAS_SCHEMA = pa.schema(
    [
        *AREA_FIELDS,
        column("county_count", pa.int32(), "Counties (or county equivalents) in the area; NULL before May 2024."),
        column("states", pa.list_(pa.string()), "USPS codes of every state the area touches."),
        column(
            "occupation_rows", pa.int64(), "Cross-industry occupation rows BLS publishes for the area.", nullable=False
        ),
    ]
)


@init_single_worker
@bind_fixed_schema
class Areas(TableFunctionGenerator[ReleaseArgs, ScanState]):
    """Every OEWS area of a release."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = AREAS_SCHEMA

    class Meta:
        """Function metadata surfaced to DuckDB, agents and vgi-lint."""

        name = "areas"
        description = "The OEWS areas of a release: US, states, territories, metro (CBSA) and nonmetro areas"
        tags = {
            **object_tags(
                title="OEWS Areas",
                category="geography",
                keywords="areas, metro areas, msa, cbsa, nonmetropolitan, states, geography, area codes",
                doc_llm=(
                    "List the areas a release publishes wages for: area_code (pass it to "
                    "wages(area := ...)), area_title, area_type/area_type_name, prim_state, state_fips, "
                    "cbsa_code (metro rows), county_count and states (from the county definitions, May 2024 "
                    "onward) and occupation_rows. Use it to find a metro's code by name, e.g. WHERE "
                    "area_title ILIKE '%austin%'. release := 'latest' or a year."
                ),
                doc_md=(
                    "## areas(release := 'latest')\n\n"
                    "The US, states, territories, metropolitan (CBSA) and nonmetropolitan areas in a "
                    "release, with codes to pass to `wages(area := ...)`. See BLS "
                    "[area definitions](https://www.bls.gov/oes/current/msa_def.htm)."
                ),
            ),
            "vgi.source_url": "https://www.bls.gov/oes/current/msa_def.htm",
            "vgi.result_columns_schema": result_columns(AREAS_SCHEMA),
        }
        examples = [
            FunctionExample(
                sql=(
                    "SELECT area_type_name, count(*) AS areas FROM us_wages.main.areas() "
                    "GROUP BY area_type_name ORDER BY areas DESC"
                ),
                description="How many areas of each type the latest release covers",
            ),
            FunctionExample(
                sql=(
                    "SELECT area_code, area_title, county_count FROM us_wages.main.areas() "
                    "WHERE area_type_name = 'metro' AND list_contains(states, 'CT') ORDER BY area_code"
                ),
                description="Metro areas that include part of Connecticut",
            ),
        ]

    @classmethod
    def initial_state(cls, params: ProcessParams[ReleaseArgs]) -> ScanState:
        """Resolve and cache the release."""
        opts = options_from(params.attach_options)
        return ScanState(year=load_release(opts, parse_release(params.args.release)).year)

    @classmethod
    def process(cls, params: ProcessParams[ReleaseArgs], state: ScanState, out: OutputCollector) -> None:
        """Summarise the release's distinct areas."""
        if state.done:
            out.finish()
            return
        state.done = True
        opts = options_from(params.attach_options)
        release = ensure_release(opts, state.year)
        sel = build_selection(
            occupation=None, area=None, area_type=None, state=None, industry="cross-industry", pushed=None
        )
        rows = read_all(release, sel).select(["area_code", "area_title", "area_type", "prim_state"])
        grouped = rows.group_by(["area_code", "area_title", "area_type", "prim_state"]).aggregate(
            [("area_code", "count")]
        )
        grouped = grouped.sort_by("area_code")
        counties: dict[str, int] = {}
        states: dict[str, set[str]] = {}
        try:
            defs, _ = ensure_area_definitions(opts, release.year)
        except Exception:
            defs = None
        if defs is not None:
            for area, st, abbr in zip(
                pylist(defs["area_code"]),
                pylist(defs["state_fips"]),
                pylist(defs["state_abbr"]),
                strict=True,
            ):
                counties[area] = counties.get(area, 0) + 1
                states.setdefault(area, set()).add(abbr or STATE_FIPS_TO_ABBR.get(st, st))
                counties[st] = counties.get(st, 0) + 1
                states.setdefault(st, set()).add(abbr or STATE_FIPS_TO_ABBR.get(st, st))
            counties["99"] = defs.num_rows
        base = grouped.select(["area_code", "area_title", "area_type", "prim_state"])
        enriched = enrich(base, release)
        codes = pylist(base["area_code"])
        prims = pylist(base["prim_state"])
        extra: dict[str, Any] = {
            "county_count": pa.array([counties.get(c) if defs is not None else None for c in codes], pa.int32()),
            "states": pa.array(
                [
                    sorted(states[c]) if c in states else ([p] if p and p != "US" else None)
                    for c, p in zip(codes, prims, strict=True)
                ],
                pa.list_(pa.string()),
            ),
            "occupation_rows": grouped["area_code_count"].cast(pa.int64()),
        }
        cols = {**enriched, **extra}
        arrays = []
        for f in params.output_schema:
            col = cols[f.name]
            if isinstance(col, pa.ChunkedArray):
                col = col.combine_chunks()
            arrays.append(col.cast(f.type) if col.type != f.type else col)
        out.emit(pa.RecordBatch.from_arrays(arrays, schema=params.output_schema))
        out.finish()


# ==================================================================== area_counties

COUNTIES_SCHEMA = pa.schema(
    [
        column("release_year", pa.int16(), "May reference year of the area definitions.", nullable=False),
        column("county_fips", pa.string(), "5-digit county FIPS (Connecticut: 2022 planning regions).", nullable=False),
        column("county_name", pa.string(), "County or county-equivalent name."),
        column("state_fips", pa.string(), "2-digit state FIPS.", nullable=False),
        column("state_abbr", pa.string(), "USPS state code."),
        column("area_code", pa.string(), "OEWS area the county belongs to (CBSA or 7-digit nonmetro).", nullable=False),
        column("area_title", pa.string(), "OEWS area name."),
        column("area_type_name", pa.string(), "metro or nonmetro.", nullable=False),
        column("cbsa_code", pa.string(), "CBSA code when the area is a metro area, else NULL."),
    ]
)


@init_single_worker
@bind_fixed_schema
class AreaCounties(TableFunctionGenerator[ReleaseArgs, ScanState]):
    """County -> OEWS area definitions of a release."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = COUNTIES_SCHEMA

    class Meta:
        """Function metadata surfaced to DuckDB, agents and vgi-lint."""

        name = "area_counties"
        description = "Which OEWS metro or nonmetro area each county belongs to (May 2024 releases onward)"
        tags = {
            **object_tags(
                title="OEWS Area Counties",
                category="geography",
                keywords="counties, county fips, area definitions, crosswalk, msa, cbsa, nonmetro",
                doc_llm=(
                    "One row per county (county_fips 5-digit FIPS; Connecticut uses its 2022 planning "
                    "regions) with the OEWS area it belongs to (area_code, area_title, area_type_name, "
                    "cbsa_code). Join county-level data to wages via area_code. Downloads the small BLS area "
                    "definitions file for the release on first use; releases before May 2024 are not "
                    "county-based in New England and are rejected. release := 'latest' or a year >= 2024."
                ),
                doc_md=(
                    "## area_counties(release := 'latest')\n\n"
                    "BLS [OEWS area definitions](https://www.bls.gov/oes/current/msa_def.htm): each county and "
                    "the metro or nonmetro area it is part of."
                ),
            ),
            "vgi.source_url": "https://www.bls.gov/oes/current/msa_def.htm",
            "vgi.result_columns_schema": result_columns(COUNTIES_SCHEMA),
        }
        examples = [
            FunctionExample(
                sql=(
                    "SELECT county_fips, county_name, area_title FROM us_wages.main.area_counties() "
                    "WHERE state_abbr = 'CT' ORDER BY county_fips"
                ),
                description="Connecticut's planning regions and the OEWS area of each",
            ),
            FunctionExample(
                sql=(
                    "SELECT area_title, count(*) AS counties FROM us_wages.main.area_counties() "
                    "WHERE area_type_name = 'metro' GROUP BY area_title ORDER BY counties DESC LIMIT 5"
                ),
                description="The metro areas made of the most counties",
            ),
        ]

    @classmethod
    def initial_state(cls, params: ProcessParams[ReleaseArgs]) -> ScanState:
        """Resolve the release year (no OEWS download needed)."""
        from .oews import resolve_year

        opts = options_from(params.attach_options)
        return ScanState(year=resolve_year(opts, parse_release(params.args.release)))

    @classmethod
    def process(cls, params: ProcessParams[ReleaseArgs], state: ScanState, out: OutputCollector) -> None:
        """Emit the release's county definitions."""
        if state.done:
            out.finish()
            return
        state.done = True
        defs, _ = ensure_area_definitions(options_from(params.attach_options), state.year)
        codes = pylist(defs["area_code"])
        is_metro = [len(c) == 5 for c in codes]
        cols: dict[str, Any] = {name: defs[name] for name in defs.column_names}
        cols["release_year"] = pa.array([state.year] * defs.num_rows, pa.int16())
        cols["area_type_name"] = pa.array([AREA_TYPE_NAMES[4] if m else AREA_TYPE_NAMES[6] for m in is_metro])
        cols["cbsa_code"] = pa.array([c if m else None for c, m in zip(codes, is_metro, strict=True)], pa.string())
        arrays = []
        for f in params.output_schema:
            col = cols[f.name]
            if isinstance(col, pa.ChunkedArray):
                col = col.combine_chunks()
            arrays.append(col.cast(f.type) if col.type != f.type else col)
        out.emit(pa.RecordBatch.from_arrays(arrays, schema=params.output_schema))
        out.finish()


__all__ = ["AreaCounties", "Areas", "Wages", "WagesForZip"]
