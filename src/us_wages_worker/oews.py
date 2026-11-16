"""OEWS release handling: find, download, convert to Parquet, and cache.

BLS publishes each May reference period's estimates as one "all data" zip,
``/oes/special-requests/oesm{YY}all.zip``, containing a single workbook with
national, state, territory, metropolitan, nonmetropolitan and national-industry
rows. The worker downloads it once, converts it into a typed Parquet file in the
cache (about 25 MB per release), and reads that from then on.

Marker cells are never coerced to numbers:

* ``*``  wage estimate not available           -> NULL, column named in ``suppressed_columns``
* ``**`` employment estimate not available     -> NULL, column named in ``suppressed_columns``
* ``#``  wage at or above the release's top-code -> NULL, column named in ``topcoded_columns``
* ``~``  fewer than 0.5% of establishments report -> NULL, ``pct_rpt_below_half`` = true
"""

from __future__ import annotations

import datetime as dt
import logging
import re
import time
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

import pyarrow as pa
import pyarrow.parquet as pq
from python_calamine import CalamineWorkbook

from .common import (
    LATEST_TTL_SECONDS,
    STATE_ABBR_TO_FIPS,
    STATE_FIPS_TO_ABBR,
    Options,
    UpstreamError,
    download,
    file_lock,
    probe,
    read_json,
    write_json,
)

log = logging.getLogger(__name__)

PARSER_VERSION = 1
ROW_GROUP_ROWS = 20_000
FIRST_RELEASE = 2011  # first May reference year with an "all data" zip

WAGE_COLUMNS = [
    "h_mean", "a_mean", "h_pct10", "h_pct25", "h_median", "h_pct75", "h_pct90",
    "a_pct10", "a_pct25", "a_median", "a_pct75", "a_pct90",
]  # fmt: skip
FLOAT_COLUMNS = ["emp_prse", "jobs_1000", "loc_quotient", "pct_total", "pct_rpt", "mean_prse", *WAGE_COLUMNS]
TEXT_COLUMNS = [
    "area_code", "area_title", "prim_state", "naics", "naics_title", "i_group", "own_code",
    "occ_code", "occ_title", "o_group",
]  # fmt: skip

HEADER_ALIASES = {
    "area": "area_code",
    "area_name": "area_title",
    "occ_group": "o_group",
    "group": "o_group",
    "jobs_1000_orig": "jobs_1000",
    "loc_quotiont": "loc_quotient",
    "loc quotient": "loc_quotient",
    "annual": "annual_only",
    "hourly": "hourly_only",
}

CACHE_SCHEMA = pa.schema(
    cast(
        "list[pa.Field[Any]]",
        [
            pa.field("area_code", pa.string(), nullable=False),
            pa.field("area_title", pa.string()),
            pa.field("area_type", pa.int8(), nullable=False),
            pa.field("prim_state", pa.string()),
            pa.field("naics", pa.string()),
            pa.field("naics_title", pa.string()),
            pa.field("i_group", pa.string()),
            pa.field("own_code", pa.string()),
            pa.field("occ_code", pa.string(), nullable=False),
            pa.field("occ_title", pa.string()),
            pa.field("o_group", pa.string()),
            pa.field("tot_emp", pa.int64()),
            *[pa.field(c, pa.float64()) for c in FLOAT_COLUMNS],
            pa.field("annual_only", pa.bool_(), nullable=False),
            pa.field("hourly_only", pa.bool_(), nullable=False),
            pa.field("suppressed_columns", pa.list_(pa.string()), nullable=False),
            pa.field("topcoded_columns", pa.list_(pa.string()), nullable=False),
            pa.field("pct_rpt_below_half", pa.bool_(), nullable=False),
        ],
    )
)

_TOPCODE_RE = re.compile(r"#\s*=.*?\$\s*([\d,]+(?:\.\d+)?)\s*per hour.*?\$\s*([\d,]+(?:\.\d+)?)\s*per year", re.I)


def zip_rel(year: int) -> str:
    """Upstream path of a release's all-data zip."""
    return f"oes/special-requests/oesm{year % 100:02d}all.zip"


def release_dir(opts: Options, year: int) -> Path:
    """Cache directory for one release."""
    return opts.source_root / "oews" / str(year)


@dataclass(frozen=True, slots=True)
class Release:
    """A cached, converted release."""

    year: int
    parquet: Path
    meta: dict[str, Any]

    @property
    def topcode_hourly(self) -> float | None:
        """The '#' threshold for hourly wages in this release, if the workbook stated it."""
        v = self.meta.get("topcode_hourly")
        return float(v) if v is not None else None

    @property
    def topcode_annual(self) -> float | None:
        """The '#' threshold for annual wages in this release, if the workbook stated it."""
        v = self.meta.get("topcode_annual")
        return float(v) if v is not None else None


def cached_years(opts: Options) -> list[int]:
    """Releases already converted in the cache, oldest first."""
    root = opts.source_root / "oews"
    if not root.is_dir():
        return []
    years = []
    for d in root.iterdir():
        if d.name.isdigit() and (d / "all_data.parquet").exists() and (d / "meta.json").exists():
            years.append(int(d.name))
    return sorted(years)


def resolve_year(opts: Options, year: int | None, *, refresh: bool = False) -> int:
    """Turn ``None`` ('latest') into a concrete May reference year."""
    if year is not None:
        if year < FIRST_RELEASE:
            raise ValueError(f"OEWS all-data files start with May {FIRST_RELEASE}; release {year} is not available")
        return year
    pointer_path = opts.source_root / "oews" / "latest.json"
    pointer = read_json(pointer_path)
    fresh = pointer is not None and time.time() - float(pointer.get("checked_at", 0)) < LATEST_TTL_SECONDS
    if pointer is not None and (fresh or opts.offline) and not refresh:
        return int(pointer["year"])
    if opts.offline:
        years = cached_years(opts)
        if years:
            return years[-1]
        raise UpstreamError("offline mode and no OEWS release is cached; run refresh() while online first")
    from .common import fetch_text

    try:
        html = fetch_text(opts, "oes/tables.htm")
    except (UpstreamError, OSError) as err:
        years = cached_years(opts)
        if years:
            log.warning("could not check for a newer OEWS release (%s); using cached May %d", err, years[-1])
            return years[-1]
        raise
    found = [int(m) for m in re.findall(r"oesm(\d\d)all\.zip", html)]
    if not found:
        raise UpstreamError("could not find any oesm{YY}all.zip link on the OEWS tables page")
    latest = 2000 + max(found)
    write_json(pointer_path, {"year": latest, "checked_at": time.time()})
    return latest


def _clean_header(name: Any) -> str:
    key = str(name).strip().lower()
    key = HEADER_ALIASES.get(key, key).replace(" ", "_")
    return HEADER_ALIASES.get(key, key)


def _text(v: Any) -> str | None:
    if v is None:
        return None
    if isinstance(v, float) and v.is_integer():
        v = int(v)
    s = str(v).strip()
    return s or None


def _num(v: Any) -> tuple[float | None, str | None]:
    """Return (value, marker) for one numeric cell."""
    if v is None:
        return None, None
    if isinstance(v, bool):
        return None, None
    if isinstance(v, int | float):
        return float(v), None
    s = str(v).strip()
    if not s:
        return None, None
    if s in {"*", "**", "#", "~"}:
        return None, s
    try:
        return float(s.replace(",", "")), None
    except ValueError:
        return None, s


def _pad_area(code: str, area_type: int) -> str:
    width = {1: 2, 2: 2, 3: 2, 4: 5, 6: 7}.get(area_type)
    return code.zfill(width) if width and code.isdigit() else code


def _prim_state(area_code: str, area_type: int, area_title: str | None) -> str | None:
    if area_type == 1:
        return "US"
    if area_type in (2, 3):
        return STATE_FIPS_TO_ABBR.get(area_code)
    if area_type == 6 and area_code[:2] in STATE_FIPS_TO_ABBR:
        return STATE_FIPS_TO_ABBR[area_code[:2]]
    if area_title and "," in area_title:
        first = area_title.rsplit(",", 1)[1].strip().split("-")[0].strip()
        return first if first in STATE_ABBR_TO_FIPS else None
    return None


def _flag(v: Any) -> bool:
    return str(v).strip().upper() == "TRUE" if v is not None else False


def convert_workbook(xlsx: Path, out: Path) -> dict[str, Any]:
    """Convert one OEWS all-data workbook into the cache Parquet layout; return its metadata."""
    wb = CalamineWorkbook.from_path(str(xlsx))
    topcode_hourly = topcode_annual = None
    for name in wb.sheet_names:
        if "field" in name.lower():
            for row in wb.get_sheet_by_name(name).to_python():
                m = _TOPCODE_RE.search(" ".join(str(c) for c in row))
                if m:
                    topcode_hourly = float(m.group(1).replace(",", ""))
                    topcode_annual = float(m.group(2).replace(",", ""))
    rows = wb.get_sheet_by_index(0).to_python()
    if not rows:
        raise UpstreamError(f"{xlsx.name} has no data rows")
    header = [_clean_header(h) for h in rows[0]]
    idx = {h: i for i, h in enumerate(header)}
    for need in ("area_code", "area_type", "occ_code"):
        if need not in idx:
            raise UpstreamError(f"{xlsx.name}: expected a {need!r} column, found {header}")

    def get(r: list[Any], name: str) -> Any:
        i = idx.get(name)
        return r[i] if i is not None and i < len(r) else None

    cols: dict[str, list[Any]] = {f.name: [] for f in CACHE_SCHEMA}
    for r in rows[1:]:
        raw_area = _text(get(r, "area_code"))
        raw_type = _text(get(r, "area_type"))
        occ = _text(get(r, "occ_code"))
        if raw_area is None or raw_type is None or occ is None:
            continue
        area_type = int(float(raw_type))
        area_code = _pad_area(raw_area, area_type)
        area_title = _text(get(r, "area_title"))
        cols["area_code"].append(area_code)
        cols["area_title"].append(area_title)
        cols["area_type"].append(area_type)
        cols["prim_state"].append(_text(get(r, "prim_state")) or _prim_state(area_code, area_type, area_title))
        naics = _text(get(r, "naics"))
        cols["naics"].append(naics.zfill(6) if naics and naics.isdigit() else naics)
        for c in ("naics_title", "i_group", "own_code", "occ_title", "o_group"):
            cols[c].append(_text(get(r, c)))
        cols["occ_code"].append(occ)
        suppressed: list[str] = []
        topcoded: list[str] = []
        emp, marker = _num(get(r, "tot_emp"))
        cols["tot_emp"].append(int(emp) if emp is not None else None)
        if marker in {"*", "**"}:
            suppressed.append("tot_emp")
        below_half = False
        for c in FLOAT_COLUMNS:
            value, marker = _num(get(r, c))
            cols[c].append(value)
            if marker == "#":
                topcoded.append(c)
            elif marker in {"*", "**"}:
                suppressed.append(c)
            elif marker == "~" and c == "pct_rpt":
                below_half = True
        cols["annual_only"].append(_flag(get(r, "annual_only")))
        cols["hourly_only"].append(_flag(get(r, "hourly_only")))
        cols["suppressed_columns"].append(suppressed)
        cols["topcoded_columns"].append(topcoded)
        cols["pct_rpt_below_half"].append(below_half)
    table = pa.table(cols, schema=CACHE_SCHEMA)
    table = table.sort_by(
        [("area_type", "ascending"), ("area_code", "ascending"), ("naics", "ascending"),
         ("own_code", "ascending"), ("occ_code", "ascending")]
    )  # fmt: skip
    tmp = out.with_name(out.name + ".tmp")
    pq.write_table(table, tmp, row_group_size=ROW_GROUP_ROWS, compression="zstd")
    tmp.replace(out)
    return {"rows": table.num_rows, "topcode_hourly": topcode_hourly, "topcode_annual": topcode_annual}


def _extract_workbook(zip_path: Path, dest: Path) -> Path:
    with zipfile.ZipFile(zip_path) as zf:
        members = [n for n in zf.namelist() if n.lower().endswith((".xlsx", ".xls"))]
        if not members:
            raise UpstreamError(f"{zip_path.name} contains no workbook")
        member = next((n for n in members if "all_data" in n.lower()), members[0])
        target = dest / ("workbook" + Path(member).suffix.lower())
        with zf.open(member) as src, open(target, "wb") as out:
            while chunk := src.read(1 << 20):
                out.write(chunk)
    return target


def ensure_release(opts: Options, year: int, *, force: bool = False) -> Release:
    """Return the cached release, downloading and converting it first if needed."""
    d = release_dir(opts, year)
    parquet, meta_path = d / "all_data.parquet", d / "meta.json"
    meta = read_json(meta_path)
    if meta and parquet.exists() and meta.get("parser_version") == PARSER_VERSION and not force:
        return Release(year, parquet, meta)
    with file_lock(d / ".lock"):
        meta = read_json(meta_path)
        if meta and parquet.exists() and meta.get("parser_version") == PARSER_VERSION and not force:
            return Release(year, parquet, meta)
        zip_path = d / "download.zip"
        try:
            fetched = download(opts, zip_rel(year), zip_path)
        except FileNotFoundError as err:
            raise UpstreamError(f"OEWS release May {year} is not published ({err})") from err
        try:
            xlsx = _extract_workbook(zip_path, d)
            try:
                info = convert_workbook(xlsx, parquet)
            finally:
                xlsx.unlink(missing_ok=True)
        finally:
            zip_path.unlink(missing_ok=True)
        meta = {
            "kind": "oews_release",
            "year": year,
            "parser_version": PARSER_VERSION,
            "source_url": fetched.url,
            "sha256": fetched.sha256,
            "download_bytes": fetched.size,
            "etag": fetched.etag,
            "last_modified": fetched.last_modified,
            "fetched_at": dt.datetime.now(dt.UTC).isoformat(),
            **info,
        }
        write_json(meta_path, meta)
        return Release(year, parquet, meta)


def upstream_changed(opts: Options, release: Release) -> bool:
    """Return True when bls.gov's copy of a cached release differs from what we converted."""
    etag, last_modified = probe(opts, zip_rel(release.year))
    if etag and release.meta.get("etag"):
        return bool(etag != release.meta["etag"])
    if last_modified and release.meta.get("last_modified"):
        return bool(last_modified != release.meta["last_modified"])
    return False


def load_release(opts: Options, year: int | None) -> Release:
    """Resolve ``year`` (None = latest) and make sure it is cached."""
    return ensure_release(opts, resolve_year(opts, year))


# --------------------------------------------------------------------- area definitions

AREA_DEFS_SCHEMA = pa.schema(
    [
        pa.field("county_fips", pa.string(), nullable=False),
        pa.field("county_name", pa.string()),
        pa.field("state_fips", pa.string(), nullable=False),
        pa.field("state_abbr", pa.string()),
        pa.field("state_name", pa.string()),
        pa.field("area_code", pa.string(), nullable=False),
        pa.field("area_title", pa.string()),
    ]
)
FIRST_COUNTY_DEFINITIONS = 2024  # earlier releases used town-based NECTAs in New England


def area_defs_rel(year: int) -> str:
    """Upstream path of a release's county -> OEWS area definitions workbook."""
    return f"oes/area_definitions_m{year}.xlsx"


def ensure_area_definitions(opts: Options, year: int) -> tuple[pa.Table, dict[str, Any]]:
    """County -> OEWS area table for a release (May 2024 onward), cached as Parquet."""
    if year < FIRST_COUNTY_DEFINITIONS:
        raise UpstreamError(
            f"county-based OEWS area definitions start with May {FIRST_COUNTY_DEFINITIONS}; May {year} "
            "used town-based NECTAs in New England, so ZIP and county lookups need release >= 2024"
        )
    d = opts.source_root / "area_definitions"
    parquet, meta_path = d / f"{year}.parquet", d / f"{year}.json"
    meta = read_json(meta_path)
    if not (meta and parquet.exists()):
        with file_lock(d / f".{year}.lock"):
            meta = read_json(meta_path)
            if not (meta and parquet.exists()):
                xlsx = d / f"{year}.xlsx"
                try:
                    fetched = download(opts, area_defs_rel(year), xlsx)
                    rows = CalamineWorkbook.from_path(str(xlsx)).get_sheet_by_index(0).to_python()
                except FileNotFoundError as err:
                    raise UpstreamError(f"no OEWS area definitions for May {year} ({err})") from err
                except Exception as err:
                    raise UpstreamError(f"could not read OEWS area definitions for May {year}: {err}") from err
                finally:
                    xlsx.unlink(missing_ok=True)
                cols: dict[str, list[Any]] = {f.name: [] for f in AREA_DEFS_SCHEMA}
                for r in rows[1:]:
                    vals = [_text(v) for v in r[:7]]
                    if len(vals) < 7 or not vals[0] or not vals[3] or not vals[5]:
                        continue
                    st, st_name, abbr, area, title, county, county_name = vals
                    assert st is not None and area is not None and county is not None
                    st = st.zfill(2)
                    cols["county_fips"].append(st + county.zfill(3))
                    cols["county_name"].append(county_name)
                    cols["state_fips"].append(st)
                    cols["state_abbr"].append(abbr)
                    cols["state_name"].append(st_name)
                    cols["area_code"].append(area.zfill(7) if len(area) > 5 else area.zfill(5))
                    cols["area_title"].append(title)
                pq.write_table(pa.table(cols, schema=AREA_DEFS_SCHEMA), parquet, compression="zstd")
                meta = {
                    "kind": "area_definitions",
                    "year": year,
                    "source_url": fetched.url,
                    "sha256": fetched.sha256,
                    "download_bytes": fetched.size,
                    "etag": fetched.etag,
                    "last_modified": fetched.last_modified,
                    "fetched_at": dt.datetime.now(dt.UTC).isoformat(),
                    "rows": len(cols["county_fips"]),
                }
                write_json(meta_path, meta)
    assert meta is not None
    return pq.read_table(parquet), meta
