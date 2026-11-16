"""Bundled-data and cache table functions: occupations, search_occupations, cache_info, refresh."""

from __future__ import annotations

import datetime as dt
import json
from dataclasses import dataclass
from typing import Annotated, Any, ClassVar

import pyarrow as pa
from vgi.arguments import Arg
from vgi.metadata import FunctionExample
from vgi.table_function import (
    OutputCollector,
    ProcessParams,
    TableFunctionGenerator,
    bind_fixed_schema,
    init_single_worker,
)

from .common import RELEASE_PATTERN, options_from, parse_release, pylist, read_json
from .meta import column, object_tags, result_columns
from .oews import (
    FIRST_COUNTY_DEFINITIONS,
    cached_years,
    ensure_area_definitions,
    ensure_release,
    release_dir,
    resolve_year,
    upstream_changed,
)
from .reference import BUNDLE_VINTAGES, bundled, onet_codes_by_soc, search


@dataclass(slots=True, frozen=True, kw_only=True)
class NoArgs:
    """No arguments."""


def _emit(out: OutputCollector, schema: pa.Schema, cols: dict[str, Any]) -> None:
    arrays = []
    for f in schema:
        col = cols[f.name]
        if isinstance(col, pa.ChunkedArray):
            col = col.combine_chunks()
        if not isinstance(col, pa.Array):
            col = pa.array(col, f.type)
        arrays.append(col.cast(f.type) if col.type != f.type else col)
    out.emit(pa.RecordBatch.from_arrays(arrays, schema=schema))


# ====================================================================== occupations

OCCUPATIONS_SCHEMA = pa.schema(
    [
        column("soc_code", pa.string(), "SOC 2018 code 'XX-XXXX'.", nullable=False),
        column("soc_title", pa.string(), "SOC 2018 title.", nullable=False),
        column("soc_level", pa.string(), "major, minor, broad or detailed.", nullable=False),
        column("major_code", pa.string(), "Major group code, e.g. '15-0000'.", nullable=False),
        column("major_title", pa.string(), "Major group title.", nullable=False),
        column("minor_code", pa.string(), "Minor group code, e.g. '15-1200' (NULL for major rows)."),
        column("minor_title", pa.string(), "Minor group title."),
        column("broad_code", pa.string(), "Broad occupation code, e.g. '15-1250' (NULL above broad level)."),
        column("broad_title", pa.string(), "Broad occupation title."),
        column("definition", pa.string(), "SOC definition (detailed occupations only)."),
        column(
            "onet_soc_codes", pa.list_(pa.string()), "O*NET-SOC codes under a detailed occupation, e.g. '15-1252.00'."
        ),
    ]
)


@init_single_worker
@bind_fixed_schema
class Occupations(TableFunctionGenerator[NoArgs, None]):
    """The SOC 2018 hierarchy, bundled."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = OCCUPATIONS_SCHEMA

    class Meta:
        """Function metadata surfaced to DuckDB, agents and vgi-lint."""

        name = "occupations_scan"
        description = "The 2018 Standard Occupational Classification: 1,447 major, minor, broad and detailed groups"
        tags = {
            **object_tags(
                title="SOC 2018 Occupations",
                category="occupations",
                keywords="soc, occupation codes, classification, job categories, hierarchy, occupation definitions",
                doc_llm=(
                    "The SOC 2018 hierarchy (bundled, no network): one row per major (23), minor, broad and "
                    "detailed (867) group with soc_code 'XX-XXXX', soc_title, soc_level, the parent codes and "
                    "titles (major_*, minor_*, broad_*), the BLS definition of detailed occupations and the "
                    "O*NET-SOC codes under each. Join to wages() on occ_code = soc_code. OEWS also publishes a "
                    "few aggregate codes not in SOC (they appear only in wages())."
                ),
                doc_md=(
                    "## occupations\n\n"
                    "The [2018 SOC](https://www.bls.gov/soc/2018/) structure and definitions, with O*NET-SOC "
                    "codes from [O*NET 31.0](https://www.onetcenter.org/database.html). Bundled."
                ),
            ),
            "vgi.source_url": "https://www.bls.gov/soc/2018/",
            "vgi.result_columns_schema": result_columns(OCCUPATIONS_SCHEMA),
            "vgi.executable_examples": json.dumps(
                [
                    {
                        "description": "Number of occupations at each SOC level",
                        "sql": (
                            "SELECT soc_level, count(*) AS n FROM us_wages.main.occupations "
                            "GROUP BY soc_level ORDER BY n"
                        ),
                        "expected_result": [["major", 23], ["minor", 98], ["broad", 459], ["detailed", 867]],
                    },
                    {
                        "description": "Detailed occupations in the Software Developers broad group",
                        "sql": (
                            "SELECT soc_code, soc_title FROM us_wages.main.occupations "
                            "WHERE broad_code = '15-1250' AND soc_level = 'detailed' ORDER BY soc_code"
                        ),
                        "expected_result": [
                            ["15-1251", "Computer Programmers"],
                            ["15-1252", "Software Developers"],
                            ["15-1253", "Software Quality Assurance Analysts and Testers"],
                            ["15-1254", "Web Developers"],
                            ["15-1255", "Web and Digital Interface Designers"],
                        ],
                    },
                ]
            ),
        }
        examples = [
            FunctionExample(
                sql=(
                    "SELECT major_title, count(*) AS detailed_occupations FROM us_wages.main.occupations "
                    "WHERE soc_level = 'detailed' GROUP BY major_title ORDER BY detailed_occupations DESC LIMIT 5"
                ),
                description="The SOC major groups with the most detailed occupations",
            ),
            FunctionExample(
                sql=(
                    "SELECT soc_code, soc_title, definition FROM us_wages.main.occupations "
                    "WHERE minor_code = '29-1000' AND soc_level = 'detailed' ORDER BY soc_code"
                ),
                description="Definitions of the health diagnosing and treating practitioners",
            ),
        ]

    @classmethod
    def process(cls, params: ProcessParams[NoArgs], state: None, out: OutputCollector) -> None:
        """Emit the bundled hierarchy."""
        soc = bundled("soc_2018")
        onet = onet_codes_by_soc()
        cols: dict[str, Any] = {name: soc[name] for name in soc.column_names}
        cols["onet_soc_codes"] = pa.array([onet.get(c) for c in pylist(soc["soc_code"])], pa.list_(pa.string()))
        _emit(out, params.output_schema, cols)
        out.finish()


# =============================================================== search_occupations

SEARCH_SCHEMA = pa.schema(
    [
        column("rank", pa.int32(), "1 = best match.", nullable=False),
        column("soc_code", pa.string(), "SOC 2018 code to pass to wages(occupation := ...).", nullable=False),
        column("soc_title", pa.string(), "SOC title of that code."),
        column("soc_level", pa.string(), "major, minor, broad or detailed."),
        column("matched_title", pa.string(), "The title that matched the search text.", nullable=False),
        column(
            "match_source",
            pa.string(),
            "soc_title, onet_title or onet_job_title (O*NET alternate title).",
            nullable=False,
        ),
        column("onet_soc_code", pa.string(), "O*NET-SOC code of an O*NET match, e.g. '29-1141.00'."),
        column("score", pa.float64(), "Match quality from 0 to 1 (1 = exact title).", nullable=False),
    ]
)


@dataclass(slots=True, frozen=True, kw_only=True)
class SearchArgs:
    """Arguments for ``search_occupations(text, max_results := 10)``."""

    text: Annotated[str, Arg(0, doc="Job title or words to look for, e.g. 'nurse' or 'software developer'.")]
    max_results: Annotated[int, Arg("max_results", default=10, doc="Rows to return, 1 to 200.", ge=1, le=200)]


@init_single_worker
@bind_fixed_schema
class SearchOccupations(TableFunctionGenerator[SearchArgs, None]):
    """Find SOC codes from a job title."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = SEARCH_SCHEMA

    class Meta:
        """Function metadata surfaced to DuckDB, agents and vgi-lint."""

        name = "search_occupations"
        description = "Find SOC 2018 codes for a job title, searching SOC titles and 54,000 O*NET alternate titles"
        tags = {
            **object_tags(
                title="Search Occupations",
                category="occupations",
                keywords="search, job title, occupation lookup, find soc code, alternate titles, o*net",
                doc_llm=(
                    "Turn a job title into SOC codes. search_occupations(text, max_results := 10) matches "
                    "every word of text (word prefixes; plurals folded) against SOC 2018 titles, O*NET "
                    "occupation titles and O*NET alternate job titles (e.g. 'barista' -> 35-3023 Fast Food "
                    "and Counter Workers), keeps the best title per SOC code and ranks by score. Use the "
                    "rank-1 soc_code with wages(occupation := ...). Bundled data; no network."
                ),
                doc_md=(
                    "## search_occupations(text, max_results := 10)\n\n"
                    "Title search over SOC 2018 and [O*NET](https://www.onetcenter.org/database.html) "
                    "alternate titles. O*NET data is CC BY 4.0, U.S. Department of Labor/ETA."
                ),
            ),
            "vgi.source_url": "https://www.onetcenter.org/database.html",
            "vgi.result_columns_schema": result_columns(SEARCH_SCHEMA),
            "vgi.executable_examples": json.dumps(
                [
                    {
                        "description": "Best SOC code for 'software developer'",
                        "sql": (
                            "SELECT soc_code, soc_title FROM us_wages.main.search_occupations('software developer') "
                            "WHERE rank = 1"
                        ),
                        "expected_result": [["15-1252", "Software Developers"]],
                    },
                    {
                        "description": "An alternate title resolves to its SOC occupation",
                        "sql": (
                            "SELECT soc_code, match_source FROM us_wages.main.search_occupations('barista', "
                            "max_results := 1) ORDER BY rank"
                        ),
                        "expected_result": [["35-3023", "onet_title"]],
                    },
                ]
            ),
        }
        examples = [
            FunctionExample(
                sql=(
                    "SELECT rank, soc_code, soc_title, matched_title FROM "
                    "us_wages.main.search_occupations('nurse', max_results := 5) ORDER BY rank"
                ),
                description="The five best SOC matches for 'nurse'",
            ),
            FunctionExample(
                sql=(
                    "SELECT s.soc_code, s.soc_title, w.a_median FROM "
                    "us_wages.main.search_occupations('truck driver', max_results := 3) s "
                    "JOIN us_wages.main.wages(area := '99') w ON w.occ_code = s.soc_code ORDER BY s.rank"
                ),
                description="National median pay of the occupations matching 'truck driver'",
            ),
        ]

    @classmethod
    def process(cls, params: ProcessParams[SearchArgs], state: None, out: OutputCollector) -> None:
        """Run the search and emit the ranked rows."""
        rows = search(params.args.text, params.args.max_results)
        cols = {f.name: [r[f.name] for r in rows] for f in SEARCH_SCHEMA}
        _emit(out, params.output_schema, cols)
        out.finish()


# ======================================================================= cache_info

CACHE_SCHEMA_OUT = pa.schema(
    [
        column("kind", pa.string(), "oews_release, area_definitions, latest_pointer or bundled.", nullable=False),
        column("release_year", pa.int16(), "May reference year (NULL for bundled tables)."),
        column("path", pa.string(), "Local file holding the data (unique per row).", nullable=False),
        column("bytes", pa.int64(), "Size of the local file, in bytes."),
        column("rows", pa.int64(), "Rows in the table."),
        column("source_url", pa.string(), "Where the data was downloaded from."),
        column("sha256", pa.string(), "SHA-256 of the downloaded upstream file."),
        column("etag", pa.string(), "Upstream ETag at download time."),
        column("last_modified", pa.string(), "Upstream Last-Modified at download time."),
        column("fetched_at", pa.timestamp("us", tz="UTC"), "When it was downloaded (or checked)."),
        column("vintage", pa.string(), "Edition of the data, e.g. 'May 2025' or 'O*NET 31.0'.", nullable=False),
    ]
)


def _ts(v: Any) -> dt.datetime | None:
    if v is None:
        return None
    if isinstance(v, int | float):
        return dt.datetime.fromtimestamp(v, dt.UTC)
    return dt.datetime.fromisoformat(str(v))


def cache_rows(opts: Any, years: list[int] | None = None) -> list[dict[str, Any]]:
    """Describe the cache (and the bundled tables) as rows of CACHE_SCHEMA_OUT."""
    rows: list[dict[str, Any]] = []
    for y in cached_years(opts) if years is None else years:
        d = release_dir(opts, y)
        meta = read_json(d / "meta.json") or {}
        p = d / "all_data.parquet"
        rows.append(
            {
                "kind": "oews_release",
                "release_year": y,
                "path": str(p),
                "bytes": p.stat().st_size if p.exists() else None,
                "rows": meta.get("rows"),
                "source_url": meta.get("source_url"),
                "sha256": meta.get("sha256"),
                "etag": meta.get("etag"),
                "last_modified": meta.get("last_modified"),
                "fetched_at": _ts(meta.get("fetched_at")),
                "vintage": f"May {y}",
            }
        )
        defs_meta = read_json(opts.source_root / "area_definitions" / f"{y}.json")
        if defs_meta:
            dp = opts.source_root / "area_definitions" / f"{y}.parquet"
            rows.append(
                {
                    "kind": "area_definitions",
                    "release_year": y,
                    "path": str(dp),
                    "bytes": dp.stat().st_size if dp.exists() else None,
                    "rows": defs_meta.get("rows"),
                    "source_url": defs_meta.get("source_url"),
                    "sha256": defs_meta.get("sha256"),
                    "etag": defs_meta.get("etag"),
                    "last_modified": defs_meta.get("last_modified"),
                    "fetched_at": _ts(defs_meta.get("fetched_at")),
                    "vintage": f"May {y}",
                }
            )
    if years is None:
        pointer = read_json(opts.source_root / "oews" / "latest.json")
        if pointer:
            rows.append(
                {
                    "kind": "latest_pointer",
                    "release_year": pointer.get("year"),
                    "path": str(opts.source_root / "oews" / "latest.json"),
                    "bytes": None,
                    "rows": None,
                    "source_url": None,
                    "sha256": None,
                    "etag": None,
                    "last_modified": None,
                    "fetched_at": _ts(pointer.get("checked_at")),
                    "vintage": f"May {pointer.get('year')}",
                }
            )
        for name, key in (
            ("soc_2018", "soc_2018"),
            ("onet_occupations", "onet"),
            ("onet_job_titles", "onet"),
            ("zcta_county", "zcta_county"),
        ):
            rows.append(
                {
                    "kind": "bundled",
                    "release_year": None,
                    "path": f"us_wages_worker/data/{name}.parquet",
                    "bytes": None,
                    "rows": bundled(name).num_rows,
                    "source_url": None,
                    "sha256": None,
                    "etag": None,
                    "last_modified": None,
                    "fetched_at": None,
                    "vintage": BUNDLE_VINTAGES[key],
                }
            )
    return rows


@init_single_worker
@bind_fixed_schema
class CacheInfo(TableFunctionGenerator[NoArgs, None]):
    """What is in the local cache."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = CACHE_SCHEMA_OUT

    class Meta:
        """Function metadata surfaced to DuckDB, agents and vgi-lint."""

        name = "cache_info_scan"
        description = "List the OEWS releases and area definitions in the local cache, plus the bundled tables"
        tags = {
            **object_tags(
                title="Download Cache Contents",
                category="cache",
                keywords="cache, downloads, provenance, checksum, releases cached, data vintage",
                doc_llm=(
                    "Show what this worker has downloaded: one row per cached OEWS release and area "
                    "definitions file (path, bytes, rows, source_url, sha256, etag, fetched_at, vintage), "
                    "the 'latest' pointer, and the four bundled reference tables with their vintages. Never "
                    "downloads anything. Set the cache location with the cache_dir ATTACH option."
                ),
                doc_md=(
                    "## cache_info\n\nProvenance of every cached download and bundled table. Never touches the network."
                ),
            ),
            "vgi.source_url": "https://github.com/Query-farm/vgi-us-wages#cache",
            "vgi.result_columns_schema": result_columns(CACHE_SCHEMA_OUT),
            "vgi.executable_examples": json.dumps(
                [
                    {
                        "description": "Row counts of the bundled reference tables",
                        "sql": ("SELECT path, rows FROM us_wages.main.cache_info WHERE kind = 'bundled' ORDER BY path"),
                        "expected_result": [
                            ["us_wages_worker/data/onet_job_titles.parquet", 54269],
                            ["us_wages_worker/data/onet_occupations.parquet", 1016],
                            ["us_wages_worker/data/soc_2018.parquet", 1447],
                            ["us_wages_worker/data/zcta_county.parquet", 46961],
                        ],
                    }
                ]
            ),
        }
        examples = [
            FunctionExample(
                sql=(
                    "SELECT release_year, bytes, source_url, fetched_at FROM us_wages.main.cache_info "
                    "WHERE kind = 'oews_release' ORDER BY release_year"
                ),
                description="Which OEWS releases are cached locally, and when they were fetched",
            ),
            FunctionExample(
                sql="SELECT kind, count(*) AS entries FROM us_wages.main.cache_info GROUP BY kind ORDER BY kind",
                description="Cache entries by kind",
            ),
        ]

    @classmethod
    def process(cls, params: ProcessParams[NoArgs], state: None, out: OutputCollector) -> None:
        """Describe the cache."""
        rows = cache_rows(options_from(params.attach_options))
        _emit(out, params.output_schema, {f.name: [r[f.name] for r in rows] for f in CACHE_SCHEMA_OUT})
        out.finish()


# ========================================================================== refresh


@dataclass(slots=True, frozen=True, kw_only=True)
class RefreshArgs:
    """Arguments for ``refresh(release := 'latest', force := false)``."""

    release: Annotated[
        str,
        Arg(
            "release",
            default="latest",
            doc="'latest' (re-checks BLS for a newer year) or a year.",
            pattern=RELEASE_PATTERN,
        ),
    ]
    force: Annotated[bool, Arg("force", default=False, doc="Re-download even when BLS's copy is unchanged.")]


@init_single_worker
@bind_fixed_schema
class Refresh(TableFunctionGenerator[RefreshArgs, None]):
    """Download or re-validate one release in the cache."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = CACHE_SCHEMA_OUT

    class Meta:
        """Function metadata surfaced to DuckDB, agents and vgi-lint."""

        name = "refresh"
        description = "Download a release into the cache, or re-download it when BLS has republished it"
        tags = {
            **object_tags(
                title="Refresh Cache",
                category="cache",
                keywords="refresh, update, download, prefetch, new release, revalidate",
                doc_llm=(
                    "Prefetch or update cached data. refresh(release := 'latest', force := false) re-checks "
                    "the BLS tables page for the newest May release (ignoring the 24-hour latest pointer), "
                    "downloads it if absent, re-downloads it when BLS's ETag/Last-Modified changed or "
                    "force := true, also fetches the county area definitions (May 2024+), and returns the "
                    "resulting cache_info rows. Only the local cache is written; nothing upstream changes."
                ),
                doc_md=(
                    "## refresh(release := 'latest', force := false)\n\n"
                    "Download or update one release in the local cache and return its cache rows."
                ),
            ),
            "vgi.source_url": "https://www.bls.gov/oes/tables.htm",
            "vgi.result_columns_schema": result_columns(CACHE_SCHEMA_OUT),
        }
        examples = [
            FunctionExample(
                sql="SELECT kind, release_year, rows, sha256 FROM us_wages.main.refresh() ORDER BY kind",
                description="Make sure the newest OEWS release is cached",
            ),
            FunctionExample(
                sql="SELECT kind, release_year, bytes FROM us_wages.main.refresh(release := '2024') ORDER BY kind",
                description="Prefetch the May 2024 release",
            ),
        ]

    @classmethod
    def process(cls, params: ProcessParams[RefreshArgs], state: None, out: OutputCollector) -> None:
        """Download/revalidate, then report."""
        opts = options_from(params.attach_options)
        year = resolve_year(opts, parse_release(params.args.release), refresh=True)
        force = params.args.force
        if not force and year in cached_years(opts):
            release = ensure_release(opts, year)
            force = not opts.offline and upstream_changed(opts, release)
        ensure_release(opts, year, force=force)
        if year >= FIRST_COUNTY_DEFINITIONS:
            ensure_area_definitions(opts, year)
        rows = cache_rows(opts, [year])
        _emit(out, params.output_schema, {f.name: [r[f.name] for r in rows] for f in CACHE_SCHEMA_OUT})
        out.finish()
