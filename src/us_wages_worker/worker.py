"""Assemble the functions into a catalog and run the worker.

Functions live in ``wages.py`` (OEWS estimates and geography) and ``catalog_fns.py``
(occupations, search, cache); add new ones to ``FUNCTIONS`` below.

Run it:

    vgi-us-wages-worker                      # stdio: what DuckDB launches by default
    vgi-us-wages-worker --http --port 8000   # HTTP: for Docker / hosted deploys
    vgi-us-wages-worker --help

Attach it from DuckDB:

    ATTACH 'us_wages' (TYPE vgi, LOCATION '/abs/path/to/.venv/bin/vgi-us-wages-worker');
"""

from __future__ import annotations

import json
from typing import Annotated, Any

from vgi import Worker
from vgi.catalog import Catalog, Schema, Table
from vgi.catalog.attach_option import AttachOption
from vgi.catalog.constraints import constraint_record

from .catalog_fns import CacheInfo, Occupations, Refresh, SearchOccupations
from .common import DEFAULT_BASE_URL, DEFAULT_USER_AGENT
from .meta import (
    CATALOG_COMMENT,
    CATALOG_NAME,
    CATALOG_TAGS,
    SCHEMA_CATEGORIES,
    SCHEMA_COMMENT,
    SCHEMA_DOC_LLM,
    SCHEMA_DOC_MD,
    SOURCE_URL,
    keywords_array,
)
from .wages import AreaCounties, Areas, Wages, WagesForZip

FUNCTIONS: list[type[Any]] = [
    Wages,
    WagesForZip,
    Areas,
    AreaCounties,
    SearchOccupations,
    Refresh,
]


def _stamp_example_queries(functions: list[type[Any]]) -> list[dict[str, str]]:
    """Publish each function's examples as ``vgi.example_queries`` (with descriptions).

    Overloads share one SQL name, so their examples are merged per name.
    Returns every example, for the schema-level list.
    """
    by_name: dict[str, list[dict[str, str]]] = {}
    for fn in functions:
        bucket = by_name.setdefault(fn.Meta.name, [])
        for ex in getattr(fn.Meta, "examples", []):
            entry = {"description": ex.description, "sql": ex.sql}
            if entry not in bucket:
                bucket.append(entry)
    for fn in functions:
        fn.Meta.tags = {**fn.Meta.tags, "vgi.example_queries": json.dumps(by_name[fn.Meta.name])}
    return [entry for entries in by_name.values() for entry in entries]


_ALL_EXAMPLES = _stamp_example_queries(FUNCTIONS)


def _as_table(fn: type[Any], name: str, key: str | None) -> Table:
    """Expose a parameterless function as a catalog table carrying the function's metadata."""
    constraints: tuple[Any, ...] = ()
    if key is not None:
        constraints = (
            constraint_record(
                ["main"], name, f"{key}_not_null", "NOT_NULL", [[key]], enforcement="ENFORCED", validation="ALL_ROWS"
            ),
            constraint_record(
                ["main"], name, "pk", "PRIMARY_KEY", [[key]], enforcement="ENFORCED", validation="ALL_ROWS"
            ),
        )
    return Table(
        name=name,
        function=fn,
        comment=fn.Meta.description,
        tags={
            **fn.Meta.tags,
            "domain": "labor statistics",
            "vgi.example_queries": json.dumps([{"description": e.description, "sql": e.sql} for e in fn.Meta.examples]),
        },
        constraints=constraints,
        column_comments={
            f.name: (f.metadata or {}).get(b"comment", b"").decode() for f in fn.FIXED_SCHEMA if f.metadata
        },
    )


TABLES = [_as_table(Occupations, "occupations", "soc_code"), _as_table(CacheInfo, "cache_info", "path")]


class UsWagesWorker(Worker):
    """The worker process: one catalog, one ``main`` schema."""

    catalog = Catalog(
        name=CATALOG_NAME,
        comment=CATALOG_COMMENT,
        source_url=SOURCE_URL,
        tags=CATALOG_TAGS,
        schemas=[
            Schema(
                path=["main"],
                comment=SCHEMA_COMMENT,
                functions=FUNCTIONS,
                tables=TABLES,
                tags={
                    "vgi.title": "US Wages Functions",
                    "domain": "labor statistics",
                    "vgi.keywords": keywords_array("wages, occupations, metro areas, soc, oews, zip code"),
                    "vgi.source_url": "https://www.bls.gov/oes/",
                    "vgi.doc_llm": SCHEMA_DOC_LLM,
                    "vgi.doc_md": SCHEMA_DOC_MD,
                    "vgi.categories": json.dumps(SCHEMA_CATEGORIES),
                    "vgi.example_queries": json.dumps(_ALL_EXAMPLES),
                },
            )
        ],
    )

    class AttachOptions:
        """Options for ``ATTACH 'us_wages' (TYPE vgi, LOCATION '...', cache_dir '...', ...)``."""

        cache_dir: Annotated[
            str,
            AttachOption(
                desc=(
                    "Directory for downloaded BLS files and their Parquet conversions (about 25 MB per release). "
                    "Default: the per-user cache directory, e.g. ~/.cache/vgi-us-wages or "
                    "~/Library/Caches/vgi-us-wages."
                )
            ),
        ] = ""
        base_url: Annotated[
            str,
            AttachOption(
                desc=(
                    f"Where BLS files are fetched from: {DEFAULT_BASE_URL} (default), a mirror URL, or a local "
                    "directory laid out like bls.gov (oes/tables.htm, oes/special-requests/oesmYYall.zip, "
                    "oes/area_definitions_mYYYY.xlsx)."
                )
            ),
        ] = DEFAULT_BASE_URL
        user_agent: Annotated[
            str,
            AttachOption(
                desc=(
                    f"User-Agent sent to bls.gov (default {DEFAULT_USER_AGENT!r}). BLS asks automated clients to "
                    "identify themselves, ideally with contact details; set your own if bls.gov answers 403."
                )
            ),
        ] = DEFAULT_USER_AGENT
        offline: Annotated[
            bool,
            AttachOption(desc="Never touch the network: use only what is cached (default false)."),
        ] = False


def main() -> None:
    """Console-script entry point: parse ``--http`` / ``--unix`` / logging flags and serve."""
    UsWagesWorker.main()


if __name__ == "__main__":
    main()
