"""Unit tests. The SQL-level tests live in test/sql and run through DuckDB."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest
from vgi.arguments import Arguments
from vgi.client import Client
from vgi.table_filter_pushdown import ComparisonOp, ConstantFilter, InFilter

from us_wages_worker.common import DEFAULT_BASE_URL, Options, UpstreamError, options_from, parse_release
from us_wages_worker.oews import _num, _prim_state, ensure_area_definitions, load_release, resolve_year
from us_wages_worker.reference import search
from us_wages_worker.wage_rows import WAGES_SCHEMA, build_selection, read_all, to_batch
from us_wages_worker.wages import zip_areas

SCHEMA = ["main"]


def _table(client: Client, name: str, *positional: Any, **named: Any) -> pa.Table:
    arguments = Arguments(
        positional=tuple(pa.scalar(v) for v in positional),
        named={k: pa.scalar(v) for k, v in named.items()},
    )
    batches = list(client.table_function(function_name=name, schema_path=SCHEMA, arguments=arguments))
    return pa.Table.from_batches(batches)


class _Pushed:
    def __init__(self, *filters: Any) -> None:
        self.filters = list(filters)


def test_marker_cells_never_become_numbers() -> None:
    assert _num("#") == (None, "#")
    assert _num("**") == (None, "**")
    assert _num("*") == (None, "*")
    assert _num("~") == (None, "~")
    assert _num("") == (None, None)
    assert _num("1,234.5") == (1234.5, None)
    assert _num(42.0) == (42.0, None)


def test_prim_state_fallbacks() -> None:
    assert _prim_state("99", 1, "U.S.") == "US"
    assert _prim_state("06", 2, "California") == "CA"
    assert _prim_state("0900001", 6, "Connecticut nonmetropolitan area") == "CT"
    assert _prim_state("71650", 4, "Boston-Cambridge-Nashua, MA-NH") == "MA"


def test_release_parsing() -> None:
    assert parse_release("latest") is None
    assert parse_release("2024") == 2024
    with pytest.raises(ValueError):
        parse_release("May 2024")


def test_default_options(tmp_path: Path) -> None:
    o = options_from({})
    assert o.base_url == DEFAULT_BASE_URL and o.is_default_source and not o.offline
    m = options_from({"base_url": "https://mirror.example", "cache_dir": str(tmp_path)})
    assert m.source_root != o.source_root and m.source_root.parent == tmp_path.resolve()


def test_latest_resolves_from_tables_page(opts: Options) -> None:
    assert resolve_year(opts, None) == 2025
    with pytest.raises(ValueError, match="2011"):
        resolve_year(opts, 2005)


def test_offline_without_cache_fails(tmp_path: Path) -> None:
    o = options_from({"cache_dir": str(tmp_path), "base_url": str(tmp_path / "nowhere"), "offline": True})
    with pytest.raises(UpstreamError, match="offline"):
        load_release(o, None)


def test_release_conversion_and_flags(opts: Options) -> None:
    release = load_release(opts, None)
    assert release.year == 2025 and release.topcode_annual == 239200.0 and release.topcode_hourly == 115.0
    rows = read_all(
        release, build_selection(occupation=None, area=None, area_type=None, state=None, industry="all", pushed=None)
    )
    for r in rows.to_pylist():
        for col in r["topcoded_columns"] + r["suppressed_columns"]:
            assert r[col] is None, (col, r)
    assert any(r["topcoded_columns"] for r in rows.to_pylist())


def test_pushed_filters_translate(opts: Options) -> None:
    release = load_release(opts, None)
    pushed = _Pushed(
        ConstantFilter(column_name="occ_code", column_index=0, op=ComparisonOp.EQ, value=pa.scalar("29-1141")),
        InFilter(column_name="area_code", column_index=1, values=pa.array(["41860", "35620"])),
    )
    sel = build_selection(occupation=None, area=None, area_type=None, state=None, industry=None, pushed=pushed)
    assert sel.area_codes == frozenset({"41860", "35620"}) and sel.occ_codes == frozenset({"29-1141"})
    batch = to_batch(read_all(release, sel), release, WAGES_SCHEMA)
    assert sorted(str(v) for v in batch.column("cbsa_code").to_pylist()) == ["35620", "41860"]


def test_necta_release(opts: Options) -> None:
    release = load_release(opts, 2023)
    sel = build_selection(occupation="00-0000", area="71650", area_type=None, state=None, industry=None, pushed=None)
    row = to_batch(read_all(release, sel), release, WAGES_SCHEMA).to_pylist()[0]
    assert row["is_necta"] is True and row["cbsa_code"] is None
    with pytest.raises(UpstreamError, match="2024"):
        ensure_area_definitions(opts, 2023)


def test_zip_areas(opts: Options) -> None:
    defs, _ = ensure_area_definitions(opts, 2025)
    hartford = zip_areas("06076", defs)
    assert [a["area_code"] for a in hartford] == ["25540", "0900001"]
    assert hartford[0]["primary"] and abs(sum(a["share"] for a in hartford) - 1.0) < 1e-5
    with pytest.raises(ValueError, match="ZCTA"):
        zip_areas("00000", defs)


def test_search_ranks_exact_titles_first() -> None:
    assert search("software developers", 1)[0]["soc_code"] == "15-1252"
    assert search("barista", 1)[0]["soc_code"] == "35-3023"
    assert search("zzzxqv", 5) == []


def test_occupations_over_rpc(client: Client) -> None:
    table = _table(client, "occupations_scan")
    assert table.num_rows == 1447
    assert set(table.column("soc_level").to_pylist()) == {"major", "minor", "broad", "detailed"}


def test_search_over_rpc(client: Client) -> None:
    table = _table(client, "search_occupations", "registered nurse", max_results=3)
    assert table.column("soc_code").to_pylist()[0] == "29-1141"
    assert table.column("rank").to_pylist() == [1, 2, 3]
