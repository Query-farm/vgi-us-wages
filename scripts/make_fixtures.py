#!/usr/bin/env python3
"""Build the offline test fixtures in ``test/fixtures/bls`` from real BLS files.

    uv run --with openpyxl python scripts/make_fixtures.py --source-dir /path/to/downloads

``--source-dir`` must hold the real ``oesm25all.zip``, ``oesm23all.zip`` and
``area_definitions_m2025.xlsx`` from bls.gov. The fixtures are a trimmed slice of
*real* OEWS rows (unchanged values and markers), laid out like bls.gov so the
worker reads them through its ordinary ``base_url`` ATTACH option.
"""

from __future__ import annotations

import argparse
import shutil
import zipfile
from pathlib import Path
from typing import Any

from openpyxl import Workbook  # type: ignore[import-untyped]
from python_calamine import CalamineWorkbook

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "test" / "fixtures" / "bls" / "oes"

OCCS = {
    "00-0000", "11-0000", "11-1011", "15-0000", "15-1200", "15-1250", "15-1251", "15-1252", "25-2021",
    "27-2042", "29-0000", "29-1141", "29-1215", "35-3023", "47-2152", "53-3032", "53-3033",
}  # fmt: skip
AREAS_2025 = {
    "99", "06", "09", "25", "36", "48", "72", "66", "7800001", "6600001",
    "41860", "35620", "14460", "25540", "12420", "31080", "14860", "0600007", "0900001",
}  # fmt: skip
AREAS_2023 = {"99", "25", "09", "71650", "73450", "41860"}
NATIONAL_INDUSTRY_OCCS = {"15-1252", "29-1141"}


def _workbook(src: Path) -> CalamineWorkbook:
    with zipfile.ZipFile(src) as zf:
        member = next(n for n in zf.namelist() if n.lower().endswith(".xlsx"))
        tmp = src.with_suffix(".fixture-src.xlsx")
        with zf.open(member) as f, open(tmp, "wb") as out:
            shutil.copyfileobj(f, out)
    wb = CalamineWorkbook.from_path(str(tmp))
    tmp.unlink()
    return wb


def _trim(rows: list[list[Any]], areas: set[str], extra_markers: bool) -> list[list[Any]]:
    h = [str(c).upper() for c in rows[0]]
    ai, oi, gi, ii = h.index("AREA"), h.index("OCC_CODE"), h.index("O_GROUP"), h.index("I_GROUP")
    keep = [rows[0]]
    seen_markers: dict[str, int] = {}
    for r in rows[1:]:
        area, occ, group, igroup = str(r[ai]), str(r[oi]), str(r[gi]), str(r[ii])
        cross = igroup == "cross-industry"
        take = cross and area in areas and (occ in OCCS or group == "major")
        if (
            not take
            and area == "99"
            and occ in NATIONAL_INDUSTRY_OCCS
            and igroup in {"sector", "cross-industry, ownership"}
        ):
            take = True
        if not take and extra_markers and cross and area in areas:
            for marker in ("*", "**", "#"):
                if marker in (str(c) for c in r) and seen_markers.get(marker, 0) < 6:
                    seen_markers[marker] = seen_markers.get(marker, 0) + 1
                    take = True
                    break
        if not take and extra_markers and area == "99" and "~" in (str(c) for c in r) and seen_markers.get("~", 0) < 3:
            seen_markers["~"] = seen_markers.get("~", 0) + 1
            take = True
        if take:
            keep.append(r)
    return keep


def _write_zip(year: int, wb_src: CalamineWorkbook, areas: set[str], dest: Path) -> int:
    rows = wb_src.get_sheet_by_index(0).to_python()
    trimmed = _trim(rows, areas, extra_markers=True)
    wb = Workbook()
    ws = wb.active
    ws.title = wb_src.sheet_names[0]
    for r in trimmed:
        ws.append(r)
    for name in wb_src.sheet_names[1:2]:  # Field Descriptions: carries the top-code note
        fd = wb.create_sheet(name)
        for r in wb_src.get_sheet_by_name(name).to_python():
            fd.append(r)
    xlsx = dest.with_suffix(".xlsx")
    wb.save(xlsx)
    with zipfile.ZipFile(dest, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.write(xlsx, f"oesm{year % 100:02d}all/all_data_M_{year}.xlsx")
    xlsx.unlink()
    return len(trimmed) - 1


def main() -> None:
    """Rebuild every fixture."""
    ap = argparse.ArgumentParser()
    ap.add_argument("--source-dir", type=Path, required=True)
    src = ap.parse_args().source_dir
    (OUT / "special-requests").mkdir(parents=True, exist_ok=True)
    n25 = _write_zip(2025, _workbook(src / "oesm25all.zip"), AREAS_2025, OUT / "special-requests" / "oesm25all.zip")
    n23 = _write_zip(2023, _workbook(src / "oesm23all.zip"), AREAS_2023, OUT / "special-requests" / "oesm23all.zip")
    shutil.copyfile(src / "area_definitions_m2025.xlsx", OUT / "area_definitions_m2025.xlsx")
    (OUT / "tables.htm").write_text(
        "<html><body><!-- trimmed fixture of https://www.bls.gov/oes/tables.htm -->\n"
        '<a href="/oes/special-requests/oesm23all.zip">May 2023 all data</a>\n'
        '<a href="/oes/special-requests/oesm25all.zip">May 2025 all data</a>\n'
        "</body></html>\n",
        encoding="utf-8",
    )
    print(f"2025: {n25} rows, 2023: {n23} rows")


if __name__ == "__main__":
    main()
