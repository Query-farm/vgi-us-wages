#!/usr/bin/env python3
"""Rebuild the bundled reference tables in ``src/us_wages_worker/data/``.

The worker ships four small, redistributable reference tables so that
``occupations()``, ``search_occupations()`` and the ZIP side of
``wages_for_zip()`` work offline. The big OEWS wage tables are *not* bundled;
the worker downloads those at query time.

    uv run python scripts/build_reference.py --source-dir /path/to/downloads [--download]

``--download`` fetches every input into ``--source-dir`` first (about 30 MB),
politely and with a generic User-Agent. The inputs, their licences and the
checksums of the copies the bundle was built from are listed in DATA-SOURCES.md.

Outputs (Parquet, zstd):

* ``soc_2018.parquet``       SOC 2018 hierarchy + definitions (BLS, public domain)
* ``onet_occupations.parquet`` O*NET-SOC occupations + descriptions (O*NET 31.0, CC BY 4.0)
* ``onet_job_titles.parquet``  O*NET alternate/reported job titles (O*NET 31.0, CC BY 4.0)
* ``zcta_county.parquet``    2020 ZCTA -> county land-area overlap, Connecticut
  re-expressed in the 2022 planning-region county equivalents (Census, public domain)
"""

from __future__ import annotations

import argparse
import csv
import io
import sys
import time
import urllib.request
from collections import defaultdict
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from python_calamine import CalamineWorkbook

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "src" / "us_wages_worker" / "data"
USER_AGENT = "vgi-us-wages/0.1 (+https://query.farm)"

SOURCES = {
    "soc_2018_definitions.xlsx": "https://www.bls.gov/soc/2018/soc_2018_definitions.xlsx",
    "occupation_data.csv": "https://www.onetcenter.org/dl_files/database/db_31_0_csv/occupation_data.csv",
    "job_titles.csv": "https://www.onetcenter.org/dl_files/database/db_31_0_csv/job_titles.csv",
    "zcta_county.txt": "https://www2.census.gov/geo/docs/maps-data/data/rel2020/zcta520/tab20_zcta520_county20_natl.txt",
    "zcta_cousub.txt": "https://www2.census.gov/geo/docs/maps-data/data/rel2020/zcta520/tab20_zcta520_cousub20_natl.txt",
    "ct_xwalk.txt": "https://www2.census.gov/geo/docs/reference/ct_change/ct_cou_to_cousub_crosswalk.txt",
}


def download(src: Path) -> None:
    """Fetch every input into ``src`` (one request per second)."""
    src.mkdir(parents=True, exist_ok=True)
    for name, url in SOURCES.items():
        target = src / name
        if target.exists():
            continue
        print(f"GET {url}", file=sys.stderr)
        req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
        with urllib.request.urlopen(req, timeout=120) as resp:
            target.write_bytes(resp.read())
        time.sleep(1)


def _write(table: pa.Table, name: str) -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, OUT / name, compression="zstd", compression_level=19)
    print(f"wrote {name}: {table.num_rows} rows, {(OUT / name).stat().st_size:,} bytes", file=sys.stderr)


def build_soc(src: Path) -> None:
    """SOC 2018 definitions workbook -> hierarchy with parent codes and titles."""
    rows = CalamineWorkbook.from_path(str(src / "soc_2018_definitions.xlsx")).get_sheet_by_index(0).to_python()
    start = next(i for i, r in enumerate(rows) if r and str(r[0]).strip() == "SOC Group") + 1
    out: dict[str, list[object]] = defaultdict(list)
    parents: dict[str, tuple[str, str]] = {}
    for r in rows[start:]:
        level, code, title, definition = (str(v).strip() for v in r[:4])
        if not code:
            continue
        level = level.lower()
        parents[level] = (code, title)
        if level == "major":
            parents.pop("minor", None)
            parents.pop("broad", None)
        elif level == "minor":
            parents.pop("broad", None)
        out["soc_code"].append(code)
        out["soc_title"].append(title)
        out["soc_level"].append(level)
        for p in ("major", "minor", "broad"):
            pc, pt = parents.get(p, (None, None)) if level != p else (code, title)
            if ["major", "minor", "broad", "detailed"].index(p) > ["major", "minor", "broad", "detailed"].index(level):
                pc, pt = None, None
            out[f"{p}_code"].append(pc)
            out[f"{p}_title"].append(pt)
        out["definition"].append(definition or None)
    _write(pa.table(out), "soc_2018.parquet")


def build_onet(src: Path) -> None:
    """O*NET occupation data and job titles -> two Parquet tables."""
    with open(src / "occupation_data.csv", encoding="utf-8-sig", newline="") as f:
        occ = list(csv.DictReader(f))
    _write(
        pa.table(
            {
                "onet_soc_code": [r["O*NET-SOC Code"] for r in occ],
                "soc_code": [r["O*NET-SOC Code"][:7] for r in occ],
                "title": [r["Title"] for r in occ],
                "description": [r["Description"] for r in occ],
            }
        ),
        "onet_occupations.parquet",
    )
    with open(src / "job_titles.csv", encoding="utf-8-sig", newline="") as f:
        jobs = list(csv.DictReader(f))
    _write(
        pa.table(
            {
                "onet_soc_code": [r["O*NET-SOC Code"] for r in jobs],
                "job_title": [r["Job Title"] for r in jobs],
                "short_title": [r["Short Title"] or None for r in jobs],
            }
        ),
        "onet_job_titles.parquet",
    )


def _rel_rows(path: Path) -> list[dict[str, str]]:
    with open(path, encoding="utf-8-sig", newline="") as f:
        return [r for r in csv.DictReader(f, delimiter="|") if r["GEOID_ZCTA5_20"]]


def build_zcta(src: Path) -> None:
    """ZCTA -> county land-area overlap, with Connecticut moved to planning regions."""
    county_rows = _rel_rows(src / "zcta_county.txt")
    # Connecticut: the 2020 file uses the 8 old counties. OEWS (May 2024+) defines CT
    # areas by the 9 planning regions, which are built from towns (county subdivisions),
    # so re-aggregate the ZCTA x town overlaps through Census's town crosswalk.
    with open(src / "ct_xwalk.txt", encoding="utf-8-sig", newline="") as f:
        text = f.read()
    town_to_region = {}
    for r in csv.reader(io.StringIO(text), delimiter="|"):
        if len(r) >= 8 and r[0] == "09" and r[6].isdigit():
            town_to_region[r[6]] = ("09" + r[3], r[4])
    land: dict[tuple[str, str], int] = defaultdict(int)
    water: dict[tuple[str, str], int] = defaultdict(int)
    names: dict[str, str] = {}
    zcta_land: dict[str, int] = {}
    zcta_water: dict[str, int] = {}
    for r in county_rows:
        z, c = r["GEOID_ZCTA5_20"], r["GEOID_COUNTY_20"]
        zcta_land[z] = int(r["AREALAND_ZCTA5_20"])
        zcta_water[z] = int(r["AREAWATER_ZCTA5_20"])
        if c.startswith("09"):
            continue
        land[(z, c)] += int(r["AREALAND_PART"])
        water[(z, c)] += int(r["AREAWATER_PART"])
        names[c] = r["NAMELSAD_COUNTY_20"]
    missing = set()
    for r in _rel_rows(src / "zcta_cousub.txt"):
        town = r["GEOID_COUSUB_20"]
        if not town.startswith("09"):
            continue
        region = town_to_region.get(town)
        if region is None:
            # "County subdivisions not defined" (water) rows: no town, no planning region.
            missing.add(town)
            continue
        z, c = r["GEOID_ZCTA5_20"], region[0]
        land[(z, c)] += int(r["AREALAND_PART"])
        water[(z, c)] += int(r["AREAWATER_PART"])
        names[c] = region[1]
    if missing:
        print(f"note: {len(missing)} CT county-subdivision codes without a town (water-only) skipped", file=sys.stderr)
    out: dict[str, list[object]] = defaultdict(list)
    for z, c in sorted(land):
        zl, zw = zcta_land[z], zcta_water[z]
        share = land[(z, c)] / zl if zl > 0 else (water[(z, c)] / zw if zw > 0 else 0.0)
        if share <= 0 and land[(z, c)] == 0 and zl > 0:
            continue  # water-only sliver of a ZCTA that has land elsewhere
        out["zcta"].append(z)
        out["county_fips"].append(c)
        out["county_name"].append(names[c])
        out["land_share"].append(round(share, 6))
        out["land_m2"].append(land[(z, c)])
    _write(pa.table(out), "zcta_county.parquet")


def main() -> None:
    """Parse arguments and rebuild every bundled table."""
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--source-dir", type=Path, required=True)
    ap.add_argument("--download", action="store_true", help="fetch missing inputs into --source-dir first")
    args = ap.parse_args()
    if args.download:
        download(args.source_dir)
    build_soc(args.source_dir)
    build_onet(args.source_dir)
    build_zcta(args.source_dir)


if __name__ == "__main__":
    main()
