<p align="center">
  <img src="https://query.farm/media-kit/products/vgi/vgi-logo.png" alt="Vector Gateway Interface (VGI)" width="320">
</p>

# US Occupational Wages by Metro, State and Occupation in DuckDB

> **vgi-us-wages** · a [Query.Farm](https://query.farm) VGI worker

[![vgi worker: query.farm](https://query.farm/media-kit/shields/vgi-worker.svg)](https://query.farm)

Employment and pay for about 830 occupations across the United States, every state and
territory, about 390 metropolitan areas and 140 nonmetropolitan areas, straight from the
Bureau of Labor Statistics [Occupational Employment and Wage Statistics](https://www.bls.gov/oes/)
(OEWS) survey, as SQL. Add the SOC 2018 occupation hierarchy, a job-title search over
54,000 O*NET alternate titles, and a ZIP-code-to-metro lookup. No API key.

```sql
-- Where do registered nurses earn the most?
SELECT area_title, a_median, tot_emp
FROM us_wages.main.wages(occupation := '29-1141', area_type := 'metro')
WHERE a_median IS NOT NULL
ORDER BY a_median DESC LIMIT 5;

-- From a job title to a SOC code ...
SELECT soc_code, soc_title FROM us_wages.main.search_occupations('barista', max_results := 1);
-- ... to pay in the metro area around a ZIP code (metro-level, not ZIP-level).
SELECT area_title, estimate_geography, a_pct25, a_median, a_pct75
FROM us_wages.main.wages_for_zip('78701', '35-3023');
```

vgi-us-wages 0.2.0 speaks VGI protocol 3.4, so it needs Haybarn 2.0.1 or newer (Haybarn 2.0.0 loads a 3.3 `vgi` extension that cannot attach it).

## Install and attach

Requires [uv](https://docs.astral.sh/uv/) and DuckDB 2.0 (or [Haybarn](https://query.farm/products/haybarn) 2.0).

```sql
-- DuckDB 2.0
SET allow_extension_repositories = 'allowed';
CREATE EXTENSION REPOSITORY IF NOT EXISTS query_farm WITH PREFIX 'https://duckdb-extensions.query.farm';
INSTALL vgi FROM query_farm;
LOAD vgi FROM query_farm;

ATTACH 'us_wages' (TYPE vgi,
  LOCATION 'uvx --from git+https://github.com/Query-farm/vgi-us-wages@v0.2.0 vgi-us-wages-worker');
```

From a checkout: `make build`, then
`ATTACH 'us_wages' (TYPE vgi, LOCATION '/abs/path/to/vgi-us-wages/.venv/bin/vgi-us-wages-worker')`.

### ATTACH options

| Option | Default | Meaning |
|---|---|---|
| `cache_dir` | per-user cache dir (`~/Library/Caches/vgi-us-wages`, `~/.cache/vgi-us-wages`, `%LOCALAPPDATA%\vgi-us-wages`) | Where downloads and their Parquet conversions live (about 17 MB per release) |
| `base_url` | `https://www.bls.gov` | A mirror, or a local directory laid out like bls.gov (`oes/tables.htm`, `oes/special-requests/oesmYYall.zip`, `oes/area_definitions_mYYYY.xlsx`) |
| `user_agent` | `vgi-us-wages/0.1 (+https://query.farm)` | BLS asks automated clients to identify themselves, ideally with contact details. If bls.gov answers 403, set your own, e.g. `user_agent 'acme-hr-analytics/1.0 (data@acme.example)'` |
| `offline` | `false` | Never touch the network; use only the cache |

## Functions and tables

| Object | What it returns |
|---|---|
| `wages(occupation :=, area :=, area_type :=, state :=, industry :=, release :=)` | OEWS rows: employment, mean and 10/25/50/75/90th percentile wages, hourly and annual |
| `wages_for_zip(zip, occupation, all_areas := false, release :=)` | The **metro or nonmetro area's** figures for the area containing a ZIP, with the area used and the ZIP's land share in it. Never ZIP-level estimates |
| `areas(release :=)` | Every OEWS area of a release with codes, CBSA, county count and states |
| `area_counties(release :=)` | County FIPS to OEWS area definitions (May 2024 onward) |
| `occupations` (table) | SOC 2018 hierarchy: 23 major, 98 minor, 459 broad, 867 detailed, with definitions and O*NET-SOC codes |
| `search_occupations(text, max_results := 10)` | SOC codes for a job title, over SOC, O*NET titles and O*NET alternate titles |
| `cache_info` (table) | Provenance of every cached download (URL, SHA-256, ETag, fetch time) and bundled table |
| `refresh(release := 'latest', force := false)` | Download or re-validate a release in the cache |

Join keys: SOC codes `VARCHAR 'XX-XXXX'` (`occ_code`, `soc_code`), CBSA codes `VARCHAR(5)`
(`cbsa_code`), 2-digit state FIPS (`state_fips`) and 5-digit county FIPS (`county_fips`).
Codes are always `VARCHAR` and keep their leading zeros.

### Releases

`release := 'latest'` (the default) finds the newest May reference year on the
[BLS tables page](https://www.bls.gov/oes/tables.htm) and remembers it for 24 hours;
`release := '2024'` picks a year. All-data files exist from May 2011; this version was checked
against May 2019, 2023 and 2025.

### Pushdown

Everything filters locally against the cached Parquet file, before rows reach DuckDB:

- Named arguments: `occupation` (exact code or prefix like `'15-'`), `area`, `area_type`,
  `state` (primary state of the area), `industry` (`'cross-industry'` default, `'all'`, or a NAICS code).
- `WHERE` comparisons, `IN` lists and `IS [NOT] NULL` on `area_code`, `occ_code`, `cbsa_code`,
  `prim_state`, `o_group`, `naics`, `tot_emp`, wage columns etc. are pushed down; Parquet row
  groups whose `area_code`/`occ_code` statistics cannot match are skipped. Only the projected
  columns are returned.

### Markers: suppressed and top-coded values

BLS marks cells instead of publishing numbers. They are never coerced to 0:

| BLS marker | Meaning | Here |
|---|---|---|
| `*` | wage estimate not available | `NULL`; column named in `suppressed_columns` |
| `**` | employment estimate not available | `NULL`; column named in `suppressed_columns` |
| `#` | wage at or above the top-code | `NULL`; column named in `topcoded_columns`; the threshold is in `topcode_hourly` / `topcode_annual` (May 2023 and May 2025: $115.00/hour, $239,200/year; May 2019: $100.00, $208,000) |
| `~` | under 0.5% of establishments report the occupation | `pct_rpt` `NULL`, `pct_rpt_below_half` true |

`topcode_hourly` / `topcode_annual` are parsed from each workbook's own notes. Caveat seen in
May 2025: the notes still state $115.00 / $239,200, yet BLS publishes many values far above that
(national `a_pct90` up to $972,450) and marks *every* wage column `#` for a few occupations
(cardiologists, surgeons, chief executives). So for May 2025 a `#` means "BLS withheld it as
top-coded", and the stated threshold is not a reliable lower bound.

### ZIP codes, ZCTAs and metro areas

OEWS publishes no ZIP-level data. `wages_for_zip` maps a ZIP to the Census 2020
**ZCTA** of the same code, the ZCTA to counties by land-area overlap, and the counties to the
release's OEWS area. ZCTAs approximate USPS ZIP codes but are not the same thing: PO-box-only
and single-organisation ZIPs have no ZCTA (the function errors), and ZIP boundaries change
after 2020. `zcta_land_share` is a land-area share, not a population share.

OEWS metropolitan area codes are OMB CBSA codes. Two differences to know:

- **New England before May 2024.** Earlier releases (checked: May 2019, May 2023) define New
  England metro areas as town-based NECTAs (codes `7xxxx`, e.g. `71650` Boston-Cambridge-Nashua).
  Those rows have `is_necta = true` and `cbsa_code = NULL`. From May 2024 OEWS uses
  county-based CBSAs everywhere, so `wages_for_zip` and `area_counties` need release 2024 or later.
- **Connecticut** uses its nine 2022 planning regions (FIPS `09110`-`09190`) as county
  equivalents. The bundled crosswalk re-expresses the 2020 ZCTA-to-county overlaps through
  Census's town-to-planning-region crosswalk.

### Cache

The first wage query for a release downloads the BLS all-data zip (about 80 MB) and converts the
workbook once (about 15 seconds) into a typed Parquet file in `cache_dir`, guarded by a
cross-process lock. Nothing downloads at import or ATTACH. The occupation hierarchy, search and
ZIP crosswalk are bundled. `cache_info` lists what is cached; `refresh()` re-checks BLS.

## Data sources and terms

The code license (below) does not cover the data. See [DATA-SOURCES.md](DATA-SOURCES.md) for
URLs, retrieval dates and checksums.

- **BLS OEWS** estimates and area definitions, and the **SOC 2018** structure and definitions:
  U.S. Bureau of Labor Statistics, public domain (U.S. government work). Fetched at runtime
  (OEWS) or bundled (SOC).
- **O*NET 31.0** occupation data and job titles: bundled under
  [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/). Attribution: *This product
  includes information from the O*NET 31.0 Database by the U.S. Department of Labor,
  Employment and Training Administration (USDOL/ETA). Used under the CC BY 4.0 license. O*NET®
  is a trademark of USDOL/ETA.* The worker reshapes the data (columns selected, Parquet).
- **Census 2020 ZCTA relationship files** and the Connecticut planning-region crosswalk:
  U.S. Census Bureau, public domain; bundled as a derived ZCTA-to-county table.

Estimates are survey-based and have sampling error (`emp_prse`, `mean_prse`). This worker is not
affiliated with or endorsed by BLS, the Census Bureau or USDOL/ETA.

## Development

```bash
make check                 # ruff, mypy, pydoclint, pytest, SQL tests over stdio/launch/http, vgi-lint
make test-unit
make test-sql TRANSPORT=stdio
```

Tests are offline. `test/fixtures/bls` is a trimmed slice of real OEWS rows (May 2025 and
May 2023, markers included) plus the real May 2025 area definitions, laid out like bls.gov and
read through the ordinary `base_url` option. The lint run (`vgi-lint.toml`) uses the same
fixtures, so it never downloads the full file. Rebuild them with `scripts/make_fixtures.py`;
rebuild the bundled reference tables with `scripts/build_reference.py`.

## Support

Open an issue on [GitHub](https://github.com/Query-farm/vgi-us-wages/issues).

## Authorship & License

Copyright 2026 Query Farm LLC - https://query.farm

Released under the [BSD Zero Clause License](LICENSE) (0BSD). Data terms are separate; see
above.
