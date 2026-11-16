# Data sources

Upstream data licences are separate from the code licence (0BSD). Retrieved 2026-11-15 with
the User-Agent `vgi-us-wages/0.1 (+https://query.farm)`; bls.gov answered every request with
HTTP 200 (no 403 for that generic, non-browser User-Agent).

## Fetched at runtime (never bundled)

| Data | URL | Licence | Notes |
|---|---|---|---|
| OEWS all-data files, May 2011 onward | `https://www.bls.gov/oes/special-requests/oesm{YY}all.zip` (index: https://www.bls.gov/oes/tables.htm) | U.S. government work, public domain | ~73-80 MB each; one `.xlsx` inside. Converted to Parquet in the cache; the zip's SHA-256, ETag and Last-Modified are recorded in `cache_info`. |
| OEWS area definitions (county -> area), May 2024 onward | `https://www.bls.gov/oes/area_definitions_m{YYYY}.xlsx` | public domain | ~130-150 KB. `area_definitions_m2023.xlsx` does not exist (bls.gov redirects to /oes/home.htm). |

Upstream checksums observed on 2026-11-15 (SHA-256 of the downloaded file):

| File | Bytes | SHA-256 |
|---|---|---|
| oesm25all.zip (Last-Modified 2026-05-15) | 78,754,064 | 86ffcbbeb27a96b7aa84d01801de71ee94ee9027452152caabebfdec9d764fde |
| oesm23all.zip | 80,129,309 | 9e0d5a320c7189b449708d4223c89bf7e8be1a6dd28613e1f1972d3b5dd60f2b |
| oesm19all.zip | 72,749,860 | 9ee0f3cc65142d02514cbe3b2982eb93b65dd04f6449f426c2a4b78f349c693b |
| area_definitions_m2025.xlsx | 127,583 | 6a0013fb0005c5492de8d13737193dc142806ca560147cd42f2bbae041c3c615 |
| area_definitions_m2024.xlsx | 147,248 | ff1c45c9746802355856443c0dc948f00f0a85bcb1d92fcd1463d7137fccfbf3 |

## Bundled in `src/us_wages_worker/data/` (1.2 MB total)

Built by `scripts/build_reference.py` from these inputs:

| Bundled table | Source | Licence | Input SHA-256 |
|---|---|---|---|
| `soc_2018.parquet` (1,447 rows) | https://www.bls.gov/soc/2018/soc_2018_definitions.xlsx | BLS, public domain | f8b9ca7cccea78e30abfc9b96ab8a9dd2632cc8c8b57ee9b38a9b871e8fe2a89 |
| `onet_occupations.parquet` (1,016 rows) | https://www.onetcenter.org/dl_files/database/db_31_0_csv/occupation_data.csv | O*NET 31.0, CC BY 4.0 (USDOL/ETA) | a09eae1d6609686e44e05b7290993a1c8b523d8ca224bc0eedc194c855c3ee02 |
| `onet_job_titles.parquet` (54,269 rows) | https://www.onetcenter.org/dl_files/database/db_31_0_csv/job_titles.csv | O*NET 31.0, CC BY 4.0 (USDOL/ETA) | 75a3b46d6edc5996117c10c9ed488c9a06f7495d02969e9d12873ab14a21540c |
| `zcta_county.parquet` (46,961 rows) | https://www2.census.gov/geo/docs/maps-data/data/rel2020/zcta520/tab20_zcta520_county20_natl.txt | Census Bureau, public domain | 3ed41278d637dc249e0323306f68be8a6c234e3090f4de88ef328dee71aeaaaf |
| (Connecticut part of `zcta_county`) | https://www2.census.gov/geo/docs/maps-data/data/rel2020/zcta520/tab20_zcta520_cousub20_natl.txt | public domain | 406d2f1b11692a185e930e53f63a68951e5b64dbb7b0cf201a934a8e54aee27b |
| (Connecticut part of `zcta_county`) | https://www2.census.gov/geo/docs/reference/ct_change/ct_cou_to_cousub_crosswalk.txt | public domain | 13ad002564e30e6dcd1df112fdaa985f2cd0cdb908f7f3017cd2723db294cff8 |

Bundled file checksums:

| File | SHA-256 |
|---|---|
| soc_2018.parquet | c3c6c8036f4192213c53e8fd879c821019a89238ce6d329c68953298badecbdd |
| onet_occupations.parquet | b3cde5af4dab12562b0b82598c310273a85f53c4d6f19f101f3bc9870a86f067 |
| onet_job_titles.parquet | 11e43271f11443658135fb5ece4611d3e7efc0faa74aa26d6bdd35c1c80984c9 |
| zcta_county.parquet | e6e68c996ccc5f10668e4bc9a9479dbacc7cdf3a635b56bc849f64a6f7c02420 |

**O*NET attribution (CC BY 4.0):** This product includes information from the O*NET 31.0
Database by the U.S. Department of Labor, Employment and Training Administration (USDOL/ETA).
Used under the CC BY 4.0 license. O*NET® is a trademark of USDOL/ETA. Changes: selected
columns, SOC code derived from the O*NET-SOC code, converted to Parquet.

**ZCTA crosswalk derivation:** `land_share` = ZCTA land area inside the county / ZCTA land
area (water area for the few all-water ZCTAs). Connecticut's 2020 counties are replaced by the
2022 planning regions (county equivalents OEWS uses from May 2024) by summing ZCTA x town
overlaps through Census's town-to-planning-region crosswalk.

## Test fixtures in `test/fixtures/bls/oes/` (316 KB)

Built by `scripts/make_fixtures.py` from the real files above: a trimmed slice of real OEWS rows
(values and markers unchanged; May 2025: 693 rows, May 2023: 272 rows) plus the unmodified
`area_definitions_m2025.xlsx`. Public domain.

| File | SHA-256 |
|---|---|
| special-requests/oesm25all.zip | c9f7f700b37b0cee9a0ea33e65ce8adc73ebe1f8eb2c3eae8ad71cac7f85c26b |
| special-requests/oesm23all.zip | b2b7a0f9d7c0d4764aa8592b1200541987f52cac5e652f5b2bd7c7dab03d6322 |
| area_definitions_m2025.xlsx | 6a0013fb0005c5492de8d13737193dc142806ca560147cd42f2bbae041c3c615 |
