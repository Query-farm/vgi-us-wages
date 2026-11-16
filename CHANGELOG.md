# Changelog

## 0.1.1 (2026-11-16)

Fix: the download cache lock is now held by the operating system, so it is released when a worker is killed mid-download (e.g. Ctrl-C on the first query). Previously the leftover lock file made the next query wait up to 15 minutes. Abandoned partial downloads are removed on the next fetch. Also: the `cache_info` example lists every entry, so it returns rows on a fresh install.

## 0.1.0 (2026-11-16)

First release: BLS OEWS wages (`wages`, `wages_for_zip`, `areas`, `area_counties`), the SOC 2018
`occupations` table, `search_occupations` over O*NET 31.0 titles, and the `cache_info` /
`refresh` cache tools. Generated from vgi-starter-python v1.3.0.

Targets: vgi-python 0.51 · VGI protocol 3.3 · Haybarn 2.0.0rc11 · vgi-lint-check 2.x (unpinned)
