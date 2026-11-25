# Changelog

## 0.2.0 (2026-11-25)

- Speaks VGI protocol 3.4: require vgi-python `>=0.55.2,<0.56`; uv.lock re-locked. This release
  needs Haybarn 2.0.1 (or stock DuckDB 2.0 with `INSTALL vgi FROM query_farm`): Haybarn 2.0.0
  loads a 3.3 `vgi` extension that cannot attach it.
- SQL tests run on haybarn-unittest 2.0.1rc3.
- `serve` extra is `vgi-python[http,oauth]`; the Docker image installs it, so the image runs in
  Orchard hosted mode (`VGI_ORCHARD_HOSTED=1` with VGI_JWT_ISSUER, VGI_JWT_AUDIENCE,
  VGI_JWT_JWKS_URI and VGI_SIGNING_KEY). The HEALTHCHECK uses `HEAD /health`, which stays
  anonymous in hosted mode.

## 0.1.2 (2026-11-16)

- Bump vgi-python to 0.52.2.
- Requires Python 3.14 (CI, Docker image and tooling moved to 3.14).

## 0.1.1 (2026-11-16)

Fix: the download cache lock is now held by the operating system, so it is released when a worker is killed mid-download (e.g. Ctrl-C on the first query). Previously the leftover lock file made the next query wait up to 15 minutes. Abandoned partial downloads are removed on the next fetch. Also: the `cache_info` example lists every entry, so it returns rows on a fresh install.

## 0.1.0 (2026-11-16)

First release: BLS OEWS wages (`wages`, `wages_for_zip`, `areas`, `area_counties`), the SOC 2018
`occupations` table, `search_occupations` over O*NET 31.0 titles, and the `cache_info` /
`refresh` cache tools. Generated from vgi-starter-python v1.3.0.

Targets: vgi-python 0.51 · VGI protocol 3.3 · Haybarn 2.0.0rc11 · vgi-lint-check 2.x (unpinned)
