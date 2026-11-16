-- Opt-in live check against bls.gov (downloads ~80 MB on first run). Run with `make live-check`.
SELECT release_year, count(*) AS rows, count(DISTINCT area_code) AS areas FROM us_wages.main.wages() GROUP BY ALL;
SELECT area_title, a_median FROM us_wages.main.wages(occupation := '29-1141', area_type := 'metro')
WHERE a_median IS NOT NULL ORDER BY a_median DESC LIMIT 3;
SELECT area_code, area_title, estimate_geography, a_median FROM us_wages.main.wages_for_zip('94103', '15-1252');
SELECT kind, release_year, bytes, sha256 FROM us_wages.main.cache_info WHERE kind <> 'bundled' ORDER BY ALL;
