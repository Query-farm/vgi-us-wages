"""Discovery metadata for the us_wages catalog.

DuckDB, coding agents and the ``vgi-lint`` metadata checker all read these
tags. Catalog-level tags live in ``CATALOG_TAGS``; every function carries
``object_tags(...)``.
"""

from __future__ import annotations

import json
from typing import Any

import pyarrow as pa

CATALOG_NAME = "us_wages"

SOURCE_URL = "https://github.com/Query-farm/vgi-us-wages"
OEWS_URL = "https://www.bls.gov/oes/"
OEWS_TABLES_URL = "https://www.bls.gov/oes/tables.htm"

SCHEMA_CATEGORIES = [
    {"name": "wages", "title": "Wages", "description": "Employment and pay estimates by occupation and area."},
    {"name": "geography", "title": "Geography", "description": "OEWS areas, their counties and ZIP lookups."},
    {"name": "occupations", "title": "Occupations", "description": "SOC 2018 occupation classification and search."},
    {"name": "cache", "title": "Cache", "description": "Inspect and refresh the local download cache."},
]


def keywords_array(keywords: str) -> str:
    """Turn ``"a, b, c"`` into the JSON array string ``vgi.keywords`` requires."""
    return json.dumps([k.strip() for k in keywords.split(",") if k.strip()])


def object_tags(*, title: str, category: str, keywords: str, doc_llm: str, doc_md: str) -> dict[str, str]:
    """Build the standard per-object discovery tags."""
    if category not in {c["name"] for c in SCHEMA_CATEGORIES}:
        raise ValueError(f"unknown category {category!r}; add it to SCHEMA_CATEGORIES")
    return {
        "vgi.title": title,
        "vgi.category": category,
        "vgi.keywords": keywords_array(keywords),
        "vgi.doc_llm": doc_llm,
        "vgi.doc_md": doc_md,
    }


def column(name: str, type_: pa.DataType, comment: str, *, nullable: bool = True) -> pa.Field[Any]:
    """Build an Arrow field whose ``comment`` shows up in DuckDB's ``DESCRIBE`` and agent tools."""
    return pa.field(name, type_, nullable=nullable, metadata={b"comment": comment.encode()})


# Terser descriptions for vgi.result_columns_schema, which agent tools cap at 4,000
# characters; DESCRIBE still shows each column's full comment.
SHORT_COLUMN_DOCS = {
    "release_year": "May reference year, e.g. 2025.",
    "area_code": "'99' US, state FIPS, 5-digit CBSA or 7-digit nonmetro code.",
    "area_title": "Area name.",
    "area_type": "1 US, 2 state, 3 territory, 4 metro, 6 nonmetro.",
    "area_type_name": "national/state/territory/metro/nonmetro.",
    "prim_state": "Primary state, USPS code.",
    "state_fips": "2-digit FIPS of prim_state.",
    "cbsa_code": "CBSA code of metro rows (NULL for NECTAs).",
    "is_necta": "New England NECTA (pre-2024 releases).",
    "naics": "NAICS code; '000000' cross-industry.",
    "naics_title": "Industry title.",
    "i_group": "Industry level, e.g. cross-industry, sector.",
    "own_code": "Ownership code; 1235 = all.",
    "occ_code": "SOC code 'XX-XXXX'.",
    "occ_title": "Occupation title.",
    "o_group": "total/major/minor/broad/detailed.",
    "tot_emp": "Employment (excl. self-employed).",
    "emp_prse": "Employment RSE, %.",
    "jobs_1000": "Jobs per 1,000 in the area.",
    "loc_quotient": "Location quotient vs US.",
    "h_mean": "Mean hourly wage, USD.",
    "a_mean": "Mean annual wage, USD.",
    "mean_prse": "Mean wage RSE, %.",
    "annual_only": "Only annual wages published.",
    "hourly_only": "Only hourly wages published.",
    "suppressed_columns": "Columns BLS marked * / ** (NULL here).",
    "topcoded_columns": "Columns BLS top-coded # (NULL here).",
    "topcode_hourly": "Hourly top-code threshold.",
    "topcode_annual": "Annual top-code threshold.",
    "pct_total": "% of industry employment.",
    "pct_rpt": "% of establishments reporting.",
    "pct_rpt_below_half": "pct_rpt was ~ (<0.5%).",
    "estimate_geography": "Which area the figures describe (never the ZIP).",
    "zcta_land_share": "Share of the ZCTA's land in this area.",
    "is_primary_area": "Area holding most of the ZCTA.",
    "counties": "County FIPS of the ZCTA in this area.",
    "zip": "ZIP code asked about.",
}
for _p in ("10", "25", "75", "90"):
    SHORT_COLUMN_DOCS[f"h_pct{_p}"] = f"Hourly wage, {_p}th percentile."
    SHORT_COLUMN_DOCS[f"a_pct{_p}"] = f"Annual wage, {_p}th percentile."
SHORT_COLUMN_DOCS["h_median"] = "Median hourly wage."
SHORT_COLUMN_DOCS["a_median"] = "Median annual wage."


def result_columns(schema: pa.Schema, *, short: bool = False) -> str:
    """``vgi.result_columns_schema`` JSON from an Arrow schema built with `column`."""
    duck = {
        pa.string(): "VARCHAR",
        pa.int8(): "TINYINT",
        pa.int16(): "SMALLINT",
        pa.int32(): "INTEGER",
        pa.int64(): "BIGINT",
        pa.float64(): "DOUBLE",
        pa.bool_(): "BOOLEAN",
        pa.list_(pa.string()): "VARCHAR[]",
        pa.timestamp("us", tz="UTC"): "TIMESTAMP WITH TIME ZONE",
    }
    out = []
    for f in schema:
        comment = (f.metadata or {}).get(b"comment", b"").decode()
        if short:
            comment = SHORT_COLUMN_DOCS.get(f.name, comment)
        out.append({"name": f.name, "type": duck[f.type], "description": comment})
    return json.dumps(out, separators=(",", ":")) if short else json.dumps(out)


CATALOG_COMMENT = (
    "US employment and wages by occupation for the nation, states, metro and nonmetro areas from the BLS "
    "Occupational Employment and Wage Statistics (OEWS) survey, with the SOC 2018 occupation classification, "
    "occupation search and a ZIP-to-metro lookup. No API key needed."
)

CATALOG_DOC_LLM = (
    "US occupational employment and wage estimates from the Bureau of Labor Statistics OEWS program "
    "(https://www.bls.gov/oes/), one row per occupation x area x industry, for each May reference year "
    "(release). Wages are in US dollars: mean, median and 10th/25th/75th/90th percentiles, hourly (h_*) and "
    "annual (a_*). Areas: the US ('99'), states and territories (2-digit FIPS), metropolitan areas (5-digit "
    "CBSA codes) and OEWS nonmetropolitan areas (7-digit codes). Occupations use SOC 2018 codes 'XX-XXXX'. "
    "Missing or top-coded values are NULL (never 0) and named in suppressed_columns / topcoded_columns. "
    "Typical flow: turn a job title into a SOC code with the occupation search, then filter the wage "
    "estimates by that code and an area; a ZIP code can stand in for its metro area. The first wage query "
    "for a release downloads the BLS file (about 80 MB, converted once into a local cache); the occupation "
    "hierarchy and search use bundled data and need no network."
)

CATALOG_DOC_MD = (
    "# US Wages\n\n"
    "Employment and pay for about 830 occupations across the US, every state, about 390 metropolitan areas "
    "and 140 nonmetropolitan areas, from the Bureau of Labor Statistics "
    "[Occupational Employment and Wage Statistics](https://www.bls.gov/oes/) (OEWS) survey. Each May "
    "reference period is a *release*; the worker reads BLS's "
    "[all-data files](https://www.bls.gov/oes/tables.htm), converts them once into a local Parquet cache "
    "and filters them locally.\n\n"
    "Occupations follow the [2018 Standard Occupational Classification](https://www.bls.gov/soc/2018/); "
    "occupation search also covers the alternate job titles of the "
    "[O*NET 31.0 database](https://www.onetcenter.org/database.html) (CC BY 4.0, by the U.S. Department of "
    "Labor, Employment and Training Administration). The ZIP lookup uses the Census Bureau's "
    "[2020 ZCTA relationship files](https://www.census.gov/geographies/reference-files/time-series/geo/"
    "relationship-files.html) and gives *metro-level* figures, never ZIP-level ones.\n\n"
    "BLS suppression and top-code markers become NULL plus a flag, never 0."
)

SCHEMA_COMMENT = (
    "OEWS wage and employment estimates, the OEWS area list and ZIP lookup, the SOC 2018 occupation "
    "hierarchy and occupation search, and cache management."
)

SCHEMA_DOC_LLM = (
    "Everything lives in `main`. Wage estimates come from table functions with named arguments that filter "
    "locally (occupation, area, area_type, state, industry, release); WHERE clauses on the result columns "
    "are pushed down too. The default release is 'latest' (most recent May reference year); pass "
    "release := '2024' for another year. Cross-industry rows are the default; industry := 'all' adds the "
    "national industry (NAICS) breakdowns. SOC codes are `VARCHAR` 'XX-XXXX'; area codes are `VARCHAR` and "
    "keep leading zeros; join metro rows to other data on cbsa_code and states on state_fips."
)

SCHEMA_DOC_MD = (
    "## main\n\n"
    "Wage estimates from the BLS [OEWS](https://www.bls.gov/oes/) survey, the areas they cover, and the "
    "SOC 2018 occupations they are classified by.\n\n"
    "- **Wages**: estimates by occupation, area and industry; a ZIP-to-metro shortcut.\n"
    "- **Geography**: the OEWS areas of a release and the counties that make them up.\n"
    "- **Occupations**: the SOC 2018 hierarchy and a title search with O*NET alternate titles.\n"
    "- **Cache**: what has been downloaded, and a way to refresh it."
)

AGENT_TEST_TASKS = [
    {
        "name": "soc_major_groups",
        "prompt": "Using the us_wages worker, how many SOC 2018 major occupation groups are there?",
    },
    {
        "name": "software_developer_code",
        "prompt": "Using the us_wages worker's occupation search, what is the SOC code for software developers?",
    },
    {
        "name": "nurse_median_metro",
        "prompt": (
            "Using the us_wages worker, which metropolitan area has the highest annual median wage for "
            "registered nurses in the latest release?"
        ),
    },
    {
        "name": "zip_metro",
        "prompt": "Using the us_wages worker, which OEWS metro area are wage figures for ZIP code 94103 taken from?",
    },
    {
        "name": "metro_area_count",
        "prompt": "Using the us_wages worker, how many metropolitan areas does the latest OEWS release cover?",
    },
    {
        "name": "connecticut_regions",
        "prompt": (
            "Using the us_wages worker, how many Connecticut counties or county equivalents belong to the "
            "Hartford metro area (CBSA 25540) in the latest area definitions?"
        ),
    },
    {
        "name": "cached_bundles",
        "prompt": "Using the us_wages worker, how many bundled reference tables does it ship with?",
    },
    {
        "name": "refresh_latest",
        "prompt": "Using the us_wages worker, make sure the latest OEWS release is cached and report its year.",
    },
]

CATALOG_TAGS = {
    "vgi.title": "US Occupational Wages",
    "vgi.subtitle": "Occupational employment and wages for the US, states and metro areas, plus SOC occupations.",
    "vgi.doc_llm": CATALOG_DOC_LLM,
    "vgi.doc_md": CATALOG_DOC_MD,
    "vgi.source_url": SOURCE_URL,
    "vgi.icon_url": "https://query.farm/media-kit/products/vgi/vgi-logo.png",
    "vgi.license": "0BSD",
    "vgi.author": "Query.Farm",
    "vgi.copyright": "Copyright 2026 Query Farm LLC - https://query.farm",
    "vgi.keywords": keywords_array(
        "wages, salaries, pay, occupations, employment, bls, oews, oes, soc, metro areas, cbsa, labor statistics"
    ),
    "vgi.support_contact": f"{SOURCE_URL}/issues",
    "vgi.support_policy_url": f"{SOURCE_URL}#support",
    "vgi.agent_test_tasks": json.dumps(AGENT_TEST_TASKS),
}
