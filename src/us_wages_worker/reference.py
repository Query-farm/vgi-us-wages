"""Bundled reference tables (SOC 2018, O*NET 31.0, Census 2020 ZCTA crosswalk) and search."""

from __future__ import annotations

import functools
import re
from dataclasses import dataclass
from importlib import resources
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from .common import pylist

BUNDLE_VINTAGES = {
    "soc_2018": "SOC 2018 (BLS, structure final November 2017, definitions updated 2020-04-15)",
    "onet": "O*NET 31.0 database (CC BY 4.0)",
    "zcta_county": "Census 2020 ZCTA-to-county relationship file; Connecticut via 2022 planning regions",
}


@functools.cache
def bundled(name: str) -> pa.Table:
    """Read one bundled Parquet table (cached for the life of the process)."""
    with resources.files("us_wages_worker").joinpath("data", f"{name}.parquet").open("rb") as f:
        return pq.read_table(f)


@functools.cache
def onet_codes_by_soc() -> dict[str, list[str]]:
    """SOC detailed code -> the O*NET-SOC codes under it."""
    out: dict[str, list[str]] = {}
    t = bundled("onet_occupations")
    for code, soc in zip(pylist(t["onet_soc_code"]), pylist(t["soc_code"]), strict=True):
        out.setdefault(soc, []).append(code)
    return out


# ------------------------------------------------------------------------ search

_WORD = re.compile(r"[a-z0-9]+")


def _tokens(text: str) -> list[str]:
    toks = _WORD.findall(text.lower())
    return [t[:-1] if len(t) > 3 and t.endswith("s") and not t.endswith("ss") else t for t in toks]


@dataclass(frozen=True, slots=True)
class Candidate:
    """One searchable title."""

    soc_code: str
    title: str
    source: str
    onet_soc_code: str | None
    norm: str
    tokens: tuple[str, ...]


SOURCE_WEIGHT = {"soc_title": 1.0, "onet_title": 0.97, "onet_job_title": 0.93}


@functools.cache
def _candidates() -> list[Candidate]:
    out: list[Candidate] = []

    def add(soc: str, title: str, source: str, onet: str | None) -> None:
        toks = tuple(_tokens(title))
        out.append(Candidate(soc, title, source, onet, " ".join(toks), toks))

    soc = bundled("soc_2018")
    for code, title in zip(pylist(soc["soc_code"]), pylist(soc["soc_title"]), strict=True):
        add(code, title, "soc_title", None)
    occ = bundled("onet_occupations")
    for code, s, title in zip(pylist(occ["onet_soc_code"]), pylist(occ["soc_code"]), pylist(occ["title"]), strict=True):
        add(s, title, "onet_title", code)
    jobs = bundled("onet_job_titles")
    for code, title in zip(pylist(jobs["onet_soc_code"]), pylist(jobs["job_title"]), strict=True):
        add(code[:7], title, "onet_job_title", code)
    return out


def _score(query_norm: str, qtoks: list[str], c: Candidate) -> float:
    if not qtoks:
        return 0.0
    if c.norm == query_norm:
        base = 1.0
    elif c.norm.startswith(query_norm + " ") or c.norm.startswith(query_norm):
        base = 0.9
    elif f" {query_norm}" in f" {c.norm}":
        base = 0.8
    elif all(any(t.startswith(q) for t in c.tokens) for q in qtoks):
        base = 0.65
    else:
        return 0.0
    # Prefer titles that are mostly the query (fewer extra words).
    extra = max(len(c.tokens) - len(qtoks), 0)
    return round(base * SOURCE_WEIGHT[c.source] * (1.0 / (1.0 + 0.05 * extra)), 4)


def search(text: str, limit: int) -> list[dict[str, Any]]:
    """Best match per SOC code for ``text`` over SOC, O*NET and O*NET job titles."""
    qtoks = _tokens(text)
    query_norm = " ".join(qtoks)
    best: dict[str, tuple[float, Candidate]] = {}
    hits: dict[str, int] = {}
    for c in _candidates():
        s = _score(query_norm, qtoks, c)
        if s <= 0:
            continue
        hits[c.soc_code] = hits.get(c.soc_code, 0) + 1
        cur = best.get(c.soc_code)
        if cur is None or s > cur[0] or (s == cur[0] and len(c.title) < len(cur[1].title)):
            best[c.soc_code] = (s, c)
    soc = bundled("soc_2018")
    info = {
        code: (title, level)
        for code, title, level in zip(
            pylist(soc["soc_code"]), pylist(soc["soc_title"]), pylist(soc["soc_level"]), strict=True
        )
    }
    level_order = {"detailed": 0, "broad": 1, "minor": 2, "major": 3}
    # Many matching alternate titles mean the occupation is a central home for the query.
    scored = [(min(1.0, round(s + min(0.05, 0.005 * (hits[c.soc_code] - 1)), 4)), c) for s, c in best.values()]
    ranked = sorted(
        scored,
        key=lambda sc: (
            -sc[0],
            level_order.get(info.get(sc[1].soc_code, ("", "detailed"))[1] or "", 0),
            sc[1].soc_code,
        ),
    )[:limit]
    rows = []
    for rank, (score, c) in enumerate(ranked, start=1):
        title, level = info.get(c.soc_code, (None, None))
        rows.append(
            {
                "rank": rank,
                "soc_code": c.soc_code,
                "soc_title": title,
                "soc_level": level,
                "matched_title": c.title,
                "match_source": c.source,
                "onet_soc_code": c.onet_soc_code,
                "score": score,
            }
        )
    return rows
