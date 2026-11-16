"""Shared fixtures: a worker client and offline options pointing at the BLS fixtures."""

from __future__ import annotations

import sys
from collections.abc import Iterator
from pathlib import Path

import pytest
from vgi.client import Client

from us_wages_worker.common import Options, options_from

ROOT = Path(__file__).resolve().parent.parent
FIXTURES = ROOT / "test" / "fixtures" / "bls"


@pytest.fixture(scope="session")
def client() -> Iterator[Client]:
    """A client that launches the worker as a subprocess, the way DuckDB does."""
    with Client([sys.executable, "-m", "us_wages_worker.worker"]) as c:
        yield c


@pytest.fixture
def opts(tmp_path: Path) -> Options:
    """Options reading the bls.gov-shaped fixture tree into a fresh cache."""
    return options_from({"cache_dir": str(tmp_path / "cache"), "base_url": str(FIXTURES)})
