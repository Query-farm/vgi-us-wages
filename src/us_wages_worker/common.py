"""Shared plumbing: ATTACH options, polite HTTP/file fetching and the on-disk cache.

Nothing here touches the network at import or ATTACH time. Downloads happen
only when a query needs data that is not cached yet.
"""

from __future__ import annotations

import contextlib
import hashlib
import json
import logging
import os
import re
import sys
import time
import urllib.error
import urllib.request
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import platformdirs

log = logging.getLogger(__name__)

DEFAULT_BASE_URL = "https://www.bls.gov"
DEFAULT_USER_AGENT = "vgi-us-wages/0.1 (+https://query.farm)"
TIMEOUT_SECONDS = 120.0
MAX_ATTEMPTS = 4
LATEST_TTL_SECONDS = 24 * 3600
LOCK_WAIT_SECONDS = 900.0

# State FIPS <-> USPS abbreviation for the states, DC and territories OEWS publishes.
STATE_FIPS_TO_ABBR = {
    "01": "AL", "02": "AK", "04": "AZ", "05": "AR", "06": "CA", "08": "CO", "09": "CT", "10": "DE",
    "11": "DC", "12": "FL", "13": "GA", "15": "HI", "16": "ID", "17": "IL", "18": "IN", "19": "IA",
    "20": "KS", "21": "KY", "22": "LA", "23": "ME", "24": "MD", "25": "MA", "26": "MI", "27": "MN",
    "28": "MS", "29": "MO", "30": "MT", "31": "NE", "32": "NV", "33": "NH", "34": "NJ", "35": "NM",
    "36": "NY", "37": "NC", "38": "ND", "39": "OH", "40": "OK", "41": "OR", "42": "PA", "44": "RI",
    "45": "SC", "46": "SD", "47": "TN", "48": "TX", "49": "UT", "50": "VT", "51": "VA", "53": "WA",
    "54": "WV", "55": "WI", "56": "WY", "60": "AS", "66": "GU", "69": "MP", "72": "PR", "78": "VI",
}  # fmt: skip
STATE_ABBR_TO_FIPS = {v: k for k, v in STATE_FIPS_TO_ABBR.items()}
NEW_ENGLAND = {"CT", "ME", "MA", "NH", "RI", "VT"}

AREA_TYPE_NAMES = {1: "national", 2: "state", 3: "territory", 4: "metro", 6: "nonmetro"}
AREA_TYPE_CODES = {v: k for k, v in AREA_TYPE_NAMES.items()}

RELEASE_PATTERN = r"^(latest|(19|20)\d\d)$"


def pylist(col: Any) -> list[Any]:
    """``col.to_pylist()`` typed as ``list[Any]`` (the pyarrow stubs say ``list[Any | None]``)."""
    return list(col.to_pylist())


class UpstreamError(RuntimeError):
    """An upstream download failed in a way a retry will not fix."""


@dataclass(frozen=True, slots=True)
class Options:
    """The ATTACH options, with defaults filled in."""

    cache_dir: Path
    base_url: str
    user_agent: str
    offline: bool

    @property
    def is_default_source(self) -> bool:
        """True when data comes from bls.gov itself."""
        return self.base_url.rstrip("/") == DEFAULT_BASE_URL

    @property
    def source_root(self) -> Path:
        """Cache directory for this ``base_url`` (mirrors never mix with bls.gov data)."""
        if self.is_default_source:
            return self.cache_dir / "bls"
        digest = hashlib.sha256(self.base_url.encode()).hexdigest()[:12]
        return self.cache_dir / f"mirror-{digest}"


def _value(raw: Any) -> Any:
    return raw.as_py() if hasattr(raw, "as_py") else raw


def options_from(attach_options: Mapping[str, Any]) -> Options:
    """Build `Options` from ``params.attach_options`` (lower-cased names, defaults absent)."""
    cache_dir = str(_value(attach_options.get("cache_dir")) or "").strip()
    base_url = str(_value(attach_options.get("base_url")) or "").strip() or DEFAULT_BASE_URL
    user_agent = str(_value(attach_options.get("user_agent")) or "").strip() or DEFAULT_USER_AGENT
    offline_raw = _value(attach_options.get("offline"))
    offline = offline_raw if isinstance(offline_raw, bool) else str(offline_raw).lower() in {"1", "true", "yes"}
    root = Path(cache_dir).expanduser() if cache_dir else Path(platformdirs.user_cache_dir("vgi-us-wages"))
    return Options(cache_dir=root.resolve(), base_url=base_url, user_agent=user_agent, offline=offline)


def _is_remote(base: str) -> bool:
    return base.startswith(("http://", "https://"))


def _local_path(base: str, rel: str) -> Path:
    root = base.removeprefix("file://")
    return Path(root).expanduser().resolve() / rel


def source_url(opts: Options, rel: str) -> str:
    """Return the URL (or local path) a relative upstream path resolves to."""
    if _is_remote(opts.base_url):
        return f"{opts.base_url.rstrip('/')}/{rel}"
    return str(_local_path(opts.base_url, rel))


@dataclass(frozen=True, slots=True)
class Fetched:
    """Metadata of a completed download."""

    url: str
    sha256: str
    size: int
    etag: str | None
    last_modified: str | None


def _http_open(opts: Options, url: str, method: str = "GET") -> Any:
    headers = {"User-Agent": opts.user_agent, "Accept": "*/*"}
    for attempt in range(1, MAX_ATTEMPTS + 1):
        req = urllib.request.Request(url, headers=headers, method=method)
        try:
            return urllib.request.urlopen(req, timeout=TIMEOUT_SECONDS)
        except urllib.error.HTTPError as err:
            if err.code == 403:
                raise UpstreamError(
                    f"{method} {url} failed with HTTP 403. bls.gov refuses some automated clients; it asks "
                    "them to send a User-Agent that identifies the requester with contact details. Set one "
                    "with the user_agent ATTACH option, e.g. ATTACH 'us_wages' (TYPE vgi, LOCATION '...', "
                    "user_agent 'my-team-wages/1.0 (ops@example.com)')."
                ) from err
            if err.code == 404:
                raise FileNotFoundError(f"{method} {url} returned HTTP 404") from err
            retryable = err.code == 429 or err.code >= 500
            if not retryable or attempt == MAX_ATTEMPTS:
                raise UpstreamError(f"{method} {url} failed with HTTP {err.code}") from err
            retry_after = err.headers.get("Retry-After")
            delay = float(retry_after) if retry_after and retry_after.isdigit() else 2.0 * 2**attempt
            time.sleep(min(delay, 60.0))
        except urllib.error.URLError as err:
            if attempt == MAX_ATTEMPTS:
                raise UpstreamError(f"{method} {url} failed: {err.reason}") from err
            time.sleep(2.0 * 2**attempt)
    raise AssertionError("unreachable")


def fetch_text(opts: Options, rel: str) -> str:
    """Fetch a small text resource (an HTML index page)."""
    if opts.offline:
        raise UpstreamError(f"offline mode: not fetching {source_url(opts, rel)}")
    if not _is_remote(opts.base_url):
        return _local_path(opts.base_url, rel).read_text(encoding="utf-8", errors="replace")
    with _http_open(opts, source_url(opts, rel)) as resp:
        return bytes(resp.read()).decode("utf-8", errors="replace")


def probe(opts: Options, rel: str) -> tuple[str | None, str | None]:
    """Return ``(etag, last_modified)`` of an upstream file without downloading it."""
    if not _is_remote(opts.base_url):
        st = _local_path(opts.base_url, rel).stat()
        return f'"{st.st_size:x}-{int(st.st_mtime):x}"', None
    with _http_open(opts, source_url(opts, rel), method="HEAD") as resp:
        return resp.headers.get("ETag"), resp.headers.get("Last-Modified")


def download(opts: Options, rel: str, target: Path) -> Fetched:
    """Stream an upstream file to ``target`` atomically, hashing it on the way."""
    if opts.offline:
        raise UpstreamError(f"offline mode: {source_url(opts, rel)} is not cached")
    url = source_url(opts, rel)
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_name(f"{target.name}.{os.getpid()}.part")
    for stale in target.parent.glob(f"{target.name}.*.part"):  # left by a killed process; callers hold file_lock
        stale.unlink(missing_ok=True)
    digest = hashlib.sha256()
    size = 0
    etag: str | None
    last_modified: str | None
    try:
        if _is_remote(opts.base_url):
            log.info("downloading %s", url)
            with _http_open(opts, url) as resp, open(tmp, "wb") as out:
                etag, last_modified = resp.headers.get("ETag"), resp.headers.get("Last-Modified")
                while chunk := resp.read(1 << 20):
                    digest.update(chunk)
                    out.write(chunk)
                    size += len(chunk)
        else:
            src = _local_path(opts.base_url, rel)
            if not src.exists():
                raise FileNotFoundError(f"{src} does not exist")
            etag, last_modified = probe(opts, rel)
            with open(src, "rb") as inp, open(tmp, "wb") as out:
                while chunk := inp.read(1 << 20):
                    digest.update(chunk)
                    out.write(chunk)
                    size += len(chunk)
        os.replace(tmp, target)
    finally:
        tmp.unlink(missing_ok=True)
    return Fetched(url=url, sha256=digest.hexdigest(), size=size, etag=etag, last_modified=last_modified)


@contextlib.contextmanager
def file_lock(path: Path) -> Iterator[None]:
    """Cross-process exclusive lock held by the operating system.

    The OS releases it when the holder exits, even if the process is killed, so an
    interrupted download never leaves the next query waiting on a stale lock file.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    deadline = time.monotonic() + LOCK_WAIT_SECONDS
    with open(path, "a+b") as handle:
        while True:
            try:
                _lock_nowait(handle.fileno())
                break
            except OSError:
                if time.monotonic() > deadline:
                    raise TimeoutError(f"timed out waiting for {path}") from None
                time.sleep(0.25)
        try:
            yield
        finally:
            _unlock(handle.fileno())


if sys.platform == "win32":
    import msvcrt

    def _lock_nowait(fd: int) -> None:
        os.lseek(fd, 0, os.SEEK_SET)
        msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)

    def _unlock(fd: int) -> None:
        os.lseek(fd, 0, os.SEEK_SET)
        msvcrt.locking(fd, msvcrt.LK_UNLCK, 1)

else:
    import fcntl

    def _lock_nowait(fd: int) -> None:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)

    def _unlock(fd: int) -> None:
        fcntl.flock(fd, fcntl.LOCK_UN)


def read_json(path: Path) -> dict[str, Any] | None:
    """Read a JSON sidecar, or None when it is missing or unreadable."""
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except OSError, ValueError:
        return None
    return data if isinstance(data, dict) else None


def write_json(path: Path, data: Mapping[str, Any]) -> None:
    """Write a JSON sidecar atomically."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f"{path.name}.{os.getpid()}.tmp")
    tmp.write_text(json.dumps(data, indent=2, sort_keys=True), encoding="utf-8")
    os.replace(tmp, path)


def parse_release(value: str | None) -> int | None:
    """``'latest'``/None -> None, ``'2024'`` -> 2024."""
    if value is None or str(value).strip().lower() in {"", "latest"}:
        return None
    text = str(value).strip()
    if not re.fullmatch(r"(19|20)\d\d", text):
        raise ValueError(f"release must be 'latest' or a four-digit May reference year such as '2024', got {value!r}")
    return int(text)
