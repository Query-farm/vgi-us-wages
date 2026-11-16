"""The cache lock must not outlive a killed worker (it used to block the next query for 15 minutes)."""

from __future__ import annotations

import subprocess
import sys
import time
from pathlib import Path

from us_wages_worker.common import file_lock

_HOLD = """
import sys, time
from pathlib import Path
from us_wages_worker.common import file_lock
with file_lock(Path(sys.argv[1])):
    print("held", flush=True)
    time.sleep(600)
"""


def test_lock_released_when_holder_is_killed(tmp_path: Path) -> None:
    lock = tmp_path / "release" / ".lock"
    holder = subprocess.Popen([sys.executable, "-c", _HOLD, str(lock)], stdout=subprocess.PIPE, text=True)
    try:
        assert holder.stdout is not None
        assert holder.stdout.readline().strip() == "held"
    finally:
        holder.kill()
        holder.wait()
    started = time.monotonic()
    with file_lock(lock):
        pass
    assert time.monotonic() - started < 5


def test_leftover_lock_file_does_not_block(tmp_path: Path) -> None:
    lock = tmp_path / ".lock"
    lock.write_text("12345")  # what the old O_EXCL lock left behind after a crash
    started = time.monotonic()
    with file_lock(lock):
        pass
    assert time.monotonic() - started < 5
