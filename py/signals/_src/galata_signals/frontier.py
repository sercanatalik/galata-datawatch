"""How far the tape reaches: the newest receipt time a calculator can read.

The tape is the archive projected hourly (`project-the-recent-days`, at :40),
so at :15 it ends about 35 minutes back: **an hour that closed at :00 is not
yet whole on it**. A calculator that took the clock's hour computed that hour
from 40 of its 60 minutes, stored it, and never looked again. Found
2026-09-29: every `basis` hour since the deploy was absent ("marks cover 40
of 60 minutes"), `flow` was fitted on 40 minutes, and `liquidity` counted the
last quote state as standing through the missing 20.

So an hourly or daily calculator takes the last whole period **before the
tape's frontier** as well as before the clock: the :45 run, after the :40
projection, computes the hour that closed at :00, whole, once.
"""

from __future__ import annotations

import fcntl
import time
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

import polars as pl

#: galata-segments' hold file (`galata_segments::HOLD_FILE`): its writers take
#: it exclusive, readers shared, through `flock`.
HOLD_FILE = ".compact.lock"
#: How long to wait out a writer: a projection has taken 25 minutes under load,
#: and galata-watch waits 50.
PATIENCE_S = 50 * 60


class Held(Exception):
    """A writer held the tape for longer than the patience."""


@contextmanager
def held(tape: Path, patience_s: float = PATIENCE_S) -> Iterator[None]:
    """The tape held shared while a calculator reads it: a projection in progress is waited out.

    **Found 2026-09-29:** the :45 run read the tape while the :40 projection
    was rewriting it. Today's partition already reached :40 while yesterday's
    still ended at the previous :40, so the frontier said the hour was whole
    and the hour read 40 minutes. The rebuild holds the tape exclusive for its
    whole run; this is the shared side of the same lock.
    """
    path = tape / HOLD_FILE
    with open(path, "a+") as fh:
        deadline = time.monotonic() + patience_s
        while True:
            try:
                fcntl.flock(fh.fileno(), fcntl.LOCK_SH | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                if time.monotonic() > deadline:
                    raise Held(f"a writer held {path} for over {patience_s:.0f} s") from None
                time.sleep(1)
        try:
            yield
        finally:
            fcntl.flock(fh.fileno(), fcntl.LOCK_UN)


def frontier(tape: Path, kinds: tuple[str, ...]) -> int | None:
    """The newest `recv_micros` the tape holds, the least of it over `kinds`; None when any has nothing."""
    ends = []
    for kind in kinds:
        root = tape / f"kind={kind}"
        dates = sorted(p for p in root.glob("date=*") if p.is_dir()) if root.is_dir() else []
        files = [str(f) for d in dates[-2:] for f in d.glob("*.parquet") if not f.name.startswith(".")]
        if not files:
            return None
        end = pl.scan_parquet(files, hive_partitioning=False).select(pl.col("recv_micros").max()).collect().item()
        if end is None:
            return None
        ends.append(int(end))
    return min(ends)


class NotWhole(Exception):
    """An asof asked for that the tape does not hold whole, or that is not a period end."""


def period(run, width_us: int, end: int | None) -> int:
    """The run's own `asof` when it gave one, checked; else the latest whole period."""
    latest = whole(run.computed_micros, width_us, end)
    if run.asof is None:
        return latest
    if run.asof % width_us:
        raise NotWhole(f"asof {run.asof} is not the end of a {width_us // 60_000_000}-minute period")
    if run.asof > latest:
        raise NotWhole(f"asof {run.asof} is after the latest period the tape holds whole ({latest})")
    return run.asof


def whole(now_micros: int, width_us: int, end: int | None) -> int:
    """The latest period end at or before both the clock and the tape's frontier."""
    by_clock = now_micros // width_us * width_us
    if end is None:
        return by_clock
    return min(by_clock, end // width_us * width_us)
