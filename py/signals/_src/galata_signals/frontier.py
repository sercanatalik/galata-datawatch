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

from pathlib import Path

import polars as pl


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


def whole(now_micros: int, width_us: int, end: int | None) -> int:
    """The latest period end at or before both the clock and the tape's frontier."""
    by_clock = now_micros // width_us * width_us
    if end is None:
        return by_clock
    return min(by_clock, end // width_us * width_us)
