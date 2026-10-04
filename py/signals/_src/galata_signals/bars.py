"""Bars of every declared width, from what the tape captured.

The tape holds 1m, 1h, 4h and 1d candles (`gr.market.INTERVALS`). 5m and 30m
are built from 1m, and 1w from 1d, on the UTC grid (a week from Monday), with
the rule of galata-research's `timeseries.resample` (in progress in item 24,
not yet committed, so not yet pinnable): a bucket is kept only when it holds
every fine bar it should — a day without one of its minutes is not a day —
with the first open, the extreme high and low, and the last close. Move to
the library call once it is released.
"""

from __future__ import annotations

from datetime import UTC, datetime

import polars as pl

import galata_research as gr

WIDTH_US = {"1m": 60_000_000, "5m": 300_000_000, "30m": 1_800_000_000, "1h": 3_600_000_000, "4h": 14_400_000_000, "1d": 86_400_000_000, "1w": 604_800_000_000}
CAPTURED = ("1m", "1h", "4h", "1d")
EVER = ("2020-01-01T00:00Z", "2100-01-01T00:00Z")


def build(fine: pl.DataFrame, every: str) -> pl.DataFrame:
    """Whole `every` bars from `fine` bars of one width: `ticker, ts, close_ts, open, high, low, close, n`."""
    coarse = WIDTH_US[every]
    if fine.height == 0:
        # A fresh tape: no bars, which each calculator reports as "nothing" —
        # not a width that cannot be read off an empty frame, which ended the
        # run as broken before any of them could.
        return fine.select("ticker", "ts", "close_ts", "open", "high", "low", "close").with_columns(pl.lit(None, dtype=pl.Int64).alias("n"))
    widths = fine.select((pl.col("close_ts") - pl.col("ts")).dt.total_microseconds().unique()).to_series().to_list()
    if len(widths) != 1:
        raise ValueError(f"bars of {len(widths)} widths ({sorted(widths)} µs) cannot be built into {every}")
    fine_width = int(widths[0])
    if fine_width >= coarse or coarse % fine_width:
        raise ValueError(f"{every} is not a coarser whole multiple of {fine_width} µs bars")
    expected = coarse // fine_width
    bucket = pl.col("ts").dt.truncate("1w" if every == "1w" else every)
    return (
        fine.sort("ticker", "ts")
        .group_by("ticker", bucket.alias("_bucket"))
        .agg(pl.col("open").first(), pl.col("high").max(), pl.col("low").min(), pl.col("close").last(), pl.len().cast(pl.Int64).alias("n"))
        .filter(pl.col("n") == expected)
        .select("ticker", pl.col("_bucket").alias("ts"), (pl.col("_bucket") + pl.duration(microseconds=coarse)).alias("close_ts"), "open", "high", "low", "close", "n")
        .sort("ticker", "ts")
    )


def since(us: int) -> str:
    """A tape read's lower bound, as `gr.market` takes it, from epoch microseconds (UTC)."""
    return datetime.fromtimestamp(us / 1_000_000, tz=UTC).strftime("%Y-%m-%dT%H:%MZ")


def bars(horizon: str, source: str, start: str = EVER[0]) -> pl.DataFrame:
    """The closed bars of width `horizon`, read as `source` from the tape, from `start` on.

    **Bounded where the caller can say how far back it looks.** Reading every
    candle since 2020 to keep a week of them was the dominant cost of the
    calculators that need a window, and it grew with every day captured. A
    `start` must fall on a `horizon` boundary, or the first bucket is partial
    and dropped — which only loses a bar the caller did not ask for.
    """
    if source not in CAPTURED:
        raise ValueError(f"bars={source!r} is not one the tape captures ({', '.join(CAPTURED)})")
    fine = gr.market.candles(None, source, start, EVER[1]).collect()
    if horizon == source:
        return fine
    return build(fine, horizon)
