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

import polars as pl

import galata_research as gr

WIDTH_US = {"1m": 60_000_000, "5m": 300_000_000, "30m": 1_800_000_000, "1h": 3_600_000_000, "4h": 14_400_000_000, "1d": 86_400_000_000, "1w": 604_800_000_000}
CAPTURED = ("1m", "1h", "4h", "1d")
EVER = ("2020-01-01T00:00Z", "2100-01-01T00:00Z")


def build(fine: pl.DataFrame, every: str) -> pl.DataFrame:
    """Whole `every` bars from `fine` bars of one width: `ticker, ts, close_ts, open, high, low, close, n`."""
    fine_width = int(fine.select((pl.col("close_ts") - pl.col("ts")).dt.total_microseconds().unique()).to_series().item())
    coarse = WIDTH_US[every]
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


def bars(horizon: str, source: str) -> pl.DataFrame:
    """The closed bars of width `horizon`, read as `source` from the tape."""
    if source not in CAPTURED:
        raise ValueError(f"bars={source!r} is not one the tape captures ({', '.join(CAPTURED)})")
    fine = gr.market.candles(None, source, *EVER).collect()
    if horizon == source:
        return fine
    return build(fine, horizon)
