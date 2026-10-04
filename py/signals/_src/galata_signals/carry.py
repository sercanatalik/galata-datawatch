"""`carry`: what holding each perpetual earns or pays in funding, hourly (Tier 16).

Carry is the return an asset earns if its price stays where it is, known in
advance (Koijen, Moskowitz, Pedersen and Vrugt 2018); on a perpetual it is the
funding. A high carry predicts crashes (Schmeling, Schrimpf and Todorov, *Crypto
carry*, BIS WP 1087), and a smoothed carry predicts better than the last print
(KMPV's carry1-12), so the trailing means are the figures.

**Settled for carry, live only as a nowcast.** Hyperliquid settles funding
every hour at F/8 (F the 8-hour rate; its docs, *Funding*), and `fundingHistory`
states it. The live rate pushed with each asset context is the venue's
projection for the hour in progress, revised until it settles: `nowcast_apr`,
never averaged into carry.

**Coverage, not interpolation.** Settled funding reaches the tape through the
walk; a trailing mean over the hours that happened to arrive is not a carry.
Under 90% coverage a figure is absent, naming the coverage and the last settled
hour, so a stopped walk reads as one.

Rates are annualised simply, × 8,760 hours.
"""

from __future__ import annotations

import json
import math
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import polars as pl

import galata_research as gr

from .frontier import frontier, period
from .split import by
from .varcov import Run, micros, stored_asof

SIGNAL = "carry"
HORIZON = "1h"
HOUR_US = 3_600_000_000
DAY_US = 24 * HOUR_US
HOURS_PER_YEAR = 8_760
COVERAGE = 0.9
# The live rate is on the tape only as recently as the hourly projection (at
# :40) has put it there: at the :15 run the newest is ~35 minutes old. Found
# 2026-09-28 by the first run on the record, whose 5-minute window was empty for
# all six. So the nowcast is the last rate received in the hour before the
# asof, and its receipt time is stated in its params.
NOWCAST_WINDOW_US = HOUR_US
AT_BASELINE = 1e-12
WINDOWS = {"24h": 24, "7d": 7 * 24, "30d": 30 * 24}


@dataclass(frozen=True)
class Declared:
    """`[carry]` in signals.toml: each dex's interest-only baseline (hourly), and which instruments are on which dex."""

    baselines: dict
    dex_of: dict

    @classmethod
    def declared(cls, table: dict) -> "Declared":
        baselines = table.get("baseline", {})
        if "main" not in baselines:
            raise ValueError("carry.baseline must declare main")
        dex_of = {t: dex for dex, tickers in table.get("dex", {}).items() for t in tickers}
        unknown = set(dex_of.values()) - set(baselines)
        if unknown:
            raise ValueError(f"carry.dex names {', '.join(sorted(unknown))} without a baseline")
        return cls(baselines=baselines, dex_of=dex_of)

    def dex(self, ticker: str) -> str:
        return self.dex_of.get(ticker, "main")


# The record, behind names a test can replace.
def settled(lo: int, hi: int) -> pl.DataFrame:
    return gr.clocks.funding(None, _iso(lo), _iso(hi)).collect()


def live(lo: int, hi: int) -> pl.DataFrame:
    return gr.clocks.funding_live(None, _iso(lo), _iso(hi)).collect()


def hourly_closes(lo: int, hi: int) -> pl.DataFrame:
    return gr.market.candles(None, "1h", _iso(lo), _iso(hi)).collect()


def _iso(us: int) -> str:
    return datetime.fromtimestamp(us / 1_000_000, tz=UTC).isoformat()


def compute(declared: Declared, tape: Path, run: Run) -> Run:
    """The hour before the run's clock, if no carry for it is stored yet."""
    # The last whole period on the tape as well as by the clock (frontier.py).
    asof = period(run, HOUR_US, frontier(tape, ("marks", "funding")))
    stored = stored_asof(tape, SIGNAL).get(HORIZON)
    if not run.redo and stored is not None and asof <= stored:
        run.said.append(f"carry: nothing new since {_iso(stored)}")
        return run
    lo = asof - (90 + 7 + 1) * DAY_US
    rates = settled(lo, asof + 1).with_columns(
        (pl.col("ts").dt.epoch("us") // HOUR_US * HOUR_US).alias("hour"),
    ).filter(pl.col("hour") <= asof).unique(["ticker", "hour"], keep="first")
    now = live(asof - NOWCAST_WINDOW_US, asof + 1)
    bars = hourly_closes(asof - 31 * DAY_US, asof)
    tickers = sorted(set(rates["ticker"].to_list()) | set(now["ticker"].to_list()))
    rates_of, now_of, bars_of = by(rates), by(now), by(bars)
    for t in tickers:
        run.rows.extend(_rows(declared, t, asof, rates_of(t), now_of(t), bars_of(t), run))
    run.said.append(f"carry: {len(tickers)} instruments at {_iso(asof)}")
    return run


def _trailing(rates: pl.DataFrame, asof: int, hours: int) -> tuple[pl.DataFrame, str | None]:
    """The settled hours in (asof − hours, asof], or why they are too few to average."""
    window = rates.filter((pl.col("hour") > asof - hours * HOUR_US) & (pl.col("hour") <= asof))
    if window.height < COVERAGE * hours:
        last = rates["hour"].max()
        said = "none on the tape" if last is None else _iso(last)[:16]
        return window, f"settled funding covers {window.height} of {hours} hours; the last settled hour is {said}"
    return window, None


def _rows(declared: Declared, ticker: str, asof: int, rates: pl.DataFrame, now: pl.DataFrame, bars: pl.DataFrame, run: Run) -> list[dict]:
    dex = declared.dex(ticker)
    baseline = declared.baselines[dex]
    base = {
        "signal": SIGNAL, "horizon": HORIZON, "ticker_i": ticker, "ticker_j": None, "h": 1, "asof_micros": asof,
        "target_micros": asof, "computed_micros": run.computed_micros, "fitted_through_micros": None, "fit_from_micros": None,
        "model": "settled-funding", "params": json.dumps({"dex": dex, "baseline_hourly": baseline, "annualise": HOURS_PER_YEAR}, sort_keys=True),
        "fitted": False, "after_gap": False, "code": run.code, "run_id": run.run_id,
    }  # fmt: skip
    rows = []

    def put(measure: str, value, reason: str | None, n=None):
        ok = value is not None and math.isfinite(value)
        rows.append({**base, "measure": measure, "value": float(value) if ok else None, "absent": None if ok else (reason or "not defined"), "n_eff": n})

    latest = now.sort("recv_ts").tail(1)
    if latest.height:
        received = micros(latest["recv_ts"][0])
        rows.append({**base, "params": json.dumps({**json.loads(base["params"]), "received_micros": received}, sort_keys=True),
                     "measure": "nowcast_apr", "value": float(latest["rate"][0]) * HOURS_PER_YEAR, "absent": None, "n_eff": None})  # fmt: skip
    else:
        put("nowcast_apr", None, "no live rate received in the hour before the asof")

    means = {}
    for name, hours in WINDOWS.items():
        window, why = _trailing(rates, asof, hours)
        means[name] = None if why else float(window["rate"].mean())
        put(f"carry_apr_{name}", None if why else means[name] * HOURS_PER_YEAR, why, window.height)

    week, why_week = _trailing(rates, asof, WINDOWS["7d"])
    put("excess_apr_7d", None if why_week else (means["7d"] - baseline) * HOURS_PER_YEAR, why_week, week.height)
    put("positive_share_7d", None if why_week else float((week["rate"] > 0).mean()), why_week, week.height)
    put("baseline_share_7d", None if why_week else float(((week["rate"] - baseline).abs() < AT_BASELINE).mean()), why_week, week.height)

    # The 7-day mean against the 90 daily means before the week.
    start = asof - WINDOWS["7d"] * HOUR_US
    days = (
        rates.filter((pl.col("hour") > start - 90 * DAY_US) & (pl.col("hour") <= start))
        .group_by(((pl.col("hour") - 1) // DAY_US).alias("day"))
        .agg(pl.col("rate").mean(), pl.len().alias("n"))
        .filter(pl.col("n") >= COVERAGE * 24)
    )
    if why_week:
        put("zscore_7d", None, why_week)
    elif days.height < 60:
        put("zscore_7d", None, f"{days.height} covered days in the 90 before the week, under 60")
    else:
        mu, sd = float(days["rate"].mean()), float(days["rate"].std())
        # Identical daily means carry a float-noise deviation (about 1e-21 on
        # rates of 1e-5, measured in the test), which would make a z-score of
        # noise; under 1e-9 of the mean's size it is no deviation at all.
        flat = sd <= 1e-9 * max(abs(mu), 1e-12)
        put("zscore_7d", None if flat else (means["7d"] - mu) / sd, "the 90 daily means do not vary", days.height)

    returns = gr.timeseries.returns(bars, kind="log").drop_nulls("return") if bars.height else bars
    if why_week:
        put("carry_to_vol_7d", None, why_week)
    elif returns.height < 20 * 24:
        put("carry_to_vol_7d", None, f"{returns.height} hourly returns in 30 days, under {20 * 24}")
    else:
        sigma = float(returns["return"].std()) * math.sqrt(HOURS_PER_YEAR)
        put("carry_to_vol_7d", means["7d"] * HOURS_PER_YEAR / sigma if sigma > 0 else None, "the hourly returns do not vary", returns.height)
    return rows
