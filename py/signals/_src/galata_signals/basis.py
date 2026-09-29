"""`basis`: where each perp trades against its oracle, hour by hour (Tier 16).

From the tape's marks (`activeAssetCtx`, about one a second) for the hour that
closed, per instrument:

- the venue's **premium**, time-weighted: each sample weighs as long as it
  stood, winsorised at the hour's 1st and 99th percentiles, the way the venue
  averages its own 5 s samples into the hour's funding. On the main dex it is
  the impact-price premium *before* the funding clamp, so it moves where
  funding cannot (Hyperliquid docs, Funding). The time-weighted median beside
  it: a mean far from the median is a spike or a thin book;
- **mark against oracle**, time-weighted, in bps: the mark is a median of the
  venue's book, its EMA and external perps (docs, Robust price indices);
- **open interest**: its log change over the hour and its dollar size at the
  hour's end, since a premium that widens while open interest grows is
  crowding (Schmeling, Schrimpf and Todorov, Crypto Carry);
- a robust z of the premium against this signal's own stored 30 days (median
  and MAD), on the main dex only. On the xyz dex outside its underlying's
  hours the oracle chases the venue's own book (trade.xyz, oracle price), so
  its premium is pulled to zero by construction and has no history to be
  unusual against; the z is absent there, saying so.

An hour with under 48 minutes covered by samples has no figure.
"""

from __future__ import annotations

import json
import math
from datetime import UTC, datetime
from pathlib import Path

import polars as pl

from .carry import Declared
from .frontier import frontier, period
from .varcov import Run, stored_asof

SIGNAL = "basis"
HORIZON = "1h"
HOUR_US = 3_600_000_000
MIN_COVERED_US = 48 * 60_000_000
Z_DAYS = 30
Z_MIN = 72
MAD_SCALE = 1.4826
#: A sample stands until the next, but no longer than this: a capture outage
#: is not a price that stood.
MAX_STAND_US = 30_000_000


def _dt(us: int) -> datetime:
    return datetime.fromtimestamp(us / 1_000_000, tz=UTC)


# The record, behind names a test can replace.
def marks(tape: Path, lo: int, hi: int) -> pl.DataFrame:
    """`ticker, t, mark, oracle, premium, open_interest` with receipt time in [lo, hi), as floats."""
    root = tape / "kind=marks"
    empty = pl.DataFrame(schema={"ticker": pl.String, "t": pl.Int64, "mark": pl.Float64, "oracle": pl.Float64, "premium": pl.Float64, "open_interest": pl.Float64})
    if not root.is_dir() or not any(root.rglob("*.parquet")):
        return empty
    return (
        pl.scan_parquet(str(root / "**" / "*.parquet"), hive_partitioning=False)
        .filter((pl.col("recv_micros") >= lo) & (pl.col("recv_micros") < hi))
        .select(
            "ticker",
            pl.col("recv_micros").alias("t"),
            *[pl.col(c).cast(pl.Float64) for c in ("mark", "oracle", "premium", "open_interest")],
        )
        .collect()
    )


def history(tape: Path, lo: int, hi: int) -> pl.DataFrame:
    """This signal's stored `premium_twa_bps` with asof in [lo, hi): its own history."""
    root = tape / "kind=signals"
    if not root.is_dir() or not any(root.rglob("*.parquet")):
        return pl.DataFrame(schema={"ticker_i": pl.String, "asof_micros": pl.Int64, "value": pl.Float64})
    return (
        pl.scan_parquet(str(root / "**" / "*.parquet"), hive_partitioning=False)
        .filter((pl.col("signal") == SIGNAL) & (pl.col("measure") == "premium_twa_bps") & (pl.col("asof_micros") >= lo) & (pl.col("asof_micros") < hi))
        .select("ticker_i", "asof_micros", "value")
        .collect()
    )


def stood(frame: pl.DataFrame, lo: int, hi: int) -> pl.DataFrame:
    """Each sample with the time it stood within [lo, hi): to the next sample, capped, one before `lo` counting from `lo`."""
    f = frame.sort("t").with_columns(pl.col("t").shift(-1).fill_null(hi).alias("_next"))
    f = f.with_columns(pl.min_horizontal(pl.col("_next"), pl.col("t") + MAX_STAND_US).alias("_until"))
    f = f.with_columns(pl.col("t").clip(lo, hi).alias("_from"), pl.col("_until").clip(lo, hi).alias("_to"))
    return f.with_columns((pl.col("_to") - pl.col("_from")).alias("w")).filter(pl.col("w") > 0)


def weighted(values: pl.Series, weights: pl.Series, q: float | None = None) -> float:
    """The weighted mean, or the value at cumulative weight share `q`."""
    if q is None:
        return float((values * weights).sum() / weights.sum())
    order = values.arg_sort()
    v, w = values.gather(order), weights.gather(order)
    cum = w.cum_sum() / w.sum()
    return float(v.filter(cum >= q)[0])


def compute(declared: Declared, tape: Path, run: Run) -> Run:
    # The last whole period on the tape as well as by the clock (frontier.py).
    asof = period(run, HOUR_US, frontier(tape, ("marks",)))
    stored = stored_asof(tape, SIGNAL).get(HORIZON)
    if not run.redo and stored is not None and asof <= stored:
        run.said.append(f"basis: nothing new since {_dt(stored):%Y-%m-%d %H:%M}")
        return run
    lo = asof - HOUR_US
    # The sample standing when the hour opened came before it: read back as far as one may stand.
    m = marks(tape, lo - MAX_STAND_US, asof)
    past = history(tape, asof - Z_DAYS * 24 * HOUR_US, asof)
    tickers = sorted(set(m["ticker"].to_list()))
    for t in tickers:
        run.rows.extend(_rows(declared.dex(t), t, asof, m.filter(pl.col("ticker") == t), past.filter(pl.col("ticker_i") == t), run))
    run.said.append(f"basis: {len(tickers)} instruments for the hour to {_dt(asof):%Y-%m-%d %H:%M}")
    return run


def _rows(dex: str, ticker: str, asof: int, m: pl.DataFrame, past: pl.DataFrame, run: Run) -> list[dict]:
    lo = asof - HOUR_US
    base = {
        "signal": SIGNAL, "horizon": HORIZON, "ticker_i": ticker, "ticker_j": None, "h": 1, "asof_micros": asof,
        "target_micros": asof, "computed_micros": run.computed_micros, "fitted_through_micros": None, "fit_from_micros": None,
        "model": "time-weighted-marks",
        "params": json.dumps({"dex": dex, "min_covered_minutes": 48, "winsorise": [0.01, 0.99], "z_days": Z_DAYS, "z_min": Z_MIN, "max_stand_s": MAX_STAND_US // 1_000_000}, sort_keys=True),
        "fitted": False, "after_gap": False, "code": run.code, "run_id": run.run_id,
    }  # fmt: skip
    rows = []

    def put(measure, value, reason=None, n=None):
        ok = value is not None and math.isfinite(value)
        rows.append({**base, "measure": measure, "value": float(value) if ok else None, "absent": None if ok else (reason or "not defined"), "n_eff": n})

    s = stood(m.drop_nulls(["mark", "oracle", "premium"]).filter(pl.col("oracle") > 0), lo, asof) if m.height else None
    covered = int(s["w"].sum()) if s is not None and s.height else 0
    twa = None
    if covered < MIN_COVERED_US:
        why = f"marks cover {covered // 60_000_000} of 60 minutes, under 48"
        for measure in ("premium_twa_bps", "premium_median_bps", "mark_oracle_bps", "open_interest_log_change", "open_interest_usd"):
            put(measure, None, why)
    else:
        p = s["premium"] * 1e4
        lo_q, hi_q = p.quantile(0.01), p.quantile(0.99)
        twa = weighted(p.clip(lo_q, hi_q), s["w"])
        put("premium_twa_bps", twa, n=s.height)
        put("premium_median_bps", weighted(p, s["w"], 0.5), n=s.height)
        put("mark_oracle_bps", weighted((s["mark"] - s["oracle"]) / s["oracle"] * 1e4, s["w"]), n=s.height)
        oi = s.drop_nulls("open_interest").filter(pl.col("open_interest") > 0)
        if oi.height >= 2:
            put("open_interest_log_change", math.log(oi["open_interest"][-1] / oi["open_interest"][0]), n=oi.height)
            put("open_interest_usd", oi["open_interest"][-1] * oi["mark"][-1], n=oi.height)
        else:
            put("open_interest_log_change", None, "under two open-interest samples in the hour")
            put("open_interest_usd", None, "under two open-interest samples in the hour")

    values = [v for v in past["value"].to_list() if v is not None]
    if dex != "main":
        put("premium_z_30d", None, f"the {dex} dex's oracle follows its own book outside the underlying's hours: its premium has no history to be unusual against")
    elif twa is None:
        put("premium_z_30d", None, "no premium this hour")
    elif len(values) < Z_MIN:
        put("premium_z_30d", None, f"{len(values)} stored hours in the {Z_DAYS} days before, under {Z_MIN}")
    else:
        ordered = sorted(values)
        med = _median(ordered)
        mad = _median(sorted(abs(v - med) for v in values))
        put("premium_z_30d", (twa - med) / (MAD_SCALE * mad) if mad > 0 else None, "the stored premiums do not vary", len(values))
    return rows


def _median(ordered: list[float]) -> float:
    n = len(ordered)
    return ordered[n // 2] if n % 2 else (ordered[n // 2 - 1] + ordered[n // 2]) / 2
