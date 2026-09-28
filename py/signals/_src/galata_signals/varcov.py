"""`varcov`: Σ per declared horizon, fitted from the volatility model (galata-research D13).

For each horizon in `signals.toml`: the closed bars of that width, their log
returns, and `gr.models.corr.walk_forward` from the last close (split there,
one origin, h = 1; every run refits on everything, so a run is stateless and
deterministic — a fit takes about a second). Rows are the signals schema:
`covariance` for every pair i ≤ j, `correlation` for i < j.

**Only what is new.** A horizon whose last close is no later than the newest
`asof` already on the tape is skipped; the tape is the cursor, nothing is
remembered anywhere else.

**A refusal is a row.** Under `min_obs`, under the warm-up: every pair is
written `absent`, carrying the model's own words, at the horizon's asof.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

import polars as pl

import galata_research as gr
from galata_research import Refused

from .bars import WIDTH_US, bars

SIGNAL = "varcov"


@dataclass(frozen=True)
class Horizon:
    """One declared horizon."""

    name: str
    bars: str
    corr: str
    model: str = "garch"
    dist: str = "t"
    lam: float = 0.94
    deseasonalise: bool = False
    min_obs: int = 500

    @classmethod
    def declared(cls, name: str, table: dict) -> "Horizon":
        if name not in WIDTH_US:
            raise ValueError(f"varcov.{name}: not a width this calculator builds ({', '.join(WIDTH_US)})")
        known = {"bars", "corr", "model", "dist", "lam", "deseasonalise", "min_obs"}
        unknown = set(table) - known
        if unknown:
            raise ValueError(f"varcov.{name}: unknown key(s) {', '.join(sorted(unknown))}")
        if table.get("corr") not in ("dcc", "cdcc", "ewma"):
            raise ValueError(f"varcov.{name}: corr must be dcc, cdcc or ewma")
        return cls(name=name, **table)

    @property
    def fitted(self) -> bool:
        return self.corr != "ewma"

    @property
    def label(self) -> str:
        return "ewma" if not self.fitted else f"{self.model}-{self.dist}/{self.corr}"


@dataclass
class Run:
    """One run's identity and what it produced."""

    computed_micros: int
    code: str
    rows: list[dict] = field(default_factory=list)
    said: list[str] = field(default_factory=list)

    @property
    def run_id(self) -> str:
        return f"{SIGNAL}-{self.computed_micros}"


def micros(ts: datetime) -> int:
    return int(ts.timestamp() * 1_000_000)


def stored_asof(tape: Path) -> dict[str, int]:
    """The newest asof per horizon already on the tape for this signal."""
    root = tape / "kind=signals"
    if not any(root.rglob("*.parquet")) if root.is_dir() else True:
        return {}
    frame = (
        pl.scan_parquet(str(root / "**" / "*.parquet"), hive_partitioning=False)
        .filter(pl.col("signal") == SIGNAL)
        .group_by("horizon")
        .agg(pl.col("asof_micros").max())
        .collect()
    )
    return dict(zip(frame["horizon"].to_list(), frame["asof_micros"].to_list()))


def compute(horizons: list[Horizon], tape: Path, run: Run) -> Run:
    """Every horizon with a new close, appended to `run.rows`."""
    stored = stored_asof(tape)
    for hz in horizons:
        returns = gr.timeseries.returns(bars(hz.name, hz.bars), kind="log")
        try:
            _, joint, _ = gr.models.corr.joint(returns)
        except Refused as refusal:
            run.said.append(f"{hz.name}: skipped, {refusal}")
            continue
        if joint.height == 0:
            run.said.append(f"{hz.name}: skipped, no joint returns")
            continue
        last = joint["close_ts"][-1]
        asof = micros(last)
        if hz.name in stored and asof <= stored[hz.name]:
            run.said.append(f"{hz.name}: nothing new since {last:%Y-%m-%d %H:%M}")
            continue
        tickers = sorted(returns["ticker"].unique().to_list())
        try:
            walked = walk(hz, returns, last)
        except Refused as refusal:
            run.rows.extend(absent(hz, tickers, asof, str(refusal), run))
            run.said.append(f"{hz.name}: absent, {refusal}")
            continue
        run.rows.extend(present(hz, walked, run))
        run.said.append(f"{hz.name}: {len(tickers)} instruments at {last:%Y-%m-%d %H:%M}")
    return run


def walk(hz: Horizon, returns: pl.DataFrame, last) -> pl.DataFrame:
    """One origin, at the last close, h = 1."""
    corr = gr.models.corr
    # A null return (a bar after a hole) is dropped rather than carried: the
    # joint sample reads a missing row exactly as a dropped bar, and marks the
    # next `after_gap`, so nothing is lost. Kept, it would ask for a seasonal
    # factor in a cell no return occupies: GOLD's 22:00 UTC at 5m, measured
    # 2026-09-28 (its market is shut then, so every such return is null).
    returns = returns.drop_nulls("return")
    if not hz.fitted:
        return corr.walk_forward(returns, corr="ewma", lam=hz.lam, split=last, horizons=[1])
    factors = None
    if hz.deseasonalise:
        factors = gr.timeseries.seasonal_factors(returns, fit=(returns["ts"].min(), last), by="hour_of_day")
    return corr.walk_forward(
        returns, model=hz.model, dist=hz.dist, corr=hz.corr, split=last, every=1_000_000_000, horizons=[1], factors=factors, min_obs=hz.min_obs
    )


def _base(hz: Horizon, run: Run, asof: int, params: dict) -> dict:
    return {
        "signal": SIGNAL,
        "horizon": hz.name,
        "h": 1,
        "asof_micros": asof,
        "computed_micros": run.computed_micros,
        "model": hz.label,
        "params": json.dumps(params, sort_keys=True),
        "fitted": hz.fitted,
        "code": run.code,
        "run_id": run.run_id,
    }


def _params(hz: Horizon, a=None, b=None) -> dict:
    p = {"lam": hz.lam} if not hz.fitted else {"a": a, "b": b, "dist": hz.dist, "deseasonalise": hz.deseasonalise}
    return p


def present(hz: Horizon, walked: pl.DataFrame, run: Run) -> list[dict]:
    rows = []
    for r in walked.iter_rows(named=True):
        base = _base(hz, run, micros(r["close_ts"]), _params(hz, r["a"], r["b"]))
        common = {
            **base,
            "target_micros": micros(r["target_ts"]),
            "n_eff": r["n_eff"],
            "fitted_through_micros": micros(r["fitted_through"]) if hz.fitted else None,
            "fit_from_micros": micros(r["fit_from"]) if hz.fitted else None,
            "after_gap": bool(r["after_gap"]),
            "ticker_i": r["ticker_i"],
            "ticker_j": r["ticker_j"],
            "absent": None,
        }
        rows.append({**common, "measure": "covariance", "value": r["covariance"]})
        if r["ticker_i"] != r["ticker_j"]:
            rows.append({**common, "measure": "correlation", "value": r["correlation"]})
    return rows


def absent(hz: Horizon, tickers: list[str], asof: int, reason: str, run: Run) -> list[dict]:
    rows = []
    base = _base(hz, run, asof, _params(hz))
    for i, a in enumerate(tickers):
        for b in tickers[i:]:
            common = {**base, "target_micros": asof, "n_eff": None, "fitted_through_micros": None, "fit_from_micros": None, "after_gap": False, "ticker_i": a, "ticker_j": b, "value": None, "absent": reason}
            rows.append({**common, "measure": "covariance"})
            if a != b:
                rows.append({**common, "measure": "correlation"})
    return rows
