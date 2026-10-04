"""`varcov`: Σ per declared horizon, fitted from the volatility model (galata-research D13).

For each horizon in `signals.toml`: the closed bars of that width, their log
returns, and `gr.models.corr.walk_forward` from the last close (split there,
one origin, h = 1; every run refits on the window `lookback_us` names — 300
days, or a year where the tail needs it — so a run is stateless and
deterministic and its sample rolls with the market). Rows are the signals schema:
`covariance` for every pair i ≤ j, `correlation` for i < j, and on a fitted
horizon `correlation_target` for i < j: R̄, the correlation the fit reverts
to, which `galata-watch` reconciles against `derive`'s equal-weight ρ.

**Only what is new.** A horizon whose last close is no later than the newest
`asof` already on the tape is skipped; the tape is the cursor, nothing is
remembered anywhere else.

**A refusal is a row.** Under `min_obs`, under the warm-up: every pair is
written `absent`, carrying the model's own words, at the horizon's asof.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path

import numpy as np
import polars as pl

import galata_research as gr
from galata_research import Refused

from . import matrix
from .bars import WIDTH_US, bars, since

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
        if table.get("corr") not in ("dcc", "cdcc", "ccc", "ewma"):
            raise ValueError(f"varcov.{name}: corr must be dcc, cdcc, ccc or ewma")
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
    signal: str = SIGNAL
    rows: list[dict] = field(default_factory=list)
    said: list[str] = field(default_factory=list)
    #: A period end to compute instead of the latest whole one (`--asof`), and
    #: whether to compute it though it is stored (`--redo`): the repair of a
    #: period stored short. The rows keep the true computed time, so a reader
    #: taking the newest computation of each asof reads the repair.
    asof: int | None = None
    redo: bool = False

    @property
    def run_id(self) -> str:
        return f"{self.signal}-{self.computed_micros}"


def micros(ts: datetime) -> int:
    return int(ts.timestamp() * 1_000_000)


def stored_asof(tape: Path, signal: str = SIGNAL) -> dict[str, int]:
    """The newest asof per horizon already on the tape for `signal`: the tape is the cursor."""
    root = tape / "kind=signals"
    if not any(root.rglob("*.parquet")) if root.is_dir() else True:
        return {}
    frame = (
        pl.scan_parquet(str(root / "**" / "*.parquet"), hive_partitioning=False)
        .filter(pl.col("signal") == signal)
        .group_by("horizon")
        .agg(pl.col("asof_micros").max())
        .collect()
    )
    return dict(zip(frame["horizon"].to_list(), frame["asof_micros"].to_list()))


#: How far back a horizon's fit looks: a **rolling window of fixed time**, the
#: same for every horizon, rather than everything ever captured. The sample a
#: model fits moves with the market instead of growing forever, and a run reads
#: a bounded tape. Widened, never narrowed, by what a horizon's own figures need
#: (`lookback_us`), so nothing that is computed today stops being computed.
LOOKBACK_DAYS = 300
DAY_US = 86_400_000_000


def lookback_us(hz: Horizon) -> int:
    """How far before now a horizon's bars are read.

    `LOOKBACK_DAYS`, or more where the horizon needs more: a fitted horizon's
    `min_obs` returns, and the year of standardised residuals its tail is
    simulated from (`TAIL_YEAR_DAYS`). Two widths more cover the bar the first
    return is taken from and a first bucket cut short by the bound.
    """
    width = WIDTH_US[hz.name]
    need = LOOKBACK_DAYS * DAY_US
    if hz.fitted:
        need = max(need, (hz.min_obs + 1) * width, TAIL_YEAR_DAYS * DAY_US)
    return need + 2 * width


def compute(horizons: list[Horizon], tape: Path, run: Run) -> Run:
    """Every horizon with a new close, appended to `run.rows`: Σ, and what is derived from it."""
    stored = stored_asof(tape)
    for hz in horizons:
        start = since(run.computed_micros - lookback_us(hz))
        returns = gr.timeseries.returns(bars(hz.name, hz.bars, start), kind="log")
        try:
            tickers, joint, _ = gr.models.corr.joint(returns)
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
        try:
            walked = walk(hz, returns, last)
        except Refused as refusal:
            run.rows.extend(absent(hz, tickers, asof, str(refusal), run))
            run.rows.extend(derived_absent(hz, tickers, asof, str(refusal), run))
            run.rows.extend(turbulence_rows(hz, joint, tickers, run))
            run.said.append(f"{hz.name}: absent, {refusal}")
            continue
        run.rows.extend(present(hz, walked, run))
        run.rows.extend(derived(hz, returns, joint, tickers, walked, run))
        run.said.append(f"{hz.name}: {len(tickers)} instruments at {last:%Y-%m-%d %H:%M}")
    return run


def walk(hz: Horizon, returns: pl.DataFrame, last) -> pl.DataFrame:
    """One origin, at `last` (a close), h = 1: Σ for the bar after it."""
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


def _base(hz: Horizon, run: Run, asof: int, params: dict, signal: str = SIGNAL) -> dict:
    return {
        "signal": signal,
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
            if hz.fitted:
                rows.append({**common, "measure": "correlation_target", "value": r["correlation_target"]})
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
                if hz.fitted:
                    rows.append({**common, "measure": "correlation_target"})
    return rows


# ── derived from the matrix ─────────────────────────────────────────────────

REFERENCE = "BTC"
#: `ticker_i` of a figure about the whole universe: no venue symbol is `*`.
EVERYONE = "*"


def sigma_at(walked: pl.DataFrame, tickers: list[str], close) -> np.ndarray:
    """The walk's h = 1 covariance at one origin, as an (N × N) array in `tickers` order."""
    at = walked.filter((pl.col("close_ts") == close) & (pl.col("h") == 1))
    index = {t: k for k, t in enumerate(tickers)}
    s = np.full((len(tickers), len(tickers)), np.nan)
    for r in at.iter_rows(named=True):
        i, j = index[r["ticker_i"]], index[r["ticker_j"]]
        s[i, j] = s[j, i] = r["covariance"]
    return s


def _row(base: dict, measure: str, ticker_i: str, ticker_j: str | None, value, reason: str | None, target: int, span: tuple) -> dict:
    return {
        **base,
        "measure": measure,
        "ticker_i": ticker_i,
        "ticker_j": ticker_j,
        "value": None if value is None else float(value),
        "absent": reason if value is None else None,
        "n_eff": None,
        "target_micros": target,
        "fitted_through_micros": span[0],
        "fit_from_micros": span[1],
        "after_gap": False,
    }


def derived(hz: Horizon, returns: pl.DataFrame, joint: pl.DataFrame, tickers: list[str], walked: pl.DataFrame, run: Run) -> list[dict]:
    """beta, absorption, surprise and turbulence, at the horizon's asof."""
    last = joint["close_ts"][-1]
    asof = micros(last)
    first = walked.row(0, named=True)
    params = _params(hz, first["a"], first["b"])
    span = (micros(first["fitted_through"]), micros(first["fit_from"])) if hz.fitted else (None, None)
    sigma = sigma_at(walked, tickers, last)
    rows: list[dict] = []

    base = _base(hz, run, asof, params, "beta")
    if REFERENCE in tickers:
        try:
            for t, (b, idio) in matrix.beta(sigma, tickers, REFERENCE).items():
                rows.append(_row(base, "beta", t, REFERENCE, b, None, asof, span))
                rows.append(_row(base, "idiosyncratic_share", t, REFERENCE, idio, None, asof, span))
        except matrix.Undefined as why:
            for t in tickers:
                if t != REFERENCE:
                    rows += [_row(base, m, t, REFERENCE, None, str(why), asof, span) for m in ("beta", "idiosyncratic_share")]

    base = _base(hz, run, asof, params, "absorption")
    try:
        cov_ar, cor_ar = matrix.absorption(sigma)
        rows += [_row(base, "covariance_ar", EVERYONE, None, cov_ar, None, asof, span), _row(base, "correlation_ar", EVERYONE, None, cor_ar, None, asof, span)]
    except matrix.Undefined as why:
        rows += [_row(base, m, EVERYONE, None, None, str(why), asof, span) for m in ("covariance_ar", "correlation_ar")]

    # The last bar against the Σ forecast before it: a second walk, one origin
    # at the previous close. The stored Σ above is untouched by it.
    y = joint.select(tickers).row(-1)
    base = _base(hz, run, asof, params, "surprise")
    measures = ("mahalanobis", "chi2_percentile", "magnitude_surprise", "correlation_surprise")
    bar_open = micros(joint["ts"][-1])
    if joint.height < 2:
        rows += [_row(base, m, EVERYONE, None, None, "no bar before the last to forecast from", bar_open, (None, None)) for m in measures]
    else:
        prev = joint["close_ts"][-2]
        try:
            before = walk(hz, returns.filter(pl.col("close_ts") <= prev), prev)
            b0 = before.row(0, named=True)
            pspan = (micros(b0["fitted_through"]), micros(b0["fit_from"])) if hz.fitted else (None, None)
            base = _base(hz, run, asof, _params(hz, b0["a"], b0["b"]), "surprise")
            found = matrix.surprise(y, sigma_at(before, tickers, prev))
            for m in measures:
                v = found[m]
                rows.append(_row(base, m, EVERYONE, None, v, None if v is not None else "no magnitude to divide by: no instrument moved", bar_open, pspan))
        except (Refused, matrix.Undefined) as why:
            rows += [_row(base, m, EVERYONE, None, None, str(why), bar_open, (None, None)) for m in measures]

    return rows + regime_rows(hz, returns, asof, params, span, run, sigma, tickers) + turbulence_rows(hz, joint, tickers, run)


#: The constancy test's trailing window, in days of the horizon's bars, and its lags.
CONSTANCY_DAYS = 30
CONSTANCY_LAGS = 5
CONSTANCY = ("engle_sheppard_stat", "engle_sheppard_p")

#: The sequential monitor's epochs, in days, per horizon: the baseline is the
#: epoch before the current one, and the current one is monitored. Sized for a
#: baseline of 500 joint returns or more (galata-research
#: `MONITOR_MIN_BASELINE`): 4h has about 6 joint bars a day, the session
#: instruments' hours; 1h about 24; 5m about 250.
MONITOR_EPOCH_DAYS = {"5m": 7, "1h": 30, "4h": 90}
MONITOR_ALPHA = 0.05
MONITOR_GAMMA = 0.25
#: The monitored span, in baselines: an epoch holds about as many returns as
#: the one before, so 1.5 covers its variation.
MONITOR_T = 1.5
MONITOR = ("wied_galeano_ratio", "wied_galeano_alarm")


def regime_rows(hz: Horizon, returns: pl.DataFrame, asof: int, params: dict, span: tuple, run: Run, sigma=None, tickers=()) -> list[dict]:
    """`constancy`, `monitor` and `tail`, from one in-sample fit of the declared margins."""
    if not hz.fitted:
        return []
    try:
        f = _margins(hz, returns)
    except Refused as why:
        return _absent_regime(hz, run, asof, params, span, str(why), tickers)
    rows = constancy_rows(hz, f, asof, params, span, run) + monitor_rows(hz, f, asof, params, span, run)
    return rows + (tail_rows(hz, f, sigma, list(tickers), asof, params, span, run) if sigma is not None else [])


#: Filtered historical simulation (Barone-Adesi, Giannopoulos and Vosper 1999):
#: a year of the margins' standardised residuals, or all there are, and at least
#: 1,000 of them, so the 1% tail holds 10 or more.
TAIL_YEAR_DAYS = 365
TAIL_MIN = 1000
TAIL = ("sigma_next", "var_99", "var_975", "es_975", "es_to_var")


def tail_rows(hz: Horizon, f, sigma, tickers: list[str], asof: int, params: dict, span: tuple, run: Run) -> list[dict]:
    """Each instrument's one-bar-ahead VaR and ES by filtered historical simulation, as positive log-return losses.

    VaR_α = −σ̂ₜ₊₁·q_α(ẑ) and ES_α = −σ̂ₜ₊₁·mean(ẑ | ẑ ≤ q_α), with σ̂ₜ₊₁ the
    walk's own one-bar forecast (Σᵢᵢ, the seasonal factor of the next bar
    included) and ẑ the margins' standardised residuals. The fat tail is the
    data's, not a distribution's: Kuester, Mittnik and Paolella (2006) find FHS
    a close second to GARCH with EVT, and ahead of normal GARCH. ES at 97.5%
    is Basel's FRTB measure; `es_to_var` = ES97.5/VaR99, 1.005 under a normal,
    is how much heavier the tail is. Per bar only: √h scaling understates a
    fat-tailed, clustered risk (Danielsson and Zigrand).
    """
    base = _base(hz, run, asof, {**params, "method": "fhs", "year_days": TAIL_YEAR_DAYS, "min_residuals": TAIL_MIN}, "tail")
    rows = []
    window = TAIL_YEAR_DAYS * 86_400_000_000 // WIDTH_US[hz.name]
    for i, t in enumerate(tickers):
        z = f.fits[t].series["z"].drop_nulls().to_numpy()[-window:] if t in f.fits else np.array([])
        s2 = sigma[i, i] if i < len(sigma) else float("nan")
        if len(z) < TAIL_MIN or not np.isfinite(s2) or s2 <= 0:
            why = f"{len(z)} standardised residuals, under {TAIL_MIN}" if len(z) < TAIL_MIN else "no variance forecast for the next bar"
            rows += [_row(base, m, t, None, None, why, asof, span) for m in TAIL]
            continue
        sd = float(np.sqrt(s2))
        q99, q975 = np.quantile(z, 0.01), np.quantile(z, 0.025)
        var99, var975, es975 = -sd * q99, -sd * q975, -sd * float(z[z <= q975].mean())
        figures = {"sigma_next": sd, "var_99": var99, "var_975": var975, "es_975": es975, "es_to_var": es975 / var99 if var99 > 0 else None}
        rows += [{**_row(base, m, t, None, v, None if v is not None else "VaR 99% is not a loss", asof, span), "n_eff": float(len(z))} for m, v in figures.items()]
    return rows


def _margins(hz: Horizon, returns: pl.DataFrame):
    # As `walk` does: a null return (after a hole) is a dropped bar, not a cell to deseasonalise.
    returns = returns.drop_nulls("return")
    column = "return"
    if hz.deseasonalise:
        last = returns["close_ts"].max()
        factors = gr.timeseries.seasonal_factors(returns, fit=(returns["ts"].min(), last), by="hour_of_day")
        returns = gr.timeseries.deseasonalize(returns, factors)
        column = "deseasonalized"
    return gr.models.corr.fit(returns, model=hz.model, dist=hz.dist, corr=hz.corr, column=column, min_obs=hz.min_obs)


def _absent_regime(hz: Horizon, run: Run, asof: int, params: dict, span: tuple, reason: str, tickers=()) -> list[dict]:
    rows = [_row(_base(hz, run, asof, {**params, "lags": CONSTANCY_LAGS, "days": CONSTANCY_DAYS}, "constancy"), m, EVERYONE, None, None, reason, asof, span) for m in CONSTANCY]
    rows += [_row(_base(hz, run, asof, params, "tail"), m, t, None, None, reason, asof, span) for t in tickers for m in TAIL]
    return rows + [_row(_base(hz, run, asof, params, "monitor"), m, EVERYONE, None, None, reason, asof, span) for m in MONITOR]


def monitor_rows(hz: Horizon, f, asof: int, params: dict, span: tuple, run: Run) -> list[dict]:
    """Wied and Galeano's (2013) sequential monitor over the current epoch, the epoch before it the baseline.

    Epochs are calendar blocks of `MONITOR_EPOCH_DAYS` from 1970-01-01, so every
    run of an epoch recomputes the same monitor from the same baseline: an alarm
    raised stays raised until the epoch ends, and the next epoch starts afresh.
    Every pair at α/15: the chance of any false alarm in an epoch is at most
    `MONITOR_ALPHA` in the limit (5.7% measured at m = 720).
    """
    days = MONITOR_EPOCH_DAYS.get(hz.name)
    if days is None:
        return []
    epoch = days * 86_400_000_000
    start = asof // epoch * epoch
    z = pl.DataFrame({t: f.fits[t].series["z"] for t in f.tickers}).with_columns(close=f.fits[f.tickers[0]].series["close_ts"].dt.epoch("us"))
    window = z.filter((pl.col("close") > start - epoch) & (pl.col("close") <= asof))
    m = window.filter(pl.col("close") <= start).height
    base = _base(hz, run, asof, {**params, "epoch_days": days, "epoch_start": datetime.fromtimestamp(start / 1e6, tz=UTC).isoformat(), "m": m, "alpha": MONITOR_ALPHA, "gamma": MONITOR_GAMMA, "T": MONITOR_T}, "monitor")
    try:
        found = gr.models.corr.monitor(window.select(f.tickers).to_numpy(), m, T=MONITOR_T, gamma=MONITOR_GAMMA, alpha=MONITOR_ALPHA, tickers=f.tickers)
    except Refused as why:
        return [_row(base, measure, EVERYONE, None, None, str(why), asof, span) for measure in MONITOR]
    # The monitored bar an alarm dates the change to: row m + k̂ of the window, by its close.
    change_at = None
    if found["change"] is not None:
        change_at = datetime.fromtimestamp(window["close"][m + found["change"] - 1] / 1e6, tz=UTC).isoformat()
    said = {"k": found["k"], "critical": found["critical"], "first": found["first"], "pair": found["pair"], "change": found["change"], "change_at": change_at}
    base = {**base, "params": json.dumps({**json.loads(base["params"]), **said}, sort_keys=True)}
    rows = [
        _row(base, "wied_galeano_ratio", EVERYONE, None, found["ratio"], None, asof, span),
        _row(base, "wied_galeano_alarm", EVERYONE, None, 1.0 if found["alarm"] else 0.0, None, asof, span),
    ]
    rows += [_row(base, "wied_galeano_ratio", a, b, r, None, asof, span) for (a, b), r in found["pairs"].items()]
    return [{**r, "n_eff": float(found["k"])} for r in rows]


def constancy_rows(hz: Horizon, f, asof: int, params: dict, span: tuple, run: Run) -> list[dict]:
    """Whether the correlation has stayed constant: Engle and Sheppard's (2001) test, on a fitted horizon.

    The margins are fitted once more in sample, on the returns the walk saw
    (deseasonalised as it deseasonalised them), and R is that fit's Q̄
    normalised: under CCC it is the declared R. The test runs on the last
    `CONSTANCY_DAYS` of the horizon's bars against that R, so a correlation
    that has moved lately, or moves with the bars before it, rejects. A small
    p-value is CCC failing; under DCC it says dynamics are present, which DCC
    already models.
    """
    base = _base(hz, run, asof, {**params, "lags": CONSTANCY_LAGS, "days": CONSTANCY_DAYS}, "constancy")
    corr = gr.models.corr
    try:
        z = np.column_stack([f.fits[t].series["z"].to_numpy() for t in f.tickers])
        window = min(len(z), CONSTANCY_DAYS * 86_400_000_000 // WIDTH_US[hz.name])
        found = corr.constancy(z[-window:], f.qbar, lags=CONSTANCY_LAGS)
        rows = [
            _row(base, "engle_sheppard_stat", EVERYONE, None, found["statistic"], None, asof, span),
            _row(base, "engle_sheppard_p", EVERYONE, None, found["p_value"], None, asof, span),
        ]
        return [{**r, "n_eff": float(found["n"])} for r in rows]
    except Refused as why:
        return [_row(base, m, EVERYONE, None, None, str(why), asof, span) for m in CONSTANCY]


def turbulence_rows(hz: Horizon, joint: pl.DataFrame, tickers: list[str], run: Run) -> list[dict]:
    """Kritzman and Li's historical turbulence of the last bar: no model, so written whatever the model said."""
    asof = micros(joint["close_ts"][-1])
    bar_open = micros(joint["ts"][-1])
    base = {**_base(hz, run, asof, {"n": joint.height}, "turbulence"), "model": "sample-covariance", "fitted": False}
    try:
        turb, pct = matrix.turbulence(joint.select(tickers).to_numpy())
        return [_row(base, "turbulence", EVERYONE, None, turb, None, bar_open, (None, None)), _row(base, "percentile", EVERYONE, None, pct, None, bar_open, (None, None))]
    except matrix.Undefined as why:
        return [_row(base, m, EVERYONE, None, None, str(why), bar_open, (None, None)) for m in ("turbulence", "percentile")]


def derived_absent(hz: Horizon, tickers: list[str], asof: int, reason: str, run: Run) -> list[dict]:
    """Every derived figure of a horizon whose Σ is absent, carrying the same reason."""
    rows = []
    none = (None, None)
    if REFERENCE in tickers:
        base = _base(hz, run, asof, _params(hz), "beta")
        rows += [_row(base, m, t, REFERENCE, None, reason, asof, none) for t in tickers if t != REFERENCE for m in ("beta", "idiosyncratic_share")]
    derived_signals = [("absorption", ("covariance_ar", "correlation_ar")), ("surprise", ("mahalanobis", "chi2_percentile", "magnitude_surprise", "correlation_surprise"))]
    if hz.fitted:
        derived_signals.append(("constancy", CONSTANCY))
        if hz.name in MONITOR_EPOCH_DAYS:
            derived_signals.append(("monitor", MONITOR))
        base = _base(hz, run, asof, _params(hz), "tail")
        rows += [_row(base, m, t, None, None, reason, asof, none) for t in tickers for m in TAIL]
    for signal, measures in derived_signals:
        base = _base(hz, run, asof, _params(hz), signal)
        rows += [_row(base, m, EVERYONE, None, None, reason, asof, none) for m in measures]
    return rows
