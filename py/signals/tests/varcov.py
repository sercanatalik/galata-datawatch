"""The calculator, on bars made in the test: the tape's candle schema is galata-research's to read."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import numpy as np
import polars as pl
import pyarrow as pa
import pyarrow.ipc as ipc
import pyarrow.parquet as pq
import pytest

from galata_signals import __main__ as cli
from galata_signals import bars as bars_mod
from galata_signals import schema, varcov

import galata_research as gr

T0 = datetime(2026, 9, 1, tzinfo=UTC)
FIXTURE = Path(__file__).resolve().parents[3] / "crates" / "galata-datawatch" / "tests" / "data" / "signals.arrow"


def _bars(tickers=("BTC", "ETH", "GOLD"), n=400, width=timedelta(hours=1), seed=1, start=T0) -> pl.DataFrame:
    rng = np.random.default_rng(seed)
    frames = []
    for k, t in enumerate(tickers):
        closes = 100 * np.exp(np.cumsum(rng.normal(0, 0.01, n) + 0.3 * rng.normal(0, 0.01, n) * k))
        frames.append(
            pl.DataFrame(
                {
                    "ticker": t,
                    "ts": [start + i * width for i in range(n)],
                    "close_ts": [start + (i + 1) * width for i in range(n)],
                    "open": closes,
                    "high": closes * 1.001,
                    "low": closes * 0.999,
                    "close": closes,
                }
            )
        )
    return pl.concat(frames)


@pytest.fixture(name="served")
def _served(monkeypatch):
    """`bars()` answers from frames made here, per horizon."""
    frames: dict[str, pl.DataFrame] = {}
    monkeypatch.setattr(varcov, "bars", lambda horizon, source, start=None: frames[horizon])
    return frames


def _ewma(name="1h", **kw) -> varcov.Horizon:
    return varcov.Horizon.declared(name, {"bars": "1h", "corr": "ewma", "lam": 0.94, **kw})


def _run() -> varcov.Run:
    return varcov.Run(computed_micros=1_790_560_800_000_000, code="abc")


def _stored(tape: Path, horizon: str, asof: datetime) -> None:
    row = {
        "signal": "varcov", "horizon": horizon, "measure": "covariance", "ticker_i": "BTC", "ticker_j": "BTC", "h": 1,
        "value": 1.0, "absent": None, "n_eff": 1.0, "asof_micros": varcov.micros(asof), "target_micros": 0,
        "computed_micros": 0, "fitted_through_micros": None, "fit_from_micros": None, "model": "ewma",
        "params": "{}", "fitted": False, "after_gap": False, "code": "c", "run_id": "r",
    }  # fmt: skip
    path = tape / "kind=signals" / "date=2026-09-17" / "t-1_1_1_0.parquet"
    path.parent.mkdir(parents=True)
    pq.write_table(pa.Table.from_pylist([row], schema=schema.SCHEMA), path)


# ── bars ─────────────────────────────────────────────────────────────────────


def a_built_bucket_missing_a_minute_is_dropped():
    minute = timedelta(minutes=1)
    fine = _bars(tickers=("BTC",), n=10, width=minute)
    fine = fine.filter(pl.col("ts") != T0 + 7 * minute)  # the second five-minute bucket loses a minute
    built = bars_mod.build(fine, "5m")
    assert built["ts"].to_list() == [T0]
    assert built["n"].to_list() == [5]
    assert built["close_ts"].to_list() == [T0 + 5 * minute]


def a_week_is_seven_whole_days_from_monday():
    day = timedelta(days=1)
    monday = datetime(2026, 9, 7, tzinfo=UTC)
    built = bars_mod.build(_bars(tickers=("BTC",), n=10, width=day, start=monday - day), "1w")
    # The Sunday before is a partial week; the week of the 7th is whole; the 14th's holds two days.
    assert built["ts"].to_list() == [monday]
    assert built["close_ts"].to_list() == [monday + 7 * day]


# ── the calculator ───────────────────────────────────────────────────────────


def the_asof_is_the_last_close(served, tmp_path):
    served["1h"] = _bars()
    run = varcov.compute([_ewma()], tmp_path, _run())
    last = served["1h"]["close_ts"].max()
    assert {r["asof_micros"] for r in run.rows} == {varcov.micros(last)}
    assert {r["computed_micros"] for r in run.rows} == {1_790_560_800_000_000}


def six_instruments_make_twenty_one_covariances_and_fifteen_correlations(served, tmp_path):
    served["1h"] = _bars(tickers=("BTC", "CL", "ETH", "GOLD", "HYPE", "XYZ100"))
    rows = [r for r in varcov.compute([_ewma()], tmp_path, _run()).rows if r["signal"] == "varcov"]
    cov = [r for r in rows if r["measure"] == "covariance"]
    cor = [r for r in rows if r["measure"] == "correlation"]
    assert (len(cov), len(cor)) == (21, 15)
    assert all(r["value"] > 0 for r in cov if r["ticker_i"] == r["ticker_j"])
    assert all(r["ticker_i"] < r["ticker_j"] and -1 <= r["value"] <= 1 for r in cor)
    assert not [r for r in rows if r["measure"] == "correlation_target"]  # EWMA reverts to nothing


def nothing_closed_since_the_last_run_exits_three(served, tmp_path, monkeypatch):
    served["1h"] = _bars()
    tape = tmp_path / "tape"
    _stored(tape, "1h", served["1h"]["close_ts"].max())
    config = tmp_path / "signals.toml"
    config.write_text('[varcov.1h]\nbars = "1h"\ncorr = "ewma"\n')
    out = tmp_path / "out.arrow"
    code = cli.main(["varcov", "--var", str(tmp_path), "--out", str(out), "--config", str(config)])
    assert code == cli.NOTHING
    assert not out.exists()


def only_the_moved_horizons_are_written(served, tmp_path):
    served["1h"] = _bars()
    served["4h"] = _bars(width=timedelta(hours=4))
    tape = tmp_path / "tape"
    _stored(tape, "4h", served["4h"]["close_ts"].max())  # 4h has nothing new; 1h does
    rows = varcov.compute([_ewma("1h"), _ewma("4h")], tape, _run()).rows
    assert {r["horizon"] for r in rows} == {"1h"}


def a_thin_sample_is_an_absent_row(served, tmp_path):
    served["4h"] = _bars(n=265)
    hz = varcov.Horizon.declared("4h", {"bars": "4h", "model": "gjr", "dist": "t", "corr": "dcc"})
    rows = [r for r in varcov.compute([hz], tmp_path, _run()).rows if r["signal"] == "varcov"]
    assert len(rows) == 6 + 3 + 3  # three instruments: six pairs i ≤ j, three i < j, and their targets
    assert all(r["value"] is None for r in rows)
    assert all("264" in r["absent"] and "500" in r["absent"] for r in rows), rows[0]["absent"]
    const = [r for r in varcov.compute([hz], tmp_path, _run()).rows if r["signal"] == "constancy"]
    assert len(const) == 2 and all(r["value"] is None and "500" in r["absent"] for r in const)


def a_fitted_horizon_states_its_parameters(served, tmp_path):
    served["1h"] = _bars(n=700)
    hz = varcov.Horizon.declared("1h", {"bars": "1h", "model": "garch", "dist": "normal", "corr": "dcc"})
    rows = [r for r in varcov.compute([hz], tmp_path, _run()).rows if r["signal"] == "varcov"]
    params = json.loads(rows[0]["params"])
    assert rows[0]["model"] == "garch-normal/dcc" and rows[0]["fitted"]
    assert 0 <= params["a"] and params["a"] + params["b"] < 1
    assert all(r["fitted_through_micros"] == r["asof_micros"] for r in rows)


def a_fitted_pair_states_the_correlation_it_reverts_to(served, tmp_path):
    served["1h"] = _bars(n=700)
    hz = varcov.Horizon.declared("1h", {"bars": "1h", "model": "garch", "dist": "normal", "corr": "dcc"})
    rows = [r for r in varcov.compute([hz], tmp_path, _run()).rows if r["signal"] == "varcov"]
    target = {(r["ticker_i"], r["ticker_j"]): r for r in rows if r["measure"] == "correlation_target"}
    rho = {(r["ticker_i"], r["ticker_j"]): r for r in rows if r["measure"] == "correlation"}
    assert target.keys() == rho.keys() and len(target) == 3
    for pair, t in target.items():
        assert -1 < t["value"] < 1
        assert (t["fit_from_micros"], t["fitted_through_micros"], t["asof_micros"]) == (
            rho[pair]["fit_from_micros"], rho[pair]["fitted_through_micros"], rho[pair]["asof_micros"],
        )  # fmt: skip
    # The walk's own R̄, recomputed from an in-sample fit on the same returns.
    returns = gr.timeseries.returns(served["1h"], kind="log").drop_nulls("return")
    f = gr.models.corr.fit(returns, model="garch", dist="normal")
    d = np.sqrt(np.diag(f.qbar))
    i, j = f.tickers.index("BTC"), f.tickers.index("ETH")
    assert target[("BTC", "ETH")]["value"] == pytest.approx(f.qbar[i, j] / (d[i] * d[j]), abs=1e-5)


def a_constant_correlation_is_its_own_target(served, tmp_path):
    served["4h"] = _bars(n=700, width=timedelta(hours=4))
    hz = varcov.Horizon.declared("4h", {"bars": "4h", "model": "gjr", "dist": "t", "corr": "ccc"})
    rows = [r for r in varcov.compute([hz], tmp_path, _run()).rows if r["signal"] == "varcov"]
    assert {r["model"] for r in rows} == {"gjr-t/ccc"} and all(r["fitted"] for r in rows)
    params = json.loads(rows[0]["params"])
    assert (params["a"], params["b"]) == (0.0, 0.0)
    value = {(r["measure"], r["ticker_i"], r["ticker_j"]): r["value"] for r in rows}
    pairs = [k[1:] for k in value if k[0] == "correlation"]
    assert len(pairs) == 3
    for pair in pairs:
        assert value[("correlation", *pair)] == pytest.approx(value[("correlation_target", *pair)], abs=1e-12)


def a_fitted_horizon_states_whether_its_correlation_stayed_constant(served, tmp_path):
    served["4h"] = _bars(n=700, width=timedelta(hours=4))
    hz = varcov.Horizon.declared("4h", {"bars": "4h", "model": "gjr", "dist": "t", "corr": "ccc"})
    rows = [r for r in varcov.compute([hz], tmp_path, _run()).rows if r["signal"] == "constancy"]
    by = {r["measure"]: r for r in rows}
    assert set(by) == {"engle_sheppard_stat", "engle_sheppard_p"}
    assert by["engle_sheppard_stat"]["value"] >= 0 and 0 <= by["engle_sheppard_p"]["value"] <= 1
    assert all(r["ticker_i"] == "*" and r["ticker_j"] is None and r["model"] == "gjr-t/ccc" for r in rows)
    params = json.loads(rows[0]["params"])
    assert (params["lags"], params["days"]) == (5, 30)
    assert rows[0]["n_eff"] == 180  # 30 days of 4h bars


def a_fitted_horizon_is_monitored_over_its_epoch(served, tmp_path, monkeypatch):
    monkeypatch.setitem(varcov.MONITOR_EPOCH_DAYS, "4h", 100)  # 600 bars an epoch, over the 500 floor
    served["4h"] = _bars(n=1400, width=timedelta(hours=4))
    hz = varcov.Horizon.declared("4h", {"bars": "4h", "model": "gjr", "dist": "t", "corr": "ccc"})
    rows = [r for r in varcov.compute([hz], tmp_path, _run()).rows if r["signal"] == "monitor"]
    universe = {r["measure"]: r for r in rows if r["ticker_i"] == "*"}
    assert set(universe) == {"wied_galeano_ratio", "wied_galeano_alarm"}
    assert universe["wied_galeano_alarm"]["value"] in (0.0, 1.0)
    assert (universe["wied_galeano_ratio"]["value"] >= 1) == (universe["wied_galeano_alarm"]["value"] == 1.0)
    pairs = [r for r in rows if r["ticker_i"] != "*"]
    assert len(pairs) == 3 and all(r["measure"] == "wied_galeano_ratio" for r in pairs)
    params = json.loads(universe["wied_galeano_ratio"]["params"])
    assert params["m"] == 600 and params["epoch_days"] == 100 and 0 < params["k"] <= 900
    assert (params["alpha"], params["gamma"], params["T"]) == (0.05, 0.25, 1.5)
    assert (params["change_at"] is None) == (params["change"] is None) == (universe["wied_galeano_alarm"]["value"] == 0.0)


def a_short_baseline_is_an_absent_monitor(served, tmp_path, monkeypatch):
    monkeypatch.setitem(varcov.MONITOR_EPOCH_DAYS, "4h", 60)  # at most 360 bars an epoch, under the 500 floor
    served["4h"] = _bars(n=700, width=timedelta(hours=4))
    hz = varcov.Horizon.declared("4h", {"bars": "4h", "model": "gjr", "dist": "t", "corr": "ccc"})
    rows = [r for r in varcov.compute([hz], tmp_path, _run()).rows if r["signal"] == "monitor"]
    assert len(rows) == 2 and all(r["value"] is None and "too short" in r["absent"] for r in rows)


def a_fitted_horizon_states_each_instruments_tail(served, tmp_path):
    served["4h"] = _bars(n=1300, width=timedelta(hours=4))
    hz = varcov.Horizon.declared("4h", {"bars": "4h", "model": "gjr", "dist": "t", "corr": "ccc"})
    rows = [r for r in varcov.compute([hz], tmp_path, _run()).rows if r["signal"] == "tail"]
    by = {(r["ticker_i"], r["measure"]): r["value"] for r in rows}
    assert {t for t, _ in by} == {"BTC", "ETH", "GOLD"}
    for t in ("BTC", "ETH", "GOLD"):
        assert 0 < by[(t, "var_975")] < by[(t, "var_99")] and by[(t, "es_975")] >= by[(t, "var_975")]
        assert by[(t, "sigma_next")] > 0 and by[(t, "es_to_var")] > 0.8
    assert {r["n_eff"] for r in rows} == {1299.0}


def a_short_sample_has_no_tail(served, tmp_path):
    served["4h"] = _bars(n=700, width=timedelta(hours=4))
    hz = varcov.Horizon.declared("4h", {"bars": "4h", "model": "gjr", "dist": "t", "corr": "ccc"})
    rows = [r for r in varcov.compute([hz], tmp_path, _run()).rows if r["signal"] == "tail"]
    assert rows and all(r["value"] is None and "under 1000" in r["absent"] for r in rows)


def no_constancy_where_nothing_is_fitted(served, tmp_path):
    served["1h"] = _bars()
    assert not [r for r in varcov.compute([_ewma()], tmp_path, _run()).rows if r["signal"] == "constancy"]


def an_undeclarable_correlation_model_is_refused():
    with pytest.raises(ValueError, match="dcc, cdcc, ccc or ewma"):
        varcov.Horizon.declared("4h", {"bars": "4h", "corr": "bekk"})


def an_unknown_key_is_a_bad_argument(tmp_path):
    (tmp_path / "tape").mkdir()
    config = tmp_path / "signals.toml"
    config.write_text('[varcov.1h]\nbars = "1h"\ncorr = "ewma"\nlambda = 0.9\n')
    code = cli.main(["varcov", "--var", str(tmp_path), "--out", str(tmp_path / "o.arrow"), "--config", str(config)])
    assert code == cli.BAD_ARGUMENT


# ── the contract across languages ────────────────────────────────────────────


def the_committed_fixture_is_the_schema(served, tmp_path):
    # The fixture the Rust side commits in its own test. If this package's
    # schema drifts, this fails here; if the Rust schema drifts, that test does.
    assert ipc.open_file(str(FIXTURE)).schema.equals(schema.SCHEMA, check_metadata=False)
    served["1h"] = _bars()
    rows = varcov.compute([_ewma()], tmp_path, _run()).rows
    written = schema.write(rows, tmp_path / "run.arrow")
    assert ipc.open_file(str(written)).schema.equals(schema.SCHEMA, check_metadata=False)


def a_value_and_a_reason_together_are_refused(tmp_path):
    with pytest.raises(ValueError, match="exactly one"):
        schema.write([{"horizon": "1h", "measure": "covariance", "ticker_i": "BTC", "ticker_j": "BTC", "value": 1.0, "absent": "x"}], tmp_path / "x.arrow")


# ── derived from the matrix ─────────────────────────────────────────────────


def a_run_writes_every_derived_signal(served, tmp_path):
    served["1h"] = _bars()
    rows = varcov.compute([_ewma()], tmp_path, _run()).rows
    measures = {(r["signal"], r["measure"]) for r in rows}
    assert {("beta", "beta"), ("beta", "idiosyncratic_share"), ("absorption", "covariance_ar"), ("absorption", "correlation_ar"),
            ("surprise", "mahalanobis"), ("surprise", "chi2_percentile"), ("surprise", "magnitude_surprise"),
            ("surprise", "correlation_surprise"), ("turbulence", "turbulence"), ("turbulence", "percentile")} <= measures  # fmt: skip
    assert {r["asof_micros"] for r in rows} == {varcov.micros(served["1h"]["close_ts"].max())}
    betas = [r for r in rows if r["measure"] == "beta"]
    assert {r["ticker_i"] for r in betas} == {"ETH", "GOLD"} and {r["ticker_j"] for r in betas} == {"BTC"}
    schema.write(rows, tmp_path / "all.arrow")  # every row is a value or a reason, in the schema


def the_stored_sigma_is_unchanged_by_the_derived_figures(served, tmp_path):
    served["1h"] = _bars(n=700)
    hz = varcov.Horizon.declared("1h", {"bars": "1h", "model": "garch", "dist": "normal", "corr": "dcc"})
    with_derived = [r for r in varcov.compute([hz], tmp_path, _run()).rows if r["signal"] == "varcov"]
    returns = gr.timeseries.returns(served["1h"], kind="log")
    alone = varcov.present(hz, varcov.walk(hz, returns, served["1h"]["close_ts"].max()), _run())
    key = lambda r: (r["measure"], r["ticker_i"], r["ticker_j"])  # noqa: E731
    assert sorted(with_derived, key=key) == sorted(alone, key=key)


def an_absent_horizon_is_absent_everywhere(served, tmp_path):
    served["4h"] = _bars(n=265)
    hz = varcov.Horizon.declared("4h", {"bars": "4h", "model": "gjr", "dist": "t", "corr": "dcc"})
    rows = varcov.compute([hz], tmp_path, _run()).rows
    model_rows = [r for r in rows if r["signal"] in ("varcov", "beta", "absorption", "surprise")]
    assert model_rows and all(r["value"] is None and "500" in r["absent"] for r in model_rows)
    # Turbulence needs no model: it is written from the sample alone.
    assert all(r["value"] is not None for r in rows if r["signal"] == "turbulence")
