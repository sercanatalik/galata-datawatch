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
    monkeypatch.setattr(varcov, "bars", lambda horizon, source: frames[horizon])
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
    rows = varcov.compute([_ewma()], tmp_path, _run()).rows
    cov = [r for r in rows if r["measure"] == "covariance"]
    cor = [r for r in rows if r["measure"] == "correlation"]
    assert (len(cov), len(cor)) == (21, 15)
    assert all(r["value"] > 0 for r in cov if r["ticker_i"] == r["ticker_j"])
    assert all(r["ticker_i"] < r["ticker_j"] and -1 <= r["value"] <= 1 for r in cor)


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
    rows = varcov.compute([hz], tmp_path, _run()).rows
    assert len(rows) == 6 + 3  # three instruments: six pairs i ≤ j, three i < j
    assert all(r["value"] is None for r in rows)
    assert all("264" in r["absent"] and "500" in r["absent"] for r in rows), rows[0]["absent"]


def a_fitted_horizon_states_its_parameters(served, tmp_path):
    served["1h"] = _bars(n=700)
    hz = varcov.Horizon.declared("1h", {"bars": "1h", "model": "garch", "dist": "normal", "corr": "dcc"})
    rows = varcov.compute([hz], tmp_path, _run()).rows
    params = json.loads(rows[0]["params"])
    assert rows[0]["model"] == "garch-normal/dcc" and rows[0]["fitted"]
    assert 0 <= params["a"] and params["a"] + params["b"] < 1
    assert all(r["fitted_through_micros"] == r["asof_micros"] for r in rows)


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
