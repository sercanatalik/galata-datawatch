"""Carry on funding made in the test: the record's funding schema is galata-research's to read."""

from __future__ import annotations

import json

from datetime import UTC, datetime, timedelta

import numpy as np
import polars as pl
import pytest

from galata_signals import __main__ as cli
from galata_signals import carry, varcov

HOUR = timedelta(hours=1)
NOW = datetime(2026, 9, 28, 6, 40, tzinfo=UTC)
ASOF = datetime(2026, 9, 28, 6, 0, tzinfo=UTC)
DECLARED = carry.Declared.declared({"baseline": {"main": 0.0000125, "xyz": 0.00000625}, "dex": {"xyz": ["GOLD"]}})


def _settled(rates: dict[str, float], hours: int, *, last: datetime = ASOF) -> pl.DataFrame:
    rows = [{"ticker": t, "ts": last - k * HOUR + timedelta(milliseconds=48), "rate": r} for t, r in rates.items() for k in range(hours)]
    return pl.DataFrame(rows, schema={"ticker": pl.String, "ts": pl.Datetime("us", "UTC"), "rate": pl.Float64})


def _live(rates: dict[str, float]) -> pl.DataFrame:
    return pl.DataFrame(
        [{"ticker": t, "recv_ts": ASOF - timedelta(seconds=30), "rate": r} for t, r in rates.items()],
        schema={"ticker": pl.String, "recv_ts": pl.Datetime("us", "UTC"), "rate": pl.Float64},
    )


def _bars(tickers, hours=30 * 24, seed=2) -> pl.DataFrame:
    rng = np.random.default_rng(seed)
    frames = []
    for t in tickers:
        close = 100 * np.exp(np.cumsum(rng.normal(0, 0.005, hours)))
        frames.append(pl.DataFrame({"ticker": t, "ts": [ASOF - (hours - i) * HOUR for i in range(hours)], "close_ts": [ASOF - (hours - i - 1) * HOUR for i in range(hours)], "close": close}))
    return pl.concat(frames)


@pytest.fixture(name="record")
def _record(monkeypatch):
    held = {}
    monkeypatch.setattr(carry, "settled", lambda lo, hi: held["settled"])
    monkeypatch.setattr(carry, "live", lambda lo, hi: held["live"])
    monkeypatch.setattr(carry, "hourly_closes", lambda lo, hi: held["bars"])
    return held


def _run():
    return varcov.Run(computed_micros=varcov.micros(NOW), code="abc", signal="carry")


def _by(rows):
    return {(r["ticker_i"], r["measure"]): r for r in rows}


def a_constant_rate_is_its_own_carry(record, tmp_path):
    record.update(settled=_settled({"BTC": 0.0000125}, 130 * 24), live=_live({"BTC": 0.0000125}), bars=_bars(["BTC"]))
    got = _by(carry.compute(DECLARED, tmp_path, _run()).rows)
    for w in ("24h", "7d", "30d"):
        assert got[("BTC", f"carry_apr_{w}")]["value"] == pytest.approx(0.1095)
    assert got[("BTC", "excess_apr_7d")]["value"] == pytest.approx(0.0, abs=1e-15)
    assert got[("BTC", "baseline_share_7d")]["value"] == 1.0
    assert got[("BTC", "nowcast_apr")]["value"] == pytest.approx(0.1095)
    assert got[("BTC", "zscore_7d")]["value"] is None  # constant history: its absent reason says so
    assert {r["asof_micros"] for r in got.values()} == {varcov.micros(ASOF)}


def a_stopped_walk_is_stated_not_averaged(record, tmp_path):
    three_days_ago = ASOF - 72 * HOUR
    record.update(settled=_settled({"BTC": 0.00002}, 40 * 24, last=three_days_ago), live=_live({"BTC": 0.00003}), bars=_bars(["BTC"]))
    got = _by(carry.compute(DECLARED, tmp_path, _run()).rows)
    day = got[("BTC", "carry_apr_24h")]
    assert day["value"] is None and "covers 0 of 24 hours" in day["absent"] and "2026-09-25T06:00" in day["absent"]
    assert got[("BTC", "carry_apr_30d")]["value"] is not None  # 27 of 30 days settled: 90% covered
    assert got[("BTC", "nowcast_apr")]["value"] == pytest.approx(0.00003 * 8760)
    assert json.loads(got[("BTC", "nowcast_apr")]["params"])["received_micros"] == varcov.micros(ASOF - timedelta(seconds=30))


def the_xyz_dex_has_its_own_baseline(record, tmp_path):
    record.update(settled=_settled({"GOLD": 0.00000625, "BTC": 0.00000625}, 10 * 24), live=_live({}), bars=_bars(["GOLD", "BTC"]))
    got = _by(carry.compute(DECLARED, tmp_path, _run()).rows)
    assert got[("GOLD", "excess_apr_7d")]["value"] == pytest.approx(0.0, abs=1e-15)
    assert got[("BTC", "excess_apr_7d")]["value"] == pytest.approx((0.00000625 - 0.0000125) * 8760)
    assert got[("GOLD", "nowcast_apr")]["absent"] == "no live rate received in the hour before the asof"


def the_carry_to_vol_is_carry_over_hourly_volatility(record, tmp_path):
    bars = _bars(["BTC"])
    record.update(settled=_settled({"BTC": 0.00002}, 10 * 24), live=_live({"BTC": 0.00002}), bars=bars)
    got = _by(carry.compute(DECLARED, tmp_path, _run()).rows)
    import galata_research as gr

    sigma = gr.timeseries.returns(bars, kind="log").drop_nulls("return")["return"].std() * np.sqrt(8760)
    assert got[("BTC", "carry_to_vol_7d")]["value"] == pytest.approx(0.00002 * 8760 / sigma)


def nothing_new_this_hour_exits_three(record, tmp_path, monkeypatch):
    record.update(settled=_settled({"BTC": 0.0000125}, 10 * 24), live=_live({"BTC": 0.0000125}), bars=_bars(["BTC"]))
    monkeypatch.setattr(carry, "stored_asof", lambda tape, signal: {"1h": varcov.micros(ASOF)})
    (tmp_path / "tape").mkdir()
    config = tmp_path / "signals.toml"
    config.write_text('[carry]\nbaseline = { main = 0.0000125 }\n')
    out = tmp_path / "carry.arrow"
    code = cli.main(["carry", "--var", str(tmp_path), "--out", str(out), "--config", str(config), "--now", NOW.isoformat()])
    assert code == cli.NOTHING and not out.exists()


def every_carry_row_is_the_schema(record, tmp_path):
    from galata_signals import schema

    record.update(settled=_settled({"BTC": 0.00002, "GOLD": 0.000005}, 5 * 24), live=_live({"BTC": 0.00002}), bars=_bars(["BTC", "GOLD"]))
    rows = carry.compute(DECLARED, tmp_path, _run()).rows
    schema.write(rows, tmp_path / "carry.arrow")
    assert {r["run_id"] for r in rows} == {f"carry-{varcov.micros(NOW)}"}
