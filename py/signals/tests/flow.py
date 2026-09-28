"""The order flow on quotes and trades made in the test."""

from __future__ import annotations

import random
from datetime import UTC, datetime, timedelta

import polars as pl
import pytest

from galata_signals import flow, varcov

ASOF = datetime(2026, 9, 28, 7, 0, tzinfo=UTC)
START = ASOF - timedelta(hours=1)
QSCHEMA = {"venue": pl.String, "ticker": pl.String, "ts": pl.Datetime("us", "UTC"), "bid_px": pl.Float64, "ask_px": pl.Float64, "bid_sz": pl.Float64, "ask_sz": pl.Float64}
TSCHEMA = {"venue": pl.String, "ticker": pl.String, "ts": pl.Datetime("us", "UTC"), "price": pl.Float64, "size": pl.Float64, "aggressor": pl.String}


def _pushed_book(seed=3):
    """One quote a second for the hour. Each second the bid queue gains g and the mid moves 2·g/5 bps: the flow pushes the price."""
    rng = random.Random(seed)
    mid, rows = 100.0, []
    bid_sz = ask_sz = 5.0
    for s in range(3600):
        g = rng.gauss(0, 1)
        bid_sz = max(bid_sz + g, 0.5)
        mid *= 1 + 2e-4 * g / 5
        rows.append({"venue": "hyperliquid", "ticker": "BTC", "ts": START + timedelta(seconds=s), "bid_px": mid - 0.01, "ask_px": mid + 0.01, "bid_sz": bid_sz, "ask_sz": ask_sz})
    return pl.DataFrame(rows, schema=QSCHEMA)


def _trades(rows=()):
    return pl.DataFrame([{"venue": "hyperliquid", "ticker": "BTC", "ts": ts, "price": p, "size": s, "aggressor": a} for ts, p, s, a in rows], schema=TSCHEMA)


@pytest.fixture(name="record")
def _record(monkeypatch):
    held = {"quotes": _pushed_book(), "trades": _trades()}
    monkeypatch.setattr(flow, "quotes", lambda lo, hi: held["quotes"])
    monkeypatch.setattr(flow, "trades", lambda lo, hi: held["trades"])
    return held


def _got(tmp_path):
    run = varcov.Run(computed_micros=varcov.micros(ASOF + timedelta(minutes=15)), code="abc", signal="flow")
    return {r["measure"]: r for r in flow.compute(tmp_path, run).rows}


def a_book_the_flow_pushes_explains_its_moves(record, tmp_path):
    got = _got(tmp_path)
    assert got["ofi_r2"]["value"] > 0.3 and got["ofi_beta_bps"]["value"] > 0
    assert got["ofi_r2"]["n_eff"] >= 300
    assert got["trade_imbalance_1h"]["value"] is None and "no trades" in got["trade_imbalance_1h"]["absent"]


def the_trade_imbalance_is_buy_minus_sell_over_both(record, tmp_path):
    record["trades"] = _trades([(START + timedelta(minutes=1), 100.0, 3.0, "bid"), (START + timedelta(minutes=2), 100.0, 1.0, "ask")])
    got = _got(tmp_path)
    assert got["trade_imbalance_1h"]["value"] == pytest.approx((300 - 100) / 400)
    assert got["trade_r2"]["value"] is None and "under 100" in got["trade_r2"]["absent"]  # two buckets are not a fit


def the_queue_imbalance_is_time_weighted(record, tmp_path):
    # 3:1 for 45 minutes, 1:3 for 15: 0.5·¾ − 0.5·¼, and the share beyond ±0.5 is none (|0.5| is not beyond).
    record["quotes"] = pl.DataFrame(
        [
            {"venue": "hyperliquid", "ticker": "BTC", "ts": START, "bid_px": 99.9, "ask_px": 100.1, "bid_sz": 3.0, "ask_sz": 1.0},
            {"venue": "hyperliquid", "ticker": "BTC", "ts": START + timedelta(minutes=45), "bid_px": 99.9, "ask_px": 100.1, "bid_sz": 1.0, "ask_sz": 3.0},
        ],
        schema=QSCHEMA,
    )
    got = _got(tmp_path)
    assert got["queue_imbalance_twa"]["value"] == pytest.approx(0.5 * 0.75 - 0.5 * 0.25)
    assert got["queue_imbalance_extreme_share"]["value"] == 0.0
    assert got["ofi_r2"]["value"] is None and "under 100" in got["ofi_r2"]["absent"]


def no_quotes_is_said(record, tmp_path):
    record["quotes"] = pl.DataFrame(schema=QSCHEMA)
    record["trades"] = _trades([(START + timedelta(minutes=1), 100.0, 1.0, "bid")])
    got = _got(tmp_path)
    assert got["ofi_norm_1h"]["absent"] == "no quotes in the hour"
    assert got["queue_imbalance_twa"]["absent"] == "no valid quote state in the hour"
    assert got["trade_r2"]["absent"] == "no quotes to take the mid's return from"
