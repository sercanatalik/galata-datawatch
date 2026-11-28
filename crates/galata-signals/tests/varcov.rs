//! The covariance-signals scenarios, on returns made here and a scratch tape.
//!
//! The candle fold itself was held to `py/signals` on the recorded tape
//! (2026-11-28: the same 315 rows, field for field, the largest difference
//! 2.2e-15 relative — `design/measured.md`); here, the rules around it.

use std::collections::BTreeMap;
use std::time::Duration;

use galata_signals::bars::{self, Bar};
use galata_signals::declared::Horizon;
use galata_signals::store::{self, Committed};
use galata_signals::varcov::{self, Run};

const DAY: i64 = 86_400_000_000;
/// 2026-01-05, a Monday.
const T0: i64 = 20_458 * DAY;

/// Daily closes for each ticker from `start` (in days after T0) to `days`:
/// a deterministic walk, no two tickers alike.
fn daily(tickers: &[(&str, i64)], days: i64) -> BTreeMap<String, Vec<Bar>> {
    let mut out = BTreeMap::new();
    for (k, (t, start)) in tickers.iter().enumerate() {
        let mut price = 100.0 + k as f64;
        let mut bars = Vec::new();
        for d in *start..days {
            let shock = (((d * 7919 + k as i64 * 104_729) % 2001) as f64 - 1000.0) / 50_000.0;
            price *= 1.0 + shock;
            bars.push(Bar {
                ts: T0 + d * DAY,
                close_ts: T0 + (d + 1) * DAY,
                close: price,
            });
        }
        out.insert(t.to_string(), bars);
    }
    out
}

fn horizon(name: &str) -> Horizon {
    Horizon {
        name: name.into(),
        bars: "1d".into(),
        lam: 0.94,
    }
}

fn run(at: i64) -> Run {
    Run {
        computed_micros: at,
        code: "galata-quant@test".into(),
    }
}

#[test]
fn a_daily_close_writes_the_horizon_with_what_is_derived_from_it() {
    let bars = daily(&[("BTC", 0), ("ETH", 0), ("SOL", 0)], 120);
    let r = bars::log_returns(&bars);
    let out = varcov::compute(&horizon("1d"), &r, None, &run(T0 + 121 * DAY));
    let measure = |s: &str, m: &str| {
        out.rows
            .iter()
            .filter(|r| r.signal == s && r.measure == m)
            .count()
    };
    assert_eq!(measure("varcov", "covariance"), 6); // i <= j over three
    assert_eq!(measure("varcov", "correlation"), 3); // i < j
    assert_eq!(measure("beta", "beta"), 2); // ETH and SOL against BTC, not BTC itself
    assert_eq!(measure("absorption", "covariance_ar"), 1);
    assert_eq!(measure("surprise", "mahalanobis"), 1);
    assert_eq!(measure("turbulence", "turbulence"), 1);
    assert!(
        out.rows.iter().all(|r| r.asof_micros == T0 + 120 * DAY),
        "every row at the last close"
    );
    assert!(
        out.rows
            .iter()
            .all(|r| r.value.is_some() != r.absent.is_some())
    );
    assert!(
        out.rows
            .iter()
            .filter(|r| r.signal == "varcov")
            .all(|r| r.value.is_some() && r.params == "{\"lam\": 0.94}")
    );

    // The week, built from the same days.
    let weeks: BTreeMap<String, Vec<Bar>> = bars
        .iter()
        .map(|(t, b)| (t.clone(), bars::build(b, DAY, 7 * DAY)))
        .collect();
    let w = varcov::compute(
        &horizon("1w"),
        &bars::log_returns(&weeks),
        None,
        &run(T0 + 121 * DAY),
    );
    assert!(
        w.rows
            .iter()
            .filter(|r| r.signal == "varcov")
            .all(|r| r.absent.is_some()),
        "17 weeks are under the warm-up of 33"
    );
}

#[test]
fn nothing_new_writes_nothing() {
    let r = bars::log_returns(&daily(&[("BTC", 0), ("ETH", 0)], 60));
    let asof = T0 + 60 * DAY;
    let out = varcov::compute(&horizon("1d"), &r, Some(asof), &run(T0 + 61 * DAY));
    assert!(out.rows.is_empty());
    assert!(
        out.said.starts_with("1d: nothing new since"),
        "{}",
        out.said
    );
}

#[test]
fn a_short_history_is_written_absent_naming_the_youngest() {
    // NVDA listed on day 100 of 120: 19 joint returns, under the warm-up of 33.
    let r = bars::log_returns(&daily(&[("BTC", 0), ("ETH", 0), ("NVDA", 100)], 120));
    let out = varcov::compute(&horizon("1d"), &r, None, &run(T0 + 121 * DAY));
    let cov: Vec<_> = out.rows.iter().filter(|r| r.signal == "varcov").collect();
    assert!(!cov.is_empty());
    for row in cov {
        assert_eq!(
            row.absent.as_deref(),
            Some(
                "the first origin has 19 joint returns, under the warm-up 33 at lam=0.94: it starts at NVDA's first return"
            )
        );
    }
    // Turbulence needs no model, so it is written whatever the model said.
    assert!(
        out.rows
            .iter()
            .any(|r| r.signal == "turbulence" && r.value.is_some())
    );
}

#[test]
fn one_run_is_one_commit_and_two_runs_store_a_horizon_once() {
    let tape = tempfile::tempdir().unwrap();
    let r = bars::log_returns(&daily(&[("BTC", 0), ("ETH", 0)], 60));
    let first = run(T0 + 61 * DAY);
    let rows = varcov::compute(&horizon("1d"), &r, None, &first).rows;
    let patience = Duration::from_secs(5);
    let done = store::commit(
        tape.path(),
        rows.clone(),
        first.computed_micros,
        &first.run_id(),
        &first.code,
        patience,
    )
    .unwrap();
    assert!(matches!(done, Committed::Written { .. }));
    assert_eq!(
        store::stored_asof(tape.path()).unwrap().get("1d"),
        Some(&(T0 + 60 * DAY))
    );

    // The same run committed again: refused, the tape unchanged.
    let again = store::commit(
        tape.path(),
        rows.clone(),
        first.computed_micros,
        &first.run_id(),
        &first.code,
        patience,
    );
    assert!(again.unwrap_err().contains("already on the tape"));

    // A second run, computed a microsecond later from the same returns: the
    // horizon is already stored, so it writes nothing.
    let second = run(first.computed_micros + 1);
    let late: Vec<_> = rows
        .into_iter()
        .map(|mut r| {
            r.computed_micros = second.computed_micros;
            r.run_id = second.run_id();
            r
        })
        .collect();
    let done = store::commit(
        tape.path(),
        late,
        second.computed_micros,
        &second.run_id(),
        &second.code,
        patience,
    )
    .unwrap();
    assert_eq!(done, Committed::Nothing);
}
