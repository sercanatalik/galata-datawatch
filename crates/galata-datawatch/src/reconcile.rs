//! The fitted correlation matrix, reconciled against `derive` (Tier 16's
//! cross-check).
//!
//! `py/signals` fits a two-step DCC and stores, per pair, `correlation_target`:
//! R̄, the correlation the fit reverts to. `derive` computes the equal-weight
//! close-to-close ρ from the same tape's candles, in Rust, sharing no code with
//! the Python path. Over the same window the two estimate nearly the same
//! thing: R̄ is ρ of the volatility-standardised returns, which weighs a calm
//! bar as much as a violent one (Forbes and Rigobon 2002).
//!
//! **Measured on a copy of the record, 2026-09-28:** `derive`'s ρ matched the
//! calculator's own joint-sample Pearson to within 0.002 on all 15 pairs, and
//! |atanh R̄ − atanh ρ| was at most 0.076 at 4h (ETH|HYPE) and 0.046 at 1h. A
//! mislabelled ticker, a misaligned bar or a wrong window moves a pair far more
//! than that; a swap of two pairs with similar ρ does not, and is not caught.
//!
//! **The bound is the operator's** (`[watch] max_correlation_target_gap`, in
//! Fisher z), as every watch number is. Only the newest asof per horizon is
//! checked: a stored figure never changes, so an older one was checked when it
//! was newest. A figure absent on either side is not a finding.

use std::collections::BTreeMap;
use std::path::Path;

use arrow::array::{Array, Float64Array, Int64Array, RecordBatch, StringArray};

use crate::derive::tape::{bars_by_venue, width_micros};
use crate::derive::{Bar, Cell, Horizon, derive};
use crate::watch::Finding;

/// The measure `py/signals` stores R̄ under.
pub const MEASURE: &str = "correlation_target";

/// How far back the newest figures are looked for: twice the widest horizon
/// the calculator declares (1w), as the tower's reader does.
const LOOKBACK_DAYS: i64 = 14;
const DAY_MICROS: i64 = 86_400_000_000;

/// One stored R̄.
#[derive(Debug, Clone, PartialEq)]
pub struct Target {
    /// `4h`.
    pub horizon: String,
    /// The pair, `ticker_i < ticker_j`.
    pub ticker_i: String,
    /// The second.
    pub ticker_j: String,
    /// R̄, or nothing when the fit was refused.
    pub value: Option<f64>,
    /// The bar close the fit stands on.
    pub asof_micros: i64,
    /// When it was computed.
    pub computed_micros: i64,
    /// Start of the first return fitted.
    pub fit_from_micros: Option<i64>,
    /// Close of the last return fitted.
    pub fitted_through_micros: Option<i64>,
}

/// Each horizon's newest `correlation_target` rows (of its newest run, should
/// one asof have been computed twice), from the date partitions of the last
/// 14 days before `now_micros`.
pub fn newest_targets(tape: &Path, now_micros: i64) -> Vec<Target> {
    let root = tape.join(format!("kind={}", galata_wire::Kind::Signals));
    let oldest = crate::calendar::date_of(now_micros - LOOKBACK_DAYS * DAY_MICROS);
    let mut dates: Vec<String> = std::fs::read_dir(&root)
        .map(|entries| {
            entries
                .filter_map(|e| e.ok())
                .filter_map(|e| {
                    e.file_name()
                        .to_str()?
                        .strip_prefix("date=")
                        .map(str::to_string)
                })
                .filter(|date| *date >= oldest)
                .collect()
        })
        .unwrap_or_default();
    dates.sort_unstable();

    let mut by_horizon: BTreeMap<String, Vec<Target>> = BTreeMap::new();
    for date in dates {
        for (_, path) in galata_segments::list_segments(&root.join(format!("date={date}"))) {
            // A segment that will not read is the layout check's to report.
            let Ok(batches) = galata_segments::read_segment(&path) else {
                continue;
            };
            for batch in &batches {
                for target in targets(batch) {
                    by_horizon
                        .entry(target.horizon.clone())
                        .or_default()
                        .push(target);
                }
            }
        }
    }
    by_horizon
        .into_values()
        .flat_map(|rows| {
            let newest = rows
                .iter()
                .map(|t| (t.asof_micros, t.computed_micros))
                .max()
                .expect("a horizon came from a row");
            rows.into_iter()
                .filter(move |t| (t.asof_micros, t.computed_micros) == newest)
        })
        .collect()
}

fn targets(batch: &RecordBatch) -> Vec<Target> {
    let text = |name: &str| {
        batch
            .column_by_name(name)
            .and_then(|c| c.as_any().downcast_ref::<StringArray>().cloned())
    };
    let int = |name: &str| {
        batch
            .column_by_name(name)
            .and_then(|c| c.as_any().downcast_ref::<Int64Array>().cloned())
    };
    let value = batch
        .column_by_name("value")
        .and_then(|c| c.as_any().downcast_ref::<Float64Array>().cloned());
    let (
        Some(measure),
        Some(horizon),
        Some(ti),
        Some(tj),
        Some(value),
        Some(asof),
        Some(computed),
        Some(from),
        Some(through),
    ) = (
        text("measure"),
        text("horizon"),
        text("ticker_i"),
        text("ticker_j"),
        value,
        int("asof_micros"),
        int("computed_micros"),
        int("fit_from_micros"),
        int("fitted_through_micros"),
    )
    else {
        return Vec::new();
    };
    let opt = |a: &Int64Array, i: usize| (!a.is_null(i)).then(|| a.value(i));
    (0..batch.num_rows())
        .filter(|&i| measure.value(i) == MEASURE && !tj.is_null(i))
        .map(|i| Target {
            horizon: horizon.value(i).to_string(),
            ticker_i: ti.value(i).to_string(),
            ticker_j: tj.value(i).to_string(),
            value: (!value.is_null(i)).then(|| value.value(i)),
            asof_micros: asof.value(i),
            computed_micros: computed.value(i),
            fit_from_micros: opt(&from, i),
            fitted_through_micros: opt(&through, i),
        })
        .collect()
}

/// Every pair whose stored R̄ and `derive`'s ρ over the same window differ by
/// more than `bound` in Fisher z.
///
/// `derive` runs per horizon over `[fit_from − width, fitted_through]`: the
/// first fitted return is the one closing at `fit_from + width`, and it needs
/// the closing bar a width before. Each declared venue's candles are read
/// once; a pair is taken from the venue whose ρ holds it.
pub fn reconcile(tape: &Path, venues: &[&str], bound: f64, now_micros: i64) -> Vec<Finding> {
    let targets = newest_targets(tape, now_micros);
    if targets.is_empty() {
        return Vec::new();
    }
    let at = tape.join(format!("kind={}", galata_wire::Kind::Signals));
    // **One read, bounded by the windows the targets were fitted over.** Every
    // window starts a width before its fit (the closing bar of the first
    // return) and ends at what it fitted through.
    let span = targets
        .iter()
        .filter(|t| t.value.is_some())
        .filter_map(|t| {
            let width = width_micros(&t.horizon)?;
            Some((
                t.fit_from_micros?.checked_sub(width)?,
                t.fitted_through_micros?,
            ))
        })
        .fold(None, |acc: Option<(i64, i64)>, (lo, hi)| {
            Some(acc.map_or((lo, hi), |(a, b)| (a.min(lo), b.max(hi))))
        });
    let Some((from, through)) = span else {
        return Vec::new();
    };
    let candles: Vec<Vec<Bar>> = match bars_by_venue(tape, venues, from, through.saturating_add(1))
    {
        Ok(by_venue) => by_venue.into_iter().map(|(bars, _)| bars).collect(),
        // **Said, not swallowed.** A candle dataset that will not read
        // used to come back as no bars, so no findings — a broken tape
        // reported as a clean reconciliation.
        Err(error) => {
            return vec![Finding {
                observed: format!("the candles would not read for reconciliation: {error}"),
                expected: "a readable kind=candles".into(),
                at: tape.join(format!("kind={}", galata_wire::Kind::Candles)),
            }];
        }
    };
    judge(&targets, &candles, bound, &at)
}

/// [`reconcile`] past the reads: `candles` holds each venue's bars.
fn judge(targets: &[Target], candles: &[Vec<Bar>], bound: f64, at: &Path) -> Vec<Finding> {
    // One `derive` per (horizon, window): every pair of a run shares its window.
    let mut windows: BTreeMap<(String, i64, i64), Vec<&Target>> = BTreeMap::new();
    for t in targets {
        if let (Some(from), Some(through), Some(_)) =
            (t.fit_from_micros, t.fitted_through_micros, t.value)
        {
            windows
                .entry((t.horizon.clone(), from, through))
                .or_default()
                .push(t);
        }
    }

    let mut findings = Vec::new();
    for ((horizon, from, through), pairs) in windows {
        // A width the build does not read (1w) has no ρ to compare.
        let Some(width) = width_micros(&horizon) else {
            continue;
        };
        let window = Horizon {
            bucket_secs: width / 1_000_000,
            from_micros: from - width,
            to_micros: through,
            min_observations: 2,
            z: 1.96,
            reference: None,
        };
        let derived: Vec<_> = candles.iter().map(|bars| derive(bars, &window)).collect();
        for t in pairs {
            let key = format!("{}|{}", t.ticker_i, t.ticker_j);
            let rho = derived
                .iter()
                .find_map(|s| match s.correlation.get(&key).map(|p| &p.rho) {
                    Some(Cell::Value { value, n, .. }) => Some((*value, *n)),
                    _ => None,
                });
            let (Some(target), Some((rho, n))) = (t.value, rho) else {
                continue;
            };
            let gap = (fisher(target) - fisher(rho)).abs();
            if gap > bound {
                findings.push(Finding {
                    observed: format!(
                        "{horizon} {key}: the fit's R̄ {target:.3} and derive's ρ {rho:.3} \
                         ({n} returns) differ by {gap:.3} in Fisher z"
                    ),
                    expected: format!("at most {bound} (max_correlation_target_gap)"),
                    at: at.to_path_buf(),
                });
            }
        }
    }
    findings
}

/// atanh, clamped off ±1 where it is infinite.
fn fisher(rho: f64) -> f64 {
    rho.clamp(-1.0 + 1e-12, 1.0 - 1e-12).atanh()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use arrow::array::BooleanArray;

    const HOUR: i64 = 3_600_000_000;
    const T0: i64 = 1_790_000_000_000_000 / HOUR * HOUR;

    /// Hourly closes for two instruments whose returns share a common part:
    /// ρ near 0.8, deterministic.
    fn candles(n: i64) -> Vec<Bar> {
        let mut state: u64 = 7;
        let mut draw = || {
            state = state
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            ((state >> 11) as f64 / (1u64 << 53) as f64) - 0.5
        };
        let (mut btc, mut eth) = (100.0_f64, 100.0_f64);
        let mut bars = Vec::new();
        for i in 0..n {
            let common = draw();
            btc *= (0.01 * (common + 0.5 * draw())).exp();
            eth *= (0.01 * (common + 0.5 * draw())).exp();
            for (ticker, close) in [("BTC", btc), ("ETH", eth)] {
                bars.push(Bar {
                    ticker: ticker.into(),
                    start_micros: T0 + i * HOUR,
                    width_micros: HOUR,
                    close,
                    is_final: true,
                    recv_micros: T0 + (i + 1) * HOUR,
                });
            }
        }
        bars
    }

    fn target(value: Option<f64>) -> Target {
        Target {
            horizon: "1h".into(),
            ticker_i: "BTC".into(),
            ticker_j: "ETH".into(),
            value,
            asof_micros: T0 + 500 * HOUR,
            computed_micros: T0 + 500 * HOUR + 900_000_000,
            // The first fitted return closes at T0 + 2h; the fit is through bar 500.
            fit_from_micros: Some(T0 + HOUR),
            fitted_through_micros: Some(T0 + 500 * HOUR),
        }
    }

    fn rho(bars: &[Bar]) -> f64 {
        let window = Horizon {
            bucket_secs: 3_600,
            from_micros: T0,
            to_micros: T0 + 500 * HOUR,
            min_observations: 2,
            z: 1.96,
            reference: None,
        };
        match &derive(bars, &window).correlation["BTC|ETH"].rho {
            Cell::Value { value, n, .. } => {
                assert_eq!(
                    *n, 499,
                    "every return of the fit window, and none before it"
                );
                *value
            }
            Cell::Absent(a) => panic!("{a:?}"),
        }
    }

    #[test]
    fn a_healthy_fit_is_quiet() {
        let bars = candles(600);
        let near = (fisher(rho(&bars)) + 0.076).tanh(); // the worst gap measured on the record
        let at = Path::new("tape/kind=signals");
        assert!(judge(&[target(Some(near))], &[bars], 0.15, at).is_empty());
    }

    #[test]
    fn a_mislabelled_pair_is_found_with_both_figures() {
        let bars = candles(600);
        let found = judge(
            &[target(Some(0.25))],
            std::slice::from_ref(&bars),
            0.15,
            Path::new("t"),
        );
        assert_eq!(found.len(), 1);
        let rho = format!("{:.3}", rho(&bars));
        let said = found[0].to_string();
        for part in [
            "1h BTC|ETH",
            "R̄ 0.250",
            rho.as_str(),
            "499 returns",
            "at most 0.15",
        ] {
            assert!(said.contains(part), "{part} missing from: {said}");
        }
    }

    #[test]
    fn an_absent_figure_is_not_a_finding() {
        assert!(judge(&[target(None)], &[candles(600)], 0.15, Path::new("t")).is_empty());
        // No candles at all: derive has no ρ, so nothing is compared.
        assert!(judge(&[target(Some(0.25))], &[Vec::new()], 0.15, Path::new("t")).is_empty());
    }

    #[test]
    fn a_week_is_not_a_width_derive_reads() {
        let week = Target {
            horizon: "1w".into(),
            ..target(Some(-0.9))
        };
        assert!(judge(&[week], &[candles(600)], 0.15, Path::new("t")).is_empty());
    }

    /// One stored row, in the signals schema.
    fn stored(horizon: &str, measure: &str, value: f64, asof: i64, computed: i64) -> RecordBatch {
        let text = |v: &str| Arc::new(StringArray::from(vec![v])) as Arc<dyn Array>;
        let int = |v: i64| Arc::new(Int64Array::from(vec![v])) as Arc<dyn Array>;
        RecordBatch::try_new(
            crate::signals::schema(),
            vec![
                text("varcov"),
                text(horizon),
                text(measure),
                text("BTC"),
                text("ETH"),
                int(1),
                Arc::new(Float64Array::from(vec![Some(value)])),
                Arc::new(StringArray::from(vec![None::<&str>])),
                Arc::new(Float64Array::from(vec![Some(32.3)])),
                int(asof),
                int(asof + HOUR),
                int(computed),
                int(asof),
                int(asof - 400 * HOUR),
                text("garch-t/dcc"),
                text("{}"),
                Arc::new(BooleanArray::from(vec![true])),
                Arc::new(BooleanArray::from(vec![false])),
                text("abc"),
                text(&format!("run-{computed}")),
            ],
        )
        .unwrap()
    }

    #[test]
    fn only_the_newest_asof_of_each_horizon_is_read() {
        let tape = tempfile::tempdir().unwrap();
        let (old, new) = (T0, T0 + HOUR);
        let runs = [
            stored("1h", MEASURE, 0.1, old, old + 10),
            stored("1h", MEASURE, 0.8, new, new + 10),
            stored("1h", "correlation", 0.7, new, new + 20),
            stored("4h", MEASURE, 0.6, old, old + 30),
        ];
        for batch in &runs {
            let computed = batch
                .column(11)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0);
            crate::signals::write(
                tape.path(),
                computed,
                &format!("run-{computed}"),
                "abc",
                batch,
            )
            .unwrap();
        }
        let mut read: Vec<_> = newest_targets(tape.path(), new + HOUR)
            .into_iter()
            .map(|t| (t.horizon, t.value, t.fit_from_micros))
            .collect();
        read.sort_by(|a, b| a.0.cmp(&b.0));
        assert_eq!(
            read,
            vec![
                ("1h".to_string(), Some(0.8), Some(new - 400 * HOUR)),
                ("4h".to_string(), Some(0.6), Some(old - 400 * HOUR)),
            ]
        );
    }
}
