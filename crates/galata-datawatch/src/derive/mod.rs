//! Statistics derived from the tape: **a returns grid that respects what the
//! record knows about its own holes**, and volatility, correlation and beta
//! that carry their own provenance.
//!
//! ```text
//!   candles ──▶ per horizon: each bucket's CLOSING bar — the final bar that
//!               ends at the bucket's end, from bars that divide it; a bucket
//!               without one is empty
//!           ──▶ ln(cₜ / cₜ₋₁) between consecutive present buckets: two real
//!               prices exactly one bucket apart, never interpolated
//!           ──▶ σ · √(365·86400/bucket), ρ per pair, β, Fisher interval
//!           ──▶ below the floor: absent, naming the thinner instrument
//! ```
//!
//! **Legacy's method** (`crates/statistics/src/lib.rs`), without polars: *a
//! horizon admits a bar whose interval divides its bucket, and no coarser*;
//! close-to-close as the one estimator that does not understate σ on a market
//! that jumps; Fisher's interval, clamped off ±1. √365, because a venue that
//! never closes is annualised by calendar days (every instrument here was
//! measured continuous, 2026-09-20).
//!
//! **f64 inside, by necessity**: logs and roots. Prices enter as decimals and
//! are converted at the door; nothing here is money, a wire type or a tape
//! column (`check-no-float-money.sh` holds those).

pub mod tape;

use std::collections::{BTreeMap, BTreeSet};

/// One bar of one instrument, as the tape holds it.
#[derive(Debug, Clone, PartialEq)]
pub struct Bar {
    /// The instrument.
    pub ticker: String,
    /// The bar's start, venue time.
    pub start_micros: i64,
    /// Its width.
    pub width_micros: i64,
    /// Its close.
    pub close: f64,
    /// False while it was still forming.
    pub is_final: bool,
    /// When we heard it.
    pub recv_micros: i64,
}

impl Bar {
    fn end_micros(&self) -> i64 {
        self.start_micros + self.width_micros
    }
}

/// When capture heard the stream, per bar width: the receipts of an
/// instrument's forming rows, sorted.
///
/// **Backfilled is judged from these, not from a final bar's receipt.** The
/// stream falls silent on a bar once the next opens and never sends it final;
/// every final comes from a walk, received long after the close. Measured
/// 2026-09-25: with that receipt as the test, every figure read backfilled
/// 1.00, including 15 hours capture heard live. A forming row within one width
/// of a close is evidence capture was listening at it; a recorded gap is not
/// the test, because history walked before capture ever ran has no gap and was
/// never heard either.
struct Heard(BTreeMap<i64, Vec<i64>>);

impl Heard {
    fn of(bars: &[&Bar]) -> Self {
        let mut by_width: BTreeMap<i64, Vec<i64>> = BTreeMap::new();
        for bar in bars.iter().filter(|b| !b.is_final) {
            by_width
                .entry(bar.width_micros)
                .or_default()
                .push(bar.recv_micros);
        }
        for receipts in by_width.values_mut() {
            receipts.sort_unstable();
        }
        Heard(by_width)
    }

    /// Whether a bar of this width was heard forming within one width of
    /// `close`.
    fn at(&self, width: i64, close: i64) -> bool {
        self.0.get(&width).is_some_and(|receipts| {
            let from = receipts.partition_point(|&r| r < close - width);
            receipts.get(from).is_some_and(|&r| r <= close + width)
        })
    }
}

/// What to compute over, and the floors, **with no defaults**.
#[derive(Debug, Clone, PartialEq)]
pub struct Horizon {
    /// The bucket, in seconds.
    pub bucket_secs: i64,
    /// Start of the window, venue time.
    pub from_micros: i64,
    /// End, exclusive.
    pub to_micros: i64,
    /// Returns a cell needs before it is a figure.
    pub min_observations: usize,
    /// Standard errors the interval on ρ spans.
    pub z: f64,
    /// The instrument betas are taken against.
    pub reference: Option<String>,
}

/// Why a cell is not a figure.
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct Absent {
    /// The instrument with fewer returns: for a pair, the thinner.
    pub instrument: String,
    /// How many it had.
    pub count: usize,
    /// How many it needed.
    pub floor: usize,
}

/// A figure, or why not.
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Cell {
    /// The figure, its count and the share of its returns touching a
    /// backfilled bar.
    Value {
        /// The figure.
        value: f64,
        /// Returns it used.
        n: usize,
        /// Of those, the share touching a bar received after it closed.
        backfilled_share: f64,
    },
    /// Below the floor.
    Absent(Absent),
}

/// A correlation's own noise.
#[derive(Debug, Clone, Copy, PartialEq, serde::Serialize)]
pub struct Interval {
    /// Lower end.
    pub low: f64,
    /// Upper end.
    pub high: f64,
}

/// One pair.
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct Pair {
    /// ρ, over the returns both hold at the same bucket.
    pub rho: Cell,
    /// Fisher's interval on ρ, where there are more than three returns.
    pub interval: Option<Interval>,
}

/// One horizon, derived.
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct Statistics {
    /// The bucket, seconds.
    pub bucket_secs: i64,
    /// The window, venue time.
    pub from_micros: i64,
    /// End, exclusive.
    pub to_micros: i64,
    /// The annualisation factor applied to σ.
    pub annualisation: f64,
    /// Annualised close-to-close volatility, per instrument.
    pub volatility: BTreeMap<String, Cell>,
    /// Per pair, `a|b` with `a < b`.
    pub correlation: BTreeMap<String, Pair>,
    /// β of each instrument on the reference.
    pub beta: BTreeMap<String, Cell>,
    /// The reference betas are taken against.
    pub reference: Option<String>,
}

/// How far ρ is clamped off ±1 before `atanh`, which is infinite there.
const RHO_EPSILON: f64 = 1e-12;

/// One instrument's returns: bucket index → (return, touches a backfilled bar).
type Returns = BTreeMap<i64, (f64, bool)>;

/// The grid and its returns for one instrument (S1–S3).
///
/// **A slot is its bucket's closing bar**, the final bar that ends exactly at
/// the bucket's end, so every return is between two real prices exactly one
/// bucket apart. Measured 2026-09-25: a rule that dropped returns across a
/// recorded gap dropped all of a window whose candles the venue had handed
/// back after a 32.6 h outage (2,088 of 2,095 bars backfilled). The prices
/// were real; the gap said only that capture had not been listening. What a
/// gap can really cost — a bucket whose closing bar never came back — is an
/// empty slot, and the returns across it are dropped for that.
fn returns(bars: &[&Bar], horizon: &Horizon) -> Returns {
    let bucket = horizon.bucket_secs * 1_000_000;
    let heard = Heard::of(bars);
    // Slot → (receipt, close, backfilled) of its closing bar, the last heard.
    let mut slots: BTreeMap<i64, (i64, f64, bool)> = BTreeMap::new();
    for bar in bars {
        if !bar.is_final
            // Flagged final before its own close: a walk's open bar, filed
            // final by the rule adapters held until 2026-09-25. Its close is
            // not the bar's.
            || bar.recv_micros < bar.end_micros()
            || bar.width_micros <= 0
            || bucket % bar.width_micros != 0
            || bar.start_micros < horizon.from_micros
            || bar.end_micros() > horizon.to_micros
            || bar.end_micros().rem_euclid(bucket) != 0
        {
            continue;
        }
        let slot = bar.start_micros.div_euclid(bucket);
        let backfilled = !heard.at(bar.width_micros, bar.end_micros());
        let candidate = (bar.recv_micros, bar.close, backfilled);
        slots
            .entry(slot)
            .and_modify(|held| {
                // A final bar re-sent: the last heard.
                if candidate.0 > held.0 {
                    *held = candidate;
                }
            })
            .or_insert(candidate);
    }
    let mut out = Returns::new();
    let mut previous: Option<(i64, f64, bool)> = None;
    for (&slot, &(_, close, backfilled)) in &slots {
        if let Some((prev_slot, prev_close, prev_backfilled)) = previous
            && prev_slot + 1 == slot
            && prev_close > 0.0
            && close > 0.0
        {
            out.insert(
                slot,
                ((close / prev_close).ln(), backfilled || prev_backfilled),
            );
        }
        previous = Some((slot, close, backfilled));
    }
    out
}

fn mean(xs: &[f64]) -> f64 {
    xs.iter().sum::<f64>() / xs.len() as f64
}

fn sample_sd(xs: &[f64]) -> f64 {
    let m = mean(xs);
    (xs.iter().map(|x| (x - m).powi(2)).sum::<f64>() / (xs.len() as f64 - 1.0)).sqrt()
}

fn pearson(xs: &[f64], ys: &[f64]) -> f64 {
    let (mx, my) = (mean(xs), mean(ys));
    let cov: f64 = xs.iter().zip(ys).map(|(x, y)| (x - mx) * (y - my)).sum();
    let vx: f64 = xs.iter().map(|x| (x - mx).powi(2)).sum();
    let vy: f64 = ys.iter().map(|y| (y - my).powi(2)).sum();
    cov / (vx * vy).sqrt()
}

/// Fisher's interval on ρ over `n` returns at `z` standard errors. `None` at
/// three or fewer: `1/√(n − 3)` is undefined there.
pub fn interval(rho: f64, n: usize, z: f64) -> Option<Interval> {
    if n <= 3 || !rho.is_finite() {
        return None;
    }
    let clamped = rho.clamp(-1.0 + RHO_EPSILON, 1.0 - RHO_EPSILON);
    let (centre, se) = (clamped.atanh(), 1.0 / ((n - 3) as f64).sqrt());
    Some(Interval {
        low: (centre - z * se).tanh(),
        high: (centre + z * se).tanh(),
    })
}

fn share(flags: impl Iterator<Item = bool>) -> f64 {
    let (mut n, mut hit) = (0usize, 0usize);
    for flag in flags {
        n += 1;
        hit += usize::from(flag);
    }
    if n == 0 { 0.0 } else { hit as f64 / n as f64 }
}

/// Derive one horizon's statistics from bars and each instrument's gaps.
pub fn derive(bars: &[Bar], horizon: &Horizon) -> Statistics {
    let annualisation = (365.0 * 86_400.0 / horizon.bucket_secs as f64).sqrt();
    let floor = horizon.min_observations;
    let tickers: BTreeSet<&str> = bars.iter().map(|b| b.ticker.as_str()).collect();
    let series: BTreeMap<&str, Returns> = tickers
        .iter()
        .map(|t| {
            let own: Vec<&Bar> = bars.iter().filter(|b| b.ticker == *t).collect();
            (*t, returns(&own, horizon))
        })
        .collect();

    let absent = |instrument: &str, count: usize| {
        Cell::Absent(Absent {
            instrument: instrument.to_string(),
            count,
            floor,
        })
    };

    let mut volatility = BTreeMap::new();
    let mut sigma: BTreeMap<&str, f64> = BTreeMap::new();
    for (t, r) in &series {
        let xs: Vec<f64> = r.values().map(|(x, _)| *x).collect();
        if xs.len() < floor.max(2) {
            volatility.insert(t.to_string(), absent(t, xs.len()));
            continue;
        }
        let sd = sample_sd(&xs);
        sigma.insert(t, sd);
        volatility.insert(
            t.to_string(),
            Cell::Value {
                value: sd * annualisation,
                n: xs.len(),
                backfilled_share: share(r.values().map(|(_, b)| *b)),
            },
        );
    }

    let joint = |a: &str, b: &str| -> (Vec<f64>, Vec<f64>, f64) {
        let (ra, rb) = (&series[a], &series[b]);
        let (mut xs, mut ys, mut flags) = (Vec::new(), Vec::new(), Vec::new());
        for (slot, (x, fa)) in ra {
            if let Some((y, fb)) = rb.get(slot) {
                xs.push(*x);
                ys.push(*y);
                flags.push(*fa || *fb);
            }
        }
        (xs, ys, share(flags.into_iter()))
    };
    let thinner = |a: &str, b: &str| -> String {
        if series[a].len() <= series[b].len() {
            a.to_string()
        } else {
            b.to_string()
        }
    };

    let mut correlation = BTreeMap::new();
    let list: Vec<&str> = tickers.iter().copied().collect();
    for (i, a) in list.iter().enumerate() {
        for b in &list[i + 1..] {
            let (xs, ys, backfilled_share) = joint(a, b);
            let n = xs.len();
            let pair = if n < floor.max(2) {
                Pair {
                    rho: absent(&thinner(a, b), n),
                    interval: None,
                }
            } else {
                let rho = pearson(&xs, &ys);
                Pair {
                    rho: Cell::Value {
                        value: rho,
                        n,
                        backfilled_share,
                    },
                    interval: interval(rho, n, horizon.z),
                }
            };
            correlation.insert(format!("{a}|{b}"), pair);
        }
    }

    let mut beta = BTreeMap::new();
    if let Some(reference) = horizon
        .reference
        .as_deref()
        .filter(|r| series.contains_key(r))
    {
        for t in &list {
            if *t == reference {
                continue;
            }
            let (xs, ys, backfilled_share) = joint(t, reference);
            let n = xs.len();
            let cell = if n < floor.max(2) {
                absent(&thinner(t, reference), n)
            } else {
                let (sx, sy) = (sample_sd(&xs), sample_sd(&ys));
                Cell::Value {
                    value: pearson(&xs, &ys) * sx / sy,
                    n,
                    backfilled_share,
                }
            };
            beta.insert(t.to_string(), cell);
        }
    }

    Statistics {
        bucket_secs: horizon.bucket_secs,
        from_micros: horizon.from_micros,
        to_micros: horizon.to_micros,
        annualisation,
        volatility,
        correlation,
        beta,
        reference: horizon.reference.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MIN: i64 = 60_000_000;

    fn bar(ticker: &str, minute: i64, close: f64) -> Bar {
        Bar {
            ticker: ticker.into(),
            start_micros: minute * MIN,
            width_micros: MIN,
            close,
            is_final: true,
            recv_micros: (minute + 1) * MIN,
        }
    }
    /// A forming row of the bar opening at `minute`, heard `secs` before its
    /// close: evidence capture was listening there.
    fn heard(ticker: &str, minute: i64, secs: i64) -> Bar {
        Bar {
            is_final: false,
            recv_micros: (minute + 1) * MIN - secs * 1_000_000,
            ..bar(ticker, minute, 0.0)
        }
    }
    fn horizon(bucket_secs: i64, minutes: i64, floor: usize) -> Horizon {
        Horizon {
            bucket_secs,
            from_micros: 0,
            to_micros: minutes * MIN,
            min_observations: floor,
            z: 2.0,
            reference: None,
        }
    }
    fn value(cell: &Cell) -> (f64, usize, f64) {
        match cell {
            Cell::Value {
                value,
                n,
                backfilled_share,
            } => (*value, *n, *backfilled_share),
            Cell::Absent(a) => panic!("absent: {a:?}"),
        }
    }

    #[test]
    fn a_forming_bar_is_not_a_close() {
        let mut forming = bar("BTC", 2, 999.0);
        forming.is_final = false;
        let bars = [bar("BTC", 0, 100.0), bar("BTC", 1, 101.0), forming];
        let r = returns(&bars.iter().collect::<Vec<_>>(), &horizon(60, 3, 1));
        assert_eq!(r.len(), 1, "only the return from minute 0 to 1");
    }

    #[test]
    fn a_coarser_bar_is_not_admitted() {
        let mut hourly = bar("BTC", 0, 100.0);
        hourly.width_micros = 60 * MIN;
        let r = returns(&[&hourly], &horizon(300, 120, 1));
        assert!(r.is_empty());
    }

    #[test]
    fn a_missing_bucket_drops_the_returns_across_it() {
        let bars = [
            bar("BTC", 0, 100.0),
            bar("BTC", 2, 102.0),
            bar("BTC", 3, 103.0),
        ];
        let r = returns(&bars.iter().collect::<Vec<_>>(), &horizon(60, 4, 1));
        assert_eq!(
            r.keys().copied().collect::<Vec<_>>(),
            vec![3],
            "none into or out of minute 1"
        );
    }

    #[test]
    fn a_bucket_without_its_closing_bar_is_empty() {
        // Five-minute buckets: minute 4 closes bucket 0, minute 9 closes
        // bucket 1. Bucket 1 holds only minute 6, which is a real price at
        // the wrong moment, so bucket 1 is empty.
        let bars = [
            bar("BTC", 4, 100.0),
            bar("BTC", 6, 101.0),
            bar("BTC", 14, 102.0),
        ];
        let r = returns(&bars.iter().collect::<Vec<_>>(), &horizon(300, 15, 1));
        assert!(r.is_empty(), "{r:?}");
    }

    #[test]
    fn prices_the_venue_handed_back_after_an_outage_are_kept_and_counted() {
        // The measured case: every bar received long after it closed.
        let bars: Vec<Bar> = (0..6)
            .map(|i| {
                let mut b = bar("BTC", i, 100.0 + i as f64);
                b.recv_micros = 1_000 * MIN;
                b
            })
            .collect();
        let s = derive(&bars, &horizon(60, 6, 2));
        let (_, n, backfilled) = value(&s.volatility["BTC"]);
        assert_eq!((n, backfilled), (5, 1.0));
    }

    #[test]
    fn backfill_is_stated_on_the_figure() {
        // Minutes 0, 1 and 3 were heard near their closes; minute 2 was not.
        // Minute 3's evidence is the next bar forming half a minute after it
        // closed: any row within a width of minute 2's close would be heard
        // there too.
        let bars = vec![
            bar("BTC", 0, 100.0),
            heard("BTC", 0, 2),
            bar("BTC", 1, 101.0),
            heard("BTC", 1, 2),
            bar("BTC", 2, 102.0),
            bar("BTC", 3, 100.0),
            heard("BTC", 4, 30),
        ];
        let s = derive(&bars, &horizon(60, 4, 2));
        let (_, n, backfilled) = value(&s.volatility["BTC"]);
        assert_eq!(n, 3);
        assert!(
            (backfilled - 2.0 / 3.0).abs() < 1e-12,
            "two of three returns touch minute 2"
        );
    }

    #[test]
    fn a_bar_heard_live_and_finalised_by_a_walk_is_not_backfilled() {
        // The live case: every final arrives by walk an hour late, and the
        // stream was heard seconds before each close.
        let bars: Vec<Bar> = (0..6)
            .flat_map(|i| {
                let mut walked = bar("BTC", i, 100.0 + i as f64);
                walked.recv_micros = (i + 61) * MIN;
                [walked, heard("BTC", i, 3)]
            })
            .collect();
        let s = derive(&bars, &horizon(60, 6, 2));
        let (_, n, backfilled) = value(&s.volatility["BTC"]);
        assert_eq!((n, backfilled), (5, 0.0));
    }

    #[test]
    fn a_final_received_before_its_close_is_not_a_close() {
        let mut early = bar("BTC", 1, 999.0);
        early.recv_micros = MIN + MIN / 2;
        let bars = [bar("BTC", 0, 100.0), early];
        let r = returns(&bars.iter().collect::<Vec<_>>(), &horizon(60, 2, 1));
        assert!(r.is_empty(), "{r:?}");
    }

    #[test]
    fn a_one_hour_horizon_annualises_by_root_8760() {
        let s = derive(&[], &horizon(3600, 1, 1));
        assert!((s.annualisation - 8760f64.sqrt()).abs() < 1e-9);
    }

    #[test]
    fn a_series_with_itself_is_one() {
        let closes = [100.0, 101.0, 99.5, 102.0, 103.5, 101.0];
        let bars: Vec<Bar> = closes
            .iter()
            .enumerate()
            .flat_map(|(i, c)| [bar("A", i as i64, *c), bar("B", i as i64, c * 2.0)])
            .collect();
        let mut h = horizon(60, 6, 3);
        h.reference = Some("A".into());
        let s = derive(&bars, &h);
        let (rho, n, _) = value(&s.correlation["A|B"].rho);
        assert!((rho - 1.0).abs() < 1e-12);
        assert_eq!(n, 5);
        let (beta, _, _) = value(&s.beta["B"]);
        assert!((beta - 1.0).abs() < 1e-12, "the same returns: β is 1");
    }

    #[test]
    fn a_thin_pair_is_not_a_figure() {
        let bars: Vec<Bar> = (0..4)
            .flat_map(|i| [bar("A", i, 100.0 + i as f64), bar("B", i, 50.0 - i as f64)])
            .collect();
        let s = derive(&bars, &horizon(60, 4, 20));
        match &s.correlation["A|B"].rho {
            Cell::Absent(a) => assert_eq!((a.count, a.floor), (3, 20)),
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn a_strong_correlations_interval_stays_below_one() {
        let i = interval(0.99, 10, 2.0).unwrap();
        assert!(i.high < 1.0 && i.low > -1.0 && i.low < 0.99);
        assert!(interval(0.5, 3, 2.0).is_none());
    }
}
