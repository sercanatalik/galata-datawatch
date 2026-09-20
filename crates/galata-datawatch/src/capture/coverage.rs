//! What was covered, what was not, and why.
//!
//! This is the hard part of capture. Recording what arrives is easy; being able
//! to state afterwards, **from the store alone**, which intervals were not
//! covered is the whole difficulty.
//!
//! Two rules do most of the work:
//!
//! **A gap is dated from the last moment actually covered**, not from when the
//! loss was noticed. Dating it from the moment of noticing understates it by
//! exactly the interval that matters.
//!
//! **Silence is never a gap.** For a push stream you cannot tell *no trades
//! occurred* from *the connection is silently dead*, so nothing is inferred
//! from a quiet window. A quiet market and a halted instrument produce no gap
//! at all — they are not losses, and reporting them as such would bury the real
//! ones.

use std::collections::BTreeMap;

use galata_wire::{Clipped, Gap, GapCause, Series, Ticker};

/// One pair's ledger.
#[derive(Debug, Clone, Copy, Default)]
struct Ledger {
    /// The last moment this pair was covered.
    last_recv_micros: Option<i64>,
    /// Messages in the window being counted.
    count: u32,
    /// When that window began.
    window_started_micros: i64,
}

/// What each declared pair was covered to.
#[derive(Debug, Default)]
pub struct Coverage {
    pairs: BTreeMap<(Ticker, Series), Ledger>,
    /// What gaps are clipped against. Continuous for a venue that never
    /// closes; a calendar otherwise.
    clipped: Clipped,
}

impl Coverage {
    /// A ledger for a venue whose clipping basis is known.
    pub fn new(clipped: Clipped) -> Coverage {
        Coverage {
            pairs: BTreeMap::new(),
            clipped,
        }
    }

    /// Establish that a pair was covered to a moment, without counting a
    /// message. Used on start, so each pair's gap is dated from something the
    /// record actually holds.
    pub fn known(&mut self, ticker: &Ticker, series: Series, at_micros: i64) {
        let entry = self.pairs.entry((ticker.clone(), series)).or_default();
        entry.last_recv_micros = Some(at_micros);
        entry.window_started_micros = at_micros;
    }

    /// A message arrived for a pair.
    pub fn received(&mut self, ticker: &Ticker, series: Series, at_micros: i64) {
        let entry = self.pairs.entry((ticker.clone(), series)).or_default();
        entry.last_recv_micros = Some(at_micros);
        entry.count += 1;
    }

    /// When a pair was last covered.
    pub fn last_recv(&self, ticker: &Ticker, series: Series) -> Option<i64> {
        self.pairs
            .get(&(ticker.clone(), series))
            .and_then(|l| l.last_recv_micros)
    }

    /// Messages counted for a pair in the current window.
    pub fn count(&self, ticker: &Ticker, series: Series) -> u32 {
        self.pairs
            .get(&(ticker.clone(), series))
            .map(|l| l.count)
            .unwrap_or(0)
    }

    /// Every pair the process knows about — declared or merely seen.
    pub fn pairs(&self) -> Vec<(Ticker, Series)> {
        self.pairs.keys().cloned().collect()
    }

    /// Roll the counters for any window that has elapsed.
    ///
    /// **Once the window is over, not once per snapshot.** Rolling per snapshot
    /// would report a pair as quiet between one message and the next.
    pub fn roll_counts(&mut self, now_micros: i64, window_micros: i64) {
        for ledger in self.pairs.values_mut() {
            if now_micros - ledger.window_started_micros >= window_micros {
                ledger.count = 0;
                ledger.window_started_micros = now_micros;
            }
        }
    }

    /// A handover completed. Coverage is continuous across it, so every pair is
    /// covered to this moment and **no gap is published, because none
    /// occurred**.
    pub fn handover_completed(&mut self, at_micros: i64) {
        for ledger in self.pairs.values_mut() {
            if ledger.last_recv_micros.is_some() {
                ledger.last_recv_micros = Some(at_micros);
            }
        }
    }

    /// The gaps for a set of pairs, each **dated from that pair's own** last
    /// covered moment.
    ///
    /// A single interval applied to every pair would claim they were all
    /// covered until the same instant, which they were not.
    pub fn gaps_for(
        &mut self,
        pairs: &[(Ticker, Series)],
        to_micros: i64,
        cause: GapCause,
    ) -> Vec<(Ticker, Gap)> {
        let mut out = Vec::new();
        for (ticker, series) in pairs {
            let Some(from) = self.last_recv(ticker, *series) else {
                // Never covered at all. There is no covered moment to date a
                // gap from, and a gap back to the beginning of time is not a
                // fact.
                continue;
            };
            if from >= to_micros {
                continue;
            }
            out.push((
                ticker.clone(),
                Gap {
                    series: *series,
                    from_micros: from,
                    to_micros,
                    cause,
                    clipped: self.clipped,
                },
            ));
        }
        out
    }

    /// The same, for every pair the ledger knows.
    pub fn gaps_for_all(&mut self, to_micros: i64, cause: GapCause) -> Vec<(Ticker, Gap)> {
        let pairs = self.pairs();
        self.gaps_for(&pairs, to_micros, cause)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SEC: i64 = 1_000_000;

    fn t(name: &str) -> Ticker {
        Ticker::new(name).unwrap()
    }

    #[test]
    fn each_pairs_gap_begins_at_its_own_last_coverage() {
        // One interval applied to every pair would claim they were all covered
        // until the same instant, which they were not.
        let mut c = Coverage::new(Clipped::Continuous);
        c.received(&t("BTC"), Series::Quotes, 10 * SEC);
        c.received(&t("ETH"), Series::Quotes, 4 * SEC);

        let gaps = c.gaps_for_all(20 * SEC, GapCause::SessionLost);
        let btc = gaps.iter().find(|(k, _)| k == &t("BTC")).unwrap();
        let eth = gaps.iter().find(|(k, _)| k == &t("ETH")).unwrap();
        assert_eq!(btc.1.from_micros, 10 * SEC);
        assert_eq!(eth.1.from_micros, 4 * SEC);
        assert_eq!(btc.1.to_micros, 20 * SEC);
    }

    #[test]
    fn a_pair_never_covered_yields_no_gap() {
        // A gap back to the beginning of time is not a fact.
        let mut c = Coverage::new(Clipped::Continuous);
        let gaps = c.gaps_for(&[(t("BTC"), Series::Quotes)], 20 * SEC, GapCause::Downtime);
        assert!(gaps.is_empty());
    }

    #[test]
    fn a_rotation_publishes_no_gap() {
        // The handover advanced every pair's coverage, so there is nothing to
        // report — because nothing was missed.
        let mut c = Coverage::new(Clipped::Continuous);
        c.received(&t("BTC"), Series::Quotes, 10 * SEC);
        c.handover_completed(480 * SEC);

        let gaps = c.gaps_for_all(480 * SEC, GapCause::RotationFailed);
        assert!(gaps.is_empty(), "a handover the system chose is not a loss");
    }

    #[test]
    fn a_quiet_interval_publishes_nothing_by_itself() {
        // Nothing here infers a gap from the passage of time. A gap is only
        // produced when a CALLER names an event — a lost session, a restart.
        let mut c = Coverage::new(Clipped::Continuous);
        c.received(&t("BTC"), Series::Quotes, 10 * SEC);
        // An hour later, still nothing asked for.
        assert_eq!(c.last_recv(&t("BTC"), Series::Quotes), Some(10 * SEC));
    }

    #[test]
    fn counters_roll_once_the_window_elapses() {
        // Not once per snapshot, which would report a pair as quiet between
        // one message and the next.
        let mut c = Coverage::new(Clipped::Continuous);
        c.known(&t("BTC"), Series::Quotes, 0);
        c.received(&t("BTC"), Series::Quotes, SEC);
        c.received(&t("BTC"), Series::Quotes, 2 * SEC);
        assert_eq!(c.count(&t("BTC"), Series::Quotes), 2);

        c.roll_counts(30 * SEC, 60 * SEC);
        assert_eq!(c.count(&t("BTC"), Series::Quotes), 2, "window not over");

        c.roll_counts(60 * SEC, 60 * SEC);
        assert_eq!(c.count(&t("BTC"), Series::Quotes), 0);
    }

    #[test]
    fn an_unknown_calendar_overstates_rather_than_erases() {
        let mut c = Coverage::default();
        c.received(&t("BTC"), Series::Quotes, 0);
        let gaps = c.gaps_for_all(SEC, GapCause::Downtime);
        assert_eq!(gaps[0].1.clipped, Clipped::Assumed24h);
    }

    #[test]
    fn a_pair_seen_but_not_declared_still_appears() {
        // So a consumer can tell "not subscribed" from "monitoring is broken".
        let mut c = Coverage::new(Clipped::Continuous);
        c.received(&t("DOGE"), Series::Trades, SEC);
        assert!(c.pairs().contains(&(t("DOGE"), Series::Trades)));
    }
}
