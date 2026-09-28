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
    /// Where this pair's last **published** gap ended, and why.
    ///
    /// A gap is accounting, not coverage: until something covers the pair
    /// again, the time after it is still not covered, and
    /// [`Coverage::asked_again`] says so.
    gapped: Option<Gapped>,
}

/// A pair's last published gap.
#[derive(Debug, Clone, Copy)]
struct Gapped {
    /// Where it ended.
    end_micros: i64,
    /// Why.
    cause: GapCause,
    /// **Still open**: published by a loss or a restart and not yet closed by
    /// asking the stream again. Once asked, the pair is listened to, and a
    /// quiet pair's silence after that is not a gap.
    open: bool,
}

/// What each declared pair was covered to.
#[derive(Debug, Default)]
pub struct Coverage {
    pairs: BTreeMap<(Ticker, Series), Ledger>,
    /// **When the counting window opened, for every pair at once.**
    ///
    /// One window rather than one per pair. Per pair, each began at that
    /// pair's first message and rolled on its own schedule — so at any moment
    /// the counts were over windows of different ages, and comparing two of
    /// them compared different denominators.
    ///
    /// **Measured 2026-09-21**, against what the record shows actually
    /// arrived in the sixty seconds before the snapshot:
    ///
    /// ```text
    ///   BTC trades    reported 181   arrived 328    55%
    ///   BTC quotes    reported 379   arrived 616    62%
    ///   BTC candles   reported  74   arrived 123    60%
    ///   BTC funding   reported  37   arrived  59    63%
    /// ```
    window_started_micros: Option<i64>,
    /// What gaps are clipped against. Continuous for a venue that never
    /// closes; a calendar otherwise.
    clipped: Clipped,
}

impl Coverage {
    /// A ledger for a venue whose clipping basis is known.
    pub fn new(clipped: Clipped) -> Coverage {
        Coverage {
            pairs: BTreeMap::new(),
            window_started_micros: None,
            clipped,
        }
    }

    /// Establish that a pair was covered to a moment, without counting a
    /// message. Used on start, so each pair's gap is dated from something the
    /// record actually holds.
    pub fn known(&mut self, ticker: &Ticker, series: Series, at_micros: i64) {
        // **The window opens when observation does**, not on the first roll:
        // a window anchored to the first `roll_counts` would be shorter than
        // it should be by however long the process took to get there.
        self.window_started_micros.get_or_insert(at_micros);
        let entry = self.pairs.entry((ticker.clone(), series)).or_default();
        entry.last_recv_micros = Some(at_micros);
    }

    /// A message arrived for a pair.
    pub fn received(&mut self, ticker: &Ticker, series: Series, at_micros: i64) {
        self.window_started_micros.get_or_insert(at_micros);
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
        let Some(opened) = self.window_started_micros else {
            // Nothing has been observed, so no window is open and there is
            // nothing to roll.
            return;
        };
        if now_micros - opened >= window_micros {
            for ledger in self.pairs.values_mut() {
                ledger.count = 0;
            }
            self.window_started_micros = Some(now_micros);
        }
    }

    /// How long the counting window has been open.
    ///
    /// **Reported beside the counts**, because a count without its window is
    /// not a rate. The field used to be called `count_1m` and held whatever
    /// had arrived since that pair's own window opened — between 55% and 63%
    /// of a minute in the run that found it, differing per pair.
    pub fn window_age_micros(&self, now_micros: i64) -> i64 {
        self.window_started_micros
            .map(|opened| (now_micros - opened).max(0))
            .unwrap_or(0)
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
            let Some(covered) = self.last_recv(ticker, *series) else {
                // Never covered at all. There is no covered moment to date a
                // gap from, and a gap back to the beginning of time is not a
                // fact.
                continue;
            };
            // **From the later of covered and already gapped**, so a second
            // loss before any frame does not publish the first one's span
            // again.
            let gapped = self
                .pairs
                .get(&(ticker.clone(), *series))
                .and_then(|l| l.gapped)
                .map(|g| g.end_micros);
            let from = gapped.map_or(covered, |end| end.max(covered));
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
                    dex: None,
                },
            ));
        }
        self.note_gapped(&out, true);
        out
    }

    fn note_gapped(&mut self, gaps: &[(Ticker, Gap)], open: bool) {
        for (ticker, gap) in gaps {
            if let Some(ledger) = self.pairs.get_mut(&(ticker.clone(), gap.series)) {
                ledger.gapped = Some(Gapped {
                    end_micros: gap.to_micros,
                    cause: gap.cause,
                    open,
                });
            }
        }
    }

    /// **The tail of a gap that is still open**, up to the moment the stream
    /// was asked again.
    ///
    /// For each pair whose last accounting is a published gap, with nothing
    /// covered since, a gap from that gap's end to `at_micros`, with the same
    /// cause. The end is the moment subscriptions were **sent**: a fact about
    /// us. The first frame would be an inference from silence, since a closed
    /// market's first trade on Monday would date a gap across the weekend.
    ///
    /// Measured 2026-09-28: a restart published 2.7 s of `downtime`, then
    /// walked for a minute before subscribing, and the ~58 s between was in no
    /// gap. A pair the walk covered after its gap (candles, funding) has
    /// nothing to add here.
    pub fn asked_again(
        &mut self,
        pairs: &[(Ticker, Series)],
        at_micros: i64,
    ) -> Vec<(Ticker, Gap)> {
        let mut out = Vec::new();
        for (ticker, series) in pairs {
            let Some(ledger) = self.pairs.get(&(ticker.clone(), *series)) else {
                continue;
            };
            let Some(Gapped {
                end_micros: from,
                cause,
                open: true,
            }) = ledger.gapped
            else {
                continue;
            };
            let covered_since = ledger.last_recv_micros.is_some_and(|recv| recv >= from);
            if covered_since || from >= at_micros {
                continue;
            }
            out.push((
                ticker.clone(),
                Gap {
                    series: *series,
                    from_micros: from,
                    to_micros: at_micros,
                    cause,
                    clipped: self.clipped,
                    dex: None,
                },
            ));
        }
        self.note_gapped(&out, false);
        // And every pair asked, tail or not: it is listened to from here.
        for pair in pairs {
            if let Some(Gapped { open, .. }) =
                self.pairs.get_mut(pair).and_then(|l| l.gapped.as_mut())
            {
                *open = false;
            }
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
    fn every_pair_counts_over_the_same_window() {
        // **Per pair, each window began at that pair's first message.** Two
        // counts were then over different spans, and nothing said so. Measured
        // against the record: between 55% and 63% of a minute, differing per
        // pair.
        let mut c = Coverage::new(Clipped::Continuous);
        c.received(&t("BTC"), Series::Quotes, 0);
        // ETH starts thirty seconds later.
        c.received(&t("ETH"), Series::Quotes, 30 * SEC);

        // Half a minute on, the window is 45 s old — for both of them.
        assert_eq!(c.window_age_micros(45 * SEC), 45 * SEC);

        // And both roll together, on the window that opened first.
        c.roll_counts(60 * SEC, 60 * SEC);
        assert_eq!(c.count(&t("BTC"), Series::Quotes), 0);
        assert_eq!(c.count(&t("ETH"), Series::Quotes), 0);
        assert_eq!(
            c.window_age_micros(60 * SEC),
            0,
            "the new window just opened"
        );
    }

    #[test]
    fn a_window_that_never_opened_has_no_age() {
        // Nothing observed yet is not "a full window of silence".
        let mut c = Coverage::new(Clipped::Continuous);
        assert_eq!(c.window_age_micros(100 * SEC), 0);
        c.roll_counts(100 * SEC, 60 * SEC);
        assert_eq!(c.window_age_micros(100 * SEC), 0);
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

    #[test]
    fn a_second_loss_before_any_frame_starts_where_the_first_ended() {
        let mut c = Coverage::new(Clipped::Continuous);
        c.received(&t("BTC"), Series::Quotes, 10 * SEC);
        let first = c.gaps_for_all(20 * SEC, GapCause::SessionLost);
        assert_eq!(first[0].1.from_micros, 10 * SEC);
        let second = c.gaps_for_all(50 * SEC, GapCause::SessionLost);
        assert_eq!(second.len(), 1);
        assert_eq!(second[0].1.from_micros, 20 * SEC, "no span published twice");
    }

    #[test]
    fn asked_again_closes_an_open_gap_and_only_that() {
        let mut c = Coverage::new(Clipped::Continuous);
        let pairs = [(t("BTC"), Series::Quotes), (t("BTC"), Series::Candles)];
        c.known(&t("BTC"), Series::Quotes, 10 * SEC);
        c.known(&t("BTC"), Series::Candles, 10 * SEC);
        c.gaps_for(&pairs, 20 * SEC, GapCause::Downtime);
        // The walk brought candles in after the gap: covered, so no tail.
        c.received(&t("BTC"), Series::Candles, 25 * SEC);

        let tails = c.asked_again(&pairs, 80 * SEC);
        assert_eq!(tails.len(), 1, "{tails:?}");
        let (ticker, gap) = &tails[0];
        assert_eq!(ticker.as_str(), "BTC");
        assert_eq!(gap.series, Series::Quotes);
        assert_eq!((gap.from_micros, gap.to_micros), (20 * SEC, 80 * SEC));
        assert_eq!(gap.cause, GapCause::Downtime);

        assert!(c.asked_again(&pairs, 90 * SEC).is_empty(), "closed once");
    }

    #[test]
    fn a_pair_never_gapped_has_no_tail() {
        let mut c = Coverage::new(Clipped::Continuous);
        c.received(&t("BTC"), Series::Quotes, 10 * SEC);
        assert!(
            c.asked_again(&[(t("BTC"), Series::Quotes)], 99 * SEC)
                .is_empty()
        );
    }
}
