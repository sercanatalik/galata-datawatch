//! **On a poll, silence is a gap — and its bounds are exact.**
//!
//! This is the one place the system's central invariant inverts, so it is worth
//! being precise about why.
//!
//! ```text
//!   stream   a quiet market and a dead socket look identical, because
//!            NOTHING HAPPENED either way
//!            → never infer a gap. Invariant 3.
//!
//!   poll     we asked at 12:00:05 and nothing came back
//!            → the asking is an event WE WITNESSED, and the interval is
//!              exactly the cadence
//! ```
//!
//! A stream cannot distinguish silence from absence because it did nothing to
//! distinguish them with. A poll can, because **our own action supplies the
//! missing half**.
//!
//! So a poll gap is not a softer `SessionLost`. It is the only gap in the
//! system whose width is known rather than dated from the last thing that
//! happened to arrive.

use galata_wire::GapCause;

/// A poll cadence, and where it has got to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Cadence {
    /// How often a poll is made.
    pub interval_micros: i64,
    /// The last poll that answered.
    last_answered_micros: Option<i64>,
}

/// What one poll amounted to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Polled {
    /// It answered. No gap.
    Answered,
    /// It did not, and here is the interval that covers.
    Missed {
        /// Start of the interval not covered.
        from_micros: i64,
        /// End of it.
        to_micros: i64,
        /// Which kind of not-answering.
        cause: GapCause,
    },
}

impl Cadence {
    /// A cadence, not yet started.
    pub fn new(interval_micros: i64) -> Cadence {
        Cadence {
            interval_micros: interval_micros.max(1),
            last_answered_micros: None,
        }
    }

    /// When the last poll answered.
    pub fn last_answered(&self) -> Option<i64> {
        self.last_answered_micros
    }

    /// A poll that answered.
    pub fn answered(&mut self, at_micros: i64) -> Polled {
        self.last_answered_micros = Some(at_micros);
        Polled::Answered
    }

    /// A poll that did not.
    ///
    /// The gap runs from **the last poll that answered** to now — not from
    /// `now - interval`, which would be right only if exactly one poll had been
    /// missed. Three consecutive failures are one gap three intervals wide, and
    /// reporting three one-interval gaps would be three claims where there is
    /// one fact.
    ///
    /// **Before any poll has ever answered there is no gap**, for the same
    /// reason a first-ever start publishes none: a gap back to the beginning of
    /// time is not a fact.
    pub fn missed(&mut self, at_micros: i64, cause: GapCause) -> Polled {
        match self.last_answered_micros {
            None => Polled::Answered,
            Some(from) if from >= at_micros => Polled::Answered,
            Some(from) => Polled::Missed {
                from_micros: from,
                to_micros: at_micros,
                cause,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SECOND: i64 = 1_000_000;

    fn cadence() -> Cadence {
        Cadence::new(5 * SECOND)
    }

    #[test]
    fn a_missed_poll_is_bounded_by_exactly_one_interval() {
        // The property that makes this different from every other gap in the
        // system: the width is KNOWN, not dated from the last thing that
        // happened to arrive.
        let mut cadence = cadence();
        cadence.answered(100 * SECOND);
        let missed = cadence.missed(105 * SECOND, GapCause::PollFailed);

        match missed {
            Polled::Missed {
                from_micros,
                to_micros,
                cause,
            } => {
                assert_eq!(to_micros - from_micros, 5 * SECOND, "exactly one interval");
                assert_eq!(cause, GapCause::PollFailed);
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn three_failures_are_one_gap_three_intervals_wide() {
        // Not three gaps. Three claims where there is one fact would make a
        // consumer count an outage three times.
        let mut cadence = cadence();
        cadence.answered(100 * SECOND);
        cadence.missed(105 * SECOND, GapCause::PollFailed);
        cadence.missed(110 * SECOND, GapCause::PollFailed);
        let third = cadence.missed(115 * SECOND, GapCause::PollFailed);

        match third {
            Polled::Missed {
                from_micros,
                to_micros,
                ..
            } => assert_eq!(to_micros - from_micros, 15 * SECOND),
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn a_recovery_closes_the_gap_and_the_next_one_starts_fresh() {
        let mut cadence = cadence();
        cadence.answered(100 * SECOND);
        cadence.missed(110 * SECOND, GapCause::PollFailed);
        cadence.answered(115 * SECOND);

        match cadence.missed(120 * SECOND, GapCause::PollFailed) {
            Polled::Missed { from_micros, .. } => assert_eq!(from_micros, 115 * SECOND),
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn nothing_is_claimed_before_the_first_answer() {
        // The same rule as a first-ever start: a gap back to the beginning of
        // time is not a fact.
        let mut cadence = cadence();
        assert_eq!(
            cadence.missed(100 * SECOND, GapCause::PollFailed),
            Polled::Answered
        );
        assert_eq!(cadence.last_answered(), None);
    }

    #[test]
    fn throttling_and_unreachability_are_different_facts() {
        // One is the venue failing; the other is us yielding. The remedies
        // differ, so a consumer must be able to tell them apart.
        let mut cadence = cadence();
        cadence.answered(100 * SECOND);
        let throttled = cadence.missed(105 * SECOND, GapCause::Throttled);
        let failed = cadence.missed(105 * SECOND, GapCause::PollFailed);
        assert_ne!(throttled, failed);
        assert!(matches!(
            throttled,
            Polled::Missed {
                cause: GapCause::Throttled,
                ..
            }
        ));
    }

    #[test]
    fn a_clock_that_went_backwards_claims_nothing() {
        // A gap that runs backwards is not a gap, and a system whose clock
        // jumped is not a system that should be inventing intervals.
        let mut cadence = cadence();
        cadence.answered(100 * SECOND);
        assert_eq!(
            cadence.missed(90 * SECOND, GapCause::PollFailed),
            Polled::Answered
        );
    }
}
