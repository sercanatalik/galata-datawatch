//! What a venue serves, pages and permits — **declared**, so that no caller
//! carries a constant measured somewhere else.
//!
//! A pace or a reach picked without reference to the declaration is one that is
//! wrong on the next venue. A session lifetime observed at one venue, inherited
//! by a second that has no cap, would rotate a healthy connection on a cadence
//! measured elsewhere.

use std::collections::BTreeMap;

use galata_wire::Series;

/// A venue's stated request budget, from which pacing is computed.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Budget {
    /// Requests the venue states it will serve, per minute.
    pub requests_per_minute: f64,
    /// The minimum the venue *asks* for between historical requests,
    /// independent of the rate.
    pub min_historical_interval_ms: u64,
}

impl Budget {
    /// The interval between historical requests at a declared share of the
    /// budget.
    ///
    /// The **maximum** of the rate-derived interval and the venue's own stated
    /// minimum: a venue that asks for a pause gets one, whatever the arithmetic
    /// says.
    pub fn walk_interval_ms(&self, share: f64) -> u64 {
        let share = share.clamp(0.001, 1.0);
        let per_minute = (self.requests_per_minute * share).max(0.001);
        let from_rate = (60_000.0 / per_minute).round() as u64;
        from_rate.max(self.min_historical_interval_ms)
    }
}

/// Which rows a page holds when more are asked for than one call returns.
///
/// **One venue does both.** Hyperliquid's candle history returns the *most
/// recent* 5,000 rows whatever range is asked; the same venue's funding history
/// returns the *oldest* 500 at or after the start, paged forward. A walk shaped
/// for one skips most of the other's rows and reports success, so the direction
/// is declared beside the page size.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum PageDirection {
    /// The newest rows in the range, up to the page size.
    MostRecent,
    /// The oldest rows at or after the start, up to the page size.
    ForwardFromStart,
}

/// How a venue hands back one series' history.
///
/// **The bound is a row count, not a day count, where the venue holds one.**
/// A venue serving the most recent 5,000 bars per `(coin, interval)` reaches
/// three and a half days at 1m and two hundred at 1h — one declaration, a
/// function of the interval. A day bound stays for a venue that bounds by date.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Paging {
    /// The most rows one call returns.
    pub max_rows_per_call: u32,
    /// The most rows the venue holds per interval. `None` where it states no
    /// bound.
    pub max_rows: Option<u32>,
    /// The furthest back the venue will go, in days. `None` where it states no
    /// bound.
    pub max_history_days: Option<u32>,
    /// Which rows a page holds.
    pub direction: PageDirection,
}

/// Where a forward-paged fetch ended, so the walk can issue the next page or
/// know it has had the last one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PageEnd {
    /// The last row's time.
    pub last_micros: i64,
    /// How many rows the page held. Fewer than the page size means the last.
    pub rows: u32,
}

const MICROS_PER_DAY: i64 = 86_400_000_000;

impl Paging {
    /// A most-recent page, with whichever reach bounds the venue states.
    pub fn most_recent(
        max_rows_per_call: u32,
        max_rows: Option<u32>,
        max_history_days: Option<u32>,
    ) -> Paging {
        Paging {
            max_rows_per_call,
            max_rows,
            max_history_days,
            direction: PageDirection::MostRecent,
        }
    }

    /// A forward page from the start, with no reach bound: the venue's whole
    /// history is in reach, one page at a time.
    pub fn forward_from_start(max_rows_per_call: u32) -> Paging {
        Paging {
            max_rows_per_call,
            max_rows: None,
            max_history_days: None,
            direction: PageDirection::ForwardFromStart,
        }
    }

    /// How far back the venue reaches at one bar width, in microseconds.
    ///
    /// The lesser of `rows x interval` and the day bound, over whichever are
    /// stated. `None` where neither is — an unbounded history.
    pub fn reach_micros(&self, interval_micros: i64) -> Option<i64> {
        let from_rows = self
            .max_rows
            .map(|rows| (rows as i64).saturating_mul(interval_micros.max(0)));
        let from_days = self
            .max_history_days
            .map(|days| (days as i64).saturating_mul(MICROS_PER_DAY));
        match (from_rows, from_days) {
            (Some(r), Some(d)) => Some(r.min(d)),
            (Some(r), None) => Some(r),
            (None, Some(d)) => Some(d),
            (None, None) => None,
        }
    }
}

/// When and how a connection is replaced.
///
/// The invariant is universal — *a handover the system chose to make publishes
/// no gap* — and the policy is the venue's own fact.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ConnectionPolicy {
    /// The venue closes sessions at a measured lifetime, so the replacement is
    /// opened and subscribed **before** the current one closes.
    RotateAhead {
        /// The lifetime observed at the venue.
        observed_lifetime_secs: u64,
        /// When to begin the handover, inside that lifetime.
        rotate_after_secs: u64,
        /// The keepalive, kept anyway.
        keepalive_secs: u64,
    },
    /// The venue states no lifetime. No rotation timer is applied.
    KeepAliveOnly {
        /// How often the venue wants to hear from us.
        keepalive_secs: u64,
    },
}

impl ConnectionPolicy {
    /// How often to send a keepalive.
    pub fn keepalive_secs(&self) -> u64 {
        match self {
            ConnectionPolicy::RotateAhead { keepalive_secs, .. }
            | ConnectionPolicy::KeepAliveOnly { keepalive_secs } => *keepalive_secs,
        }
    }

    /// When the handover begins, where the venue declares a lifetime.
    pub fn rotate_after_secs(&self) -> Option<u64> {
        match self {
            ConnectionPolicy::RotateAhead {
                rotate_after_secs, ..
            } => Some(*rotate_after_secs),
            ConnectionPolicy::KeepAliveOnly { .. } => None,
        }
    }
}

/// Everything a caller may derive behaviour from.
#[derive(Debug, Clone, PartialEq)]
pub struct Declaration {
    /// The series the venue **pushes**. Boot refuses a configuration naming one
    /// that is neither here nor in `historical`.
    pub streams: Vec<Series>,
    /// The series the venue serves **historically**. A venue will hand back
    /// candles; it will not hand back the book as it stood, so a walk attempts
    /// only these and the rest stay gaps.
    pub historical: Vec<Series>,
    /// How each historical series pages. [`Declaration::validate`] holds that
    /// every one has an entry.
    pub paging: BTreeMap<Series, Paging>,
    /// What the venue permits.
    pub budget: Budget,
    /// How a connection is replaced.
    pub connection: ConnectionPolicy,
    /// The websocket endpoint.
    ///
    /// A **code identity, never a configurable endpoint**: a service running
    /// against a venue its configuration did not name is the failure this
    /// prevents.
    pub ws_url: &'static str,
    /// The REST root for history and reference data. Same rule.
    pub rest_url: &'static str,
}

/// Why a declaration is not usable.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
#[non_exhaustive]
pub enum DeclarationError {
    /// A series is declared historical with no paging shape.
    #[error(
        "{series} is declared historical and declares no paging shape — it would walk through \
         another series' plan, and a plan shaped for the wrong page skips rows silently"
    )]
    NoPagingShape {
        /// The offending series.
        series: &'static str,
    },
}

impl Declaration {
    /// Whether the venue pushes a series.
    pub fn serves(&self, series: Series) -> bool {
        self.streams.contains(&series)
    }

    /// Whether the venue hands a series back on request.
    pub fn serves_historically(&self, series: Series) -> bool {
        self.historical.contains(&series)
    }

    /// Whether the venue supplies a series **at all** — pushed or on request.
    ///
    /// Both count, because both reach the record by the same path. A
    /// configuration seam asking *whether* a series can arrive must not refuse
    /// one that arrives by the other route.
    pub fn supplies(&self, series: Series) -> bool {
        self.serves(series) || self.serves_historically(series)
    }

    /// How one series pages, where the venue serves it historically.
    pub fn paging(&self, series: Series) -> Option<&Paging> {
        self.paging.get(&series)
    }

    /// The venue's reach for one series at one bar width.
    pub fn reach_micros(&self, series: Series, interval_micros: i64) -> Option<i64> {
        self.paging(series)
            .and_then(|p| p.reach_micros(interval_micros))
    }

    /// Every historical series has a paging shape.
    pub fn validate(&self) -> Result<(), DeclarationError> {
        for series in &self.historical {
            if !self.paging.contains_key(series) {
                return Err(DeclarationError::NoPagingShape {
                    series: series.as_str(),
                });
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MINUTE: i64 = 60_000_000;
    const HOUR: i64 = 60 * MINUTE;

    fn declaration(historical: Vec<Series>, paging: BTreeMap<Series, Paging>) -> Declaration {
        Declaration {
            streams: vec![Series::Candles, Series::Funding],
            historical,
            paging,
            budget: Budget {
                requests_per_minute: 60.0,
                min_historical_interval_ms: 1_000,
            },
            connection: ConnectionPolicy::KeepAliveOnly { keepalive_secs: 60 },
            ws_url: "wss://example.invalid",
            rest_url: "https://example.invalid",
        }
    }

    #[test]
    fn every_historical_series_declares_its_paging() {
        let missing = declaration(
            vec![Series::Candles, Series::Funding],
            BTreeMap::from([(Series::Candles, Paging::most_recent(720, Some(5_000), None))]),
        );
        let err = missing.validate().unwrap_err();
        assert!(err.to_string().contains("funding"), "{err}");

        let full = declaration(
            vec![Series::Candles, Series::Funding],
            BTreeMap::from([
                (Series::Candles, Paging::most_recent(720, Some(5_000), None)),
                (Series::Funding, Paging::forward_from_start(500)),
            ]),
        );
        full.validate().unwrap();
    }

    #[test]
    fn two_series_on_one_venue_may_page_oppositely() {
        // Not hypothetical: this is Hyperliquid's candles and funding.
        let d = declaration(
            vec![Series::Candles, Series::Funding],
            BTreeMap::from([
                (
                    Series::Candles,
                    Paging::most_recent(5_000, Some(5_000), None),
                ),
                (Series::Funding, Paging::forward_from_start(500)),
            ]),
        );
        assert_eq!(
            d.paging(Series::Candles).unwrap().direction,
            PageDirection::MostRecent
        );
        assert_eq!(
            d.paging(Series::Funding).unwrap().direction,
            PageDirection::ForwardFromStart
        );
    }

    #[test]
    fn a_row_bound_scales_with_the_interval() {
        // Which is why the bound is a ROW COUNT. A day bound cannot say "3.5
        // days at 1m and 208 days at 1h" — it is one number.
        let p = Paging::most_recent(5_000, Some(5_000), None);
        assert_eq!(p.reach_micros(MINUTE), Some(5_000 * MINUTE));
        assert_eq!(p.reach_micros(HOUR), Some(5_000 * HOUR));
    }

    #[test]
    fn both_bounds_stated_the_lesser_wins() {
        let p = Paging::most_recent(720, Some(720), Some(3));
        assert_eq!(
            p.reach_micros(MINUTE),
            Some(720 * MINUTE),
            "720m is under 3d"
        );
        assert_eq!(
            p.reach_micros(24 * HOUR),
            Some(3 * MICROS_PER_DAY),
            "720 days is over 3 days"
        );
    }

    #[test]
    fn neither_bound_is_unbounded() {
        assert_eq!(Paging::forward_from_start(500).reach_micros(MINUTE), None);
    }

    #[test]
    fn a_venue_that_asks_for_a_pause_gets_one() {
        // The rate alone would allow 50 ms here. The venue asked for 1,000.
        let budget = Budget {
            requests_per_minute: 1_200.0,
            min_historical_interval_ms: 1_000,
        };
        assert_eq!(budget.walk_interval_ms(1.0), 1_000);
    }

    #[test]
    fn a_share_of_the_budget_paces_slower_than_all_of_it() {
        let budget = Budget {
            requests_per_minute: 1_200.0,
            min_historical_interval_ms: 0,
        };
        assert_eq!(budget.walk_interval_ms(1.0), 50);
        assert_eq!(budget.walk_interval_ms(0.5), 100);
    }

    #[test]
    fn supplies_counts_both_routes() {
        // A series served only historically still arrives, by the same path.
        // Asking only `serves` would refuse a legitimate configuration at boot.
        let d = declaration(
            vec![Series::Candles],
            BTreeMap::from([(Series::Candles, Paging::forward_from_start(500))]),
        );
        assert!(!d.serves(Series::Trades));
        assert!(d.supplies(Series::Candles));
        assert!(!d.supplies(Series::Book));
    }
}
