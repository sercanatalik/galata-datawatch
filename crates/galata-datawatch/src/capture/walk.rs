//! Deciding what history to ask for: where to resume, how far the venue
//! reaches, how fast, and what it says when either bound stops it.
//!
//! **The cursor is implicit.** Where a walk resumes is derived from what the
//! record already holds, never from a cursor file kept beside it. A separate
//! cursor is a second source of truth about the same fact, and losing it is
//! undetectable — the walk would silently start from the beginning, or from
//! nowhere, and every component would report success.
//!
//! **But the record's clock is a receipt clock**, and that is the trap this
//! module is shaped around. A segment is named for when bytes *arrived*, not
//! for the range they cover — so a candle page covering last March, fetched
//! today, lands in a segment named today. For the bar width the live stream
//! pushes, that is harmless: capture keeps the receipt within seconds of now
//! and the stream covers the bars anyway. For **any other** bar width it is
//! ruinous, and an hourly walk resuming from it would ask for one minute and
//! report success. So a bar width the stream does not push states what it
//! needs, and asks for that on every boot. See [`WalkInterval`].
//!
//! **The ask is clipped to the venue's reach, and the report says so.**
//! Hyperliquid holds the most recent 5,000 bars per interval whatever range is
//! asked: a seven-day cold start at 1m is a 3.5-day one, and a walk reporting
//! *covered* over the difference is the shape of a hole nobody looks for. The
//! venue's bound is a stated fact and exits zero; **our own cap** truncating
//! exits non-zero, because that one is ours to raise.
//!
//! **Every fetch overlaps.** A venue's handling of a chunk boundary is its own
//! business, and re-fetching a little is cheaper than discovering a one-record
//! hole in month four. The duplicate costs nothing downstream because identity
//! is content-derived: a re-fetched bar is the same bar.
//!
//! Nothing here reads a clock and nothing here fetches. It plans; the loop
//! executes and pays for the requests.

use galata_wire::Series;

use crate::record::Archive;
use crate::venue::{Declaration, PageDirection};

const MICROS_PER_MINUTE: i64 = 60_000_000;
const MICROS_PER_HOUR: i64 = 60 * MICROS_PER_MINUTE;
const MICROS_PER_DAY: i64 = 24 * MICROS_PER_HOUR;

/// A span in the units a person reads: `62d`, `3d 11h 20m`, `1h`, `0m`.
pub fn span(micros: i64) -> String {
    let micros = micros.max(0);
    let (d, rem) = (micros / MICROS_PER_DAY, micros % MICROS_PER_DAY);
    let (h, rem) = (rem / MICROS_PER_HOUR, rem % MICROS_PER_HOUR);
    let m = rem / MICROS_PER_MINUTE;
    let mut parts = Vec::new();
    if d > 0 {
        parts.push(format!("{d}d"));
    }
    if h > 0 {
        parts.push(format!("{h}h"));
    }
    if m > 0 || parts.is_empty() {
        parts.push(format!("{m}m"));
    }
    parts.join(" ")
}

/// One request the walk intends to make, at one bar width.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Step {
    /// Which series.
    pub series: Series,
    /// The bar width.
    pub interval_micros: i64,
    /// The range's start.
    pub from_micros: i64,
    /// The range's end.
    pub to_micros: i64,
}

/// One bar width the walk fetches, and how far back it wants it.
///
/// **The distinction exists because the record is dated by receipt.** The bar
/// width the venue pushes live keeps the record's clock within seconds of now,
/// so resuming from it is right. Every other width must say how much it needs,
/// because the record cannot tell it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WalkInterval {
    /// The bar width.
    pub interval_micros: i64,
    /// How far back this width is needed. `None` for the width the stream
    /// pushes, which resumes from the record.
    pub need_micros: Option<i64>,
}

impl WalkInterval {
    /// The width the stream pushes: resumes from what the record holds.
    pub fn live(interval_micros: i64) -> WalkInterval {
        WalkInterval {
            interval_micros,
            need_micros: None,
        }
    }

    /// A width the stream does not push: asks for `need_micros` back, every
    /// boot, and lets the duplicate rows be collapsed downstream.
    pub fn needed(interval_micros: i64, need_micros: i64) -> WalkInterval {
        WalkInterval {
            interval_micros,
            need_micros: Some(need_micros),
        }
    }

    /// Whether this width resumes from the record.
    pub fn is_live(&self) -> bool {
        self.need_micros.is_none()
    }
}

/// What the walk asked for at one width, and where the plan begins after the
/// venue's reach clipped it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Ask {
    /// The bar width.
    pub interval_micros: i64,
    /// Where the walk wanted to begin.
    pub asked_from_micros: i64,
    /// Where the venue's declared reach begins, when it declares one.
    pub venue_reach_micros: Option<i64>,
    /// Where the plan begins: the later of the two.
    pub from_micros: i64,
}

/// How a walk ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalkOutcome {
    /// Which series.
    pub series: Series,
    /// The bar width.
    pub interval_micros: i64,
    /// Where the walk wanted to begin.
    pub asked_from_micros: i64,
    /// Where the venue's reach begins, if it states one.
    pub venue_reach_micros: Option<i64>,
    /// Where the plan began — the ask, clipped to the reach.
    pub requested_from_micros: i64,
    /// Where it was asked to reach.
    pub requested_to_micros: i64,
    /// How far it actually got.
    pub reached_micros: i64,
    /// Whether **our** cap stopped it short. The venue's reach never sets
    /// this: a bound the venue states is a fact, not a truncation.
    pub capped: bool,
    /// How many requests were made.
    pub requests_made: u32,
    /// Paged forward from the start rather than by spans of recent rows.
    pub forward: bool,
}

impl WalkOutcome {
    /// Whether the venue's reach cut the ask short.
    pub fn clipped_by_reach(&self) -> bool {
        self.venue_reach_micros
            .is_some_and(|reach| reach > self.asked_from_micros)
    }

    /// A line an operator can act on: what was asked, what the venue holds,
    /// and what was covered — **all three**, because "covered 3d" alone cannot
    /// be told from "covered 3d of the 7d asked".
    pub fn report(&self) -> String {
        let what = match self.series {
            Series::Candles => format!("candles at {}", span(self.interval_micros)),
            other if self.forward => format!("{}, paged forward", other.as_str()),
            other => other.as_str().to_string(),
        };
        let asked = span(self.requested_to_micros - self.asked_from_micros);
        let holds = match self.venue_reach_micros {
            Some(reach) => span(self.requested_to_micros - reach),
            None => "no stated bound".to_string(),
        };
        let covered = span(self.reached_micros - self.requested_from_micros);
        if self.capped {
            format!(
                "walk of {what} truncated by its cap: asked {asked}, the venue holds {holds}, \
                 covered {covered} ({}..{}) in {} requests",
                self.requested_from_micros, self.reached_micros, self.requests_made
            )
        } else {
            format!(
                "walk of {what}: asked {asked}, the venue holds {holds}, covered {covered} \
                 ({}..{}) in {} requests",
                self.requested_from_micros, self.requested_to_micros, self.requests_made
            )
        }
    }

    /// Zero unless **our own** cap stopped it short.
    ///
    /// A run that silently covered less than asked is the shape of a hole
    /// nobody looks for, so an operator sees a failed unit. A bound the venue
    /// states is not that: it is a fact, reported and exited zero, because no
    /// amount of raising our cap would change it.
    pub fn exit_code(&self) -> i32 {
        i32::from(self.capped)
    }
}

/// The walk's plan for one venue.
#[derive(Debug)]
pub struct Walk<'a> {
    declaration: &'a Declaration,
    /// The share of the venue's stated budget this walk may take.
    walk_share: f64,
    /// How far back a cold start goes.
    cold_start_days: u32,
    /// The most requests one run will make — a bound so a run says so rather
    /// than spending a budget nobody watched.
    cap: u32,
    /// How far successive backward steps overlap.
    overlap_micros: i64,
}

impl<'a> Walk<'a> {
    /// A planner for one venue.
    pub fn new(
        declaration: &'a Declaration,
        walk_share: f64,
        cold_start_days: u32,
        cap: u32,
    ) -> Walk<'a> {
        Walk {
            declaration,
            walk_share,
            cold_start_days,
            cap,
            // One minute. Enough to cover a venue's own boundary handling for a
            // one-minute bar, and small enough that the duplicate costs nothing.
            overlap_micros: MICROS_PER_MINUTE,
        }
    }

    /// The interval between requests, from the venue's stated budget and the
    /// declared share. **No interval constant appears here.**
    pub fn request_interval_ms(&self) -> u64 {
        self.declaration.budget.walk_interval_ms(self.walk_share)
    }

    /// Which series this venue serves historically. The rest stay gaps, and are
    /// published as such rather than left as absences.
    pub fn walkable(&self, declared: &[Series]) -> Vec<Series> {
        let mut out: Vec<Series> = declared
            .iter()
            .copied()
            .filter(|s| self.declaration.serves_historically(*s))
            .collect();
        out.sort();
        out.dedup();
        out
    }

    /// How the venue pages a series.
    pub fn direction(&self, series: Series) -> PageDirection {
        self.declaration
            .paging(series)
            .map(|p| p.direction)
            .unwrap_or(PageDirection::MostRecent)
    }

    /// The most rows one call returns, for telling a full page from the last.
    pub fn page_rows(&self, series: Series) -> u32 {
        self.declaration
            .paging(series)
            .map(|p| p.max_rows_per_call)
            .unwrap_or(0)
    }

    /// Where a live width resumes, **read from the record's own contents**.
    ///
    /// Not a bookmark. A bookmark can disagree with the data: delete the store
    /// and keep the bookmark, and the walk is convinced of coverage that is not
    /// there. There is nothing to keep in sync because there is nothing to keep.
    ///
    /// A cold start covers the last `cold_start_days`, bounded by the venue's
    /// own maximum history where it states one in days.
    pub fn resume_from(
        &self,
        archive: &Archive,
        venue: &str,
        series: Series,
        now_micros: i64,
    ) -> i64 {
        let cold_start = {
            let venue_max = self
                .declaration
                .paging(series)
                .and_then(|p| p.max_history_days);
            let days = match venue_max {
                Some(venue_max) => self.cold_start_days.min(venue_max),
                None => self.cold_start_days,
            };
            now_micros - (days as i64) * MICROS_PER_DAY
        };

        match archive.last_durable_for(venue, series.as_str()) {
            // Resume from what the record already holds, less the overlap.
            Some(last) => (last - self.overlap_micros).max(cold_start),
            None => cold_start,
        }
    }

    /// Where the venue's reach begins, when it states one.
    pub fn reach_from(&self, series: Series, interval_micros: i64, now_micros: i64) -> Option<i64> {
        self.declaration
            .reach_micros(series, interval_micros)
            .map(|reach| now_micros - reach)
    }

    /// What to ask for at one width: a live one resumes from the record, one
    /// the stream does not push asks for what it needs. Both are clipped to the
    /// venue's reach.
    pub fn ask(
        &self,
        archive: &Archive,
        venue: &str,
        series: Series,
        interval: WalkInterval,
        now_micros: i64,
    ) -> Ask {
        let asked_from = match interval.need_micros {
            None => self.resume_from(archive, venue, series, now_micros),
            Some(need) => now_micros - need,
        };
        let venue_reach = self.reach_from(series, interval.interval_micros, now_micros);
        Ask {
            interval_micros: interval.interval_micros,
            asked_from_micros: asked_from,
            venue_reach_micros: venue_reach,
            from_micros: match venue_reach {
                Some(reach) => asked_from.max(reach),
                None => asked_from,
            },
        }
    }

    /// The steps covering `from..to` at one bar width, each overlapping the one
    /// before. Nothing before `from` is named, so a clipped ask makes no request
    /// the venue cannot answer.
    ///
    /// Bounded by the cap: the plan states what it will do, and
    /// [`Walk::outcome`] says whether that was everything asked for.
    ///
    /// A series paged **forward from the start** is one step over the whole
    /// range: the venue answers the oldest page at or after the start, and the
    /// loop issues the next from where that page ended. A span plan would name
    /// starts the venue skips past — 480 of every 500 hours on Hyperliquid's
    /// funding.
    pub fn plan(
        &self,
        series: Series,
        from_micros: i64,
        to_micros: i64,
        bar_micros: i64,
    ) -> Vec<Step> {
        if from_micros >= to_micros {
            return Vec::new();
        }
        if self.direction(series) == PageDirection::ForwardFromStart {
            return vec![Step {
                series,
                interval_micros: bar_micros,
                from_micros,
                to_micros,
            }];
        }
        let span = (self.page_rows(series) as i64) * bar_micros;
        if span <= 0 {
            return Vec::new();
        }
        let mut steps = Vec::new();
        let mut cursor = from_micros;
        while cursor < to_micros && (steps.len() as u32) < self.cap {
            let end = (cursor + span).min(to_micros);
            steps.push(Step {
                series,
                interval_micros: bar_micros,
                from_micros: cursor,
                to_micros: end,
            });
            if end >= to_micros {
                break;
            }
            // Overlap rather than abut: a venue's handling of a chunk boundary
            // is its own business.
            cursor = end - self.overlap_micros;
        }
        steps
    }

    /// What a plan amounts to, over `instruments` fetched per step.
    pub fn outcome(
        &self,
        series: Series,
        steps: &[Step],
        ask: &Ask,
        to_micros: i64,
        instruments: usize,
    ) -> WalkOutcome {
        let reached = steps.last().map(|s| s.to_micros).unwrap_or(ask.from_micros);
        WalkOutcome {
            series,
            interval_micros: ask.interval_micros,
            asked_from_micros: ask.asked_from_micros,
            venue_reach_micros: ask.venue_reach_micros,
            requested_from_micros: ask.from_micros,
            requested_to_micros: to_micros,
            reached_micros: reached,
            capped: reached < to_micros,
            requests_made: (steps.len() * instruments) as u32,
            forward: self.direction(series) == PageDirection::ForwardFromStart,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::venue::{Budget, ConnectionPolicy, Paging};
    use std::collections::BTreeMap;

    const MINUTE: i64 = MICROS_PER_MINUTE;
    const HOUR: i64 = MICROS_PER_HOUR;
    const DAY: i64 = MICROS_PER_DAY;

    fn declaration(max_history_days: Option<u32>, max_rows: Option<u32>) -> Declaration {
        Declaration {
            streams: vec![Series::Trades, Series::Quotes, Series::Candles],
            historical: vec![Series::Candles, Series::Funding],
            paging: BTreeMap::from([
                (
                    Series::Candles,
                    Paging::most_recent(5_000, max_rows, max_history_days),
                ),
                (Series::Funding, Paging::forward_from_start(500)),
            ]),
            budget: Budget {
                requests_per_minute: 1_200.0,
                min_historical_interval_ms: 100,
            },
            connection: ConnectionPolicy::KeepAliveOnly { keepalive_secs: 20 },
            ws_url: "wss://example.invalid",
            rest_url: "https://example.invalid",
        }
    }

    fn walk(d: &Declaration) -> Walk<'_> {
        Walk::new(d, 0.5, 7, 500)
    }

    #[test]
    fn the_resume_point_comes_from_the_record_and_no_cursor_file_exists() {
        // A separate cursor is a second source of truth about the same fact,
        // and losing it is undetectable.
        let root = tempfile::tempdir().unwrap();
        let archive = Archive::open(root.path());
        let d = declaration(None, None);
        let now = 100 * DAY;

        assert_eq!(
            walk(&d).resume_from(&archive, "hyperliquid", Series::Candles, now),
            now - 7 * DAY,
            "an empty record starts cold"
        );

        let files: Vec<String> = std::fs::read_dir(root.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().to_string())
            .collect();
        assert!(
            !files
                .iter()
                .any(|f| f.contains("cursor") || f.contains("bookmark")),
            "{files:?}"
        );
    }

    #[test]
    fn a_cold_start_is_bounded_by_the_venues_own_history() {
        // Asking for seven days of a venue that states three is asking for a
        // range it will not answer.
        let d = declaration(Some(3), None);
        let root = tempfile::tempdir().unwrap();
        let archive = Archive::open(root.path());
        let now = 100 * DAY;
        assert_eq!(
            walk(&d).resume_from(&archive, "hyperliquid", Series::Candles, now),
            now - 3 * DAY
        );
    }

    #[test]
    fn a_width_the_stream_does_not_push_asks_for_what_it_needs() {
        // THE trap: the record is dated by RECEIPT. Live capture keeps that
        // within seconds of now, so an hourly walk resuming from it would ask
        // for one minute and report success.
        let root = tempfile::tempdir().unwrap();
        let archive = Archive::open(root.path());
        let d = declaration(None, None);
        let w = walk(&d);
        let now = 100 * DAY;

        let live = w.ask(
            &archive,
            "hyperliquid",
            Series::Candles,
            WalkInterval::live(MINUTE),
            now,
        );
        let needed = w.ask(
            &archive,
            "hyperliquid",
            Series::Candles,
            WalkInterval::needed(HOUR, 60 * DAY),
            now,
        );
        assert_eq!(
            live.asked_from_micros,
            now - 7 * DAY,
            "resumes from the record"
        );
        assert_eq!(
            needed.asked_from_micros,
            now - 60 * DAY,
            "asks for its need"
        );
        assert!(!WalkInterval::needed(HOUR, 60 * DAY).is_live());
    }

    #[test]
    fn steps_overlap_rather_than_abut() {
        // A venue's handling of a chunk boundary is its own business, and
        // abutting assumes it is exclusive at one end.
        let d = declaration(None, None);
        let w = walk(&d);
        let page = 5_000 * MINUTE;
        let steps = w.plan(Series::Candles, 0, page * 3, MINUTE);
        assert!(steps.len() >= 3);
        for pair in steps.windows(2) {
            assert!(
                pair[1].from_micros < pair[0].to_micros,
                "steps must overlap: {:?} then {:?}",
                pair[0],
                pair[1]
            );
            assert!(pair[0].to_micros - pair[0].from_micros <= page);
        }
    }

    #[test]
    fn a_forward_series_gets_one_open_step() {
        // A span plan would name starts the venue skips past: 480 of every 500
        // hours on Hyperliquid's funding.
        let d = declaration(None, None);
        let steps = walk(&d).plan(Series::Funding, 0, 500 * DAY, HOUR);
        assert_eq!(steps.len(), 1);
        assert_eq!(steps[0].to_micros, 500 * DAY);
    }

    #[test]
    fn a_range_past_the_reach_is_clipped_reported_and_exits_zero() {
        // The venue holds 5,000 bars. At a one-minute bar that is 3.5 days, so
        // a seven-day cold start cannot be served — and that is a stated fact,
        // not our truncation.
        let root = tempfile::tempdir().unwrap();
        let archive = Archive::open(root.path());
        let d = declaration(None, Some(5_000));
        let w = walk(&d);
        let now = 100 * DAY;

        let ask = w.ask(
            &archive,
            "hyperliquid",
            Series::Candles,
            WalkInterval::live(MINUTE),
            now,
        );
        assert_eq!(ask.asked_from_micros, now - 7 * DAY);
        assert_eq!(ask.venue_reach_micros, Some(now - 5_000 * MINUTE));
        assert_eq!(
            ask.from_micros,
            now - 5_000 * MINUTE,
            "clipped to the reach"
        );

        let steps = w.plan(Series::Candles, ask.from_micros, now, MINUTE);
        let outcome = w.outcome(Series::Candles, &steps, &ask, now, 1);
        assert!(outcome.clipped_by_reach());
        assert!(!outcome.capped);
        assert_eq!(
            outcome.exit_code(),
            0,
            "the venue's bound is not our failure"
        );

        let report = outcome.report();
        assert!(report.contains("asked 7d"), "{report}");
        assert!(report.contains("the venue holds 3d 11h 20m"), "{report}");
        assert!(report.contains("covered 3d 11h 20m"), "{report}");
    }

    #[test]
    fn a_reach_that_is_unstated_clips_nothing() {
        let root = tempfile::tempdir().unwrap();
        let archive = Archive::open(root.path());
        let d = declaration(None, None);
        let now = 100 * DAY;
        let ask = walk(&d).ask(
            &archive,
            "hyperliquid",
            Series::Funding,
            WalkInterval::live(HOUR),
            now,
        );
        assert_eq!(ask.venue_reach_micros, None);
        assert_eq!(ask.from_micros, ask.asked_from_micros);
        assert!(
            walk(&d)
                .outcome(Series::Funding, &[], &ask, now, 1)
                .report()
                .contains("no stated bound")
        );
    }

    #[test]
    fn our_own_cap_truncating_says_so_and_fails() {
        let d = declaration(None, None);
        let w = Walk::new(&d, 0.5, 3_650, 2); // a cap of two steps
        let to = 10_000 * DAY;
        let ask = Ask {
            interval_micros: MINUTE,
            asked_from_micros: 0,
            venue_reach_micros: None,
            from_micros: 0,
        };
        let steps = w.plan(Series::Candles, 0, to, MINUTE);
        assert_eq!(steps.len(), 2, "the cap stopped the plan");

        let outcome = w.outcome(Series::Candles, &steps, &ask, to, 1);
        assert!(outcome.capped);
        assert_eq!(outcome.exit_code(), 1, "this one IS ours to raise");
        assert!(
            outcome.report().contains("truncated by its cap"),
            "{}",
            outcome.report()
        );
    }

    #[test]
    fn requests_are_counted_per_instrument_not_per_step() {
        // Every instrument is fetched at every step, and the budget is spent
        // per call.
        let d = declaration(None, None);
        let w = walk(&d);
        let ask = Ask {
            interval_micros: MINUTE,
            asked_from_micros: 0,
            venue_reach_micros: None,
            from_micros: 0,
        };
        let steps = w.plan(Series::Candles, 0, 5_000 * MINUTE, MINUTE);
        assert_eq!(steps.len(), 1);
        assert_eq!(
            w.outcome(Series::Candles, &steps, &ask, 5_000 * MINUTE, 6)
                .requests_made,
            6
        );
    }

    #[test]
    fn only_what_the_venue_serves_is_walked() {
        // The rest stay gaps, which are published rather than left as absences.
        let d = declaration(None, None);
        let walkable = walk(&d).walkable(&[
            Series::Trades,
            Series::Quotes,
            Series::Candles,
            Series::Funding,
        ]);
        assert_eq!(walkable, vec![Series::Candles, Series::Funding]);
    }

    #[test]
    fn the_pace_respects_the_venues_stated_minimum() {
        let d = declaration(None, None);
        // 1,200/min at half the budget is 100 ms by arithmetic, and the venue
        // asks for 100 ms — so the floor binds exactly.
        assert_eq!(walk(&d).request_interval_ms(), 100);
    }

    #[test]
    fn an_empty_range_plans_nothing() {
        let d = declaration(None, None);
        assert!(walk(&d).plan(Series::Candles, 100, 100, MINUTE).is_empty());
        assert!(walk(&d).plan(Series::Candles, 200, 100, MINUTE).is_empty());
    }

    #[test]
    fn a_span_reads_the_way_a_person_reads_one() {
        assert_eq!(span(0), "0m");
        assert_eq!(span(5_000 * MINUTE), "3d 11h 20m");
        assert_eq!(span(7 * DAY), "7d");
        assert_eq!(span(-1), "0m");
    }
}
