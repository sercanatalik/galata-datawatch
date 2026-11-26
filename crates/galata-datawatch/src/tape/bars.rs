//! **Candles at the period a client asks for**, built on the server, exactly
//! (`add-galata-markets`, `market-periods`).
//!
//! Moved from galata-tower's screen (`shape::candles`), with what it lacked.
//! The tape's `candles` dataset holds several intervals at once — galata's
//! live 1m bars, 96% of whose rows are updates to the forming bar, and the
//! venue's 1h, 4h and 1d bars from capture's history walks — and a fold that
//! does not read `interval` lets a venue hourly bar, received later, win the
//! minute it shares. Here every row is filed under its own interval.
//!
//! # A bar
//!
//! ```text
//!   period P      one of PERIODS, aligned to UTC: a bar begins at a multiple
//!                 of P from the epoch (a 1d bar at 00:00 UTC)
//!   source        the finest held interval that divides P and covers the
//!                 bar with final source bars; failing that, the finest with
//!                 any bars in it, which the bar then says is incomplete. A
//!                 venue hourly bar beside 59 of 60 final minutes is the
//!                 complete answer; for the hour still forming, galata's live
//!                 minutes are, not a snapshot the history walk took
//!   values        first open, highest high, lowest low, last close, summed
//!                 volume and trades, each source bar at its latest receipt;
//!                 raw decimal units, no float anywhere
//!   says          its source interval, how many source bars it holds and how
//!                 many its period does, whether a recorded gap touches it,
//!                 and whether every source bar in it was final
//! ```
//!
//! A bar is the same whatever window it was asked in: a window selects the
//! bars it overlaps, each built from its whole period. A period with no
//! source at all is **unsourced**, named and left empty — never filled from a
//! coarser interval.
//!
//! The batch read and the live stream build a bar with the same function,
//! [`bar_at`]; [`Forming`] is only the state the live stream keeps between
//! updates.

use std::collections::BTreeMap;

use arrow::array::{Array, BooleanArray, Decimal128Array, Int64Array, StringArray, UInt32Array};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;

const MINUTE: i64 = 60_000_000;
const HOUR: i64 = 60 * MINUTE;

/// The periods offered, by the spelling a client asks with.
pub const PERIODS: [(&str, i64); 11] = [
    ("1m", MINUTE),
    ("3m", 3 * MINUTE),
    ("5m", 5 * MINUTE),
    ("15m", 15 * MINUTE),
    ("30m", 30 * MINUTE),
    ("1h", HOUR),
    ("2h", 2 * HOUR),
    ("4h", 4 * HOUR),
    ("8h", 8 * HOUR),
    ("12h", 12 * HOUR),
    ("1d", 24 * HOUR),
];

/// The decimal scale every price and size in the tape is stored at.
pub const SCALE: i8 = 18;

/// Why bars could not be built.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
#[non_exhaustive]
pub enum BarsError {
    /// A period not offered.
    #[error("{asked:?} is not a period offered here; ask for one of {}", offered())]
    Period {
        /// What was asked.
        asked: String,
    },
    /// A candle row of an interval this module cannot read.
    #[error("a candle row states the interval {0:?}, which is not a width")]
    Interval(String),
    /// The dataset is not the shape the tape writes.
    #[error("the candles dataset is missing {0}, or holds it in another type")]
    Shape(&'static str),
}

fn offered() -> String {
    PERIODS.map(|(label, _)| label).join(", ")
}

/// A period a client may ask for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Period {
    label: &'static str,
    micros: i64,
}

impl Period {
    /// From the spelling a client used; refused, listing the offered ones,
    /// where it is not offered.
    pub fn parse(label: &str) -> Result<Period, BarsError> {
        PERIODS
            .iter()
            .find(|(offered, _)| *offered == label)
            .map(|&(label, micros)| Period { label, micros })
            .ok_or_else(|| BarsError::Period {
                asked: label.to_string(),
            })
    }

    /// Its spelling.
    pub fn label(self) -> &'static str {
        self.label
    }

    /// Its width.
    pub fn micros(self) -> i64 {
        self.micros
    }

    /// The start of the bar holding a moment.
    pub fn start_of(self, at_micros: i64) -> i64 {
        at_micros - at_micros.rem_euclid(self.micros)
    }
}

/// A source interval's width, from the tape's spelling: `1m`, `15m`, `4h`,
/// `1d`, `1w`.
pub fn interval_micros(label: &str) -> Option<i64> {
    let (count, unit) = label.split_at(label.len().checked_sub(1)?);
    let count: i64 = count.parse().ok().filter(|n| *n > 0)?;
    let unit = match unit {
        "m" => MINUTE,
        "h" => HOUR,
        "d" => 24 * HOUR,
        "w" => 7 * 24 * HOUR,
        _ => return None,
    };
    count.checked_mul(unit)
}

/// One source bar, at its latest stated version.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SourceBar {
    /// Its open, venue time.
    pub at_micros: i64,
    /// When this version of it arrived.
    pub recv_micros: i64,
    /// Raw decimal units at [`SCALE`].
    pub open: i128,
    /// Raw decimal units at [`SCALE`].
    pub high: i128,
    /// Raw decimal units at [`SCALE`].
    pub low: i128,
    /// Raw decimal units at [`SCALE`].
    pub close: i128,
    /// Raw decimal units at [`SCALE`].
    pub volume: i128,
    /// Trades in it, where the venue states them.
    pub trade_count: Option<u32>,
    /// Whether the venue had closed it.
    pub is_final: bool,
}

/// One instrument's source bars, filed by interval, each at its latest
/// receipt.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Sources {
    by_interval: BTreeMap<i64, BTreeMap<i64, SourceBar>>,
}

impl Sources {
    /// Take a source bar: kept where it is newer than what is held for that
    /// interval and moment, so each is folded to the version received last.
    pub fn take(&mut self, interval_micros: i64, bar: SourceBar) {
        let held = self.by_interval.entry(interval_micros).or_default();
        match held.get(&bar.at_micros) {
            Some(seen) if seen.recv_micros > bar.recv_micros => {}
            _ => {
                held.insert(bar.at_micros, bar);
            }
        }
    }

    /// One instrument's candle rows from the tape's batches. Rows of another
    /// venue or ticker, or with no venue time, are not its.
    pub fn from_batches(
        batches: &[RecordBatch],
        venue: &str,
        ticker: &str,
    ) -> Result<Sources, BarsError> {
        let mut sources = Sources::default();
        for batch in batches {
            let v = text(batch, "venue")?;
            let t = text(batch, "ticker")?;
            let interval = text(batch, "interval")?;
            let at = int(batch, "at_micros")?;
            let recv = int(batch, "recv_micros")?;
            let open = dec(batch, "open")?;
            let high = dec(batch, "high")?;
            let low = dec(batch, "low")?;
            let close = dec(batch, "close")?;
            let volume = dec(batch, "volume")?;
            let trades = batch
                .column_by_name("trade_count")
                .and_then(|c| c.as_any().downcast_ref::<UInt32Array>());
            let is_final = batch
                .column_by_name("is_final")
                .and_then(|c| c.as_any().downcast_ref::<BooleanArray>())
                .ok_or(BarsError::Shape("is_final"))?;
            for i in 0..batch.num_rows() {
                if v.value(i) != venue || t.value(i) != ticker || at.is_null(i) {
                    continue;
                }
                let width = interval_micros(interval.value(i))
                    .ok_or_else(|| BarsError::Interval(interval.value(i).to_string()))?;
                sources.take(
                    width,
                    SourceBar {
                        at_micros: at.value(i),
                        recv_micros: recv.value(i),
                        open: open.value(i),
                        high: high.value(i),
                        low: low.value(i),
                        close: close.value(i),
                        volume: volume.value(i),
                        trade_count: trades.filter(|c| !c.is_null(i)).map(|c| c.value(i)),
                        is_final: !is_final.is_null(i) && is_final.value(i),
                    },
                );
            }
        }
        Ok(sources)
    }

    /// Take every source bar another holds, each kept where it is newer — so a
    /// window read a day at a time folds to what reading it whole would.
    pub fn merge(&mut self, other: Sources) {
        for (interval, held) in other.by_interval {
            for bar in held.into_values() {
                self.take(interval, bar);
            }
        }
    }

    /// The intervals held, finest first.
    pub fn intervals(&self) -> impl Iterator<Item = i64> + '_ {
        self.by_interval.keys().copied()
    }

    /// The span held at one interval: from its first bar's open to its last
    /// bar's close.
    pub fn span(&self, interval_micros: i64) -> Option<(i64, i64)> {
        let held = self.by_interval.get(&interval_micros)?;
        let first = *held.keys().next()?;
        let last = *held.keys().next_back()?;
        Some((first, last + interval_micros))
    }

    /// Forget every source bar that opened before a moment, at every interval.
    pub fn forget_before(&mut self, micros: i64) {
        for held in self.by_interval.values_mut() {
            *held = held.split_off(&micros);
        }
    }
}

/// One bar at a period.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Bar {
    /// Its open: a multiple of its period from the UTC epoch.
    pub start_micros: i64,
    /// Its period.
    pub period_micros: i64,
    /// The interval it was built from.
    pub source_interval_micros: i64,
    /// Raw decimal units at [`SCALE`].
    pub open: i128,
    /// Raw decimal units at [`SCALE`].
    pub high: i128,
    /// Raw decimal units at [`SCALE`].
    pub low: i128,
    /// Raw decimal units at [`SCALE`].
    pub close: i128,
    /// Raw decimal units at [`SCALE`].
    pub volume: i128,
    /// Trades, where every source bar states them.
    pub trade_count: Option<u64>,
    /// Source bars it holds.
    pub sources: u32,
    /// Source bars its period holds.
    pub sources_expected: u32,
    /// Whether a recorded gap touches it.
    pub gap: bool,
    /// Whether every source bar in it was final.
    pub is_final: bool,
}

impl Bar {
    /// Whether it holds every source bar of its period.
    pub fn is_complete(&self) -> bool {
        self.sources == self.sources_expected
    }
}

/// The bars over a window, and the parts of it no source covers.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Bars {
    /// Oldest first.
    pub bars: Vec<Bar>,
    /// `[from, to)` spans of the window that no source covers, merged.
    pub unsourced: Vec<(i64, i64)>,
}

/// **The one bar of `period` beginning at `start`**, or `None` where no source
/// covers any of it. `gaps` are the recorded gaps of the instrument's candles,
/// `[from, to)`.
pub fn bar_at(sources: &Sources, period: Period, start: i64, gaps: &[(i64, i64)]) -> Option<Bar> {
    let end = start + period.micros;
    let candidates: Vec<(i64, Vec<&SourceBar>)> = sources
        .by_interval
        .iter()
        .filter(|(width, _)| **width <= period.micros && period.micros % **width == 0)
        .map(|(width, held)| (*width, held.range(start..end).map(|(_, b)| b).collect()))
        .collect();
    let complete = |width: i64, held: &[&SourceBar]| {
        held.len() as i64 == period.micros / width && held.iter().all(|b| b.is_final)
    };
    let (width, held) = candidates
        .iter()
        .find(|(width, held)| complete(*width, held))
        .or_else(|| candidates.iter().find(|(_, held)| !held.is_empty()))?;
    let first = held.first()?;
    let last = held.last()?;
    Some(Bar {
        start_micros: start,
        period_micros: period.micros,
        source_interval_micros: *width,
        open: first.open,
        high: held.iter().map(|b| b.high).max()?,
        low: held.iter().map(|b| b.low).min()?,
        close: last.close,
        volume: held.iter().map(|b| b.volume).sum(),
        trade_count: held
            .iter()
            .map(|b| b.trade_count.map(u64::from))
            .sum::<Option<u64>>(),
        sources: held.len() as u32,
        sources_expected: (period.micros / width) as u32,
        gap: gaps.iter().any(|&(from, to)| from < end && to > start),
        is_final: held.iter().all(|b| b.is_final),
    })
}

/// **Every bar a window overlaps**, each built from its whole period, and the
/// spans no source covers.
pub fn bars(sources: &Sources, period: Period, from: i64, to: i64, gaps: &[(i64, i64)]) -> Bars {
    let mut out = Bars::default();
    let mut start = period.start_of(from);
    while start < to {
        match bar_at(sources, period, start, gaps) {
            Some(bar) => out.bars.push(bar),
            None => {
                let end = start + period.micros;
                match out.unsourced.last_mut() {
                    Some(last) if last.1 == start => last.1 = end,
                    _ => out.unsourced.push((start, end)),
                }
            }
        }
        start += period.micros;
    }
    out
}

/// **The forming bar, kept up to date one source bar at a time** — what the
/// live stream holds between updates. Every bar it hands back is [`bar_at`]'s,
/// so it is the bar a history read returns for the same period.
#[derive(Debug, Clone)]
pub struct Forming {
    period: Period,
    sources: Sources,
}

impl Forming {
    /// Start from what is already held — the tape and the window up to now.
    pub fn new(period: Period, sources: Sources) -> Forming {
        Forming { period, sources }
    }

    /// Take a source bar, and hand back the bar of the period it falls in, as
    /// it now stands. Source bars of periods before it are forgotten, so what
    /// is held is at most the current period's.
    pub fn take(
        &mut self,
        interval_micros: i64,
        bar: SourceBar,
        gaps: &[(i64, i64)],
    ) -> Option<Bar> {
        let start = self.period.start_of(bar.at_micros);
        self.sources.forget_before(start);
        self.sources.take(interval_micros, bar);
        bar_at(&self.sources, self.period, start, gaps)
    }
}

fn text<'a>(batch: &'a RecordBatch, name: &'static str) -> Result<&'a StringArray, BarsError> {
    batch
        .column_by_name(name)
        .and_then(|c| c.as_any().downcast_ref::<StringArray>())
        .ok_or(BarsError::Shape(name))
}

fn int<'a>(batch: &'a RecordBatch, name: &'static str) -> Result<&'a Int64Array, BarsError> {
    batch
        .column_by_name(name)
        .and_then(|c| c.as_any().downcast_ref::<Int64Array>())
        .ok_or(BarsError::Shape(name))
}

fn dec<'a>(batch: &'a RecordBatch, name: &'static str) -> Result<&'a Decimal128Array, BarsError> {
    let column = batch.column_by_name(name).ok_or(BarsError::Shape(name))?;
    if !matches!(column.data_type(), DataType::Decimal128(_, s) if *s == SCALE) {
        return Err(BarsError::Shape(name));
    }
    column
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .ok_or(BarsError::Shape(name))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 10:00 UTC on 2026-11-26, in micros.
    const T10: i64 = 1_795_687_200_000_000;
    const UNIT: i128 = 1_000_000_000_000_000_000;

    fn period(label: &str) -> Period {
        Period::parse(label).unwrap()
    }

    /// A source bar whose prices are `base` and around it, in whole units.
    fn source(at: i64, base: i128, recv: i64, is_final: bool) -> SourceBar {
        SourceBar {
            at_micros: at,
            recv_micros: recv,
            open: base * UNIT,
            high: (base + 2) * UNIT,
            low: (base - 1) * UNIT,
            close: (base + 1) * UNIT,
            volume: 3 * UNIT,
            trade_count: Some(4),
            is_final,
        }
    }

    /// `n` final minutes from `from`, the `k`th at base `100 + k`.
    fn minutes(from: i64, n: i64) -> Sources {
        let mut sources = Sources::default();
        for k in 0..n {
            let at = from + k * MINUTE;
            sources.take(MINUTE, source(at, 100 + k as i128, at + MINUTE, true));
        }
        sources
    }

    #[test]
    fn five_minute_bars_over_a_whole_hour() {
        let got = bars(&minutes(T10, 60), period("5m"), T10, T10 + HOUR, &[]);
        assert_eq!(got.bars.len(), 12);
        assert!(got.unsourced.is_empty());
        for (n, bar) in got.bars.iter().enumerate() {
            let first = 100 + 5 * n as i128;
            assert_eq!(bar.start_micros, T10 + 5 * MINUTE * n as i64);
            assert_eq!(bar.open, first * UNIT, "the first minute's open");
            assert_eq!(bar.high, (first + 4 + 2) * UNIT, "the highest high");
            assert_eq!(bar.low, (first - 1) * UNIT, "the lowest low");
            assert_eq!(bar.close, (first + 4 + 1) * UNIT, "the last minute's close");
            assert_eq!(bar.volume, 15 * UNIT, "five volumes summed");
            assert_eq!(bar.trade_count, Some(20));
            assert_eq!(bar.source_interval_micros, MINUTE);
            assert_eq!((bar.sources, bar.sources_expected), (5, 5));
            assert!(bar.is_complete() && bar.is_final && !bar.gap);
        }
    }

    #[test]
    fn a_period_not_offered_is_refused_listing_the_offered() {
        let refusal = Period::parse("7m").unwrap_err().to_string();
        assert!(refusal.contains("\"7m\""), "{refusal}");
        assert!(
            refusal.contains("1m, 3m, 5m") && refusal.contains("12h, 1d"),
            "{refusal}"
        );
    }

    #[test]
    fn hourly_bars_over_a_captured_day_are_built_from_its_minutes() {
        let day = T10 - 10 * HOUR;
        let got = bars(
            &minutes(day, 24 * 60),
            period("1h"),
            day,
            day + 24 * HOUR,
            &[],
        );
        assert_eq!(got.bars.len(), 24);
        assert!(
            got.bars.iter().all(|b| b.source_interval_micros == MINUTE
                && (b.sources, b.sources_expected) == (60, 60))
        );
    }

    #[test]
    fn before_minutes_were_captured_the_venues_own_bar_is_the_source() {
        let mut sources = Sources::default();
        let old = T10 - 400 * 24 * HOUR;
        sources.take(HOUR, source(old, 50, old + HOUR, true));
        let got = bars(&sources, period("1h"), old, old + HOUR, &[]);
        assert_eq!(got.bars.len(), 1);
        assert_eq!(got.bars[0].source_interval_micros, HOUR);
        assert_eq!(got.bars[0].open, 50 * UNIT);
        assert!(got.bars[0].is_complete());
    }

    #[test]
    fn a_venue_hourly_bar_is_never_taken_for_a_minute() {
        // Received after the minutes, at the same moment as the first of them:
        // the fold tower's screen used would have let it win.
        let mut sources = minutes(T10, 60);
        sources.take(HOUR, source(T10, 9_000, T10 + 2 * HOUR, true));
        let got = bars(&sources, period("1m"), T10, T10 + 5 * MINUTE, &[]);
        assert_eq!(got.bars.len(), 5);
        assert_eq!(got.bars[0].open, 100 * UNIT, "the minute, not the hour");
        assert!(got.bars.iter().all(|b| b.source_interval_micros == MINUTE));
    }

    #[test]
    fn a_complete_final_venue_bar_beats_minutes_with_one_missing() {
        let mut sources = minutes(T10, 60);
        sources
            .by_interval
            .get_mut(&MINUTE)
            .unwrap()
            .remove(&(T10 + 30 * MINUTE));
        sources.take(HOUR, source(T10, 7, T10 + 2 * HOUR, true));
        let bar = bar_at(&sources, period("1h"), T10, &[]).unwrap();
        assert_eq!(bar.source_interval_micros, HOUR);
        assert!(bar.is_complete());
    }

    #[test]
    fn the_forming_hour_is_built_from_live_minutes_not_a_stale_venue_bar() {
        // Seven minutes so far, and the history walk's snapshot of the hour,
        // not final, taken at capture's start.
        let mut sources = minutes(T10, 7);
        sources.take(HOUR, source(T10, 7, T10 + MINUTE, false));
        let bar = bar_at(&sources, period("1h"), T10, &[]).unwrap();
        assert_eq!(bar.source_interval_micros, MINUTE);
        assert_eq!((bar.sources, bar.sources_expected), (7, 60));
        assert!(!bar.is_complete());
    }

    #[test]
    fn a_missing_minute_leaves_the_bar_incomplete_and_says_so() {
        let mut sources = minutes(T10, 5);
        sources
            .by_interval
            .get_mut(&MINUTE)
            .unwrap()
            .remove(&(T10 + 2 * MINUTE));
        let bar = bar_at(&sources, period("5m"), T10, &[]).unwrap();
        assert_eq!((bar.sources, bar.sources_expected), (4, 5));
        assert!(!bar.is_complete());
    }

    #[test]
    fn a_first_or_last_minute_missing_is_incomplete_too() {
        for missing in [0, 4] {
            let mut sources = minutes(T10, 5);
            sources
                .by_interval
                .get_mut(&MINUTE)
                .unwrap()
                .remove(&(T10 + missing * MINUTE));
            assert!(
                !bar_at(&sources, period("5m"), T10, &[])
                    .unwrap()
                    .is_complete()
            );
        }
    }

    #[test]
    fn the_forming_bar_says_not_all_its_minutes_are_final() {
        let mut sources = minutes(T10, 4);
        sources.take(
            MINUTE,
            source(T10 + 4 * MINUTE, 104, T10 + 4 * MINUTE + 1, false),
        );
        let got = bars(&sources, period("5m"), T10, T10 + 5 * MINUTE, &[]);
        assert!(!got.bars.last().unwrap().is_final);
    }

    #[test]
    fn a_recorded_gap_that_touches_a_bar_is_stated() {
        let gaps = [(T10 + 7 * MINUTE, T10 + 8 * MINUTE)];
        let got = bars(
            &minutes(T10, 15),
            period("5m"),
            T10,
            T10 + 15 * MINUTE,
            &gaps,
        );
        let flags: Vec<bool> = got.bars.iter().map(|b| b.gap).collect();
        assert_eq!(flags, [false, true, false]);
    }

    #[test]
    fn bars_are_aligned_to_utc_and_whole_whatever_the_window() {
        let sources = minutes(T10, 30);
        let got = bars(
            &sources,
            period("15m"),
            T10 + 7 * MINUTE,
            T10 + 30 * MINUTE,
            &[],
        );
        assert_eq!(
            got.bars[0].start_micros, T10,
            "the bar holding 10:07 begins at 10:00"
        );
        assert_eq!(
            (got.bars[0].sources, got.bars[0].sources_expected),
            (15, 15)
        );
        assert_eq!(
            got.bars[0],
            bars(&sources, period("15m"), T10, T10 + 30 * MINUTE, &[]).bars[0],
            "the same bar, asked in another window"
        );
        // A day begins at midnight UTC.
        assert_eq!(period("1d").start_of(T10), T10 - 10 * HOUR);
    }

    #[test]
    fn a_range_no_source_covers_is_named_and_left_empty() {
        // Venue hourly bars two years back, and minutes only for the last hour.
        let mut sources = minutes(T10, 60);
        let old = T10 - 2 * 365 * 24 * HOUR;
        sources.take(HOUR, source(old, 50, old + HOUR, true));
        let got = bars(&sources, period("5m"), old, T10 + HOUR, &[]);
        assert_eq!(got.bars.len(), 12, "only where minutes are held");
        assert!(got.bars.iter().all(|b| b.source_interval_micros == MINUTE));
        assert_eq!(got.unsourced, [(old, T10)], "one span, named");
    }

    #[test]
    fn the_forming_bar_kept_one_update_at_a_time_is_the_batch_bar() {
        // Every update of a live 1m stream — each minute changing several
        // times before it closes — fed to the forming state, against the
        // batch read of the same rows.
        let mut all = Sources::default();
        let mut forming = Forming::new(period("5m"), Sources::default());
        let mut last: BTreeMap<i64, Bar> = BTreeMap::new();
        for k in 0..23i64 {
            let at = T10 + k * MINUTE;
            for version in 0..3 {
                let recv = at + version * 20_000_000;
                let bar = source(at, 100 + (k * 3 + version) as i128, recv, version == 2);
                all.take(MINUTE, bar);
                let built = forming.take(MINUTE, bar, &[]).unwrap();
                last.insert(built.start_micros, built);
            }
        }
        let batch = bars(&all, period("5m"), T10, T10 + 25 * MINUTE, &[]);
        assert_eq!(batch.bars, last.into_values().collect::<Vec<_>>());
        assert!(!batch.bars.last().unwrap().is_complete(), "three of five");
    }

    #[test]
    fn from_the_tape_a_venue_hourly_bar_received_later_is_not_a_minute() {
        // Through the tape itself: a day's live minutes, and the venue's hourly
        // bar for the first hour written after them, as a history walk does.
        use crate::tape::{Reader, Row, Tape, Window};
        use galata_wire::{Candle, Envelope, Event, Kind, Num, Ticker, Venue};
        use std::str::FromStr;

        let candle = |interval: &str, at: i64, recv: i64, open: &str| Row {
            stream_seq: 1,
            source_recv_micros: recv,
            envelope: Envelope::new(
                Venue::new("hyperliquid").unwrap(),
                Ticker::new("BTC").unwrap(),
                Some(at),
                recv,
                Event::Candle(Candle {
                    interval: interval.into(),
                    open: Num::from_str(open).unwrap(),
                    high: Num::from_str("70000").unwrap(),
                    low: Num::from_str("50000").unwrap(),
                    close: Num::from_str(open).unwrap(),
                    volume: Num::from_str("1.5").unwrap(),
                    trade_count: Some(3),
                    is_final: true,
                }),
            ),
        };
        let root = tempfile::tempdir().unwrap();
        let mut tape = Tape::open(root.path());
        for k in 0..60 {
            let at = T10 + k * MINUTE;
            tape.take(candle("1m", at, at + MINUTE, "60000"));
        }
        tape.take(candle("1h", T10, T10 + 2 * HOUR, "61234.5"));
        tape.commit().unwrap();

        let reader = Reader::open(root.path(), &["kind=candles"]).unwrap();
        let batches = reader
            .view(Window {
                kind: Kind::Candles,
                from_micros: T10,
                to_micros: T10 + HOUR,
                ticker: Some("BTC".into()),
            })
            .unwrap();
        let sources = Sources::from_batches(&batches, "hyperliquid", "BTC").unwrap();
        assert_eq!(sources.intervals().collect::<Vec<_>>(), [MINUTE, HOUR]);

        let one = bars(&sources, period("1m"), T10, T10 + MINUTE, &[]);
        assert_eq!(one.bars[0].open, 60_000 * UNIT, "the minute, not the hour");
        let hour = bars(&sources, period("1h"), T10, T10 + HOUR, &[]);
        assert_eq!(
            hour.bars[0].source_interval_micros, MINUTE,
            "minutes, all final"
        );
        assert_eq!(hour.bars[0].volume, 90 * UNIT, "sixty volumes of 1.5");
        assert_eq!(hour.bars[0].trade_count, Some(180));
    }

    #[test]
    fn a_source_interval_is_read_from_the_tapes_spelling() {
        assert_eq!(interval_micros("1m"), Some(MINUTE));
        assert_eq!(interval_micros("15m"), Some(15 * MINUTE));
        assert_eq!(interval_micros("4h"), Some(4 * HOUR));
        assert_eq!(interval_micros("1d"), Some(24 * HOUR));
        assert_eq!(interval_micros("0m"), None);
        assert_eq!(interval_micros("m"), None);
        assert_eq!(interval_micros("1y"), None);
    }
}
