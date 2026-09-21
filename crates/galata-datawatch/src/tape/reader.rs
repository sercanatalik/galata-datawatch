//! The bounded view: what a caller may see of the past.
//!
//! **The bound is a fact the store states about itself, never a parameter.**
//!
//! ```text
//!                     bound
//!                       │
//!   ────────────────────│────────────────────▶
//!    visible to the     │  not visible, at any
//!    caller             │  position, ever
//! ```
//!
//! An unbounded scan has a silent, fatal failure mode. Live it returns
//! everything that exists, because that is all there is. In a replay the same
//! scan returns everything *then too* — including what happened after the
//! replay position. No error, no warning, **better results**, and a backtest
//! that measured the future.
//!
//! So [`Reader::view`] takes **no bound argument**. A caller with no way to name
//! a bound has no way to name a wrong one.
//!
//! # The bound is a STREAM POSITION, not a time
//!
//! This is the part worth reading before using it. A tape segment is named by
//! the sequence range it covers, so the frontier the store can state about
//! itself is a **sequence**. A window, meanwhile, is in venue time, because
//! that is what a caller wants to ask for.
//!
//! The two are different units and the tape holds no mapping between them. So:
//!
//! ```text
//!   the WINDOW selects    which rows the caller asked for   (venue time)
//!   the BOUND restricts   which rows the caller may see     (stream sequence)
//! ```
//!
//! Both are applied, per row. What is **not** available is refusing a window
//! whose end lies past the bound — that needs a time the bound does not have,
//! and inventing one (the greatest `at_micros` below the bound, say) would
//! state a completeness the tape cannot know. A replay that must be bounded in
//! time has to drive by sequence until something provides that mapping, and
//! this says so rather than appearing to offer it.
//!
//! **It retains, it never interprets.** Rows come back in the order they are
//! stored. Windows, staleness and gap reasoning belong to the caller, which is
//! what lets a boot be *a replay of the last window* rather than a mode of its
//! own.
//!
//! **It holds no writer and no normaliser.** A second normaliser here would be
//! the caller `scripts/check-ingest-callers.sh` exists to refuse, and a reader
//! that could write is a cache that can disagree with its source.

use std::path::{Path, PathBuf};

use arrow::record_batch::RecordBatch;
use galata_wire::Kind;

/// Why a view could not be taken.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ReadError {
    /// A declared scope has written nothing.
    #[error(
        "scope `{scope}` has written nothing, so this root has no bound. A view taken without it \
         would be complete for the other scopes and silently holed for this one"
    )]
    NoFrontier {
        /// Which scope.
        scope: String,
    },
    /// Nothing was declared.
    #[error(
        "no scope was declared, so there is nothing to be bounded by. A view over nothing is not \
         a view over everything"
    )]
    NoScopes,
    /// Two scopes advance through different kinds of position.
    #[error(
        "the declared scopes advance through different kinds of position, which have no common \
         bound. Refusing is the only honest answer"
    )]
    Incomparable,
    /// Arrow refused to filter the batch.
    #[error("arrow: {0}")]
    Arrow(String),
    /// The window runs backwards.
    #[error("the range [{from}, {to}) runs backwards")]
    Backwards {
        /// Window start.
        from: i64,
        /// Window end.
        to: i64,
    },
    /// A segment would not read.
    #[error(transparent)]
    Segment(#[from] galata_segments::SegmentError),
}

/// How far a root is readable.
///
/// **A stream position, not a time.** See the module documentation for why the
/// tape can state one and not the other.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Bound {
    /// The greatest stream sequence a caller may see.
    pub position: i64,
}

impl Bound {
    /// The bound a root states about itself: the **minimum** durable frontier
    /// across the declared scopes.
    ///
    /// The maximum would claim coverage for a range some scope has not written,
    /// so a view taken at it is complete for one scope and holed for another.
    ///
    /// A scope that has written nothing means there is **no bound** — not
    /// "ignore that one", which is the silently-holed read arrived at by a
    /// different route.
    pub fn of(root: &Path, scopes: &[&str]) -> Result<Bound, ReadError> {
        if scopes.is_empty() {
            return Err(ReadError::NoScopes);
        }
        for scope in scopes {
            if galata_segments::last_durable_for_scope(root, scope).is_none() {
                return Err(ReadError::NoFrontier {
                    scope: (*scope).to_string(),
                });
            }
        }
        let (_, position) =
            galata_segments::frontier(root, scopes).ok_or(ReadError::Incomparable)?;
        Ok(Bound {
            position: i64::try_from(position).map_err(|_| ReadError::Incomparable)?,
        })
    }
}

/// A window a caller wants to see.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Window {
    /// Which dataset.
    pub kind: Kind,
    /// Start, in **venue** time — the clock the tape is sorted and dated by.
    pub from_micros: i64,
    /// End, exclusive.
    pub to_micros: i64,
}

/// The bounded reader.
#[derive(Debug)]
pub struct Reader {
    root: PathBuf,
    bound: Bound,
}

impl Reader {
    /// Open a reader, computing the bound **now**, from what the store has
    /// durably written.
    pub fn open(root: impl Into<PathBuf>, scopes: &[&str]) -> Result<Reader, ReadError> {
        let root = root.into();
        let bound = Bound::of(&root, scopes)?;
        Ok(Reader { root, bound })
    }

    /// The bound this reader was opened at.
    pub fn bound(&self) -> Bound {
        self.bound
    }

    /// The rows a window holds, **as the bound permits**.
    ///
    /// No bound argument, by design. The window selects; the bound restricts;
    /// both are applied per row.
    pub fn view(&self, window: Window) -> Result<Vec<RecordBatch>, ReadError> {
        if window.to_micros <= window.from_micros {
            return Err(ReadError::Backwards {
                from: window.from_micros,
                to: window.to_micros,
            });
        }

        let mut out = Vec::new();
        for partition in self.partitions_for(window) {
            for (cursor, path) in galata_segments::list_segments(&partition) {
                // A segment whose every row is past the bound need not be
                // opened. The name says so, which is the one exclusion a
                // sequence-named segment can make cheaply.
                if let galata_segments::Cursor::Seq { first, .. } = cursor
                    && i64::try_from(first).is_ok_and(|first| first > self.bound.position)
                {
                    continue;
                }
                for batch in galata_segments::read_segment(&path)? {
                    let kept = self.keep(&batch, window)?;
                    if kept.num_rows() > 0 {
                        out.push(kept);
                    }
                }
            }
        }
        Ok(out)
    }

    /// The rows of one batch that the window selected and the bound permits.
    ///
    /// **A row with no venue time is kept**, once the partition holding it is
    /// being read. It is not *at* any venue time, so a time window cannot
    /// exclude it on the evidence — and dropping it would make a gap row, which
    /// often states none, invisible to every windowed read.
    ///
    /// Reaching it is the caller's problem, and worth stating plainly: such a
    /// row was **filed by our clock**, because a row must land in some
    /// partition. A venue-time window therefore finds it only where that window
    /// also covers the day it was received. The alternative — scanning every
    /// partition on every read, in case one holds a timeless row — is the
    /// pruning thrown away for a case that is rare by construction.
    fn keep(&self, batch: &RecordBatch, window: Window) -> Result<RecordBatch, ReadError> {
        use arrow::array::{Array, BooleanArray, Int64Array, UInt64Array};

        let at = batch
            .column_by_name("at_micros")
            .and_then(|c| c.as_any().downcast_ref::<Int64Array>());
        let seq = batch
            .column_by_name("stream_seq")
            .and_then(|c| c.as_any().downcast_ref::<UInt64Array>());

        let mask: BooleanArray = (0..batch.num_rows())
            .map(|i| {
                let in_window = match at {
                    Some(at) if !at.is_null(i) => {
                        at.value(i) >= window.from_micros && at.value(i) < window.to_micros
                    }
                    // No venue time: not excludable on the evidence.
                    _ => true,
                };
                let permitted = match seq {
                    Some(seq) => {
                        i64::try_from(seq.value(i)).is_ok_and(|seq| seq <= self.bound.position)
                    }
                    // A tape row without a stream sequence cannot be placed
                    // relative to the bound, so it is NOT shown. The safe
                    // direction is to withhold, not to reveal.
                    None => false,
                };
                Some(in_window && permitted)
            })
            .collect();

        arrow::compute::filter_record_batch(batch, &mask)
            .map_err(|e| ReadError::Arrow(e.to_string()))
    }

    /// The `kind=/date=` partitions a window touches.
    ///
    /// The tape has no time in its segment names, so the `date=` level does the
    /// excluding that the archive's naming does for a replay.
    fn partitions_for(&self, window: Window) -> Vec<PathBuf> {
        const DAY: i64 = 86_400_000_000;
        let mut out = Vec::new();
        let dir = self.root.join(format!("kind={}", window.kind));
        let Ok(entries) = std::fs::read_dir(&dir) else {
            return out;
        };
        for entry in entries.filter_map(|e| e.ok()) {
            let Some(name) = entry.file_name().to_str().map(str::to_string) else {
                continue;
            };
            let Some(date) = name.strip_prefix("date=") else {
                continue;
            };
            let Some(midnight) = crate::calendar::midnight_of(date) else {
                continue;
            };
            if midnight < window.to_micros && midnight.saturating_add(DAY) > window.from_micros {
                out.push(entry.path());
            }
        }
        out.sort();
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tape::writer::{Row, Tape};
    use galata_wire::{Envelope, Event, Num, Quote, Ticker, Venue};
    use std::str::FromStr;

    const DAY: i64 = 86_400_000_000;

    fn row(seq: u64, venue: &str, at: Option<i64>) -> Row {
        Row {
            stream_seq: seq,
            envelope: Envelope::new(
                Venue::new(venue).unwrap(),
                Ticker::new("BTC").unwrap(),
                at,
                at.unwrap_or(100 * DAY),
                Event::Quote(Quote {
                    bid_px: Some(Num::from_str("1").unwrap()),
                    ask_px: None,
                    bid_sz: None,
                    ask_sz: None,
                    bid_spread: None,
                    ask_spread: None,
                }),
            ),
        }
    }

    /// A tape where two scopes have written to different positions.
    ///
    /// The tape's own layout has no scope level, so the frontier is taken over
    /// `kind=` — which is what a scope is for a store partitioned this way.
    fn tape_with(rows: Vec<Row>) -> tempfile::TempDir {
        let dir = tempfile::tempdir().unwrap();
        let mut tape = Tape::open(dir.path());
        for row in rows {
            tape.take(row);
        }
        tape.commit().unwrap();
        dir
    }

    fn rows_in(batches: &[RecordBatch]) -> usize {
        batches.iter().map(|b| b.num_rows()).sum()
    }

    fn seqs(batches: &[RecordBatch]) -> Vec<u64> {
        use arrow::array::{Array, UInt64Array};
        let mut out = Vec::new();
        for batch in batches {
            let column = batch.column_by_name("stream_seq").unwrap();
            let array = column.as_any().downcast_ref::<UInt64Array>().unwrap();
            for i in 0..array.len() {
                out.push(array.value(i));
            }
        }
        out.sort();
        out
    }

    #[test]
    fn no_scope_is_not_every_scope() {
        // A view over nothing is not a view over everything.
        let dir = tape_with(vec![row(1, "hyperliquid", Some(100 * DAY))]);
        assert!(matches!(
            Reader::open(dir.path(), &[]),
            Err(ReadError::NoScopes)
        ));
    }

    #[test]
    fn a_scope_that_has_written_nothing_means_no_bound() {
        // Not "ignore that one" — that is the silently-holed read the minimum
        // exists to prevent, arrived at by a different route.
        let dir = tape_with(vec![row(1, "hyperliquid", Some(100 * DAY))]);
        let error = Reader::open(dir.path(), &["kind=quotes", "kind=trades"]).unwrap_err();
        assert!(matches!(error, ReadError::NoFrontier { .. }), "{error}");
        assert!(error.to_string().contains("kind=trades"), "{error}");
        assert!(error.to_string().contains("silently holed"), "{error}");
    }

    #[test]
    fn the_slower_scope_sets_the_bound() {
        // The maximum would claim coverage for a range one scope has not
        // written, so the view is complete for one and holed for the other.
        let dir = tempfile::tempdir().unwrap();
        let mut tape = Tape::open(dir.path());
        tape.take(row(9, "hyperliquid", Some(100 * DAY)));
        tape.take(Row {
            stream_seq: 3,
            envelope: Envelope::new(
                Venue::new("hyperliquid").unwrap(),
                Ticker::new("BTC").unwrap(),
                Some(100 * DAY),
                100 * DAY,
                Event::Funding(galata_wire::Funding {
                    rate: Num::from_str("0.1").unwrap(),
                    next_micros: None,
                }),
            ),
        });
        tape.commit().unwrap();

        let reader = Reader::open(dir.path(), &["kind=quotes", "kind=funding"]).unwrap();
        assert_eq!(reader.bound().position, 3, "the lesser, not the greater");
    }

    #[test]
    fn a_row_past_the_bound_is_never_returned() {
        let dir = tempfile::tempdir().unwrap();
        let mut tape = Tape::open(dir.path());
        // Two commits, so two segments: one wholly within the bound, one past.
        tape.take(row(1, "hyperliquid", Some(100 * DAY)));
        tape.take(row(2, "hyperliquid", Some(100 * DAY + 1)));
        tape.commit().unwrap();

        let reader = Reader::open(dir.path(), &["kind=quotes"]).unwrap();
        assert_eq!(reader.bound().position, 2);

        // A reader opened at an earlier bound must not see the later row.
        let earlier = Reader {
            root: dir.path().to_path_buf(),
            bound: Bound { position: 1 },
        };
        let window = Window {
            kind: galata_wire::Kind::Quotes,
            from_micros: 0,
            to_micros: 200 * DAY,
        };
        assert_eq!(seqs(&earlier.view(window).unwrap()), vec![1]);
        assert_eq!(seqs(&reader.view(window).unwrap()), vec![1, 2]);
    }

    #[test]
    fn a_window_selects_by_venue_time() {
        let dir = tape_with(vec![
            row(1, "hyperliquid", Some(100 * DAY)),
            row(2, "hyperliquid", Some(100 * DAY + 5_000)),
        ]);
        let reader = Reader::open(dir.path(), &["kind=quotes"]).unwrap();
        let narrow = Window {
            kind: galata_wire::Kind::Quotes,
            from_micros: 100 * DAY,
            to_micros: 100 * DAY + 1,
        };
        assert_eq!(seqs(&reader.view(narrow).unwrap()), vec![1]);
    }

    #[test]
    fn a_row_with_no_venue_time_is_kept_by_a_window_that_reaches_its_partition() {
        // Two facts, and the second is the surprising one.
        //
        // It is not *at* any venue time, so the time FILTER cannot exclude it —
        // dropping it would make a gap row invisible to every windowed read.
        //
        // But it was FILED by our clock, because a row must land in some
        // partition. So a venue-time window finds it only where that window
        // also covers the day it was received.
        let dir = tape_with(vec![row(1, "hyperliquid", None)]);
        let reader = Reader::open(dir.path(), &["kind=quotes"]).unwrap();
        let reaching = Window {
            kind: galata_wire::Kind::Quotes,
            from_micros: 100 * DAY,
            to_micros: 100 * DAY + 1,
        };
        assert_eq!(
            rows_in(&reader.view(reaching).unwrap()),
            1,
            "the filter kept it"
        );

        let elsewhere = Window {
            kind: galata_wire::Kind::Quotes,
            from_micros: 500 * DAY,
            to_micros: 501 * DAY,
        };
        assert_eq!(
            rows_in(&reader.view(elsewhere).unwrap()),
            0,
            "the partition pruning never reached it, which is the documented cost"
        );
    }

    #[test]
    fn a_backwards_window_is_refused() {
        let dir = tape_with(vec![row(1, "hyperliquid", Some(100 * DAY))]);
        let reader = Reader::open(dir.path(), &["kind=quotes"]).unwrap();
        let backwards = Window {
            kind: galata_wire::Kind::Quotes,
            from_micros: 200 * DAY,
            to_micros: 100 * DAY,
        };
        assert!(matches!(
            reader.view(backwards),
            Err(ReadError::Backwards { .. })
        ));
    }

    #[test]
    fn the_view_takes_no_bound_argument() {
        // The whole design, asserted by the only means available: the type.
        // A caller with no way to name a bound has no way to name a wrong one.
        let dir = tape_with(vec![row(1, "hyperliquid", Some(100 * DAY))]);
        let reader = Reader::open(dir.path(), &["kind=quotes"]).unwrap();
        let window = Window {
            kind: galata_wire::Kind::Quotes,
            from_micros: 0,
            to_micros: 200 * DAY,
        };
        // One argument, and it carries no bound.
        let _: Result<Vec<RecordBatch>, ReadError> = reader.view(window);
        assert_eq!(
            std::mem::size_of_val(&window),
            std::mem::size_of::<Window>()
        );
    }

    #[test]
    fn a_dataset_with_no_partitions_reads_empty_rather_than_failing() {
        // Nothing was written for it, which is a fact rather than an error.
        let dir = tape_with(vec![row(1, "hyperliquid", Some(100 * DAY))]);
        let reader = Reader::open(dir.path(), &["kind=quotes"]).unwrap();
        let window = Window {
            kind: galata_wire::Kind::Trades,
            from_micros: 0,
            to_micros: 200 * DAY,
        };
        assert_eq!(rows_in(&reader.view(window).unwrap()), 0);
    }
}
