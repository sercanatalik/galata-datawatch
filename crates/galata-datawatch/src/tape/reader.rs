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

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use arrow::record_batch::RecordBatch;
use galata_wire::Kind;

/// Why a view could not be taken.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ReadError {
    /// One or more declared scopes have written nothing.
    #[error(
        "{} of the declared scopes have written nothing, so this root has no bound: {}. A view \
         taken without them would be complete for the other scopes and silently holed for these",
        scopes.len(),
        scopes.join(", ")
    )]
    NoFrontier {
        /// **All** of them, not the first one found.
        ///
        /// Naming one at a time makes a misconfiguration take as many runs to
        /// discover as there are scopes wrong in it: fix, re-run, meet the
        /// next. The listing costs one pass either way.
        scopes: Vec<String>,
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
    /// A segment does not say whose rows it holds, so it cannot be bounded.
    #[error("{path}: {}", crate::tape::UNLABELLED_REMEDY)]
    Unlabelled {
        /// The segment.
        path: PathBuf,
    },
}

/// How far a root is readable — **per venue**.
///
/// **A stream position, not a time.** See the module documentation for why the
/// tape can state one and not the other.
///
/// **And one position per venue**, because a stream sequence is numbered per
/// venue, from that venue's own process's boot. Two venues' numbers are
/// related only by the accident of when each process started, so a single
/// position bounds one venue and is arbitrary for the rest: it hid durable rows
/// of a venue whose process started later, and would have shown undurable rows
/// of one that started earlier.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Bound {
    /// The greatest stream sequence a caller may see, by venue.
    pub positions: BTreeMap<String, i64>,
}

impl Bound {
    /// The greatest stream sequence a caller may see of one venue's rows.
    ///
    /// `None` for a venue the declared scopes hold nothing of: its rows are
    /// withheld, because the safe direction is to withhold.
    pub fn of_venue(&self, venue: &str) -> Option<i64> {
        self.positions.get(venue).copied()
    }

    /// The bound a root states about itself: for each venue, the **minimum**
    /// of that venue's durable frontier across the declared scopes.
    ///
    /// The maximum would claim coverage for a range some scope has not written,
    /// so a view taken at it is complete for one scope and holed for another.
    ///
    /// A scope that has written nothing means there is **no bound** — not
    /// "ignore that one", which is the silently-holed read arrived at by a
    /// different route. The same holds one level down: a venue that has
    /// written to one declared scope and not another has no bound either, and
    /// every such `(scope, venue)` is named. A caller reading datasets that not
    /// every venue supplies reads them one scope at a time.
    pub fn of(root: &Path, scopes: &[&str]) -> Result<Bound, ReadError> {
        Bound::of_cached(root, scopes, &LabelCache::default())
    }

    /// The same, reading each segment's label through a cache the caller
    /// owns — for a caller that computes the bound again and again over a tape
    /// that mostly has not changed.
    pub fn of_cached(
        root: &Path,
        scopes: &[&str],
        labels: &LabelCache,
    ) -> Result<Bound, ReadError> {
        if scopes.is_empty() {
            return Err(ReadError::NoScopes);
        }
        // One cached walk per scope answers both questions: whether it has
        // written anything, and how far each venue in it has. Asking
        // `unwritten` first walked the whole scope a second time.
        let listings: Vec<_> = scopes
            .iter()
            .map(|scope| {
                (
                    *scope,
                    labels.listing.partitions_with_segments(&root.join(scope)),
                )
            })
            .collect();
        let unwritten: Vec<String> = listings
            .iter()
            .filter(|(_, partitions)| partitions.is_empty())
            .map(|(scope, _)| (*scope).to_string())
            .collect();
        if !unwritten.is_empty() {
            return Err(ReadError::NoFrontier { scopes: unwritten });
        }
        let mut frontiers = Vec::with_capacity(scopes.len());
        for (scope, partitions) in &listings {
            frontiers.push((*scope, venue_frontiers(partitions, labels)?));
        }
        let venues: std::collections::BTreeSet<&String> =
            frontiers.iter().flat_map(|(_, f)| f.keys()).collect();

        let mut missing = Vec::new();
        let mut positions = BTreeMap::new();
        for venue in venues {
            let mut least: Option<i64> = None;
            for (scope, frontier) in &frontiers {
                match frontier.get(venue) {
                    Some(position) => least = Some(least.map_or(*position, |l| l.min(*position))),
                    None => missing.push(format!("{scope} for venue={venue}")),
                }
            }
            if let Some(position) = least {
                positions.insert(venue.clone(), position);
            }
        }
        if !missing.is_empty() {
            return Err(ReadError::NoFrontier { scopes: missing });
        }
        Ok(Bound { positions })
    }
}

/// Each venue's durable frontier under one scope: the greatest sequence any
/// of its segments reaches.
///
/// Whose a segment is comes from its label, never from a column statistic.
fn venue_frontiers(
    partitions: &[(PathBuf, Vec<(galata_segments::Cursor, PathBuf)>)],
    labels: &LabelCache,
) -> Result<BTreeMap<String, i64>, ReadError> {
    let mut out: BTreeMap<String, i64> = BTreeMap::new();
    for (_, segments) in partitions {
        for (cursor, path) in segments {
            let galata_segments::Cursor::Seq { last, .. } = *cursor else {
                return Err(ReadError::Incomparable);
            };
            let last = i64::try_from(last).map_err(|_| ReadError::Incomparable)?;
            let venue = labels.venue(path)?;
            let entry = out.entry(venue).or_insert(last);
            *entry = (*entry).max(last);
        }
    }
    Ok(out)
}

/// Segment labels already read, for a caller that asks again and again.
///
/// **Measured** (`examples/cost-of-labels.rs`): a label is a footer read at
/// about 20 µs, so a bound over 1,000 segments cost 20.8 ms — and galata-tower
/// computes one every second for each of six kinds, over a tape retention
/// keeps forever. A segment is immutable once renamed, so its label cannot
/// change while the file does not.
///
/// **Keyed by path, valid while the file's size and modification time are
/// unchanged** — the key DataFusion's parquet metadata cache uses — so a
/// segment replaced at the same path is read again. A `stat` guards every hit.
/// Caller-owned rather than global: a library holding state nobody asked for
/// is state nobody can bound or drop.
///
/// **It caches the listing too** ([`galata_segments::ListingCache`]). With
/// the footers cached, what remained was reading every directory, twice:
/// 108 ms warm over 2,230 candle partitions on 2026-09-26, every second.
#[derive(Debug, Default)]
pub struct LabelCache {
    entries: std::sync::Mutex<BTreeMap<PathBuf, (u64, std::time::SystemTime, String)>>,
    reads: std::sync::atomic::AtomicU64,
    listing: galata_segments::ListingCache,
}

impl LabelCache {
    /// A segment's venue, from the cache if the file is unchanged.
    fn venue(&self, path: &Path) -> Result<String, ReadError> {
        let meta = std::fs::metadata(path).map_err(|source| {
            ReadError::Segment(galata_segments::SegmentError::Write {
                path: path.to_path_buf(),
                source,
            })
        })?;
        let (len, modified) = (meta.len(), meta.modified().ok());
        if let (Some(modified), Ok(entries)) = (modified, self.entries.lock())
            && let Some((l, m, venue)) = entries.get(path)
            && *l == len
            && *m == modified
        {
            return Ok(venue.clone());
        }
        let venue = segment_venue(path)?;
        self.reads
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if let (Some(modified), Ok(mut entries)) = (modified, self.entries.lock()) {
            entries.insert(path.to_path_buf(), (len, modified, venue.clone()));
        }
        Ok(venue)
    }

    /// How many footers this cache has had to read — for asserting that a
    /// warm cache reads none.
    pub fn footer_reads(&self) -> u64 {
        self.reads.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// How many directories this cache has had to read, for asserting that a
    /// warm bound reads only what moved.
    pub fn directory_reads(&self) -> u64 {
        self.listing.directory_reads()
    }

    /// Forget every segment and directory that no longer exists, so the cache
    /// holds at most what the tape holds. Called by whoever owns it, on its
    /// own cadence.
    pub fn prune(&self) {
        if let Ok(mut entries) = self.entries.lock() {
            entries.retain(|path, _| path.exists());
        }
        self.listing.prune();
    }
}

/// Whose rows a tape segment holds, from its label, or a refusal naming it.
fn segment_venue(path: &Path) -> Result<String, ReadError> {
    galata_segments::label(path, crate::tape::VENUE_LABEL)?.ok_or_else(|| ReadError::Unlabelled {
        path: path.to_path_buf(),
    })
}

/// The declared scopes that have written nothing.
///
/// **Exposed because *nothing has happened yet* and *this scope is missing
/// while the others are live* are different facts**, and only the caller knows
/// which one matters to it. [`Bound::of`] refuses either way — that is the
/// right rule for a bound, since excluding a scope would narrow the window
/// silently and a misconfigured venue would look exactly like a quiet one —
/// but a caller deciding whether to take a view **at all** needs to tell them
/// apart first.
///
/// Without this a caller reaches for the filesystem, which is how the
/// `superseded` example began: it stated a directory to find out whether a
/// venue had ever recorded a reorganisation, reimplementing a rule the store
/// owns.
pub fn unwritten_cached(root: &Path, scopes: &[&str], labels: &LabelCache) -> Vec<String> {
    scopes
        .iter()
        .filter(|scope| {
            labels
                .listing
                .partitions_with_segments(&root.join(scope))
                .is_empty()
        })
        .map(|scope| (*scope).to_string())
        .collect()
}

/// The declared scopes that have written nothing, walking each one afresh.
///
/// [`unwritten_cached`] answers the same through a cache, for a caller that
/// asks every second.
pub fn unwritten(root: &Path, scopes: &[&str]) -> Vec<String> {
    scopes
        .iter()
        .filter(|scope| galata_segments::last_durable_for_scope(root, scope).is_none())
        .map(|scope| (*scope).to_string())
        .collect()
}

/// A window a caller wants to see.
///
/// No longer `Copy`: the ticker is owned, so that a caller may hold a window
/// built from a string it read rather than borrowing one for the read's life.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Window {
    /// Which dataset.
    pub kind: Kind,
    /// Start, in **venue** time — the clock the tape is sorted and dated by.
    pub from_micros: i64,
    /// End, exclusive.
    pub to_micros: i64,
    /// One instrument, or every instrument in the window.
    ///
    /// **Optional because *every* is the right answer for a table of the
    /// newest rows and for anything rebuilding a partition.** A required field
    /// would make the common case state something it does not mean.
    pub ticker: Option<String>,
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
        Reader::open_cached(root, scopes, &LabelCache::default())
    }

    /// The same, computing the bound through a label cache the caller owns.
    pub fn open_cached(
        root: impl Into<PathBuf>,
        scopes: &[&str],
        labels: &LabelCache,
    ) -> Result<Reader, ReadError> {
        let root = root.into();
        let bound = Bound::of_cached(&root, scopes, labels)?;
        Ok(Reader { root, bound })
    }

    /// The bound this reader was opened at.
    pub fn bound(&self) -> &Bound {
        &self.bound
    }

    /// The rows a window holds, **as the bound permits**.
    ///
    /// No bound argument, by design. The window selects; the bound restricts;
    /// both are applied per row.
    pub fn view(&self, window: Window) -> Result<Vec<RecordBatch>, ReadError> {
        let window = &window;
        if window.to_micros <= window.from_micros {
            return Err(ReadError::Backwards {
                from: window.from_micros,
                to: window.to_micros,
            });
        }

        let mut out = Vec::new();
        for partition in self.partitions_for(window) {
            for (cursor, path) in galata_segments::list_segments(&partition) {
                // **Bounded by its own venue's position.** A segment of a venue
                // the declared scopes hold nothing of is withheld whole.
                let Some(position) = self.bound.of_venue(&segment_venue(&path)?) else {
                    continue;
                };
                // A segment whose every row is past the bound need not be
                // opened. The name says so, which is the one exclusion a
                // sequence-named segment can make cheaply.
                if let galata_segments::Cursor::Seq { first, .. } = cursor
                    && i64::try_from(first).is_ok_and(|first| first > position)
                {
                    continue;
                }
                for batch in galata_segments::read_segment(&path)? {
                    let kept = self.keep(&batch, window, position)?;
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
    fn keep(
        &self,
        batch: &RecordBatch,
        window: &Window,
        position: i64,
    ) -> Result<RecordBatch, ReadError> {
        use arrow::array::{Array, BooleanArray, Int64Array, StringArray, UInt64Array};

        let at = batch
            .column_by_name("at_micros")
            .and_then(|c| c.as_any().downcast_ref::<Int64Array>());
        let ticker = batch
            .column_by_name("ticker")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>());
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
                    Some(seq) => i64::try_from(seq.value(i)).is_ok_and(|seq| seq <= position),
                    // A tape row without a stream sequence cannot be placed
                    // relative to the bound, so it is NOT shown. The safe
                    // direction is to withhold, not to reveal.
                    None => false,
                };
                // **The opposite of the time rule above, deliberately.** A
                // row with no venue time is kept, because it is not AT any
                // time and a window cannot exclude it on the evidence. A row
                // with no ticker is not the instrument the caller asked for,
                // and that IS evidence. Matched whole: `BTC` is not `BTCUSD`,
                // and a prefix match returns a superset while reading as a
                // subset.
                let is_the_instrument = match (&window.ticker, ticker) {
                    (None, _) => true,
                    (Some(wanted), Some(column)) if !column.is_null(i) => column.value(i) == wanted,
                    (Some(_), _) => false,
                };
                Some(in_window && permitted && is_the_instrument)
            })
            .collect();

        arrow::compute::filter_record_batch(batch, &mask)
            .map_err(|e| ReadError::Arrow(e.to_string()))
    }

    /// The `kind=/date=` partitions a window touches.
    ///
    /// The tape has no time in its segment names, so the `date=` level does the
    /// excluding that the archive's naming does for a replay.
    fn partitions_for(&self, window: &Window) -> Vec<PathBuf> {
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
        row_for(seq, venue, "BTC", at)
    }

    /// The same, naming the instrument — for the reads that ask for one.
    fn row_for(seq: u64, venue: &str, ticker: &str, at: Option<i64>) -> Row {
        Row {
            stream_seq: seq,
            source_recv_micros: at.unwrap_or(100 * DAY),
            envelope: Envelope::new(
                Venue::new(venue).unwrap(),
                Ticker::new(ticker).unwrap(),
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
    fn the_refusal_names_every_unwritten_scope_not_the_first() {
        // **Naming one at a time makes a misconfiguration take as many runs to
        // discover as there are scopes wrong in it**: fix, re-run, meet the
        // next. The listing costs one pass either way.
        let dir = tape_with(vec![row(1, "hyperliquid", Some(100 * DAY))]);
        let declared = ["kind=quotes", "kind=trades", "kind=funding", "kind=candles"];
        let error = Reader::open(dir.path(), &declared).unwrap_err();
        let said = error.to_string();
        for missing in ["kind=trades", "kind=funding", "kind=candles"] {
            assert!(said.contains(missing), "{said}");
        }
        assert!(said.contains("3 of the declared scopes"), "{said}");
    }

    #[test]
    fn what_has_written_nothing_can_be_asked_before_a_view_is_taken() {
        // *Nothing has happened yet* and *this scope is missing while the
        // others are live* are different facts, and only the caller knows
        // which matters. Without this a caller reaches for the filesystem and
        // reimplements a rule the store owns.
        let dir = tape_with(vec![row(1, "hyperliquid", Some(100 * DAY))]);
        assert_eq!(
            unwritten(dir.path(), &["kind=quotes", "kind=trades"]),
            vec!["kind=trades".to_string()]
        );
        // The written one is not named, and a fully written set is empty.
        assert!(unwritten(dir.path(), &["kind=quotes"]).is_empty());
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
            source_recv_micros: 100 * DAY,
            envelope: Envelope::new(
                Venue::new("hyperliquid").unwrap(),
                Ticker::new("BTC").unwrap(),
                Some(100 * DAY),
                100 * DAY,
                Event::Funding(galata_wire::Funding {
                    rate: Num::from_str("0.1").unwrap(),
                    next_micros: None,
                    premium: None,
                }),
            ),
        });
        tape.commit().unwrap();

        let reader = Reader::open(dir.path(), &["kind=quotes", "kind=funding"]).unwrap();
        assert_eq!(
            reader.bound().of_venue("hyperliquid"),
            Some(3),
            "the lesser, not the greater"
        );
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
        assert_eq!(reader.bound().of_venue("hyperliquid"), Some(2));

        // A reader opened at an earlier bound must not see the later row.
        let earlier = Reader {
            root: dir.path().to_path_buf(),
            bound: Bound {
                positions: [("hyperliquid".to_string(), 1)].into(),
            },
        };
        let window = Window {
            kind: galata_wire::Kind::Quotes,
            from_micros: 0,
            to_micros: 200 * DAY,
            ticker: None,
        };
        assert_eq!(seqs(&earlier.view(window.clone()).unwrap()), vec![1]);
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
            ticker: None,
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
            ticker: None,
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
            ticker: None,
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
            ticker: None,
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
            ticker: None,
        };
        // One argument, and it carries no bound.
        let _: Result<Vec<RecordBatch>, ReadError> = reader.view(window.clone());
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
            ticker: None,
        };
        assert_eq!(rows_in(&reader.view(window).unwrap()), 0);
    }
    /// **The gap this change exists for.** Six tickers in the tape and no way
    /// to look at any but the busiest — the newest forty rows were all one
    /// instrument, so five of the six were unreachable from the screen.
    #[test]
    fn a_window_can_name_one_instrument() {
        let dir = tape_with(vec![
            row_for(1, "hyperliquid", "BTC", Some(10)),
            row_for(2, "hyperliquid", "ETH", Some(20)),
            row_for(3, "hyperliquid", "BTC", Some(30)),
        ]);
        let reader = Reader::open(dir.path(), &["kind=quotes"]).unwrap();
        let mut window = Window {
            kind: galata_wire::Kind::Quotes,
            from_micros: 0,
            to_micros: 200 * DAY,
            ticker: Some("BTC".to_owned()),
        };
        assert_eq!(seqs(&reader.view(window.clone()).unwrap()), vec![1, 3]);

        // Naming none is every instrument, which is the tape table's default.
        window.ticker = None;
        assert_eq!(seqs(&reader.view(window).unwrap()), vec![1, 2, 3]);
    }

    /// **`BTC` is not `BTCUSD`.** A prefix match returns a superset while
    /// reading as a subset, which is the shape of a wrong answer nobody
    /// checks.
    #[test]
    fn an_instrument_is_matched_whole() {
        let dir = tape_with(vec![
            row_for(1, "hyperliquid", "BTC", Some(10)),
            row_for(2, "hyperliquid", "BTCUSD", Some(20)),
        ]);
        let reader = Reader::open(dir.path(), &["kind=quotes"]).unwrap();
        let window = Window {
            kind: galata_wire::Kind::Quotes,
            from_micros: 0,
            to_micros: 200 * DAY,
            ticker: Some("BTC".to_owned()),
        };
        assert_eq!(seqs(&reader.view(window).unwrap()), vec![1]);
    }

    /// An instrument the window does not hold is empty, never an error.
    #[test]
    fn an_instrument_that_is_not_there_is_empty() {
        let dir = tape_with(vec![row_for(1, "hyperliquid", "BTC", Some(10))]);
        let reader = Reader::open(dir.path(), &["kind=quotes"]).unwrap();
        let window = Window {
            kind: galata_wire::Kind::Quotes,
            from_micros: 0,
            to_micros: 200 * DAY,
            ticker: Some("DOGE".to_owned()),
        };
        assert_eq!(rows_in(&reader.view(window).unwrap()), 0);
    }

    fn funding_row(seq: u64, venue: &str) -> Row {
        Row {
            stream_seq: seq,
            source_recv_micros: 100 * DAY,
            envelope: Envelope::new(
                Venue::new(venue).unwrap(),
                Ticker::new("BTC").unwrap(),
                Some(100 * DAY),
                100 * DAY,
                Event::Funding(galata_wire::Funding {
                    rate: Num::from_str("0.1").unwrap(),
                    next_micros: None,
                    premium: None,
                }),
            ),
        }
    }

    fn quotes_window() -> Window {
        Window {
            kind: galata_wire::Kind::Quotes,
            from_micros: 0,
            to_micros: 200 * DAY,
            ticker: None,
        }
    }

    #[test]
    fn each_venue_has_its_own_bound() {
        // Each venue numbers its stream from its own process's boot, so the
        // numbers are comparable within a venue and nowhere else.
        let dir = tempfile::tempdir().unwrap();
        let mut tape = Tape::open(dir.path());
        tape.take(row(1_000, "venue-a", Some(100 * DAY)));
        tape.take(row(5_000_000, "venue-b", Some(100 * DAY + 1)));
        tape.commit().unwrap();

        let reader = Reader::open(dir.path(), &["kind=quotes"]).unwrap();
        assert_eq!(reader.bound().of_venue("venue-a"), Some(1_000));
        assert_eq!(reader.bound().of_venue("venue-b"), Some(5_000_000));
    }

    #[test]
    fn another_venues_numbering_does_not_hide_a_row() {
        // Reproduced before the fix: venue-b's funding set one bound of 150,
        // and venue-a's durable quote at 5,000,000 vanished from the view with
        // no error. The same tape now refuses the two-scope read (venue-a has
        // no funding), and each scope read alone returns every durable row.
        let dir = tempfile::tempdir().unwrap();
        let mut tape = Tape::open(dir.path());
        tape.take(row(5_000_000, "venue-a", Some(100 * DAY)));
        tape.take(row(100, "venue-b", Some(100 * DAY + 1)));
        tape.take(funding_row(150, "venue-b"));
        tape.commit().unwrap();

        let reader = Reader::open(dir.path(), &["kind=quotes"]).unwrap();
        assert_eq!(
            seqs(&reader.view(quotes_window()).unwrap()),
            vec![100, 5_000_000]
        );
    }

    #[test]
    fn a_venue_missing_from_one_declared_scope_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let mut tape = Tape::open(dir.path());
        tape.take(row(5_000_000, "venue-a", Some(100 * DAY)));
        tape.take(row(100, "venue-b", Some(100 * DAY + 1)));
        tape.take(funding_row(150, "venue-b"));
        tape.commit().unwrap();

        match Reader::open(dir.path(), &["kind=quotes", "kind=funding"]) {
            Err(ReadError::NoFrontier { scopes }) => {
                assert_eq!(scopes, vec!["kind=funding for venue=venue-a".to_string()]);
            }
            other => panic!("expected NoFrontier, got {other:?}"),
        }
    }

    #[test]
    fn a_segment_written_before_labelling_refuses_the_view() {
        let dir = tempfile::tempdir().unwrap();
        let mut tape = Tape::open(dir.path());
        tape.take(row(1, "hyperliquid", Some(100 * DAY)));
        tape.commit().unwrap();
        // The same rows, rewritten without a label — as every segment written
        // before labelling is.
        let batch = galata_segments::read_segment(
            &galata_segments::list_segments(&dir.path().join("kind=quotes/date=1970-04-11"))[0].1,
        )
        .unwrap()
        .remove(0);
        let written = galata_segments::write_segment_pruned(
            &dir.path().join("kind=quotes/date=1970-04-11"),
            galata_segments::Cursor::Seq { first: 7, last: 9 },
            &batch,
            galata_segments::Codec::Zstd,
            &crate::tape::PRUNE_ON,
        )
        .unwrap();

        match Reader::open(dir.path(), &["kind=quotes"]) {
            Err(ReadError::Unlabelled { path }) => assert_eq!(path, written),
            other => panic!("expected Unlabelled, got {other:?}"),
        }
    }

    #[test]
    fn a_warm_cache_reads_no_footer() {
        let dir = tempfile::tempdir().unwrap();
        let mut tape = Tape::open(dir.path());
        for seq in [1, 2, 3] {
            tape.take(row(seq, "venue-a", Some(100 * DAY + seq as i64)));
            tape.commit().unwrap();
        }
        let labels = LabelCache::default();
        let cold = Bound::of_cached(dir.path(), &["kind=quotes"], &labels).unwrap();
        assert_eq!(labels.footer_reads(), 3);
        let warm = Bound::of_cached(dir.path(), &["kind=quotes"], &labels).unwrap();
        assert_eq!(labels.footer_reads(), 3, "a warm cache re-read a footer");
        assert_eq!(cold, warm);
    }

    /// Every directory under a scope set an hour back, and the scope's own
    /// directory a minute back: a history nothing has touched lately.
    fn quiet(scope_root: &Path) {
        let set = |dir: &Path, secs: u64| {
            std::fs::File::open(dir)
                .unwrap()
                .set_modified(std::time::SystemTime::now() - std::time::Duration::from_secs(secs))
                .unwrap();
        };
        for partition in galata_segments::partitions(scope_root) {
            set(&partition, 3_600);
        }
        set(scope_root, 60);
    }

    #[test]
    fn a_deep_quiet_history_reads_no_directory_warm() {
        let dir = tempfile::tempdir().unwrap();
        let mut tape = Tape::open(dir.path());
        for day in 0..40_i64 {
            tape.take(row(day as u64 + 1, "venue-a", Some((100 + day) * DAY)));
            tape.commit().unwrap();
        }
        quiet(&dir.path().join("kind=quotes"));
        let labels = LabelCache::default();
        let cold = Bound::of_cached(dir.path(), &["kind=quotes"], &labels).unwrap();
        let read = labels.directory_reads();
        assert_eq!(read, 41, "the scope and forty dates, once each");
        let warm = Bound::of_cached(dir.path(), &["kind=quotes"], &labels).unwrap();
        assert_eq!(cold, warm);
        // The scope's own directory is the newest in the walk, so it sits in
        // the racy margin and is read again; no quiet date is.
        assert_eq!(labels.directory_reads() - read, 1);
        assert_eq!(cold, Bound::of(dir.path(), &["kind=quotes"]).unwrap());
    }

    #[test]
    fn a_new_segment_in_an_old_partition_moves_the_cached_bound() {
        let dir = tempfile::tempdir().unwrap();
        let mut tape = Tape::open(dir.path());
        for day in 0..5_i64 {
            tape.take(row(day as u64 + 1, "venue-a", Some((100 + day) * DAY)));
            tape.commit().unwrap();
        }
        quiet(&dir.path().join("kind=quotes"));
        let labels = LabelCache::default();
        Bound::of_cached(dir.path(), &["kind=quotes"], &labels).unwrap();
        let before = Bound::of_cached(dir.path(), &["kind=quotes"], &labels).unwrap();
        assert_eq!(before.of_venue("venue-a"), Some(5));

        // A fill into the oldest day, as the history walk writes one.
        let mut tape = Tape::open(dir.path());
        tape.take(row(99, "venue-a", Some(100 * DAY + 7)));
        tape.commit().unwrap();
        let after = Bound::of_cached(dir.path(), &["kind=quotes"], &labels).unwrap();
        assert_eq!(after.of_venue("venue-a"), Some(99));
    }

    #[test]
    fn unwritten_through_the_cache_names_what_unwritten_names() {
        let dir = tempfile::tempdir().unwrap();
        let mut tape = Tape::open(dir.path());
        tape.take(row(1, "venue-a", Some(100 * DAY)));
        tape.commit().unwrap();
        std::fs::create_dir_all(dir.path().join("kind=trades/date=1970-04-11")).unwrap();
        let scopes = ["kind=quotes", "kind=trades", "kind=funding"];
        let labels = LabelCache::default();
        assert_eq!(
            unwritten_cached(dir.path(), &scopes, &labels),
            unwritten(dir.path(), &scopes)
        );
        assert_eq!(
            unwritten_cached(dir.path(), &scopes, &labels),
            ["kind=trades", "kind=funding"]
        );
    }

    #[test]
    fn a_bound_is_the_same_with_and_without_a_cache() {
        let dir = tempfile::tempdir().unwrap();
        let mut tape = Tape::open(dir.path());
        tape.take(row(10, "venue-a", Some(100 * DAY)));
        tape.take(row(20, "venue-b", Some(100 * DAY + 1)));
        tape.commit().unwrap();
        assert_eq!(
            Bound::of(dir.path(), &["kind=quotes"]).unwrap(),
            Bound::of_cached(dir.path(), &["kind=quotes"], &LabelCache::default()).unwrap()
        );
    }

    #[test]
    fn a_replaced_segment_is_read_again() {
        // A replacement at the same path is a new file: new mtime, and here a
        // new venue. The cache must not answer with the old label.
        let dir = tempfile::tempdir().unwrap();
        let mut tape = Tape::open(dir.path());
        tape.take(row(5, "venue-a", Some(100 * DAY)));
        let written = tape.commit().unwrap().remove(0);
        let labels = LabelCache::default();
        let before = Bound::of_cached(dir.path(), &["kind=quotes"], &labels).unwrap();
        assert_eq!(before.of_venue("venue-a"), Some(5));

        // Same name, other venue, a moment later.
        std::thread::sleep(std::time::Duration::from_millis(20));
        std::fs::remove_file(&written).unwrap();
        let mut tape = Tape::open(dir.path());
        tape.take(row(5, "venue-b", Some(100 * DAY)));
        let rewritten = tape.commit().unwrap().remove(0);
        assert_eq!(rewritten, written, "the same path");

        let after = Bound::of_cached(dir.path(), &["kind=quotes"], &labels).unwrap();
        assert_eq!(after.of_venue("venue-b"), Some(5));
        assert_eq!(
            after.of_venue("venue-a"),
            None,
            "answered from a stale entry"
        );
    }
}
