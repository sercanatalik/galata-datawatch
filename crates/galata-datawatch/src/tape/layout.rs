//! The tree's shape, and a check that asserts it.
//!
//! ```text
//!   tape/
//!     kind=quotes/
//!       date=2026-09-20/
//!         s-000000000123_000000000456.parquet
//! ```
//!
//! **Two levels, and `venue` is not one of them.** The venue is a column. See
//! the module documentation of [`crate::tape`] for the measurement that decided
//! it; in one line, a value written both as a partition level and as a column
//! has a value that depends on a reader flag.
//!
//! **`kind` sits above `date` here, and below `venue` in the archive.** Two
//! stores, two orderings, one reason each: the archive's unit is the capture —
//! a venue's bytes are retained, replayed or dropped as a subtree — while the
//! tape's unit is the dataset, whose schema and meaning are dataset-rooted, so
//! *"this dataset, everywhere"* is one prefix.

use std::path::{Path, PathBuf};

use galata_wire::Kind;

use crate::calendar::date_of;

/// `kind=<kind>/date=<yyyy-mm-dd>`
pub fn partition_of(kind: Kind, at_micros: i64) -> PathBuf {
    PathBuf::from(format!("kind={kind}")).join(format!("date={}", date_of(at_micros)))
}

/// Something wrong with the tree.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum LayoutProblem {
    /// A directory that is not `kind=` or `date=` where one was expected.
    Shape {
        /// Where.
        path: PathBuf,
        /// Why.
        reason: String,
    },
    /// A directory named for a ticker.
    TickerIsADirectory {
        /// Where.
        path: PathBuf,
    },
    /// A `venue=` level, which the tape does not have.
    VenueIsADirectory {
        /// Where.
        path: PathBuf,
    },
    /// A `kind=` naming something that is not a dataset.
    UnknownDataset {
        /// Where.
        path: PathBuf,
        /// What it said.
        name: String,
    },
    /// A segment that does not say whose rows it holds.
    ///
    /// Written before tape segments were labelled with their venue. It cannot
    /// be compared with anything, because a sequence means something only
    /// within one venue's stream.
    Unlabelled {
        /// Where.
        path: PathBuf,
    },
    /// Two segments of one venue claiming the same sequence range.
    OverlappingRanges {
        /// One.
        a: PathBuf,
        /// The other.
        b: PathBuf,
    },
}

impl std::fmt::Display for LayoutProblem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LayoutProblem::Shape { path, reason } => write!(f, "{}: {reason}", path.display()),
            LayoutProblem::TickerIsADirectory { path } => write!(
                f,
                "{}: a directory is named for a ticker. Ticker is a column, sorted within a \
                 segment — as a partition level it is tens of thousands of files a day for \
                 trades alone",
                path.display()
            ),
            LayoutProblem::VenueIsADirectory { path } => write!(
                f,
                "{}: a directory is named for a venue. The tape carries the venue as a COLUMN — \
                 written both ways, its value depends on whether the reader passes \
                 hive_partitioning, and nothing warns",
                path.display()
            ),
            LayoutProblem::UnknownDataset { path, name } => write!(
                f,
                "{}: {name:?} is not a dataset this tape projects",
                path.display()
            ),
            LayoutProblem::Unlabelled { path } => {
                write!(f, "{}: {}", path.display(), crate::tape::UNLABELLED_REMEDY)
            }
            LayoutProblem::OverlappingRanges { a, b } => write!(
                f,
                "{} and {} claim overlapping sequence ranges — a redelivery that would \
                 double-count",
                a.display(),
                b.display()
            ),
        }
    }
}

/// Assert the tree's shape.
///
/// Returns **everything** wrong with it rather than the first thing: a tree with
/// two problems reported one at a time is two runs.
pub fn check_layout(root: &Path) -> Vec<LayoutProblem> {
    let mut problems = Vec::new();
    for kind_dir in children(root) {
        let Some(name) = file_name(&kind_dir) else {
            continue;
        };
        let Some(kind) = name.strip_prefix("kind=") else {
            problems.push(classify(&kind_dir, &name));
            continue;
        };
        if kind.parse::<Kind>().is_err() {
            problems.push(LayoutProblem::UnknownDataset {
                path: kind_dir.clone(),
                name: kind.to_string(),
            });
        }
        for date_dir in children(&kind_dir) {
            let Some(name) = file_name(&date_dir) else {
                continue;
            };
            match name.strip_prefix("date=") {
                Some(date) if crate::calendar::midnight_of(date).is_some() => {}
                Some(date) => problems.push(LayoutProblem::Shape {
                    path: date_dir.clone(),
                    reason: format!("{date:?} is not a YYYY-MM-DD date"),
                }),
                None => problems.push(classify(&date_dir, &name)),
            }
            // **Below a date partition there are files and nothing else.** A
            // directory here is the shape this check exists for: `BTC/` under
            // a date is a ticker that became a partition level, which is tens
            // of thousands of files a day for trades alone.
            for deeper in children(&date_dir) {
                if let Some(name) = file_name(&deeper) {
                    problems.push(classify(&deeper, &name));
                }
            }
        }
    }
    // Reused rather than reimplemented: `galata-segments` already answers this,
    // and a second implementation of "do these ranges overlap" does not fail
    // when it drifts — it disagrees.
    //
    // **Within a venue.** A partition is shared by every venue that supplies
    // the dataset, and each venue numbers its stream from its own process's
    // boot — two venues started together have intersecting ranges that are no
    // overlap at all. So ranges are compared per venue label.
    let (overlaps, unlabelled) =
        galata_segments::overlapping_ranges_by_label(root, crate::tape::VENUE_LABEL);
    problems.extend(
        unlabelled
            .into_iter()
            .map(|path| LayoutProblem::Unlabelled { path }),
    );
    problems.extend(
        overlaps
            .into_iter()
            .map(|(a, b)| LayoutProblem::OverlappingRanges { a, b }),
    );
    problems
}

/// A directory that is not a partition level we recognise.
///
/// A bare name with no `=` is almost always a ticker, and a `venue=` level is
/// the one mistake this tape exists to prevent — so both are named rather than
/// folded into a general shape complaint.
fn classify(path: &Path, name: &str) -> LayoutProblem {
    if name.starts_with("venue=") {
        return LayoutProblem::VenueIsADirectory {
            path: path.to_path_buf(),
        };
    }
    if !name.contains('=') {
        return LayoutProblem::TickerIsADirectory {
            path: path.to_path_buf(),
        };
    }
    LayoutProblem::Shape {
        path: path.to_path_buf(),
        reason: format!("{name:?} is not a partition level the tape has"),
    }
}

fn children(dir: &Path) -> Vec<PathBuf> {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return Vec::new();
    };
    let mut out: Vec<PathBuf> = entries
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.is_dir())
        .collect();
    out.sort();
    out
}

fn file_name(path: &Path) -> Option<String> {
    Some(path.file_name()?.to_string_lossy().to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tree(dirs: &[&str]) -> tempfile::TempDir {
        let root = tempfile::tempdir().unwrap();
        for dir in dirs {
            std::fs::create_dir_all(root.path().join(dir)).unwrap();
        }
        root
    }

    #[test]
    fn a_correct_tree_has_nothing_wrong_with_it() {
        let root = tree(&["kind=quotes/date=2026-09-20", "kind=trades/date=2026-09-20"]);
        assert_eq!(check_layout(root.path()), Vec::new());
    }

    #[test]
    fn a_ticker_directory_is_reported() {
        // Tens of thousands of files a day for trades alone.
        let root = tree(&["kind=trades/date=2026-09-20/BTC"]);
        let problems = check_layout(root.path());
        assert!(
            problems
                .iter()
                .any(|p| matches!(p, LayoutProblem::TickerIsADirectory { .. })),
            "{problems:?}"
        );
    }

    #[test]
    fn a_venue_directory_is_reported() {
        // The mistake this tape exists to prevent: written as a level AND a
        // column, the value depends on a reader flag.
        let root = tree(&["kind=quotes/venue=hyperliquid"]);
        let problems = check_layout(root.path());
        assert!(
            problems
                .iter()
                .any(|p| matches!(p, LayoutProblem::VenueIsADirectory { .. })),
            "{problems:?}"
        );
        assert!(problems[0].to_string().contains("hive_partitioning"));
    }

    #[test]
    fn an_unknown_dataset_is_reported_by_name() {
        let root = tree(&["kind=quotez/date=2026-09-20"]);
        let problems = check_layout(root.path());
        assert!(problems[0].to_string().contains("quotez"), "{problems:?}");
    }

    #[test]
    fn a_date_that_is_not_one_is_reported() {
        let root = tree(&["kind=quotes/date=2026-13-45"]);
        let problems = check_layout(root.path());
        assert!(
            problems
                .iter()
                .any(|p| matches!(p, LayoutProblem::Shape { .. })),
            "{problems:?}"
        );
    }

    #[test]
    fn two_problems_are_reported_in_one_run() {
        // A tree with two problems reported one at a time is two runs.
        let root = tree(&[
            "kind=trades/date=2026-09-20/BTC",
            "kind=nonsense/date=2026-09-20",
        ]);
        let problems = check_layout(root.path());
        assert!(problems.len() >= 2, "{problems:?}");
    }

    #[test]
    fn a_partition_names_the_kind_and_the_date_and_nothing_else() {
        let path = partition_of(Kind::Quotes, 1_789_941_180_000_000);
        assert_eq!(path, PathBuf::from("kind=quotes").join("date=2026-09-20"));
        assert!(!path.to_string_lossy().contains("venue"));
    }

    /// Commit one venue's quotes at these sequences, as one segment.
    fn committed(root: &Path, venue: &str, seqs: &[u64]) {
        use crate::tape::writer::{Row, Tape};
        use galata_wire::{Envelope, Event, Num, Quote, Ticker, Venue};
        use std::str::FromStr;
        let mut tape = Tape::open(root);
        for (i, seq) in seqs.iter().enumerate() {
            tape.take(Row {
                stream_seq: *seq,
                envelope: Envelope::new(
                    Venue::new(venue).unwrap(),
                    Ticker::new("BTC").unwrap(),
                    Some(86_400_000_000 + i as i64),
                    86_400_000_000,
                    Event::Quote(Quote {
                        bid_px: Some(Num::from_str("1").unwrap()),
                        ask_px: None,
                        bid_sz: None,
                        ask_sz: None,
                        bid_spread: None,
                        ask_spread: None,
                    }),
                ),
            });
        }
        tape.commit().unwrap();
    }

    #[test]
    fn two_venues_ranges_intersecting_is_not_an_overlap() {
        // Each venue numbers its stream from its own process's boot; two
        // started together intersect, and that is no redelivery.
        let dir = tempfile::tempdir().unwrap();
        committed(dir.path(), "venue-a", &[100, 200]);
        committed(dir.path(), "venue-b", &[150, 250]);
        assert_eq!(check_layout(dir.path()), Vec::new());
    }

    #[test]
    fn one_venues_ranges_intersecting_is() {
        let dir = tempfile::tempdir().unwrap();
        committed(dir.path(), "venue-a", &[100, 200]);
        committed(dir.path(), "venue-a", &[150, 250]);
        let problems = check_layout(dir.path());
        assert_eq!(problems.len(), 1, "{problems:?}");
        assert!(matches!(
            problems[0],
            LayoutProblem::OverlappingRanges { .. }
        ));
    }

    #[test]
    fn an_unlabelled_segment_is_reported() {
        let dir = tempfile::tempdir().unwrap();
        committed(dir.path(), "venue-a", &[100, 200]);
        let date = dir.path().join("kind=quotes/date=1970-01-02");
        let unlabelled = date.join("s-300_400.parquet");
        std::fs::write(&unlabelled, b"written before labels").unwrap();
        let problems = check_layout(dir.path());
        assert_eq!(
            problems,
            vec![LayoutProblem::Unlabelled { path: unlabelled }]
        );
    }
}
