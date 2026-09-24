//! **The component that judges**, which the status surface deliberately is not.
//!
//! `status.rs` reports and never judges — every field there is an elapsed time,
//! a count or a state, and none says whether any of them is bad. That was
//! decided when it was written:
//!
//! > A threshold inside the capture process cannot be changed without a deploy,
//! > and is wrong for the next instrument anyway. The component that judges is a
//! > different one, and it can be changed without stopping capture.
//!
//! This is that different one.
//!
//! # A heartbeat is a claim; a file on disk is a fact
//!
//! ```text
//!   a claim                    a fact
//!   ───────                    ──────
//!   "I am alive"               this partition holds 1,412 segments
//!   "last poll succeeded"      the newest segment is 40 minutes old
//!   "24 subscriptions held"    kind=gaps holds 24 rows since midnight
//! ```
//!
//! A process reporting itself well is the least reliable witness available: the
//! failures that matter most are the ones where it does not know. So this reads
//! **the tree**, and reads the status file only for what the tree cannot say.

use std::path::{Path, PathBuf};

/// Something worth telling an operator.
///
/// **Observed, expected and where** — all three. An alert saying *compaction
/// overdue* makes somebody go and find out; a finding naming the path and both
/// numbers has already done that, and can be **checked** rather than trusted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Finding {
    /// What was measured.
    pub observed: String,
    /// What was declared acceptable.
    pub expected: String,
    /// Where to look.
    pub at: PathBuf,
}

impl std::fmt::Display for Finding {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} (expected {}) — {}",
            self.observed,
            self.expected,
            self.at.display()
        )
    }
}

/// What the operator declared acceptable.
///
/// **No `Default` that invents a number.** Same argument as retention horizons:
/// a default is right for one venue's cadence and wrong for the next, and a
/// threshold nobody chose is one nobody will believe when it fires.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Thresholds {
    /// The most segments a **closed** partition may hold before compaction is
    /// overdue. `None` checks nothing.
    pub max_segments_in_closed_partition: Option<usize>,
    /// How old the newest segment may be, in seconds, before the record is
    /// stale. `None` checks nothing.
    pub max_record_age_secs: Option<u64>,
}

impl Thresholds {
    /// Whether anything at all is declared.
    pub fn is_empty(&self) -> bool {
        self.max_segments_in_closed_partition.is_none() && self.max_record_age_secs.is_none()
    }
}

/// What one run saw.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Report {
    /// Everything worth telling somebody.
    pub findings: Vec<Finding>,
    /// Whether there was anything to look at.
    pub checked: usize,
}

impl Report {
    /// Whether anything is wrong.
    pub fn is_clean(&self) -> bool {
        self.findings.is_empty()
    }

    /// Whether there was anything to check.
    ///
    /// **Separate from clean.** *Checked and found nothing* and *had nothing to
    /// check* are different facts, and the second is what an empty archive
    /// looks like when capture has silently stopped.
    pub fn had_nothing(&self) -> bool {
        self.checked == 0
    }
}

/// Read the record and judge it.
///
/// `now_micros` is the caller's clock — nothing below the loop reads one, and
/// this is not below the loop, it is beside it.
pub fn watch(
    archive_root: &Path,
    tape_root: &Path,
    today: &str,
    thresholds: &Thresholds,
    now_micros: i64,
    venues: &[&str],
) -> Report {
    let mut report = Report::default();

    // **Unconditional, and only on the tape**: a layout nobody declared a
    // threshold for is still wrong, and a ticker that became a directory is not
    // a matter of degree.
    //
    // **Not on the archive**, which has a different shape and a different rule.
    // Running the tape's check there was the first thing this watcher did
    // wrong, and the real archive found it: twenty-four gaps written in one
    // flush all carry the same microsecond, so their segments have identical
    // time ranges — which the tape's *sequence ranges must not overlap* rule
    // reports as forty-six redeliveries. They are not redeliveries. The archive
    // distinguishes them by pid and flush sequence precisely so that this is
    // legal.
    for problem in crate::tape::check_layout(tape_root) {
        report.findings.push(Finding {
            observed: problem.to_string(),
            expected: "a tree of kind=/date= partitions".into(),
            at: tape_root.to_path_buf(),
        });
    }

    let partitions = galata_segments::partitions(archive_root);
    report.checked = partitions.len();

    // **Nesting, unconditionally, and on BOTH stores** — which is not the
    // overlap rule refused above.
    //
    // Two segments sharing a range is ordinary here: those twenty-four gaps
    // share a microsecond and are told apart by pid and flush sequence. One
    // segment *containing* another cannot arise that way at all — a live
    // writer flushes in receipt order and a tape in sequence order, so
    // segments written normally abut. Containment is an interrupted
    // compaction, and nothing else.
    //
    // Compaction repairs it on its next sweep. This is the window before that,
    // in which a rebuild would double those rows **silently**: the duplicated
    // payloads keep their sequences, so nothing downstream overlaps either.
    for root in [archive_root, tape_root] {
        for partition in galata_segments::partitions(root) {
            let nested = galata_segments::nested(&partition);
            if !nested.is_empty() {
                report.findings.push(Finding {
                    observed: format!(
                        "{} segment(s) a wider one in this partition already holds",
                        nested.len()
                    ),
                    expected: "segments that abut — containment is an interrupted compaction, \
                               which `galata-compact` finishes"
                        .into(),
                    at: partition.clone(),
                });
            }
        }
    }

    if let Some(max) = thresholds.max_segments_in_closed_partition {
        // **Closed only.** A partition still being written to is supposed to
        // hold many small segments; that is what `flush_secs = 2` buys.
        for (path, segments) in galata_segments::overdue_closed(archive_root, today, max) {
            report.findings.push(Finding {
                observed: format!("{segments} segments in a closed partition"),
                expected: format!("at most {max}"),
                at: path,
            });
        }
    }

    // **Per declared venue.** Each venue is its own capture process, so the
    // newest segment of the whole archive is only the freshest venue's — and
    // a max across venues hid every other one: one process could die while
    // another kept writing, and this reported a clean record indefinitely.
    // Declared venues, not every `venue=` found: an archive keeps a retired
    // venue's record, and judging it would apply a bound nobody set for it.
    if let Some(max_age) = thresholds.max_record_age_secs {
        for venue in venues {
            let scope = format!("venue={venue}");
            let at = archive_root.join(&scope);
            match galata_segments::last_durable_for_scope(archive_root, &scope) {
                // The most stale a declared venue can be — and it used to
                // read as clean.
                None => report.findings.push(Finding {
                    observed: format!("{venue} is declared and has captured nothing"),
                    expected: format!("a segment at most {max_age} s old"),
                    at,
                }),
                Some((_, position)) => {
                    let age_secs = (now_micros - position as i64).div_euclid(1_000_000);
                    if age_secs > max_age as i64 {
                        report.findings.push(Finding {
                            observed: format!("{venue}'s newest segment is {age_secs} s old"),
                            expected: format!("at most {max_age} s"),
                            at,
                        });
                    }
                }
            }
        }
    }

    report
}

#[cfg(test)]
mod tests {
    use super::*;

    const SECOND: i64 = 1_000_000;

    fn tree(dirs: &[&str]) -> tempfile::TempDir {
        let root = tempfile::tempdir().unwrap();
        for dir in dirs {
            std::fs::create_dir_all(root.path().join(dir)).unwrap();
        }
        root
    }

    /// Segment files, named only — `check_layout` and `nested` read the names.
    fn segments(root: &std::path::Path, partition: &str, names: &[&str]) {
        let dir = root.join(partition);
        std::fs::create_dir_all(&dir).unwrap();
        for name in names {
            std::fs::write(dir.join(name), b"").unwrap();
        }
    }

    #[test]
    fn segments_that_abut_are_not_an_interrupted_compaction() {
        // The ordinary archive: one flush after another, ranges abutting.
        // **Separate roots.** The archive puts `venue=` above `kind=` and the
        // tape carries the venue as a column, so one directory cannot be both
        // — handing it as both is what `check_layout` correctly complains
        // about.
        let archive = tempfile::tempdir().unwrap();
        let tape = tempfile::tempdir().unwrap();
        segments(
            archive.path(),
            "venue=hyperliquid/kind=trades/date=2026-09-20",
            &["t-100_199_4711_1.parquet", "t-200_299_4711_2.parquet"],
        );
        let report = watch(
            archive.path(),
            tape.path(),
            "2026-09-21",
            &Thresholds::default(),
            300,
            &[],
        );
        assert!(report.findings.is_empty(), "{:?}", report.findings);
    }

    #[test]
    fn two_segments_sharing_a_range_are_not_either() {
        // **The lesson this watcher learned the hard way.** Twenty-four gaps
        // flushed in one microsecond carry the same time range and are told
        // apart by pid and flush sequence. The tape's *ranges must not
        // overlap* rule called forty-six of those redeliveries; they are not.
        let archive = tempfile::tempdir().unwrap();
        let tape = tempfile::tempdir().unwrap();
        segments(
            archive.path(),
            "venue=hyperliquid/kind=gaps/date=2026-09-20",
            &["t-100_100_4711_1.parquet", "t-100_100_4711_2.parquet"],
        );
        let report = watch(
            archive.path(),
            tape.path(),
            "2026-09-21",
            &Thresholds::default(),
            300,
            &[],
        );
        assert!(report.findings.is_empty(), "{:?}", report.findings);
    }

    #[test]
    fn a_segment_inside_another_is_reported() {
        // Containment cannot come from concurrent flushes — a writer flushes
        // in receipt order, so segments abut. This is a compaction that wrote
        // its replacement and died before removing what it replaced.
        let archive = tempfile::tempdir().unwrap();
        let tape = tempfile::tempdir().unwrap();
        segments(
            archive.path(),
            "venue=hyperliquid/kind=trades/date=2026-09-20",
            &[
                "t-100_299_4711_9.parquet",
                "t-100_199_4711_1.parquet",
                "t-200_299_4711_2.parquet",
            ],
        );
        let report = watch(
            archive.path(),
            tape.path(),
            "2026-09-21",
            &Thresholds::default(),
            300,
            &[],
        );
        let said = report
            .findings
            .iter()
            .map(|f| f.to_string())
            .collect::<Vec<_>>()
            .join(" | ");
        assert!(said.contains("already holds"), "{said}");
        // Both of the narrow ones, not one.
        assert!(said.contains("2 segment"), "{said}");
        // And it says what to do about it.
        assert!(said.contains("galata-compact"), "{said}");
    }

    #[test]
    fn no_thresholds_still_reports_what_needs_none() {
        // A ticker that became a directory is not a matter of degree, so it is
        // reported whether or not anybody declared a number.
        let root = tree(&["kind=trades/date=2026-09-20/BTC"]);
        let report = watch(
            root.path(),
            root.path(),
            "2026-09-21",
            &Thresholds::default(),
            0,
            &[],
        );
        assert!(!report.is_clean());
        assert!(
            report.findings[0].observed.contains("ticker"),
            "{:?}",
            report.findings[0]
        );
    }

    #[test]
    fn no_thresholds_invents_no_bound() {
        // A threshold nobody chose is one nobody will believe when it fires.
        let root = tree(&["kind=quotes/date=2026-09-20"]);
        let report = watch(
            root.path(),
            root.path(),
            "2026-09-21",
            &Thresholds::default(),
            0,
            &[],
        );
        assert!(report.is_clean());
    }

    #[test]
    fn a_finding_states_the_number_the_bound_and_the_path() {
        // An alert saying "compaction overdue" makes somebody go and find out.
        let finding = Finding {
            observed: "3589 segments in a closed partition".into(),
            expected: "at most 64".into(),
            at: PathBuf::from("var/archive/venue=hyperliquid/kind=trades/date=2026-09-20"),
        };
        let said = finding.to_string();
        assert!(said.contains("3589"), "{said}");
        assert!(said.contains("at most 64"), "{said}");
        assert!(said.contains("kind=trades"), "{said}");
    }

    #[test]
    fn an_empty_tree_is_not_the_same_as_a_clean_one() {
        // *Checked and found nothing* and *had nothing to check* are different
        // facts, and the second is what an empty archive looks like when
        // capture has silently stopped.
        let root = tempfile::tempdir().unwrap();
        let report = watch(
            root.path(),
            root.path(),
            "2026-09-21",
            &Thresholds::default(),
            0,
            &[],
        );
        assert!(report.is_clean());
        assert!(report.had_nothing(), "an empty tree read as merely clean");
    }

    #[test]
    fn a_stale_record_is_reported_against_the_callers_clock() {
        use galata_segments::{Codec, Cursor, write_segment};
        let root = tempfile::tempdir().unwrap();
        // The record's age is the archive's; the tape is its own root, empty
        // here. One root doing both made an archive segment a tape segment,
        // which the tape now reports for carrying no venue label.
        let tape = tempfile::tempdir().unwrap();
        let dir = root
            .path()
            .join("venue=hyperliquid/kind=quotes/date=2026-09-20");
        let batch = arrow::record_batch::RecordBatch::try_new(
            std::sync::Arc::new(arrow::datatypes::Schema::new(vec![
                arrow::datatypes::Field::new(
                    "recv_micros",
                    arrow::datatypes::DataType::Int64,
                    false,
                ),
            ])),
            vec![std::sync::Arc::new(arrow::array::Int64Array::from(vec![
                100 * SECOND,
            ]))],
        )
        .unwrap();
        write_segment(
            &dir,
            Cursor::Time {
                first_micros: 100 * SECOND,
                last_micros: 100 * SECOND,
                pid: 1,
                seq: 1,
            },
            &batch,
            Codec::Uncompressed,
        )
        .unwrap();

        let thresholds = Thresholds {
            max_segments_in_closed_partition: None,
            max_record_age_secs: Some(30),
        };
        // Sixty seconds later: stale.
        let report = watch(
            root.path(),
            tape.path(),
            "2026-09-21",
            &thresholds,
            160 * SECOND,
            &["hyperliquid"],
        );
        assert!(
            !report.is_clean(),
            "a minute-old record passed a 30 s bound"
        );
        assert!(
            report.findings[0].observed.contains("60 s old"),
            "{:?}",
            report.findings[0]
        );

        // Ten seconds later: not.
        let fresh = watch(
            root.path(),
            tape.path(),
            "2026-09-21",
            &thresholds,
            110 * SECOND,
            &["hyperliquid"],
        );
        assert!(fresh.is_clean(), "{:?}", fresh.findings);
    }

    #[test]
    fn today_is_never_judged_for_segment_count() {
        // A partition still being written to is SUPPOSED to hold many small
        // segments; that is what a two-second flush buys.
        let root = tree(&["kind=quotes/date=2026-09-21"]);
        let thresholds = Thresholds {
            max_segments_in_closed_partition: Some(1),
            max_record_age_secs: None,
        };
        assert!(watch(root.path(), root.path(), "2026-09-21", &thresholds, 0, &[]).is_clean());
    }

    #[test]
    fn the_archive_is_not_judged_by_the_tapes_layout_rule() {
        // **Found by the first real run.** Twenty-four gaps written in one
        // flush all carry the same microsecond, so their segments have
        // identical time ranges — which the tape's *sequence ranges must not
        // overlap* rule reported as forty-six redeliveries against a real
        // nine-hour archive. They are not redeliveries: the archive
        // distinguishes them by pid and flush sequence precisely so this is
        // legal.
        use galata_segments::{Codec, Cursor, write_segment};
        let archive = tempfile::tempdir().unwrap();
        let tape = tempfile::tempdir().unwrap();
        let dir = archive.path().join("venue=v/kind=gaps/date=2026-09-20");
        let batch = arrow::record_batch::RecordBatch::try_new(
            std::sync::Arc::new(arrow::datatypes::Schema::new(vec![
                arrow::datatypes::Field::new(
                    "recv_micros",
                    arrow::datatypes::DataType::Int64,
                    false,
                ),
            ])),
            vec![std::sync::Arc::new(arrow::array::Int64Array::from(vec![
                100 * SECOND,
            ]))],
        )
        .unwrap();
        // Two flushes at the SAME instant, as a burst of gaps really is.
        for seq in 1..=2u64 {
            write_segment(
                &dir,
                Cursor::Time {
                    first_micros: 100 * SECOND,
                    last_micros: 100 * SECOND,
                    pid: 7,
                    seq,
                },
                &batch,
                Codec::Uncompressed,
            )
            .unwrap();
        }

        let report = watch(
            archive.path(),
            tape.path(),
            "2026-09-21",
            &Thresholds::default(),
            0,
            &[],
        );
        assert!(
            report.is_clean(),
            "the archive was judged by the tape's rule: {:?}",
            report.findings
        );
    }

    fn aged() -> Thresholds {
        Thresholds {
            max_segments_in_closed_partition: None,
            max_record_age_secs: Some(300),
        }
    }

    #[test]
    fn one_venue_stopping_is_reported_while_another_runs() {
        // Reproduced before the fix: venue-a silent 600 s beside a venue-b
        // written 5 s ago, and the watch reported nothing at all.
        let archive = tempfile::tempdir().unwrap();
        let tape = tempfile::tempdir().unwrap();
        segments(
            archive.path(),
            "venue=venue-a/kind=quotes/date=1970-01-01",
            &["t-100000000_100000000_1_1.parquet"],
        );
        segments(
            archive.path(),
            "venue=venue-b/kind=quotes/date=1970-01-01",
            &["t-695000000_695000000_2_1.parquet"],
        );
        let report = watch(
            archive.path(),
            tape.path(),
            "1970-01-02",
            &aged(),
            700 * SECOND,
            &["venue-a", "venue-b"],
        );
        assert_eq!(report.findings.len(), 1, "{:?}", report.findings);
        assert!(report.findings[0].observed.starts_with("venue-a"));
        assert!(report.findings[0].at.ends_with("venue=venue-a"));
    }

    #[test]
    fn a_declared_venue_that_captured_nothing_is_reported() {
        let archive = tempfile::tempdir().unwrap();
        let tape = tempfile::tempdir().unwrap();
        segments(
            archive.path(),
            "venue=venue-b/kind=quotes/date=1970-01-01",
            &["t-695000000_695000000_2_1.parquet"],
        );
        let report = watch(
            archive.path(),
            tape.path(),
            "1970-01-02",
            &aged(),
            700 * SECOND,
            &["venue-b", "venue-c"],
        );
        assert_eq!(report.findings.len(), 1, "{:?}", report.findings);
        assert!(report.findings[0].observed.contains("captured nothing"));
    }

    #[test]
    fn an_undeclared_venue_is_not_judged() {
        let archive = tempfile::tempdir().unwrap();
        let tape = tempfile::tempdir().unwrap();
        segments(
            archive.path(),
            "venue=retired/kind=quotes/date=1970-01-01",
            &["t-1000000_1000000_1_1.parquet"],
        );
        segments(
            archive.path(),
            "venue=venue-b/kind=quotes/date=1970-01-01",
            &["t-695000000_695000000_2_1.parquet"],
        );
        let report = watch(
            archive.path(),
            tape.path(),
            "1970-01-02",
            &aged(),
            700 * SECOND,
            &["venue-b"],
        );
        assert!(report.is_clean(), "{:?}", report.findings);
    }
}
