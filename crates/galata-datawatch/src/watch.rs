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

    if let Some(max_age) = thresholds.max_record_age_secs
        && let Some((_, position)) = galata_segments::last_durable(archive_root)
    {
        let age_secs = (now_micros - position as i64).div_euclid(1_000_000);
        if age_secs > max_age as i64 {
            report.findings.push(Finding {
                observed: format!("the newest segment is {age_secs} s old"),
                expected: format!("at most {max_age} s"),
                at: archive_root.to_path_buf(),
            });
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
        );
        assert!(report.is_clean());
        assert!(report.had_nothing(), "an empty tree read as merely clean");
    }

    #[test]
    fn a_stale_record_is_reported_against_the_callers_clock() {
        use galata_segments::{Codec, Cursor, write_segment};
        let root = tempfile::tempdir().unwrap();
        let dir = root.path().join("kind=quotes/date=2026-09-20");
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
            root.path(),
            "2026-09-21",
            &thresholds,
            160 * SECOND,
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
            root.path(),
            "2026-09-21",
            &thresholds,
            110 * SECOND,
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
        assert!(watch(root.path(), root.path(), "2026-09-21", &thresholds, 0).is_clean());
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
        );
        assert!(
            report.is_clean(),
            "the archive was judged by the tape's rule: {:?}",
            report.findings
        );
    }
}
