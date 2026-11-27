//! `galata-watch` writes its report to the status folder on every run, and
//! exits as it always has (`add-service-health-monitoring`, task 2.1).

use std::path::{Path, PathBuf};
use std::process::Command;

use galata_datawatch::watch::Written;

/// The committed example, its roots moved under `dir`.
fn configured(dir: &Path) -> PathBuf {
    let example = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../config/datawatch.toml");
    let mut text = std::fs::read_to_string(example).unwrap();
    for root in ["archive", "status", "tape"] {
        let path = dir.join(root);
        std::fs::create_dir_all(&path).unwrap();
        let line = format!("{root} = \"var/{root}\"");
        assert!(text.contains(&line), "the example names {line}");
        text = text.replace(&line, &format!("{root} = \"{}\"", path.display()));
    }
    let config = dir.join("datawatch.toml");
    std::fs::write(&config, text).unwrap();
    config
}

/// Segment files, named only: the structural checks read the names.
fn segments(root: &Path, partition: &str, names: &[&str]) {
    let at = root.join(partition);
    std::fs::create_dir_all(&at).unwrap();
    for name in names {
        std::fs::write(at.join(name), b"").unwrap();
    }
}

/// Run the judge; its exit code and the report it wrote.
fn judge(dir: &Path) -> (Option<i32>, Written) {
    let status = Command::new(env!("CARGO_BIN_EXE_galata-watch"))
        .env("GALATA_CONFIG", configured(dir))
        .env("RUST_LOG", "off")
        .status()
        .unwrap();
    let report = std::fs::read_to_string(dir.join("status/watch.json")).unwrap();
    (status.code(), serde_json::from_str(&report).unwrap())
}

/// An archive of one closed partition whose segments abut, as flushes leave them.
fn tidy_archive(dir: &Path) {
    segments(
        &dir.join("archive"),
        "venue=hyperliquid/kind=trades/date=2026-09-20",
        &["t-100_199_4711_1.parquet", "t-200_299_4711_2.parquet"],
    );
}

#[test]
fn a_clean_run_writes_its_time_and_no_finding() {
    let dir = tempfile::tempdir().unwrap();
    tidy_archive(dir.path());
    let before =
        galata_datawatch::capture::Clock::now_micros(&galata_datawatch::capture::SystemClock);
    let (code, report) = judge(dir.path());
    assert_eq!(code, Some(0), "clean, as before");
    assert!(report.findings.is_empty(), "{report:?}");
    assert!(report.checked > 0);
    assert!(report.judged_at_micros >= before, "the run's own time");
}

#[test]
fn a_run_with_findings_writes_each() {
    let dir = tempfile::tempdir().unwrap();
    tidy_archive(dir.path());
    // A ticker that became a directory, in the tape: wrong whatever the
    // thresholds.
    std::fs::create_dir_all(dir.path().join("tape/kind=trades/date=2026-09-20/BTC")).unwrap();
    let (code, report) = judge(dir.path());
    assert_eq!(code, Some(1), "findings, as before");
    assert!(!report.findings.is_empty());
    assert!(
        report.findings.iter().any(|f| f.observed.contains("ticker")
            && f.at.ends_with("tape")
            && !f.expected.is_empty()),
        "{report:?}"
    );
}

#[test]
fn nothing_to_check_is_written_as_a_finding() {
    // An empty archive is what capture having silently stopped looks like: it
    // exits 3 as before, and its report must not read as clean.
    let dir = tempfile::tempdir().unwrap();
    let (code, report) = judge(dir.path());
    assert_eq!(code, Some(3), "nothing to check, as before");
    assert_eq!(report.checked, 0);
    assert_eq!(report.findings.len(), 1, "{report:?}");
    assert!(report.findings[0].observed.contains("no partitions"));
}

#[test]
fn a_run_that_cannot_judge_says_why() {
    // A root that cannot be read is a refusal, not an empty result.
    let dir = tempfile::tempdir().unwrap();
    let config = configured(dir.path());
    std::fs::remove_dir(dir.path().join("archive")).unwrap();
    let status = Command::new(env!("CARGO_BIN_EXE_galata-watch"))
        .env("GALATA_CONFIG", config)
        .env("RUST_LOG", "off")
        .status()
        .unwrap();
    assert_eq!(status.code(), Some(1), "an error exits 1, as before");
    let report: Written = serde_json::from_str(
        &std::fs::read_to_string(dir.path().join("status/watch.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(report.findings.len(), 1, "{report:?}");
    assert!(
        report.findings[0]
            .observed
            .starts_with("the judge could not run"),
        "{report:?}"
    );
}
