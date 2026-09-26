//! Holds across processes — the only case that matters.
//!
//! A hold exists to keep a compaction, a deletion and a rebuild **in different
//! processes** out of each other's way; the unit tests in `hold.rs` take both
//! holds inside one process, which proves the modes and not the claim. Here
//! the test binary re-runs itself as the other holder.
//!
//! Written because the first cross-process check of this change, run by hand
//! with a Python `flock` holder, saw an exclusive hold succeed beside another
//! process's exclusive lock three times, then refuse correctly in every one of
//! the next 46 trials. **Explained since** (`design/measured.md`, 2026-09-26):
//! that holder held for a fixed 3–4 s, and each failing tool had sat 50–120 s
//! in macOS's launch-time assessment of a new binary before it reached
//! `flock`. By then nothing held the file.
//!
//! **So the other holder here holds until it is told to stop, never for a
//! duration**, and the test acts only after reading `HELD`. A launch delay on
//! either side then makes the test slower, never falsely green.

use std::io::{BufRead, BufReader};
use std::path::Path;
use std::process::{Child, Command, Stdio};

use galata_segments::{SegmentError, hold, hold_shared};

/// Marks the child's instruction: `other-holder:<mode>:<root>`.
///
/// Passed as an extra argument rather than an environment variable, because
/// `check-secret-reach.sh` holds that the environment is read in one module.
/// libtest takes it as a second filter, which matches no test and so changes
/// nothing about which test runs.
const INSTRUCTION: &str = "other-holder:";

/// The child's half: take the hold, say so, keep it until stdin closes.
fn be_the_other_holder() -> bool {
    let Some(instruction) =
        std::env::args().find_map(|a| a.strip_prefix(INSTRUCTION).map(String::from))
    else {
        return false;
    };
    let (mode, root) = instruction.split_once(':').expect("mode:root");
    let root = Path::new(root);
    let _held = match mode {
        "exclusive" => hold(root).unwrap(),
        _ => hold_shared(root).unwrap(),
    };
    println!("HELD");
    // Holds until the parent drops its end of stdin.
    let _ = std::io::stdin().read_line(&mut String::new());
    true
}

/// Another process, holding a root, and the pipe it reports on.
///
/// The pipe is kept, not dropped: the child's test harness prints its result
/// after the hold is released, and a closed pipe turns that into a failure.
struct Other {
    child: Child,
    said: std::io::Lines<BufReader<std::process::ChildStdout>>,
}

/// Start another process holding `root`, and return once it holds it.
fn another_process_holds(root: &Path, mode: &str, test: &str) -> Other {
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", test, "--nocapture", "--test-threads=1"])
        .arg(format!("{INSTRUCTION}{mode}:{}", root.display()))
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    let mut said = BufReader::new(child.stdout.take().unwrap()).lines();
    // Ends with, not equals: libtest prints `test <name> ... ` without a
    // newline before the test's own output, so the marker ends that line.
    for line in said.by_ref() {
        if line.is_ok_and(|line| line.ends_with("HELD")) {
            return Other { child, said };
        }
    }
    let _ = child.kill();
    let status = child.wait();
    panic!("the other process never took its hold ({status:?})");
}

fn release(mut other: Other) {
    drop(other.child.stdin.take());
    for line in other.said {
        line.unwrap();
    }
    assert!(
        other.child.wait().unwrap().success(),
        "the other holder failed"
    );
}

#[test]
fn another_process_holding_exclusively_excludes_every_holder() {
    if be_the_other_holder() {
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let other = another_process_holds(
        dir.path(),
        "exclusive",
        "another_process_holding_exclusively_excludes_every_holder",
    );
    for _ in 0..20 {
        assert!(matches!(hold(dir.path()), Err(SegmentError::Held { .. })));
        assert!(matches!(
            hold_shared(dir.path()),
            Err(SegmentError::Held { .. })
        ));
    }
    release(other);
    hold(dir.path()).expect("released by the other process's exit");
}

#[test]
fn another_process_reading_excludes_a_writer_and_not_a_reader() {
    if be_the_other_holder() {
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let other = another_process_holds(
        dir.path(),
        "shared",
        "another_process_reading_excludes_a_writer_and_not_a_reader",
    );
    for _ in 0..20 {
        assert!(matches!(hold(dir.path()), Err(SegmentError::Held { .. })));
        drop(hold_shared(dir.path()).expect("two readers coexist across processes"));
    }
    release(other);
}
