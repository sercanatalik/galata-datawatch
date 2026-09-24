//! Holds on a root: who may change a store while who reads it.
//!
//! **Two modes, one file.** A writer that removes or rewrites segments —
//! compaction, deletion, a replacing rebuild — takes the root
//! [`Mode::Exclusive`]. A reader that lists segments and then opens them takes
//! it [`Mode::Shared`], which excludes the writers without excluding other
//! readers. The file is [`HOLD_FILE`] in either mode.
//!
//! **The lock is the kernel's advisory lock on the open file** (`flock` on
//! Unix, through std's `File::lock_shared`/`try_lock`, stable since Rust
//! 1.89), not a file whose presence means anything. Dropping a [`Hold`] — or
//! dying with it — releases it, so a killed holder leaves nothing to clear.
//!
//! **Advisory means every writer must ask.** Every writer of a galata store
//! does; a process that does not ask is not stopped. And `flock` is
//! unreliable over some network filesystems: the stores are local disk, and a
//! store moved onto NFS should be assumed unheld.

use std::fs::File;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use crate::error::SegmentError;

/// The file every holder of a root locks.
///
/// Dotted and not a segment name, so it is neither a segment to a listing nor
/// a reason to call its directory a partition. The name predates the shared
/// mode and is kept: renaming it would strand an old file nothing reads.
pub const HOLD_FILE: &str = ".compact.lock";

/// How often a waiting holder asks again.
///
/// The holds it waits for are measured in seconds (a 64,587-segment
/// compaction took 9.44 s), so a quarter-second poll costs nothing against
/// them and keeps the wait from spinning.
const POLL: Duration = Duration::from_millis(250);

/// Whether a hold excludes readers too.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    /// Excludes every other holder. For a writer that removes or rewrites.
    Exclusive,
    /// Excludes exclusive holders only. For a reader.
    Shared,
}

/// One holder's hold on a root, in one mode.
#[derive(Debug)]
pub struct Hold {
    _file: File,
    root: PathBuf,
    mode: Mode,
}

impl Hold {
    /// The root this hold covers.
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// The mode it was taken in.
    pub fn mode(&self) -> Mode {
        self.mode
    }
}

/// Take the root exclusively, or learn that somebody has it.
///
/// Two compactors on one partition each write a replacement over the same
/// originals; both renames succeed, both removals succeed, and the partition
/// holds its rows twice under two names. The interruption rule handles
/// *nesting*, not twins — so the case is refused by construction rather than
/// resolved after the fact.
///
/// The hold is the tool's, not a scheduler's: a `mkdir` lock in a shell would
/// be one scheduler's opinion and would go stale on a kill.
pub fn hold(root: &Path) -> Result<Hold, SegmentError> {
    try_hold(root, Mode::Exclusive)
}

/// Take the root shared, or learn that a writer has it.
pub fn hold_shared(root: &Path) -> Result<Hold, SegmentError> {
    try_hold(root, Mode::Shared)
}

/// Take the root in `mode`, waiting up to `patience` for a holder to finish.
///
/// For a holder whose work is worth doing late rather than not at all — a
/// rebuild behind a compaction. **Bounded**, because a wait without a bound is
/// a hang the moment a holder hangs, and the refusal after it names the root
/// exactly as [`hold`] does. Saying *that* it is waiting is the caller's: this
/// crate logs nothing.
pub fn wait(root: &Path, mode: Mode, patience: Duration) -> Result<Hold, SegmentError> {
    let deadline = Instant::now() + patience;
    loop {
        match try_hold(root, mode) {
            Err(SegmentError::Held { .. }) if Instant::now() < deadline => {
                std::thread::sleep(POLL.min(deadline.saturating_duration_since(Instant::now())));
            }
            other => return other,
        }
    }
}

fn try_hold(root: &Path, mode: Mode) -> Result<Hold, SegmentError> {
    std::fs::create_dir_all(root).map_err(|source| SegmentError::CreateDir {
        path: root.to_path_buf(),
        source,
    })?;
    let file = File::options()
        .create(true)
        .truncate(false)
        .write(true)
        .open(root.join(HOLD_FILE))
        .map_err(|source| SegmentError::Hold {
            root: root.to_path_buf(),
            source,
        })?;
    let taken = match mode {
        Mode::Exclusive => file.try_lock(),
        Mode::Shared => file.try_lock_shared(),
    };
    match taken {
        Ok(()) => Ok(Hold {
            _file: file,
            root: root.to_path_buf(),
            mode,
        }),
        Err(std::fs::TryLockError::WouldBlock) => Err(SegmentError::Held {
            root: root.to_path_buf(),
        }),
        Err(std::fs::TryLockError::Error(source)) => Err(SegmentError::Hold {
            root: root.to_path_buf(),
            source,
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn two_shared_holds_coexist() {
        let dir = tempfile::tempdir().unwrap();
        let _a = hold_shared(dir.path()).unwrap();
        let _b = hold_shared(dir.path()).unwrap();
    }

    #[test]
    fn a_compaction_is_refused_while_a_reader_holds_the_root() {
        let dir = tempfile::tempdir().unwrap();
        let _reader = hold_shared(dir.path()).unwrap();
        match hold(dir.path()) {
            Err(SegmentError::Held { root }) => assert_eq!(root, dir.path()),
            other => panic!("expected Held, got {other:?}"),
        }
    }

    #[test]
    fn a_reader_is_refused_while_a_writer_holds_the_root() {
        let dir = tempfile::tempdir().unwrap();
        let _writer = hold(dir.path()).unwrap();
        assert!(matches!(
            hold_shared(dir.path()),
            Err(SegmentError::Held { .. })
        ));
    }

    #[test]
    fn a_reader_waits_out_an_exclusive_hold() {
        let dir = tempfile::tempdir().unwrap();
        let writer = hold(dir.path()).unwrap();
        let releaser = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(400));
            drop(writer);
        });
        let started = Instant::now();
        let reader = wait(dir.path(), Mode::Shared, Duration::from_secs(10)).unwrap();
        assert_eq!(reader.mode(), Mode::Shared);
        assert!(
            started.elapsed() >= Duration::from_millis(300),
            "it did not wait"
        );
        releaser.join().unwrap();
    }

    #[test]
    fn a_reader_gives_up_after_its_patience() {
        let dir = tempfile::tempdir().unwrap();
        let _writer = hold(dir.path()).unwrap();
        let started = Instant::now();
        let refused = wait(dir.path(), Mode::Shared, Duration::from_millis(300));
        assert!(matches!(refused, Err(SegmentError::Held { .. })));
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "the patience was not a bound"
        );
    }
}
