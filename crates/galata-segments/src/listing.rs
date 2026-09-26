//! Reading a tree without opening a file.
//!
//! Everything here answers from `readdir` alone. That is the point: the restart
//! path, the walk's resume and a reader's bound all ask *how far are we
//! durable*, over trees of tens of thousands of files, on every boot. A
//! watermark that required a footer read per segment would pay for the whole
//! tree to answer one number.

use std::path::{Path, PathBuf};

use crate::cursor::{Cursor, Variant};
use crate::error::SegmentError;

/// **Refuse a root that cannot be read**, rather than sweeping nothing.
///
/// Every listing in this module answers an unreadable directory with an empty
/// vector, which is right for a *subtree* — a partition that vanished mid-walk
/// is not a reason to abandon the others. It is wrong for a **declared root**:
/// no partitions means no candidates, which the binaries report as *nothing to
/// do* and exit 3. A mistyped path, an unmounted volume or a permissions
/// change then looks exactly like a tidy store, for as long as nobody checks.
///
/// The guards in `scripts/` have said this about themselves since Tier 0 — *a
/// guard handed a root it cannot scan reports success forever* — and the
/// binaries that act on the record did not.
///
/// A store that does not exist **yet** is refused too. A fresh install is a
/// one-time failure that says what to do; a typo is silent for months, and
/// nothing can tell them apart from the outside.
pub fn scannable(root: &Path) -> Result<(), SegmentError> {
    match std::fs::read_dir(root) {
        Ok(_) => Ok(()),
        Err(error) => Err(SegmentError::Unscannable {
            path: root.to_path_buf(),
            reason: error.to_string(),
        }),
    }
}

/// Every committed segment under a directory, oldest first.
///
/// Partial writes are skipped: they carry the temporary prefix and do not parse
/// as a segment name.
///
/// Ordering is **within a variant**. A partition is written by exactly one
/// source, so a partition holding two variants is a defect — reported by
/// [`mixed_cursors`], not silently ordered here.
///
/// **An unreadable directory is an empty listing here**, which is right for a
/// subtree and wrong for a declared root — see [`scannable`].
pub fn list_segments(dir: &Path) -> Vec<(Cursor, PathBuf)> {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return Vec::new();
    };
    let mut out: Vec<(Cursor, PathBuf)> = entries
        .filter_map(|e| e.ok())
        .filter_map(|e| {
            // The type readdir already returned, not a `stat` per entry — a
            // listing is walked per boot, per read and per compaction. A
            // symlink is the one case the cheap answer cannot settle, and only
            // then is the file asked.
            let file_type = e.file_type().ok()?;
            let path = e.path();
            if !(file_type.is_file() || (file_type.is_symlink() && path.is_file())) {
                return None;
            }
            let name = path.file_name()?.to_str()?;
            Cursor::parse(name).map(|c| (c, path))
        })
        .collect();
    out.sort_by_key(|(c, _)| (c.variant(), c.sort_key()));
    out
}

/// The two variants found in a partition, where it holds more than one.
///
/// `None` is the healthy case.
pub fn mixed_cursors(dir: &Path) -> Option<(Variant, Variant)> {
    let mut seen: Option<Variant> = None;
    for (cursor, _) in list_segments(dir) {
        match seen {
            None => seen = Some(cursor.variant()),
            Some(first) if first != cursor.variant() => return Some((first, cursor.variant())),
            Some(_) => {}
        }
    }
    None
}

/// Every partition directory under a root — one that holds segments.
pub fn partitions(root: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    walk_partitions(root, &mut out);
    out.sort();
    out
}

fn walk_partitions(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    let mut has_segments = false;
    for entry in entries.filter_map(|e| e.ok()) {
        let is_dir = entry
            .file_type()
            .map(|t| t.is_dir() || (t.is_symlink() && entry.path().is_dir()))
            .unwrap_or(false);
        if is_dir {
            walk_partitions(&entry.path(), out);
        } else if entry
            .file_name()
            .to_str()
            .is_some_and(|n| Cursor::parse(n).is_some())
        {
            has_segments = true;
        }
    }
    if has_segments {
        out.push(dir.to_path_buf());
    }
}

/// The furthest position a tree is durable to, and which kind of position it
/// is.
///
/// This is a **maximum**, which is right for the caller it was written for: a
/// process asking about its own scope wants the furthest point it reached. It
/// is wrong as a *bound* over a root several scopes write — see [`frontier`].
///
/// `None` when the tree holds no segment, which is a fresh start rather than an
/// error.
pub fn last_durable(root: &Path) -> Option<(Variant, i128)> {
    let mut best: Option<(Variant, i128)> = None;
    fold_cursors(root, &mut |cursor| {
        let here = (cursor.variant(), cursor.last_position());
        best = Some(match best {
            None => here,
            // Only comparable within a variant; a mixed tree is a defect
            // reported elsewhere, and taking the max of the first variant seen
            // is the conservative reading here.
            Some((v, p)) if v == here.0 => (v, p.max(here.1)),
            Some(existing) => existing,
        });
    });
    best
}

/// The furthest position of **one scope** under a root.
///
/// A scope is a directory directly under the root — one per writing process.
pub fn last_durable_for_scope(root: &Path, scope: &str) -> Option<(Variant, i128)> {
    last_durable(&root.join(scope))
}

/// The position a root is durable to **for every declared scope**: the minimum.
///
/// Several processes write one root, each under its own scope and neither
/// reading the other's watermark. The maximum would claim durability for a
/// range some scope has not written, so a view taken at it is complete for one
/// scope and holed for another — and an absent row and a not-yet-written row
/// are the same picture.
///
/// `None` when any declared scope has no position at all, **including when no
/// scope is declared**. A caller that must know *which* scope is missing walks
/// [`last_durable_for_scope`] itself; this answers the narrower question and
/// answers it safely.
pub fn frontier(root: &Path, scopes: &[&str]) -> Option<(Variant, i128)> {
    if scopes.is_empty() {
        return None;
    }
    let mut out: Option<(Variant, i128)> = None;
    for scope in scopes {
        let (variant, position) = last_durable_for_scope(root, scope)?;
        out = Some(match out {
            None => (variant, position),
            Some((v, p)) if v == variant => (v, p.min(position)),
            // Two scopes advancing through different kinds of position have no
            // common bound. Refusing is the only honest answer.
            Some(_) => return None,
        });
    }
    out
}

/// Segments whose position ranges overlap, which is how a redelivery shows up.
///
/// Reported rather than merged: the overlap is a fact about what happened, and
/// a reader that silently deduplicated would lose the evidence that a
/// redelivery occurred at all.
pub fn overlapping_ranges(root: &Path) -> Vec<(PathBuf, PathBuf)> {
    let mut overlaps = Vec::new();
    for dir in partitions(root) {
        overlaps.extend(overlaps_in(&list_segments(&dir)));
    }
    overlaps
}

/// The same, comparing only segments that carry the same value for a label.
///
/// For a store whose partitions are shared by several independently numbered
/// streams — the tape, where every venue that supplies a dataset writes into
/// one `kind=/date=` partition and numbers its sequences from its own clock.
/// Two streams' ranges intersecting there is not an overlap, and reporting it
/// as one would fail every run that meets it.
///
/// Returns the overlaps, and every segment that carries no such label: it
/// cannot be grouped, and guessing its group is the defect this exists to
/// avoid. A segment whose footer will not read is returned as unlabelled too,
/// since it is not known whose it is.
pub fn overlapping_ranges_by_label(
    root: &Path,
    key: &str,
) -> (Vec<(PathBuf, PathBuf)>, Vec<PathBuf>) {
    let mut overlaps = Vec::new();
    let mut unlabelled = Vec::new();
    for dir in partitions(root) {
        let mut streams: std::collections::BTreeMap<String, Vec<(Cursor, PathBuf)>> =
            std::collections::BTreeMap::new();
        for (cursor, path) in list_segments(&dir) {
            match crate::reader::label(&path, key) {
                Ok(Some(value)) => streams.entry(value).or_default().push((cursor, path)),
                Ok(None) | Err(_) => unlabelled.push(path),
            }
        }
        for listed in streams.values() {
            overlaps.extend(overlaps_in(listed));
        }
    }
    (overlaps, unlabelled)
}

/// Consecutive segments of one sorted listing whose ranges intersect.
///
/// The one definition of *overlap* both of the above use, so the two cannot
/// drift into disagreeing about it.
fn overlaps_in(listed: &[(Cursor, PathBuf)]) -> Vec<(PathBuf, PathBuf)> {
    listed
        .windows(2)
        .filter(|window| {
            let (a, _) = &window[0];
            let (b, _) = &window[1];
            a.variant() == b.variant() && b.first_position() <= a.last_position()
        })
        .map(|window| (window[0].1.clone(), window[1].1.clone()))
        .collect()
}

fn fold_cursors(dir: &Path, f: &mut impl FnMut(Cursor)) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.filter_map(|e| e.ok()) {
        let is_dir = entry
            .file_type()
            .map(|t| t.is_dir() || (t.is_symlink() && entry.path().is_dir()))
            .unwrap_or(false);
        if is_dir {
            fold_cursors(&entry.path(), f);
        } else if let Some(name) = entry.file_name().to_str()
            && let Some(cursor) = Cursor::parse(name)
        {
            f(cursor);
        }
    }
}

/// How close to the walk's newest directory mtime a cached listing may be and
/// still be trusted: **2 s**, the coarsest mtime granularity this may meet
/// (FAT; HFS+ is 1 s, APFS and ext4 1 ns).
///
/// A write in the same mtime tick as the listing that was cached leaves the
/// directory's mtime unchanged. Git's untracked cache meets the same thing and
/// calls it "racily clean". The reference here is the filesystem's own time:
/// any write after a walk is stamped at least as late as the newest mtime that
/// walk saw, so it cannot share a tick with a directory more than a margin
/// older. No clock is read, which is what lets `galata-datawatch` use this
/// below its loop.
pub const RACY_MARGIN: std::time::Duration = std::time::Duration::from_secs(2);

/// Directory listings already read, for a caller that walks the same tree
/// again and again.
///
/// **Measured** (`design/measured.md`, 2026-09-26): with 2,230 date partitions
/// under `kind=candles`, [`partitions`] plus [`list_segments`] took 72 ms
/// warm, because they read every directory twice. A watch asks that every
/// second, about a history that has not changed.
///
/// **Keyed by each directory's own mtime.** POSIX `rename()` marks the mtime
/// of each parent directory for update, and so do creating and unlinking. A
/// segment arrives, leaves and is replaced only by rename, so a directory whose
/// mtime has not moved holds the entries it held. A change inside a
/// subdirectory does not move its parent, so every directory is still
/// `stat`ed. Only the `read_dir` is skipped, and only for a directory more than
/// [`RACY_MARGIN`] older than the newest one in the walk that read it.
///
/// Caller-owned, like `galata-datawatch`'s `LabelCache`: a library holding
/// state nobody asked for is state nobody can bound or drop.
#[derive(Debug, Default)]
pub struct ListingCache {
    dirs: std::sync::Mutex<std::collections::BTreeMap<PathBuf, Listed>>,
    reads: std::sync::atomic::AtomicU64,
}

#[derive(Debug, Clone)]
struct Listed {
    modified: std::time::SystemTime,
    /// Older than the newest mtime of the walk that read it by more than the
    /// margin: safe to reuse while `modified` is unchanged.
    trusted: bool,
    subdirs: Vec<PathBuf>,
    segments: Vec<(Cursor, PathBuf)>,
}

impl ListingCache {
    /// Every partition under `root` that holds a segment, with its segments
    /// oldest first: what [`partitions`] and [`list_segments`] answer
    /// together, in the same order, reading only directories that moved.
    ///
    /// **An unreadable directory is an empty listing**, as elsewhere in this
    /// module; see [`scannable`].
    pub fn partitions_with_segments(&self, root: &Path) -> Vec<(PathBuf, Vec<(Cursor, PathBuf)>)> {
        let mut seen: Vec<(PathBuf, std::time::SystemTime)> = Vec::new();
        let mut out = Vec::new();
        self.walk(root, &mut seen, &mut out);

        // Settle which of this walk's listings the next walk may trust.
        if let Some(newest) = seen.iter().map(|(_, m)| *m).max()
            && let Ok(mut dirs) = self.dirs.lock()
        {
            for (dir, modified) in &seen {
                if let Some(listed) = dirs.get_mut(dir) {
                    listed.trusted = modified
                        .checked_add(RACY_MARGIN)
                        .is_some_and(|edge| edge < newest);
                }
            }
        }
        out.sort_by(|a, b| a.0.cmp(&b.0));
        out
    }

    fn walk(
        &self,
        dir: &Path,
        seen: &mut Vec<(PathBuf, std::time::SystemTime)>,
        out: &mut Vec<(PathBuf, Vec<(Cursor, PathBuf)>)>,
    ) {
        let Ok(modified) = std::fs::metadata(dir).and_then(|m| m.modified()) else {
            return;
        };
        seen.push((dir.to_path_buf(), modified));
        let cached = self.dirs.lock().ok().and_then(|dirs| {
            dirs.get(dir)
                .filter(|l| l.trusted && l.modified == modified)
                .cloned()
        });
        let listed = match cached {
            Some(listed) => listed,
            None => {
                let listed = self.read(dir, modified);
                if let Ok(mut dirs) = self.dirs.lock() {
                    dirs.insert(dir.to_path_buf(), listed.clone());
                }
                listed
            }
        };
        if !listed.segments.is_empty() {
            out.push((dir.to_path_buf(), listed.segments));
        }
        for sub in &listed.subdirs {
            self.walk(sub, seen, out);
        }
    }

    /// One `read_dir`: the subdirectories and the committed segments, by the
    /// rules [`partitions`] and [`list_segments`] apply.
    fn read(&self, dir: &Path, modified: std::time::SystemTime) -> Listed {
        self.reads
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let mut subdirs = Vec::new();
        let mut segments = Vec::new();
        if let Ok(entries) = std::fs::read_dir(dir) {
            for entry in entries.filter_map(|e| e.ok()) {
                let Ok(file_type) = entry.file_type() else {
                    continue;
                };
                let path = entry.path();
                if file_type.is_dir() || (file_type.is_symlink() && path.is_dir()) {
                    subdirs.push(path);
                } else if (file_type.is_file() || (file_type.is_symlink() && path.is_file()))
                    && let Some(cursor) = path
                        .file_name()
                        .and_then(|n| n.to_str())
                        .and_then(Cursor::parse)
                {
                    segments.push((cursor, path));
                }
            }
        }
        subdirs.sort();
        segments.sort_by_key(|(c, _)| (c.variant(), c.sort_key()));
        Listed {
            modified,
            trusted: false,
            subdirs,
            segments,
        }
    }

    /// How many directories this cache has had to read, for asserting that
    /// a warm walk reads only what moved.
    pub fn directory_reads(&self) -> u64 {
        self.reads.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Forget every directory that no longer exists. Called by whoever owns
    /// the cache, on its own cadence.
    pub fn prune(&self) {
        if let Ok(mut dirs) = self.dirs.lock() {
            dirs.retain(|path, _| path.exists());
        }
    }
}
