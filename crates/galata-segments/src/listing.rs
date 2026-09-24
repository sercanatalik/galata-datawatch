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
