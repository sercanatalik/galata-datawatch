//! Reading a tree without opening a file.
//!
//! Everything here answers from `readdir` alone. That is the point: the restart
//! path, the walk's resume and a reader's bound all ask *how far are we
//! durable*, over trees of tens of thousands of files, on every boot. A
//! watermark that required a footer read per segment would pay for the whole
//! tree to answer one number.

use std::path::{Path, PathBuf};

use crate::cursor::{Cursor, Variant};

/// Every committed segment under a directory, oldest first.
///
/// Partial writes are skipped: they carry the temporary prefix and do not parse
/// as a segment name.
///
/// Ordering is **within a variant**. A partition is written by exactly one
/// source, so a partition holding two variants is a defect — reported by
/// [`mixed_cursors`], not silently ordered here.
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
        let listed = list_segments(&dir);
        for window in listed.windows(2) {
            let (a, a_path) = &window[0];
            let (b, b_path) = &window[1];
            if a.variant() == b.variant() && b.first_position() <= a.last_position() {
                overlaps.push((a_path.clone(), b_path.clone()));
            }
        }
    }
    overlaps
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
