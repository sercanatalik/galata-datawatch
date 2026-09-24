//! Compaction: many small segments into few large ones.
//!
//! It ships **with** the writer rather than after it. The predecessor measured
//! 145,387 segments a day at a two-second flush, and a compaction that has
//! never run is a compaction that does not work — whose first real run will be
//! under disk pressure.
//!
//! **Order is the whole safety argument.** Write the replacement, sync it,
//! rename it — *then* remove the originals, one at a time. Interrupted, the
//! range is on disk twice, which costs a duplicate read rather than a loss.
//!
//! **Cost is bounded by a segment, not a partition.** The merged name is a fold
//! over the input names, known before a byte is read, so batches stream from
//! each input straight into the replacement. A day-partition at a two-second
//! flush is tens of thousands of segments, and holding it decoded — then
//! doubling that with a concatenation — is an out-of-memory at this design's
//! own projected rate.
//!
//! Compaction is content-preserving repacking, which is what makes it safe on a
//! record. A *rebuild* would not be, and is not this.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use crate::cursor::Cursor;
use crate::error::SegmentError;
use crate::listing::{list_segments, mixed_cursors, partitions};
use crate::reader::read_segment;
use crate::writer::{Codec, SegmentWriter};

/// What one partition's compaction did.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct Compacted {
    /// Segments present before, including any left by an interrupted run.
    pub segments_before: usize,
    /// Segments present after, counted rather than computed.
    pub segments_after: usize,
    /// Rows written into the replacement.
    pub rows: usize,
}

/// Rewrite one partition into a single segment.
///
/// A partition already holding one segment is left alone — rewriting it would
/// churn disk for nothing.
pub fn compact_partition(dir: &Path, codec: Codec) -> Result<Compacted, SegmentError> {
    if let Some((a, b)) = mixed_cursors(dir) {
        return Err(SegmentError::MixedCursors {
            path: dir.to_path_buf(),
            a,
            b,
        });
    }

    let listed = list_segments(dir);
    let started_with = listed.len();

    // An interrupted compaction left the range on disk twice. Finishing it is
    // removing what the replacement already holds — NOT merging the duplicate
    // in again, which would make every resumed compaction double the partition
    // it was meant to shrink.
    let doomed: BTreeSet<PathBuf> = superseded(&listed).into_iter().collect();
    for path in &doomed {
        remove(path)?;
    }
    let existing: Vec<(Cursor, PathBuf)> = listed
        .into_iter()
        .filter(|(_, path)| !doomed.contains(path))
        .collect();

    if existing.len() <= 1 {
        return Ok(Compacted {
            segments_before: started_with,
            segments_after: existing.len(),
            rows: 0,
        });
    }

    // Written and synced **before** anything is removed. An interruption here
    // leaves both, which the names make visible.
    let Some((new_path, rows)) = merge(dir, &existing, codec)? else {
        return Ok(Compacted {
            segments_before: started_with,
            segments_after: existing.len(),
            rows: 0,
        });
    };

    // One at a time, so an interruption is a partition holding the replacement
    // and some of what it replaced — a duplicate read, never a loss.
    for (_, path) in &existing {
        if path != &new_path {
            remove(path)?;
        }
    }

    // Counted rather than computed. Subtracting removals from the previous
    // count forgets the segment just written, which is exactly the kind of
    // arithmetic that reads as obviously right and is not.
    Ok(Compacted {
        segments_before: started_with,
        segments_after: list_segments(dir).len(),
        rows,
    })
}

/// Compact every partition under a root whose `date=` level is strictly before
/// `today`.
///
/// **Closed days only.** Today's partitions are still being written, and a
/// compaction racing the writer is churn by design — so the race is removed by
/// construction rather than by schedule, and any cron line an operator picks is
/// safe.
pub fn compact_closed(root: &Path, today: &str, codec: Codec) -> Result<Compacted, SegmentError> {
    let mut total = Compacted::default();
    for dir in closed_partitions(root, today) {
        let one = compact_partition(&dir, codec)?;
        total.segments_before += one.segments_before;
        total.segments_after += one.segments_after;
        total.rows += one.rows;
    }
    Ok(total)
}

/// Closed partitions still holding more segments than they should — the
/// **read-only twin** of [`compact_closed`].
///
/// A heartbeat says a process is alive. This says *this closed day still holds
/// 1,412 segments*, which is a fact on disk rather than a claim, and it catches
/// the wrong var directory, the stale binary and the `--dry-run` left in — none
/// of which a heartbeat notices.
///
/// It shares one definition of *closed* with the action above, deliberately:
/// two definitions of closed do not fail when they drift, they disagree.
///
/// The listing is cheap — directory entries only, no parquet decode.
pub fn overdue_closed(root: &Path, today: &str, max_segments: usize) -> Vec<(PathBuf, usize)> {
    let mut out: Vec<(PathBuf, usize)> = closed_partitions(root, today)
        .into_iter()
        .map(|dir| {
            let count = list_segments(&dir).len();
            (dir, count)
        })
        .filter(|(_, count)| *count > max_segments)
        .collect();
    // Worst first: an operator reading this wants the partition that has been
    // neglected longest at the top, not the alphabetically first one.
    out.sort_by_key(|(_, count)| std::cmp::Reverse(*count));
    out
}

/// Partitions whose `date=` level names a day strictly before `today`.
///
/// The day is handed in rather than read from a clock, so the rule stays
/// replayable and a test can drive it without waiting for midnight.
fn closed_partitions(root: &Path, today: &str) -> Vec<PathBuf> {
    partitions(root)
        .into_iter()
        .filter(|dir| {
            dir.components()
                .filter_map(|c| c.as_os_str().to_str())
                .filter_map(|level| level.strip_prefix("date="))
                // Lexicographic on YYYY-MM-DD is chronological, which is the
                // one thing the partition format buys that a shorter one would
                // not.
                .any(|date| date < today)
        })
        .collect()
}

/// **Segments an interrupted compaction left behind**, in one partition.
///
/// A compaction writes its replacement and syncs it **before** removing
/// anything, so a crash in that window costs a duplicate read and never a
/// loss. This is what that state looks like on disk, and
/// [`compact_partition`] removes it before merging anything — so a resumed
/// compaction finishes the interrupted one rather than merging the duplicate
/// in again.
///
/// **Nesting is not overlap, and only this one is diagnostic.** Two segments
/// sharing a range is ordinary in the archive: twenty-four gaps flushed in one
/// microsecond carry the same time range and are told apart by pid and flush
/// sequence. But *containment* cannot arise that way — a live writer flushes in
/// receipt order and a tape in sequence order, so segments written normally
/// **abut and never contain one another**. Finding one inside another is
/// therefore finding an interruption, in either store.
///
/// Exposed so a watcher can say the partition needs a sweep, in the window
/// between the crash and the sweep that repairs it. A rebuild over an
/// un-repaired partition doubles those rows **silently**: the duplicated
/// payloads keep their original sequences, so nothing downstream overlaps
/// either.
pub fn nested(dir: &Path) -> Vec<PathBuf> {
    superseded(&list_segments(dir))
}

/// Segments a wider segment in the same partition already holds.
///
/// An ordered sweep rather than a cross-product: comparing every segment
/// against every other has billions of steps at a day-partition's segment
/// count.
fn superseded(listed: &[(Cursor, PathBuf)]) -> Vec<PathBuf> {
    // **A container must be seen before what it contains**, or the sweep walks
    // past the narrow one and only catches what follows the wide one.
    //
    // The ordinary listing sorts by `(first, last)` ascending, so `[100,199]`
    // precedes `[100,299]` and escapes. Sorting the LAST position descending
    // puts the widest segment starting at each position first. Done on a copy:
    // every other caller wants the listing in range order.
    let mut ordered: Vec<(Cursor, PathBuf)> = listed.to_vec();
    ordered.sort_by_key(|(c, _)| {
        (
            c.variant(),
            c.first_position(),
            std::cmp::Reverse(c.last_position()),
        )
    });

    let mut doomed = Vec::new();
    let mut widest: Option<(Cursor, PathBuf)> = None;
    for (cursor, path) in &ordered {
        match &widest {
            // **STRICTLY wider, in at least one direction.**
            //
            // `<=` and `>=` alone call an IDENTICAL range contained — and on
            // the archive an identical range is legal and common: twenty-four
            // gaps flushed in one microsecond share `[t, t]` and are told
            // apart by pid and flush sequence. They hold DIFFERENT ROWS.
            //
            // Treating one as superseded removed it **without merging it**,
            // because this runs before the merge — five rows of eight, gone
            // from the record, which is the one store that cannot be rebuilt.
            //
            // The cost of strictness is a corner that stays undetected: a
            // compaction interrupted in a partition whose segments all share
            // one range leaves a replacement with that same range, and this
            // will not see it. That is a duplicate read. **Failing to spot an
            // interruption costs a duplicate; deleting an unmerged segment
            // costs the rows.**
            Some((w, w_path))
                if w.variant() == cursor.variant()
                    && w.first_position() <= cursor.first_position()
                    && w.last_position() >= cursor.last_position()
                    && (w.first_position() < cursor.first_position()
                        || w.last_position() > cursor.last_position())
                    && w_path != path =>
            {
                doomed.push(path.clone());
            }
            _ => widest = Some((*cursor, path.clone())),
        }
    }
    doomed
}

/// Stream every input into one replacement, named by a fold over the inputs.
fn merge(
    dir: &Path,
    existing: &[(Cursor, PathBuf)],
    codec: Codec,
) -> Result<Option<(PathBuf, usize)>, SegmentError> {
    let Some(merged) = merged_cursor(existing) else {
        return Ok(None);
    };

    // The schema comes from the first input, read before the writer is opened.
    // Every segment in a partition shares one schema; one that did not would be
    // a defect the writer refuses on the first batch.
    let first = read_segment(&existing[0].1)?;
    let Some(schema) = first.first().map(|b| b.schema()) else {
        return Ok(None);
    };

    let mut writer = SegmentWriter::create(dir, merged, schema, codec)?;
    for batch in &first {
        writer.write(batch)?;
    }
    for (_, path) in &existing[1..] {
        for batch in read_segment(path)? {
            writer.write(&batch)?;
        }
    }
    if writer.rows() == 0 {
        return Ok(None);
    }
    let rows = writer.rows();
    Ok(Some((writer.finish()?, rows)))
}

/// The name the replacement takes: the span of everything it holds.
///
/// A fold over the input names, so it is known before a byte is read.
fn merged_cursor(existing: &[(Cursor, PathBuf)]) -> Option<Cursor> {
    let (first, _) = existing.first()?;
    match first {
        Cursor::Time { pid, .. } => {
            let mut lo = i64::MAX;
            let mut hi = i64::MIN;
            for (cursor, _) in existing {
                if let Cursor::Time {
                    first_micros,
                    last_micros,
                    ..
                } = cursor
                {
                    lo = lo.min(*first_micros);
                    hi = hi.max(*last_micros);
                }
            }
            Some(Cursor::Time {
                first_micros: lo,
                last_micros: hi,
                pid: *pid,
                // Zero, deliberately: a compacted segment is not a flush, and
                // reusing a flush counter would make two runs over one
                // partition produce the same name for different content.
                seq: 0,
            })
        }
        Cursor::Block { .. } => {
            let lo = existing.iter().map(|(c, _)| c.first_position()).min()? as u64;
            let hi = existing.iter().map(|(c, _)| c.last_position()).max()? as u64;
            Some(Cursor::Block {
                first: lo,
                last: hi,
            })
        }
        Cursor::Seq { .. } => {
            let lo = existing.iter().map(|(c, _)| c.first_position()).min()? as u64;
            let hi = existing.iter().map(|(c, _)| c.last_position()).max()? as u64;
            Some(Cursor::Seq {
                first: lo,
                last: hi,
            })
        }
    }
}

fn remove(path: &Path) -> Result<(), SegmentError> {
    std::fs::remove_file(path).map_err(|source| SegmentError::Write {
        path: path.to_path_buf(),
        source,
    })
}
