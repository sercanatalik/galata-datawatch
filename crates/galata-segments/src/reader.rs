//! Reading a segment, and pruning what is decoded.
//!
//! **Pruning may only ever answer "no".** A row group whose statistics are
//! absent, or which carries a statistic without both bounds, is included; a
//! file lacking the pruning column is read whole. Pruning on an absence drops
//! rows silently, and a read that can change an answer is worse than a read
//! that is slow.
//!
//! That rule is what makes the property testable as an *equality* rather than
//! as a performance assertion: for any range, a pruned read holds exactly what
//! a full read holds once filtered to the same range.

use std::fs::File;
use std::path::Path;

use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::file::metadata::RowGroupMetaData;
use parquet::file::statistics::Statistics;

use crate::error::SegmentError;
use crate::writer::PRUNE_COLUMN;

/// Read every row of a segment back.
///
/// Every row, whatever range a caller wants — which is right for compaction and
/// for anything rebuilding a whole partition. Where a range is known, prefer
/// [`read_segment_range`], which decodes only the row groups that can hold it.
pub fn read_segment(path: &Path) -> Result<Vec<RecordBatch>, SegmentError> {
    let reader = builder(path)?
        .build()
        .map_err(|source| SegmentError::Parquet {
            path: path.to_path_buf(),
            source,
        })?;
    collect(path, reader)
}

/// The rows of a segment whose pruning column intersects `[from, to)`.
///
/// **Row groups outside the range are never decoded.** Selection is from each
/// group's statistics, which parquet stores in the footer — so choosing them
/// costs a footer read and no column data.
///
/// This prunes *decoding*, not rows: it returns whole batches from the groups
/// it selected, and a group straddling the range boundary comes back whole.
/// Callers already filter rows and must keep doing so.
///
/// How much this buys depends on how the store orders its rows. A record
/// written in arrival order has tight statistics and a range picks a contiguous
/// few. A cache sorted by `(identity, time)` has statistics that are wide on
/// arrival time — which is a fact about a sort order chosen for the predicate
/// that store's readers actually use, not a defect.
pub fn read_segment_range(
    path: &Path,
    from: i64,
    to: i64,
) -> Result<Vec<RecordBatch>, SegmentError> {
    let builder = builder(path)?;

    // Resolved once per file — the index is the same in every row group, and
    // joining a column path per group was a string allocation per group for a
    // constant.
    let column = builder
        .parquet_schema()
        .columns()
        .iter()
        .position(|c| c.path().string() == PRUNE_COLUMN);

    let selected: Vec<usize> = (0..builder.metadata().num_row_groups())
        .filter(|i| match column {
            Some(index) => intersects(builder.metadata().row_group(*i), index, from, to),
            // No such column: nothing can be ruled out, so read everything.
            None => true,
        })
        .collect();

    if selected.is_empty() {
        return Ok(Vec::new());
    }

    let reader = builder
        .with_row_groups(selected)
        .build()
        .map_err(|source| SegmentError::Parquet {
            path: path.to_path_buf(),
            source,
        })?;
    collect(path, reader)
}

/// How many row groups a range would decode, without decoding them.
///
/// Exists so a test can assert that pruning *happened* rather than only that
/// the answer was right — an equality test passes just as well against a reader
/// that prunes nothing.
pub fn row_groups_for_range(
    path: &Path,
    from: i64,
    to: i64,
) -> Result<(usize, usize), SegmentError> {
    let builder = builder(path)?;
    let column = builder
        .parquet_schema()
        .columns()
        .iter()
        .position(|c| c.path().string() == PRUNE_COLUMN);
    let total = builder.metadata().num_row_groups();
    let selected = (0..total)
        .filter(|i| match column {
            Some(index) => intersects(builder.metadata().row_group(*i), index, from, to),
            None => true,
        })
        .count();
    Ok((selected, total))
}

/// A string column's exact bounds across every row group, from the footer.
///
/// `Ok(None)` when any row group carries no statistics for the column, or
/// carries bounds the writer marked inexact (truncated): an answer made of
/// some groups' bounds is not an answer about the segment. Reads the footer
/// only — no data page is decoded.
///
/// Exists so a writer that removes segments can ask *whose rows are these*
/// without reading them: the tape's replacement keeps another venue's segment
/// by its `venue` bounds.
pub fn string_bounds(path: &Path, column: &str) -> Result<Option<(String, String)>, SegmentError> {
    let builder = builder(path)?;
    let Some(index) = builder
        .parquet_schema()
        .columns()
        .iter()
        .position(|c| c.path().string() == column)
    else {
        return Ok(None);
    };
    let mut bounds: Option<(Vec<u8>, Vec<u8>)> = None;
    for group in builder.metadata().row_groups() {
        let Some(stats) = group.column(index).statistics() else {
            return Ok(None);
        };
        let (Some(min), Some(max)) = (stats.min_bytes_opt(), stats.max_bytes_opt()) else {
            return Ok(None);
        };
        if !stats.min_is_exact() || !stats.max_is_exact() {
            return Ok(None);
        }
        bounds = Some(match bounds {
            None => (min.to_vec(), max.to_vec()),
            Some((lo, hi)) => (lo.min(min.to_vec()), hi.max(max.to_vec())),
        });
    }
    Ok(
        bounds
            .and_then(|(lo, hi)| Some((String::from_utf8(lo).ok()?, String::from_utf8(hi).ok()?))),
    )
}

fn builder(path: &Path) -> Result<ParquetRecordBatchReaderBuilder<File>, SegmentError> {
    let file = File::open(path).map_err(|source| SegmentError::Write {
        path: path.to_path_buf(),
        source,
    })?;
    ParquetRecordBatchReaderBuilder::try_new(file).map_err(|source| SegmentError::Parquet {
        path: path.to_path_buf(),
        source,
    })
}

fn collect(
    path: &Path,
    reader: impl Iterator<Item = Result<RecordBatch, arrow::error::ArrowError>>,
) -> Result<Vec<RecordBatch>, SegmentError> {
    let mut out = Vec::new();
    for batch in reader {
        out.push(batch.map_err(|source| SegmentError::Parquet {
            path: path.to_path_buf(),
            source: source.into(),
        })?);
    }
    Ok(out)
}

/// Whether a row group can hold a row in `[from, to)`.
///
/// **`true` whenever it cannot be ruled out** — missing statistics, or a
/// statistic without both bounds, mean *read it*. The only answer this may give
/// confidently is "no".
fn intersects(group: &RowGroupMetaData, index: usize, from: i64, to: i64) -> bool {
    match group.column(index).statistics() {
        Some(Statistics::Int64(s)) => match (s.min_opt(), s.max_opt()) {
            // Half-open, matching every other range in the workspace.
            (Some(lo), Some(hi)) => *hi >= from && *lo < to,
            _ => true,
        },
        _ => true,
    }
}
