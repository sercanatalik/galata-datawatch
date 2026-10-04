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

/// A label the writer stated in the footer, exactly as written.
///
/// `Ok(None)` when the segment carries no such key. Reads the footer only.
/// See [`crate::SegmentWriter::create_labelled`] for why this, and not a column
/// statistic, is what a reader may act on as a *yes*.
pub fn label(path: &Path, key: &str) -> Result<Option<String>, SegmentError> {
    let builder = builder(path)?;
    Ok(builder
        .metadata()
        .file_metadata()
        .key_value_metadata()
        .and_then(|pairs| pairs.iter().find(|pair| pair.key == key))
        .and_then(|pair| pair.value.clone()))
}

/// What a row group must be able to hold to be decoded: the tests
/// [`Segment::read_where`] applies to each group's footer statistics.
///
/// Every test answers **"no" only on the evidence** of both bounds, as the
/// rest of this module does; an absent statistic reads the group.
#[derive(Debug, Clone)]
pub enum Prune<'a> {
    /// An `Int64` column with a value in `[from, to)`. With `keep_nulls`, a
    /// group holding any null in the column is read whatever its bounds say:
    /// statistics bound the values present, and a row with no value may be
    /// one the caller keeps.
    Int64Range {
        /// The column.
        column: &'a str,
        /// Inclusive.
        from: i64,
        /// Exclusive.
        to: i64,
        /// Read a group holding a null regardless.
        keep_nulls: bool,
    },
    /// A UTF-8 column equal to `value`.
    Equals {
        /// The column.
        column: &'a str,
        /// The value.
        value: &'a str,
    },
}

/// One segment, opened once: its footer read, nothing decoded yet.
///
/// For a reader that asks the footer something — whose segment this is —
/// before deciding what to decode. Asking [`label`] and then reading opened
/// and parsed the file twice.
pub struct Segment {
    path: std::path::PathBuf,
    builder: ParquetRecordBatchReaderBuilder<File>,
}

impl Segment {
    /// Open a segment and read its footer.
    pub fn open(path: &Path) -> Result<Segment, SegmentError> {
        Ok(Segment {
            path: path.to_path_buf(),
            builder: builder(path)?,
        })
    }

    /// A label the writer stated, as [`label`] reads it.
    pub fn label(&self, key: &str) -> Option<String> {
        self.builder
            .metadata()
            .file_metadata()
            .key_value_metadata()
            .and_then(|pairs| pairs.iter().find(|pair| pair.key == key))
            .and_then(|pair| pair.value.clone())
    }

    /// The row groups every test can hold, decoded. Rows are not filtered:
    /// a group passing comes back whole, and the caller filters rows as it
    /// must anyway.
    pub fn read_where(self, prunes: &[Prune<'_>]) -> Result<Vec<RecordBatch>, SegmentError> {
        let columns = self.builder.parquet_schema().columns();
        let resolved: Vec<(Option<usize>, &Prune<'_>)> = prunes
            .iter()
            .map(|p| {
                let name = match p {
                    Prune::Int64Range { column, .. } | Prune::Equals { column, .. } => *column,
                };
                (columns.iter().position(|c| c.path().string() == name), p)
            })
            .collect();
        let total = self.builder.metadata().num_row_groups();
        let selected: Vec<usize> = (0..total)
            .filter(|i| {
                let group = self.builder.metadata().row_group(*i);
                resolved.iter().all(|(index, prune)| match index {
                    Some(index) => may_hold(group, *index, prune),
                    None => true,
                })
            })
            .collect();
        if selected.is_empty() {
            return Ok(Vec::new());
        }
        let path = self.path;
        let builder = if selected.len() == total {
            self.builder
        } else {
            self.builder.with_row_groups(selected)
        };
        let reader = builder.build().map_err(|source| SegmentError::Parquet {
            path: path.clone(),
            source,
        })?;
        collect(&path, reader)
    }
}

fn may_hold(group: &RowGroupMetaData, index: usize, prune: &Prune<'_>) -> bool {
    let statistics = group.column(index).statistics();
    match (prune, statistics) {
        (
            Prune::Int64Range {
                from,
                to,
                keep_nulls,
                ..
            },
            Some(Statistics::Int64(s)),
        ) => {
            if *keep_nulls && s.null_count_opt() != Some(0) {
                return true;
            }
            match (s.min_opt(), s.max_opt()) {
                (Some(lo), Some(hi)) => *hi >= *from && *lo < *to,
                _ => true,
            }
        }
        (Prune::Equals { value, .. }, Some(Statistics::ByteArray(s))) => {
            // Truncated statistics still bound the values (a truncated max is
            // rounded up), so the comparison stays a safe "no".
            match (s.min_opt(), s.max_opt()) {
                (Some(lo), Some(hi)) => {
                    let v = value.as_bytes();
                    lo.data() <= v && v <= hi.data()
                }
                _ => true,
            }
        }
        _ => true,
    }
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
