//! Writing a segment, and the commit discipline that makes it durable.
//!
//! **Order is the whole safety argument.** Write to a temporary, sync it,
//! rename it into place, sync the directory. A crash between the sync and the
//! rename leaves a temporary; a crash after leaves a complete segment. Neither
//! leaves a segment that is partly written.

use std::fs::File;
use std::path::{Path, PathBuf};

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_writer::ArrowWriterOptions;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use parquet::schema::types::ColumnPath;

use crate::cursor::Cursor;
use crate::error::SegmentError;

/// The column a range read prunes on, and therefore the only column the footer
/// carries statistics for.
pub const PRUNE_COLUMN: &str = "recv_micros";

/// **Declared, not inherited.**
///
/// The library's default is 1,048,576 rows, which put a whole compacted
/// five-hour partition in ONE row group — and a segment holding one row group
/// prunes to all-or-nothing however tight its statistics are, so a range read
/// had nothing to skip.
///
/// Measured on the predecessor's largest real segment (1,757,937 rows, 24.9 MB,
/// spanning 5 hours), reading a 60-second window at its end:
///
/// ```text
///   group size    groups   file MB   vs default   window MB
///    1,048,576         2     24.86         0.0%      10.034
///      262,144         7     27.38       +10.1%       2.972
///       65,536        27     30.58       +23.0%       0.918
///       16,384       108     31.46       +26.5%       0.373   ← chosen
///        8,192       215     32.23       +29.6%       0.233
///        4,096       430     32.05       +28.9%       0.156
/// ```
///
/// 16,384 is the knee. Below it each halving buys progressively less pruning
/// for the same few points of size; above it the window cost climbs fast. At
/// that partition's rate a group spans about three minutes, so a minute-scale
/// window selects one or two of a hundred.
///
/// **General guidance recommends roughly one million rows** for time-sorted
/// data read by range, and it is not wrong — it answers a different question.
/// It assumes large files read in large slices. Segments here are small and
/// frequent, and the read that matters is a minute inside a day.
///
/// It costs nothing on the write path: a flush writes a few rows, which is one
/// row group at any of these sizes.
///
/// # A better instrument exists, and is not used yet
///
/// `parquet` 60 added [`set_max_row_group_bytes`], which flushes a row group
/// when its *estimated encoded size* crosses a threshold. That expresses this
/// constant's actual intent — *how much gets decoded for one window* — far
/// more directly, and it is **row-width independent**: 16,384 rows of a narrow
/// quote is a wholly different number of bytes from 16,384 rows of a wide book
/// message, and the table above was measured on one dataset and then applied
/// to all of them.
///
/// It stays unset. The row count is what was measured, and a constant is not
/// moved on reasoning alone in this tree. The byte bound is the right thing to
/// measure against in the first soak that produces real segments per dataset.
///
/// [`set_max_row_group_bytes`]: parquet::file::properties::WriterPropertiesBuilder::set_max_row_group_bytes
pub const MAX_ROW_GROUP_ROWS: usize = 16_384;

/// The codec a store writes with.
///
/// Declarable because the two stores have opposite read/write ratios: a record
/// is written every few seconds and read rarely, while a cache is written once
/// per window and read constantly. `LZ4_RAW` decompresses markedly faster at a
/// worse ratio, which is plainly one store's trade and plainly not the other's.
///
/// **No default is changed on someone else's benchmark.** Both current callers
/// declare [`Codec::Zstd`]; the measurement that would turn this is a soak that
/// does not exist yet.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[non_exhaustive]
pub enum Codec {
    /// Zstandard at the library's default level. Better ratio.
    #[default]
    Zstd,
    /// LZ4 raw. Faster to decompress, worse ratio.
    Lz4,
    /// None, for a measurement that wants the codec out of the picture.
    Uncompressed,
}

impl Codec {
    fn to_parquet(self) -> Compression {
        match self {
            Codec::Zstd => Compression::ZSTD(ZstdLevel::default()),
            Codec::Lz4 => Compression::LZ4_RAW,
            Codec::Uncompressed => Compression::UNCOMPRESSED,
        }
    }
}

/// The writer properties every segment is written with.
fn properties(codec: Codec) -> WriterProperties {
    WriterProperties::builder()
        .set_compression(codec.to_parquet())
        .set_max_row_group_row_count(Some(MAX_ROW_GROUP_ROWS))
        // Unset on purpose: see MAX_ROW_GROUP_ROWS. When both are set the
        // smaller limit wins, so turning this on later narrows groups rather
        // than widening them — which is the safe direction to discover.
        .set_max_row_group_bytes(None)
        // **The footer carries what reads prune on, and nothing else.**
        //
        // Range reads consult chunk-level statistics for one column — that is
        // all of the pruning — so that is all the writer pays for. The
        // library's default adds a page index and truncated min/max for every
        // column, including opaque payload columns nothing will ever push a
        // predicate down into; on a mean segment of a few kilobytes that is a
        // measurable share of the file, times the whole file count.
        .set_statistics_enabled(EnabledStatistics::None)
        .set_column_statistics_enabled(ColumnPath::from(PRUNE_COLUMN), EnabledStatistics::Chunk)
        .build()
}

/// A segment written incrementally, committed once, in [`SegmentWriter::finish`].
///
/// Batches stream into the temporary, so peak memory is one batch rather than a
/// whole partition. Compaction needs exactly this: its merged name is known
/// from the input names before any data is read, so batches flow straight
/// through.
///
/// A writer dropped without finishing removes its temporary. The temporary name
/// is already recognisably not a segment, so this is tidiness rather than
/// safety.
pub struct SegmentWriter {
    dir: PathBuf,
    temp_path: PathBuf,
    final_path: PathBuf,
    writer: Option<ArrowWriter<File>>,
    rows: usize,
}

impl SegmentWriter {
    /// Open a writer for one segment.
    pub fn create(
        dir: &Path,
        cursor: Cursor,
        schema: SchemaRef,
        codec: Codec,
    ) -> Result<Self, SegmentError> {
        std::fs::create_dir_all(dir).map_err(|source| SegmentError::CreateDir {
            path: dir.to_path_buf(),
            source,
        })?;

        let final_path = dir.join(cursor.file_name());
        let temp_path = dir.join(cursor.temp_file_name());

        let file = File::create(&temp_path).map_err(|source| SegmentError::Write {
            path: temp_path.clone(),
            source,
        })?;

        // No embedded Arrow schema: every column type here round-trips exactly
        // from the parquet logical types, and a both-ways read-parity test
        // holds that claim rather than assuming it.
        let options = ArrowWriterOptions::new()
            .with_properties(properties(codec))
            .with_skip_arrow_metadata(true);

        let writer =
            ArrowWriter::try_new_with_options(file, schema, options).map_err(|source| {
                SegmentError::Parquet {
                    path: temp_path.clone(),
                    source,
                }
            })?;

        Ok(SegmentWriter {
            dir: dir.to_path_buf(),
            temp_path,
            final_path,
            writer: Some(writer),
            rows: 0,
        })
    }

    /// Append a batch.
    pub fn write(&mut self, batch: &RecordBatch) -> Result<(), SegmentError> {
        self.writer
            .as_mut()
            .expect("present until finish")
            .write(batch)
            .map_err(|source| SegmentError::Parquet {
                path: self.temp_path.clone(),
                source,
            })?;
        self.rows += batch.num_rows();
        Ok(())
    }

    /// Rows written so far — what decides whether there is anything to commit.
    pub fn rows(&self) -> usize {
        self.rows
    }

    /// Sync, rename, sync the directory. On return the bytes are on disk and
    /// the name is visible to a listing; before return neither is true.
    ///
    /// A writer holding no rows refuses and removes its temporary. A segment
    /// that says a range is covered and holds nothing is a lie a reader cannot
    /// detect.
    pub fn finish(mut self) -> Result<PathBuf, SegmentError> {
        if self.rows == 0 {
            return Err(SegmentError::Empty);
        }
        let file = self
            .writer
            .take()
            .expect("present until finish")
            .into_inner()
            .map_err(|source| SegmentError::Parquet {
                path: self.temp_path.clone(),
                source,
            })?;

        // Durable before visible.
        file.sync_all().map_err(|source| SegmentError::Write {
            path: self.temp_path.clone(),
            source,
        })?;
        drop(file);

        std::fs::rename(&self.temp_path, &self.final_path).map_err(|source| {
            SegmentError::Commit {
                from: self.temp_path.clone(),
                to: self.final_path.clone(),
                source,
            }
        })?;

        // The directory entry itself has to be durable, or the rename can be
        // lost while the file survives — which is a segment that exists and
        // cannot be found.
        if let Ok(handle) = File::open(&self.dir) {
            let _ = handle.sync_all();
        }

        Ok(self.final_path.clone())
    }
}

impl Drop for SegmentWriter {
    fn drop(&mut self) {
        if let Some(writer) = self.writer.take() {
            drop(writer);
            let _ = std::fs::remove_file(&self.temp_path);
        }
    }
}

/// Write one batch as a segment, durably, under `dir`.
///
/// Returns the committed path. On return the bytes are on disk and the name is
/// visible to a listing; before return neither is true, so a reader cannot
/// observe a partial segment.
pub fn write_segment(
    dir: &Path,
    cursor: Cursor,
    batch: &RecordBatch,
    codec: Codec,
) -> Result<PathBuf, SegmentError> {
    if batch.num_rows() == 0 {
        return Err(SegmentError::Empty);
    }
    let mut writer = SegmentWriter::create(dir, cursor, batch.schema(), codec)?;
    writer.write(batch)?;
    writer.finish()
}
