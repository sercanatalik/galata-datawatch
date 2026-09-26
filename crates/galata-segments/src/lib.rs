//! Parquet segments: buffer, write, fsync, atomically rename.
//!
//! One implementation, used by every store above it. They differ in what they
//! name a segment after, and in nothing else.
//!
//! **Naming carries the durability fact.** A directory listing has to answer
//! *what is durable* without opening a file, because that is what a restart
//! path needs. See [`Cursor`].
//!
//! **Rename is the commit.** A segment is written to a temporary name, synced,
//! renamed into place, and the directory entry itself then synced — so a reader
//! never sees a partial file, and a crash leaves a temporary that is
//! recognisably not a segment.

#![cfg_attr(docsrs, feature(doc_cfg))]

pub mod compact;
pub mod cursor;
pub mod error;
pub mod hold;
pub mod listing;
pub mod reader;
pub mod writer;

pub use compact::{Compacted, compact_closed, compact_partition, nested, overdue_closed};
pub use cursor::{Cursor, Variant};
pub use error::SegmentError;
pub use hold::{HOLD_FILE, Hold, Mode, hold, hold_shared, wait};
pub use listing::{
    ListingCache, RACY_MARGIN, frontier, last_durable, last_durable_for_scope, list_segments,
    mixed_cursors, overlapping_ranges, overlapping_ranges_by_label, partitions, scannable,
};
pub use reader::{label, read_segment, read_segment_range, row_groups_for_range};
pub use writer::{
    Codec, MAX_ROW_GROUP_ROWS, PRUNE_COLUMN, SegmentWriter, write_file, write_segment,
    write_segment_labelled, write_segment_pruned,
};
