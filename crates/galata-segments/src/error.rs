//! What can go wrong, named so a refusal says which path and which file.

use std::path::PathBuf;

/// Why a segment operation could not complete.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum SegmentError {
    /// A store that was to be scanned could not be read.
    ///
    /// **Never an empty listing.** A root that cannot be read produces no
    /// partitions, and a sweep over no partitions selects nothing — which
    /// reads as *nothing to do* and is indistinguishable from compliance. A
    /// mistyped path then looks healthy for as long as nobody checks.
    #[error(
        "cannot scan {path}: {reason}. A store that cannot be read is not a store with \
             nothing in it"
    )]
    Unscannable {
        /// The root.
        path: PathBuf,
        /// Why.
        reason: String,
    },
    /// The partition directory could not be created.
    #[error("could not create {path}: {source}")]
    CreateDir {
        /// The directory.
        path: PathBuf,
        /// The underlying cause.
        #[source]
        source: std::io::Error,
    },
    /// A write or a sync failed.
    #[error("could not write {path}: {source}")]
    Write {
        /// The file.
        path: PathBuf,
        /// The underlying cause.
        #[source]
        source: std::io::Error,
    },
    /// The rename that commits a segment failed.
    #[error("could not commit {from} to {to}: {source}")]
    Commit {
        /// The temporary.
        from: PathBuf,
        /// Where it was to land.
        to: PathBuf,
        /// The underlying cause.
        #[source]
        source: std::io::Error,
    },
    /// The columnar layer refused.
    #[error("parquet {path}: {source}")]
    Parquet {
        /// The file.
        path: PathBuf,
        /// The underlying cause.
        #[source]
        source: parquet::errors::ParquetError,
    },
    /// Nothing was written.
    ///
    /// Refused rather than committed: a segment that claims a range and holds
    /// nothing is a lie a reader cannot detect.
    #[error("nothing to write")]
    Empty,
    /// Another holder has this root in a mode that excludes the one asked for.
    ///
    /// A scheduler beside an operator's hand is two compactors on one tree, and
    /// two compactors on one partition leave the same rows twice under two
    /// names — a twin the interruption rule was never asked to resolve. A
    /// reader beside a compactor lists a segment that is about to be removed.
    /// So the holder that would collide refuses. See [the `hold` module](mod@crate::hold).
    #[error("{root} is held by another writer or reader (a compaction, a deletion or a rebuild)")]
    Held {
        /// The root somebody else holds.
        root: PathBuf,
    },
    /// The hold itself could not be taken.
    #[error("could not take the hold on {root}: {source}")]
    Hold {
        /// The root.
        root: PathBuf,
        /// The underlying cause.
        #[source]
        source: std::io::Error,
    },
    /// A partition holds segments whose positions are not comparable.
    ///
    /// One writer owns one partition, so a mixed partition is always a bug. It
    /// is reported rather than resolved, because guessing which ordering was
    /// meant is how a store starts lying.
    #[error("{path} holds both {a:?} and {b:?} cursors, which do not order against each other")]
    MixedCursors {
        /// The partition.
        path: PathBuf,
        /// One variant found.
        a: crate::cursor::Variant,
        /// The other.
        b: crate::cursor::Variant,
    },
}
