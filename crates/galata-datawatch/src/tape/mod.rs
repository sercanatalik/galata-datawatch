//! The tape: the projection that makes the record queryable.
//!
//! ```text
//!   archive/                       tape/
//!     venue=hyperliquid/             kind=quotes/
//!       kind=quotes/                   date=2026-09-20/
//!         date=2026-09-20/               s-…_….parquet
//!           t-…_…_…_….parquet
//!
//!   bytes, verbatim, by receipt    rows, typed, by venue time
//! ```
//!
//! **The archive is a record; the tape is a cache.** Everything here can be
//! thrown away and rebuilt from the archive, and nothing may depend on it
//! holding something the archive does not. That is what lets it be freely
//! rewritten when a schema changes.
//!
//! # `venue` is a column, and deliberately not a partition level
//!
//! Both stores once carried it both ways. **Measured on DuckDB 1.5.5**, against
//! a tree whose path and column were made to disagree on purpose:
//!
//! ```text
//!   hive_partitioning=true   (and the DEFAULT)   the PATH wins
//!   hive_partitioning=false                      the COLUMN wins
//! ```
//!
//! A value written both ways therefore has a value that **depends on a reader
//! flag**, and nothing warns. So it is written once, in the data, where no flag
//! reaches it — and a bare `read_parquet` over the whole tape still states the
//! venue on every row.
//!
//! Pruning moves from directories to row-group statistics on a sorted column,
//! which is the same mechanism that already keeps ticker out of the path. The
//! argument that removes ticker removes venue for the same reason, one level
//! up.

pub mod layout;
pub mod reader;
pub mod rebuild;
pub mod schema;
pub mod writer;

pub use layout::{LayoutProblem, check_layout, partition_of};
pub use reader::{Bound, ReadError, Reader, Window, unwritten};
pub use rebuild::{Rebuilt, Replace, rebuild, rebuild_with};
pub use schema::{PRUNE_ON, schema_for};
pub use writer::{Row, Tape, TapeError};
