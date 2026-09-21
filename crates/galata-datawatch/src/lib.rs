//! Market data capture: the record every payload lands in verbatim, and the
//! one path it crosses to get there.
//!
//! ```text
//!   ingest(archive, normaliser, sink, payload)
//!     1. archive    the bytes, verbatim, before any parse is attempted
//!     2. normalise  inside a panic boundary — a panic costs a parse
//!     3. emit       may fail; the record does not depend on it
//! ```
//!
//! # What holds the ordering
//!
//! `Archive::append` is `pub(crate)`, so no other crate can reach
//! past [`ingest()`](crate::ingest::ingest). `scripts/check-ingest-callers.sh` asserts no other module
//! in this crate does either — which is the half the compiler cannot see.
//!
//! # What is not here
//!
//! No venue, no socket, no poll, no runtime. The venue seam and the capture
//! loop are a separate change, because a panicking normaliser and a killed
//! process are what most of this design is for, and both are easier to arrange
//! against a fake than against a venue.

#![cfg_attr(docsrs, feature(doc_cfg))]

pub mod adapters;
pub mod calendar;
/// **Behind the `capture` feature**: it needs a runtime.
#[cfg(feature = "capture")]
pub mod capture;
pub mod config;
pub mod ingest;
pub mod normalise;
pub mod record;
pub mod reorg;
pub mod replay;
pub mod retain;
pub mod sink;
/// **Behind the `capture` feature**: it needs a runtime.
#[cfg(feature = "capture")]
pub mod source;
pub mod tape;
pub mod venue;
pub mod watch;

#[cfg(test)]
mod tests;

pub use calendar::{date_of, midnight_of};
pub use ingest::{Ingested, ingest};
pub use normalise::{Normalise, NormaliseError};
pub use record::{Archive, Failure, Payload, PayloadAddress, RecordError};
pub use sink::{NullSink, Sink, SinkError};
pub use venue::{Adapter, Construct, Declaration, Keepalive, Subscription, Symbols};
