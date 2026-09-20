//! The record: one row per payload, verbatim, before any parse was attempted.
//!
//! Venue-agnostic by construction. It stays eight columns however many adapters
//! exist, because it stores what arrived rather than what it meant.
//!
//! ```text
//!   archive/
//!    venue=hyperliquid/
//!      kind=quotes/
//!        date=2026-09-20/
//!          t-1758326400000000_1758326460000000_4711_3.parquet
//!          failures/
//!            t-…_…_4711_4.parquet      ← shares the payload's seq
//! ```
//!
//! **The address sits above kind**, so one venue's bytes form a single subtree
//! that can be retained, replayed or dropped whole.

mod schema;

pub use schema::{
    FAILURE_COLUMNS, MARKET_COLUMNS, PAYLOAD_COLUMNS, failure_batch, failure_schema, market_schema,
    payload_batch, payload_schema, schema_for,
};

use std::path::{Path, PathBuf};

use galata_segments::{Codec, Cursor, SegmentError, last_durable, write_segment};
use galata_wire::{GapCause, Origin};

use crate::calendar::date_of;

/// Written when the process shuts down having flushed everything it held.
///
/// Its **absence** on restart is what distinguishes *we were killed with a
/// buffer outstanding* from *we were stopped*. Both are uncovered intervals;
/// only one of them is our own loss, and the discipline that makes a gap
/// trustworthy applies to our loss too.
const CLEAN_SHUTDOWN: &str = ".clean-shutdown";

/// Why a payload could not be recorded.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum RecordError {
    /// A replayed payload was offered to the record.
    #[error(
        "a payload with origin `replay` was offered to the record on {address}/{channel}. \
         Replay reads the record back out; writing it would grow the thing it is reading."
    )]
    ReplayIsNotWritten {
        /// The address it claimed.
        address: String,
        /// The channel it claimed.
        channel: String,
    },
    /// The store refused.
    #[error(transparent)]
    Segment(#[from] SegmentError),
    /// A batch could not be built.
    #[error("arrow: {0}")]
    Arrow(String),
}

/// Where a payload partitions, above `kind=`.
///
/// One field rather than a venue beside a market, because exactly one applies
/// and a pair could hold both or neither. The record's top level is whatever
/// the retain/replay/drop-whole unit is — the venue for bytes a venue sent, and
/// the market for numbers this system computed, since a market spanning two
/// venues cannot be dropped with either without destroying half of itself.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[non_exhaustive]
pub enum PayloadAddress {
    /// Bytes a venue sent.
    Venue(String),
    /// Numbers this system computed about a market.
    Market(String),
}

impl PayloadAddress {
    /// The partition level's key, without its value.
    pub fn key(&self) -> &'static str {
        match self {
            PayloadAddress::Venue(_) => "venue",
            PayloadAddress::Market(_) => "market",
        }
    }

    /// The partition level's value.
    pub fn value(&self) -> &str {
        match self {
            PayloadAddress::Venue(v) | PayloadAddress::Market(v) => v,
        }
    }
}

impl std::fmt::Display for PayloadAddress {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}={}", self.key(), self.value())
    }
}

/// One payload as it arrived.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Payload {
    /// Monotonic within the process. A failure row shares the seq of the
    /// payload it refers to.
    pub seq: u64,
    /// **Our** clock, when the bytes arrived. The join key back from any row
    /// derived from them.
    pub recv_micros: i64,
    /// Where this partitions above `kind=`.
    pub address: PayloadAddress,
    /// The venue's own channel name, verbatim.
    pub channel: String,
    /// The partition level this payload lands under — the series the channel
    /// belongs to, decided by the adapter, so the tree is stable across a venue
    /// renaming a channel.
    pub kind: String,
    /// The venue's own symbol on that channel. `None` where a payload covers
    /// many, as a universe fetch does.
    pub symbol: Option<String>,
    /// Where it came from, and therefore how durable it must be.
    pub origin: Origin,
    /// The bytes, untouched, before any parse was attempted.
    pub payload: Vec<u8>,
}

/// One payload that failed to normalise.
///
/// Written into a `failures/` sibling, **sharing the sequence of the payload it
/// refers to**, so the two join on a number rather than on an ordering
/// convention — which survives a flush boundary falling between them.
///
/// The payload itself stays in the main segment: filtering a record by parse
/// success discards exactly the evidence a normalisation defect is diagnosed
/// from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Failure {
    /// The sequence of the payload this refers to.
    pub seq: u64,
    /// When those bytes arrived.
    pub recv_micros: i64,
    /// The venue that sent them.
    pub venue: String,
    /// The channel they came on.
    pub channel: String,
    /// What went wrong.
    pub error: String,
}

/// The record's writer.
///
/// **Durability follows the payload's [`Origin`], never a caller's choice:**
/// there is no durability parameter to get wrong.
pub struct Archive {
    root: PathBuf,
    /// The venue this process owns, where it owns one.
    ///
    /// Payloads partition by address regardless, so the scope changes nothing
    /// about where bytes land. What it changes is the two questions that are
    /// **about this process** rather than about the tree: how far *its* record
    /// is durable, and whether *it* stopped cleanly. One process per venue
    /// sharing a root would otherwise read the other's answer to both — and
    /// would report a colleague's clean stop as its own.
    scope: Option<String>,
    codec: Codec,
    pid: u32,
    flush_seq: u64,
    next_seq: u64,
    buffered: Vec<Payload>,
    failures: Vec<Failure>,
}

impl Archive {
    /// Open the record under a root.
    pub fn open(root: impl Into<PathBuf>) -> Archive {
        Archive {
            root: root.into(),
            scope: None,
            codec: Codec::Zstd,
            pid: std::process::id(),
            flush_seq: 0,
            next_seq: 0,
            buffered: Vec::new(),
            failures: Vec::new(),
        }
    }

    /// Scope this handle to one venue's subtree.
    pub fn scoped_to(mut self, venue: &str) -> Archive {
        self.scope = Some(format!("venue={venue}"));
        self
    }

    /// Write with a declared codec rather than the default.
    pub fn with_codec(mut self, codec: Codec) -> Archive {
        self.codec = codec;
        self
    }

    /// The tree this handle writes into.
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// The subtree this handle answers questions about.
    fn scope_path(&self) -> PathBuf {
        match &self.scope {
            Some(scope) => self.root.join(scope),
            None => self.root.clone(),
        }
    }

    /// The next sequence, taken by the caller before it appends.
    ///
    /// Monotonic within a process, so a failure row can name the payload it
    /// refers to.
    pub fn next_seq(&mut self) -> u64 {
        let seq = self.next_seq;
        self.next_seq += 1;
        seq
    }

    /// Payloads received and not yet durable.
    ///
    /// The live size of the window a crash would convert into a gap — the one
    /// number in the status surface that is a direct measure of standing risk.
    pub fn buffered(&self) -> usize {
        self.buffered.len()
    }

    /// Append a payload.
    ///
    /// `pub(crate)` on purpose: **the one path is the only caller.** Archive →
    /// normalise → emit holds because there is one function and everything
    /// calls it, and a second caller would not fail — it would quietly hold a
    /// weaker guarantee, for exactly the payloads that crossed it. The compiler
    /// holds this against every other crate; `check-ingest-callers.sh` holds it
    /// against every other module in this one.
    pub(crate) fn append(&mut self, payload: Payload) -> Result<(), RecordError> {
        match payload.origin {
            Origin::Replay => Err(RecordError::ReplayIsNotWritten {
                address: payload.address.to_string(),
                channel: payload.channel,
            }),
            // Covers a range nothing will fetch again, so it is durable before
            // the walk advances past it.
            Origin::Fetched => self.commit(vec![payload]).map(|_| ()),
            Origin::Streamed => {
                self.buffered.push(payload);
                Ok(())
            }
        }
    }

    // `append_batch` is deliberately absent.
    //
    // The predecessor has one, and its justification is a measurement from a
    // component that does not exist here: an algo host's restart backfill
    // committed 10,797 rows one at a time and produced 10,797 segments at
    // 2.9 KB each — 30.1 MB of almost pure segment overhead.
    //
    // Nothing in this design has that shape. A walked page goes through the
    // one path individually because each page must also be normalised and
    // emitted; a rebuild writes the tape, not the record. Adding the method
    // for a caller that does not exist would be a `pub(crate)` hole in the
    // one-path wall, held open for nobody. It goes in when something needs it,
    // with the reason beside it.

    /// Record that a payload would not normalise.
    ///
    /// Written into `failures/` on the next flush, sharing the payload's
    /// sequence.
    pub(crate) fn append_failure(&mut self, failure: Failure) {
        self.failures.push(failure);
    }

    /// Commit everything buffered. Called on the flush timer, and at shutdown.
    pub fn flush(&mut self) -> Result<Vec<PathBuf>, RecordError> {
        let payloads = std::mem::take(&mut self.buffered);
        let failures = std::mem::take(&mut self.failures);
        let mut written = self.commit(payloads)?;
        written.extend(self.commit_failures(failures)?);
        Ok(written)
    }

    fn commit(&mut self, payloads: Vec<Payload>) -> Result<Vec<PathBuf>, RecordError> {
        let mut written = Vec::new();
        for (partition, group) in group_by_partition(payloads) {
            let dir = self.root.join(partition);
            let batch = payload_batch(&group).map_err(|e| RecordError::Arrow(e.to_string()))?;
            let cursor = self.cursor_for(group.iter().map(|p| p.recv_micros));
            written.push(write_segment(&dir, cursor, &batch, self.codec)?);
        }
        Ok(written)
    }

    fn commit_failures(&mut self, failures: Vec<Failure>) -> Result<Vec<PathBuf>, RecordError> {
        let mut written = Vec::new();
        for (partition, group) in group_failures_by_partition(failures) {
            // The `failures/` sibling of the partition holding the bytes, so
            // the two are found together.
            let dir = self.root.join(partition).join("failures");
            let batch = failure_batch(&group).map_err(|e| RecordError::Arrow(e.to_string()))?;
            let cursor = self.cursor_for(group.iter().map(|f| f.recv_micros));
            written.push(write_segment(&dir, cursor, &batch, self.codec)?);
        }
        Ok(written)
    }

    fn cursor_for(&mut self, times: impl Iterator<Item = i64> + Clone) -> Cursor {
        let first = times.clone().min().unwrap_or(0);
        let last = times.max().unwrap_or(0);
        self.flush_seq += 1;
        Cursor::Time {
            first_micros: first,
            last_micros: last,
            pid: self.pid,
            seq: self.flush_seq,
        }
    }

    /// The last moment this handle's scope is durable to, read from segment
    /// names alone — **no file is opened**.
    pub fn last_durable(&self) -> Option<i64> {
        last_durable(&self.scope_path()).map(|(_, position)| position as i64)
    }

    /// Record that this process stopped having flushed everything it held.
    ///
    /// **Best-effort by construction**: if writing this fails, the next start
    /// reports `crash_unflushed` over an interval that was in fact covered,
    /// which **overstates** the loss rather than erasing it. That is the safe
    /// direction — a gap that is too wide costs a re-fetch, while one that is
    /// too narrow is a hole nobody looks for.
    pub fn mark_clean_shutdown(&self, at_micros: i64) {
        let scope = self.scope_path();
        let _ = std::fs::create_dir_all(&scope);
        let _ = std::fs::write(scope.join(CLEAN_SHUTDOWN), at_micros.to_string());
    }

    /// Clear the marker, so a kill from here on is reported as one.
    ///
    /// Called once at startup, **after** the restart window has been read.
    pub fn clear_clean_shutdown(&self) {
        let _ = std::fs::remove_file(self.scope_path().join(CLEAN_SHUTDOWN));
    }

    /// The interval this process was not capturing, and why.
    ///
    /// Dated from the **last moment actually covered** — the last durable
    /// receipt — rather than from the moment the loss was noticed, which would
    /// understate it by exactly the buffer that was outstanding.
    ///
    /// `None` on a first-ever start: there is no covered moment to date a gap
    /// from, and a gap back to the beginning of time is not a fact.
    pub fn restart_window(&self, now_micros: i64) -> Option<(i64, i64, GapCause)> {
        let from = self.last_durable()?;
        if from >= now_micros {
            return None;
        }
        let clean = std::fs::read_to_string(self.scope_path().join(CLEAN_SHUTDOWN))
            .ok()
            .and_then(|s| s.trim().parse::<i64>().ok());
        let cause = match clean {
            // Stopped, having flushed. The interval is downtime.
            Some(_) => GapCause::Downtime,
            // Killed. What was received and not yet durable is our own loss.
            None => GapCause::CrashUnflushed,
        };
        Some((from, now_micros, cause))
    }
}

/// `<venue|market>=<value>/kind=<kind>/date=<yyyy-mm-dd>`
///
/// The address above kind: the record's unit is whatever is retained, replayed
/// or dropped as a subtree.
pub fn partition_of(address: &PayloadAddress, kind: &str, recv_micros: i64) -> PathBuf {
    PathBuf::from(address.to_string())
        .join(format!("kind={kind}"))
        .join(format!("date={}", date_of(recv_micros)))
}

/// The same, for bytes a venue sent.
pub fn venue_partition_of(venue: &str, kind: &str, recv_micros: i64) -> PathBuf {
    partition_of(&PayloadAddress::Venue(venue.to_string()), kind, recv_micros)
}

fn group_by_partition(payloads: Vec<Payload>) -> Vec<(PathBuf, Vec<Payload>)> {
    let mut out: Vec<(PathBuf, Vec<Payload>)> = Vec::new();
    for payload in payloads {
        let partition = partition_of(&payload.address, &payload.kind, payload.recv_micros);
        match out.iter_mut().find(|(p, _)| *p == partition) {
            Some((_, group)) => group.push(payload),
            None => out.push((partition, vec![payload])),
        }
    }
    out
}

fn group_failures_by_partition(failures: Vec<Failure>) -> Vec<(PathBuf, Vec<Failure>)> {
    let mut out: Vec<(PathBuf, Vec<Failure>)> = Vec::new();
    for failure in failures {
        // A failure lands beside the payload it refers to, partitioned by the
        // same channel-derived kind the payload was. A failure is always about
        // bytes a venue sent: a computed payload has no parse to fail.
        let partition = venue_partition_of(&failure.venue, &failure.channel, failure.recv_micros);
        match out.iter_mut().find(|(p, _)| *p == partition) {
            Some((_, group)) => group.push(failure),
            None => out.push((partition, vec![failure])),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_address_sits_above_kind() {
        let p = venue_partition_of("hyperliquid", "quotes", 1_758_326_400_000_000);
        assert_eq!(
            p,
            PathBuf::from("venue=hyperliquid/kind=quotes/date=2025-09-20"),
            "one venue's bytes must be a single subtree"
        );
    }

    #[test]
    fn a_market_addressed_payload_names_no_venue() {
        let address = PayloadAddress::Market("btc_basis".into());
        let p = partition_of(&address, "signals", 0);
        assert_eq!(
            p,
            PathBuf::from("market=btc_basis/kind=signals/date=1970-01-01")
        );
        assert_eq!(address.key(), "market");
    }
}
