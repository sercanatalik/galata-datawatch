//! Reading the archive back out, as payloads.
//!
//! **Replay reads the record and never grows it.** Every payload it returns
//! carries [`Origin::Replay`], which [`ingest`](crate::ingest::ingest) honours
//! by not appending — so a replay cannot write to the store it is reading.
//!
//! **Payloads that failed to parse come back like any other.** Filtering an
//! archive by parse success discards exactly the evidence a normalisation
//! defect is diagnosed from; the `failures/` sibling is not read here at all,
//! because those rows *name* payloads rather than being them.
//!
//! # Two prunings, in this order
//!
//! ```text
//!   1. the NAME     t-<first>_<last>_<pid>_<seq> — a window outside that
//!                   range cannot be in the file, and no file is opened
//!
//!   2. the FOOTER   within a segment that could hold it, only the row groups
//!                   whose recv_micros statistics overlap
//! ```
//!
//! The first is a directory listing. The second is why `MAX_ROW_GROUP_ROWS` was
//! measured at all. A day of this venue is tens of thousands of segments, and
//! opening all of them to rebuild an hour is the cost these two exist to avoid.

use std::path::{Path, PathBuf};

use arrow::array::{Array, BinaryArray, Int64Array, StringArray, UInt64Array};
use galata_wire::Origin;

use crate::record::{Payload, PayloadAddress};

/// Why a replay could not be read.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ReplayError {
    /// A segment would not read.
    #[error(transparent)]
    Segment(#[from] galata_segments::SegmentError),
    /// A segment is not the shape the archive writes.
    #[error("{path}: the archive segment is missing column {column}")]
    Shape {
        /// Which segment.
        path: PathBuf,
        /// Which column.
        column: &'static str,
    },
}

/// Every payload the archive holds, in receipt order.
///
/// Opens every segment under the root. Where a range is known, prefer
/// [`read_range`], which excludes segments from their names before opening
/// anything.
pub fn read_all(root: &Path) -> Result<Vec<Payload>, ReplayError> {
    read_range(root, None, i64::MIN, i64::MAX)
}

/// The payloads a **half-open** receipt range `[from, to)` holds, in receipt
/// order.
///
/// `scopes` are subtrees under the root — `venue=hyperliquid`, or
/// `venue=hyperliquid/kind=candles` — matched at path-component boundaries, so
/// `venue=h` is not a prefix of `venue=hyperliquid`. `None` reads every scope;
/// an **empty slice reads none**, because a caller that declared nothing asked
/// for nothing.
pub fn read_range(
    root: &Path,
    scopes: Option<&[&str]>,
    from_micros: i64,
    to_micros: i64,
) -> Result<Vec<Payload>, ReplayError> {
    let mut out = Vec::new();
    for partition in galata_segments::partitions(root) {
        // The failure rows NAME payloads; they are not payloads. Replaying them
        // would ingest an error message as though it were a venue frame.
        if partition.ends_with("failures") {
            continue;
        }
        if !in_scope(root, &partition, scopes) {
            continue;
        }
        // Read from the path rather than stored twice: a second copy is a
        // second thing that can disagree.
        let kind = kind_of(&partition);
        let level = address_level(&partition);

        for (cursor, path) in galata_segments::list_segments(&partition) {
            // The name carries the range, so a segment that cannot hold the
            // window is skipped without being opened.
            if let galata_segments::Cursor::Time {
                first_micros,
                last_micros,
                ..
            } = cursor
                && (last_micros < from_micros || first_micros >= to_micros)
            {
                continue;
            }
            for batch in galata_segments::read_segment_range(&path, from_micros, to_micros)? {
                out.extend(payloads_of(
                    &batch,
                    &path,
                    level,
                    kind.as_deref(),
                    from_micros,
                    to_micros,
                )?);
            }
        }
    }
    // Receipt order, then sequence — the order capture produced them in, so a
    // rebuild's rows land in the same order capture's did.
    out.sort_by_key(|p| (p.recv_micros, p.seq));
    Ok(out)
}

fn payloads_of(
    batch: &arrow::record_batch::RecordBatch,
    path: &Path,
    level: &'static str,
    kind: Option<&str>,
    from_micros: i64,
    to_micros: i64,
) -> Result<Vec<Payload>, ReplayError> {
    let shape = |column: &'static str| ReplayError::Shape {
        path: path.to_path_buf(),
        column,
    };
    let get = |name: &'static str| batch.column_by_name(name).ok_or_else(|| shape(name));

    let seq = get("seq")?
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| shape("seq"))?;
    let recv = get("recv_micros")?
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| shape("recv_micros"))?;
    let address = strings(get(level)?, path, level)?;
    let channel = strings(get("channel")?, path, "channel")?;
    let symbol = strings(get("symbol")?, path, "symbol")?;
    let payload = get("payload")?
        .as_any()
        .downcast_ref::<BinaryArray>()
        .ok_or_else(|| shape("payload"))?;

    let mut out = Vec::with_capacity(batch.num_rows());
    for i in 0..batch.num_rows() {
        let recv_micros = recv.value(i);
        // The footer pruned whole row groups; this drops the rows inside a
        // group that straddles the edge.
        if recv_micros < from_micros || recv_micros >= to_micros {
            continue;
        }
        let value = address.value(i).to_string();
        out.push(Payload {
            seq: seq.value(i),
            recv_micros,
            address: match level {
                "market" => PayloadAddress::Market(value),
                _ => PayloadAddress::Venue(value),
            },
            channel: channel.value(i).to_string(),
            kind: kind
                .map(str::to_string)
                .unwrap_or_else(|| channel.value(i).to_string()),
            symbol: (!symbol.is_null(i)).then(|| symbol.value(i).to_string()),
            // **Names a payload LEAVING the archive**, not arriving. The one
            // path refuses to append it.
            origin: Origin::Replay,
            payload: payload.value(i).to_vec(),
        });
    }
    Ok(out)
}

fn strings<'a>(
    array: &'a dyn Array,
    path: &Path,
    column: &'static str,
) -> Result<&'a StringArray, ReplayError> {
    array
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| ReplayError::Shape {
            path: path.to_path_buf(),
            column,
        })
}

/// A scope names a subtree **at component boundaries**.
fn in_scope(root: &Path, partition: &Path, scopes: Option<&[&str]>) -> bool {
    let Some(scopes) = scopes else {
        return true;
    };
    let Ok(relative) = partition.strip_prefix(root) else {
        return false;
    };
    let relative: Vec<_> = relative.components().collect();
    scopes.iter().any(|scope| {
        let wanted: Vec<_> = Path::new(scope).components().collect();
        !wanted.is_empty() && relative.starts_with(&wanted)
    })
}

/// Which column carries the address, read from the partition rather than
/// guessed at.
fn address_level(partition: &Path) -> &'static str {
    partition
        .components()
        .filter_map(|c| c.as_os_str().to_str())
        .find_map(|c| c.starts_with("market=").then_some("market"))
        .unwrap_or("venue")
}

fn kind_of(partition: &Path) -> Option<String> {
    partition
        .components()
        .filter_map(|c| c.as_os_str().to_str())
        .find_map(|c| c.strip_prefix("kind=").map(str::to_string))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record::Archive;

    const DAY: i64 = 86_400_000_000;

    fn payload(seq: u64, recv: i64, kind: &str, symbol: &str) -> Payload {
        Payload {
            seq,
            recv_micros: recv,
            address: PayloadAddress::Venue("hyperliquid".into()),
            channel: "bbo".into(),
            kind: kind.into(),
            symbol: Some(symbol.into()),
            origin: Origin::Streamed,
            payload: format!(r#"{{"seq":{seq}}}"#).into_bytes(),
        }
    }

    fn written(root: &Path, rows: Vec<Payload>) {
        let mut archive = Archive::open(root);
        for row in rows {
            archive.append(row).unwrap();
        }
        archive.flush().unwrap();
    }

    #[test]
    fn a_replayed_payload_is_marked_as_leaving_the_archive() {
        // Which is what `Archive::append` refuses, so a replay cannot write to
        // the store it is reading.
        let dir = tempfile::tempdir().unwrap();
        written(dir.path(), vec![payload(1, DAY, "quotes", "BTC")]);

        let back = read_all(dir.path()).unwrap();
        assert_eq!(back.len(), 1);
        assert_eq!(back[0].origin, Origin::Replay);
    }

    #[test]
    fn a_replayed_payload_keeps_its_sequence_and_its_bytes() {
        let dir = tempfile::tempdir().unwrap();
        written(
            dir.path(),
            vec![
                payload(7, DAY, "quotes", "BTC"),
                payload(9, DAY + 1, "trades", "ETH"),
            ],
        );

        let back = read_all(dir.path()).unwrap();
        assert_eq!(back.len(), 2);
        // Receipt order, which is the order capture produced them in.
        assert_eq!(back[0].seq, 7);
        assert_eq!(back[1].seq, 9);
        assert_eq!(back[0].payload, br#"{"seq":7}"#.to_vec());
        assert_eq!(back[1].symbol.as_deref(), Some("ETH"));
        assert_eq!(back[1].kind, "trades");
    }

    #[test]
    fn a_failure_row_is_not_replayed_as_a_payload() {
        // Those rows NAME payloads; they are not payloads. Replaying one would
        // ingest an error message as though it were a venue frame.
        let dir = tempfile::tempdir().unwrap();
        let mut archive = Archive::open(dir.path());
        archive.append(payload(1, DAY, "quotes", "BTC")).unwrap();
        archive.append_failure(crate::record::Failure {
            seq: 1,
            recv_micros: DAY,
            venue: "hyperliquid".into(),
            channel: "bbo".into(),
            kind: "quotes".into(),
            error: "nope".into(),
        });
        archive.flush().unwrap();

        let back = read_all(dir.path()).unwrap();
        assert_eq!(back.len(), 1, "the failure row came back as a payload");
        assert_eq!(back[0].payload, br#"{"seq":1}"#.to_vec());
    }

    #[test]
    fn a_range_excludes_what_lies_outside_it() {
        let dir = tempfile::tempdir().unwrap();
        // Three flushes, so three segments with three distinct name ranges.
        for (seq, recv) in [(1u64, 10 * DAY), (2, 20 * DAY), (3, 30 * DAY)] {
            written(dir.path(), vec![payload(seq, recv, "quotes", "BTC")]);
        }

        let middle = read_range(dir.path(), None, 15 * DAY, 25 * DAY).unwrap();
        assert_eq!(middle.len(), 1);
        assert_eq!(middle[0].seq, 2);

        // Half-open: the row exactly at `to` is excluded, the one at `from` is
        // not.
        assert_eq!(
            read_range(dir.path(), None, 20 * DAY, 20 * DAY + 1)
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            read_range(dir.path(), None, 20 * DAY + 1, 30 * DAY)
                .unwrap()
                .len(),
            0
        );
    }

    #[test]
    fn a_scope_names_a_subtree_at_component_boundaries() {
        // `venue=h` is not a prefix of `venue=hyperliquid`.
        let dir = tempfile::tempdir().unwrap();
        written(
            dir.path(),
            vec![
                payload(1, DAY, "quotes", "BTC"),
                payload(2, DAY + 1, "trades", "BTC"),
            ],
        );

        assert_eq!(
            read_range(dir.path(), None, i64::MIN, i64::MAX)
                .unwrap()
                .len(),
            2
        );
        assert_eq!(
            read_range(
                dir.path(),
                Some(&["venue=hyperliquid/kind=quotes"]),
                i64::MIN,
                i64::MAX
            )
            .unwrap()
            .len(),
            1
        );
        assert_eq!(
            read_range(dir.path(), Some(&["venue=h"]), i64::MIN, i64::MAX)
                .unwrap()
                .len(),
            0,
            "a partial component is not a scope"
        );
    }

    #[test]
    fn a_caller_that_declared_nothing_asked_for_nothing() {
        let dir = tempfile::tempdir().unwrap();
        written(dir.path(), vec![payload(1, DAY, "quotes", "BTC")]);
        assert_eq!(
            read_range(dir.path(), Some(&[]), i64::MIN, i64::MAX)
                .unwrap()
                .len(),
            0
        );
    }

    #[test]
    fn the_kind_comes_from_the_path_and_not_from_a_second_copy() {
        // A second copy is a second thing that can disagree.
        let dir = tempfile::tempdir().unwrap();
        written(dir.path(), vec![payload(1, DAY, "candles", "BTC")]);
        assert_eq!(read_all(dir.path()).unwrap()[0].kind, "candles");
    }
}
