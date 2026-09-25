//! Reading the archive back out, as payloads.
//!
//! **Replay reads the record and never grows it**, and that is held by a type
//! rather than by a field. Everything here comes back as a [`Replayed`], which
//! only this module constructs and which cannot be turned back into a
//! [`Payload`] by anyone else. It is accepted by
//! [`ingest_replayed`](crate::ingest::ingest_replayed), which does not archive,
//! and there is no way to hand it to the entry point that does.
//!
//! The previous form asked a caller to set `origin: Origin::Replay` correctly
//! and had `Archive::append` refuse it. That worked, and it spent the one slot
//! that should have said **how the bytes came to exist** on saying *which route
//! they are taking now* — which is why a payload the system generated had
//! nowhere to say so, and why twenty-four recorded gaps rebuilt as `unparsed`.
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

use crate::record::{ACCOUNT_FP_LABEL, AccountAddress, Payload, PayloadAddress};

/// A payload that came **out of** the record.
///
/// Constructible only here, and openable only by the one path. That is the
/// whole of it: a caller cannot route a replayed payload back into the
/// archiving entry point, because it cannot get the [`Payload`] out.
#[derive(Debug, Clone, PartialEq)]
pub struct Replayed(Payload);

impl Replayed {
    /// What it is about, for a caller deciding whether to ingest it at all.
    pub fn payload(&self) -> &Payload {
        &self.0
    }

    /// The sequence it was stored under.
    pub fn seq(&self) -> u64 {
        self.0.seq
    }

    /// Open it. `pub(crate)` on purpose — see the type's own documentation.
    pub(crate) fn into_payload(self) -> Payload {
        self.0
    }
}

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
pub fn read_all(root: &Path) -> Result<Vec<Replayed>, ReplayError> {
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
) -> Result<Vec<Replayed>, ReplayError> {
    let mut out = Vec::new();
    for partition in partitions_in_range(root, from_micros, to_micros) {
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

        let account = account_of(&partition);

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
            // An account's payloads come back addressed to the account, with
            // the fingerprint its segment's footer carries — not flattened to
            // the venue, which would lose whose they were.
            let address = match &account {
                Some((venue, alias)) => Some(PayloadAddress::Account(AccountAddress {
                    venue: venue.clone(),
                    account: alias.clone(),
                    fingerprint: galata_segments::label(&path, ACCOUNT_FP_LABEL)?
                        .unwrap_or_default(),
                })),
                None => None,
            };
            for batch in galata_segments::read_segment_range(&path, from_micros, to_micros)? {
                let mut payloads = payloads_of(
                    &batch,
                    &path,
                    level,
                    kind.as_deref(),
                    from_micros,
                    to_micros,
                )?;
                if let Some(address) = &address {
                    for replayed in &mut payloads {
                        replayed.0.address = address.clone();
                    }
                }
                out.extend(payloads);
            }
        }
    }
    // Receipt order, then sequence — the order capture produced them in, so a
    // rebuild's rows land in the same order capture's did.
    out.sort_by_key(|p| (p.0.recv_micros, p.0.seq));
    Ok(out)
}

fn payloads_of(
    batch: &arrow::record_batch::RecordBatch,
    path: &Path,
    level: &'static str,
    kind: Option<&str>,
    from_micros: i64,
    to_micros: i64,
) -> Result<Vec<Replayed>, ReplayError> {
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

    // The stored origin, read back rather than overwritten. It says how the
    // bytes came to exist, which is what the one path routes on — a generated
    // payload is decoded, a venue's frame is normalised.
    let origin = strings(get("origin")?, path, "origin")?;
    let mut out = Vec::with_capacity(batch.num_rows());
    for i in 0..batch.num_rows() {
        let recv_micros = recv.value(i);
        // The footer pruned whole row groups; this drops the rows inside a
        // group that straddles the edge.
        if recv_micros < from_micros || recv_micros >= to_micros {
            continue;
        }
        let value = address.value(i).to_string();
        out.push(Replayed(Payload {
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
            origin: match origin.value(i) {
                "fetched" => Origin::Fetched,
                "generated" => Origin::Generated,
                // Anything else, including a discriminator written by a build
                // this one does not know, is treated as a venue's own bytes.
                // That is the conservative reading: it goes to the adapter,
                // which reports it unparsed rather than decoding it as
                // something it is not.
                _ => Origin::Streamed,
            },
            payload: payload.value(i).to_vec(),
        }));
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

/// The partitions that could hold a receipt range, **without descending into
/// the ones that cannot**.
///
/// `galata_segments::partitions` reads every directory entry under the root to
/// decide which directories hold segments — which for a day of this venue is
/// tens of thousands of file names, **measured at 25 seconds** to conclude
/// there was nothing to rebuild for an empty date.
///
/// The archive writes `date=YYYY-MM-DD` and that is a UTC day, so a directory
/// whose day cannot overlap the range is skipped before it is opened. The
/// knowledge lives here rather than in `galata-segments` because it is a fact
/// about **this store's layout**, and a segment store that knew one caller's
/// partitioning scheme would be wrong for the next.
fn partitions_in_range(root: &Path, from_micros: i64, to_micros: i64) -> Vec<PathBuf> {
    let mut out = Vec::new();
    walk(root, from_micros, to_micros, &mut out);
    out.sort();
    out
}

fn walk(dir: &Path, from_micros: i64, to_micros: i64, out: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    let mut has_segments = false;
    for entry in entries.filter_map(|e| e.ok()) {
        let is_dir = entry
            .file_type()
            .map(|t| t.is_dir() || (t.is_symlink() && entry.path().is_dir()))
            .unwrap_or(false);
        if is_dir {
            let path = entry.path();
            if covers(&path, from_micros, to_micros) {
                walk(&path, from_micros, to_micros, out);
            }
        } else if !has_segments
            && entry
                .file_name()
                .to_str()
                .is_some_and(|n| galata_segments::Cursor::parse(n).is_some())
        {
            // One is enough to know this is a partition. The names are read
            // again by `list_segments`, and reading them twice to count them
            // is the other half of the same waste.
            has_segments = true;
        }
    }
    if has_segments {
        out.push(dir.to_path_buf());
    }
}

/// Whether a directory's own day can overlap the range.
///
/// **Only a `date=` component answers this.** Anything else — `venue=`,
/// `kind=`, or a directory nobody planned — is descended into, because a
/// pruning that guessed would skip data.
fn covers(path: &Path, from_micros: i64, to_micros: i64) -> bool {
    const DAY: i64 = 86_400_000_000;
    let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
        return true;
    };
    let Some(date) = name.strip_prefix("date=") else {
        return true;
    };
    let Some(midnight) = crate::calendar::midnight_of(date) else {
        // A directory named `date=` something that is not a date. Descended
        // into rather than skipped: `check_layout` reports it, and a reader
        // that silently dropped it would make a malformed partition look empty.
        return true;
    };
    midnight < to_micros && midnight.saturating_add(DAY) > from_micros
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

/// `(venue, alias)` where a partition sits under `venue=<v>/account=<alias>/`.
fn account_of(partition: &Path) -> Option<(String, String)> {
    let parts: Vec<&str> = partition
        .components()
        .filter_map(|c| c.as_os_str().to_str())
        .collect();
    parts.windows(2).find_map(|pair| {
        let venue = pair[0].strip_prefix("venue=")?;
        let alias = pair[1].strip_prefix("account=")?;
        Some((venue.to_string(), alias.to_string()))
    })
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
    fn a_replayed_payload_keeps_the_origin_the_record_stored() {
        // Read back rather than overwritten. It says how the bytes came to
        // exist, which is what the one path routes on — and the route itself is
        // a type, not a field, so nothing is lost by keeping the fact.
        let dir = tempfile::tempdir().unwrap();
        written(dir.path(), vec![payload(1, DAY, "quotes", "BTC")]);

        let back = read_all(dir.path()).unwrap();
        assert_eq!(back.len(), 1);
        assert_eq!(back[0].payload().origin, Origin::Streamed);
    }

    #[test]
    fn a_generated_payload_comes_back_as_generated() {
        // Which is how the one path knows to DECODE it rather than hand it to a
        // venue adapter that would rightly refuse it.
        let dir = tempfile::tempdir().unwrap();
        let mut p = payload(1, DAY, "gaps", "BTC");
        p.origin = Origin::Generated;
        written(dir.path(), vec![p]);

        let back = read_all(dir.path()).unwrap();
        assert_eq!(back[0].payload().origin, Origin::Generated);
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
        assert_eq!(back[0].seq(), 7);
        assert_eq!(back[1].seq(), 9);
        assert_eq!(back[0].payload().payload, br#"{"seq":7}"#.to_vec());
        assert_eq!(back[1].payload().symbol.as_deref(), Some("ETH"));
        assert_eq!(back[1].payload().kind, "trades");
    }

    #[test]
    fn an_accounts_payload_comes_back_addressed_to_the_account() {
        let dir = tempfile::tempdir().unwrap();
        let mut archive = Archive::open(dir.path());
        let address = PayloadAddress::Account(AccountAddress {
            venue: "hyperliquid".into(),
            account: "main".into(),
            fingerprint: "0123456789abcdef".into(),
        });
        archive
            .append(Payload {
                address: address.clone(),
                origin: Origin::Fetched,
                ..payload(1, DAY, "margin", "BTC")
            })
            .unwrap();
        let back = read_all(dir.path()).unwrap();
        assert_eq!(back.len(), 1);
        assert_eq!(
            back[0].payload().address,
            address,
            "the account and its fingerprint"
        );
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
            account: None,
            channel: "bbo".into(),
            kind: "quotes".into(),
            error: "nope".into(),
        });
        archive.flush().unwrap();

        let back = read_all(dir.path()).unwrap();
        assert_eq!(back.len(), 1, "the failure row came back as a payload");
        assert_eq!(back[0].payload().payload, br#"{"seq":1}"#.to_vec());
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
        assert_eq!(middle[0].seq(), 2);

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
        assert_eq!(read_all(dir.path()).unwrap()[0].payload().kind, "candles");
    }

    #[test]
    fn a_date_partition_outside_the_range_is_never_opened() {
        // Measured at 25 seconds to conclude "nothing to rebuild" over a
        // 17,000-segment tree, because the walk read every file name first.
        let dir = tempfile::tempdir().unwrap();
        for (seq, recv) in [(1u64, 10 * DAY), (2, 20 * DAY)] {
            written(dir.path(), vec![payload(seq, recv, "quotes", "BTC")]);
        }

        let all = partitions_in_range(dir.path(), i64::MIN, i64::MAX);
        assert_eq!(all.len(), 2, "both days are partitions");

        let one = partitions_in_range(dir.path(), 20 * DAY, 21 * DAY);
        assert_eq!(one.len(), 1);
        assert!(
            one[0]
                .to_string_lossy()
                .contains(&crate::calendar::date_of(20 * DAY))
        );

        assert!(partitions_in_range(dir.path(), 500 * DAY, 501 * DAY).is_empty());
    }

    #[test]
    fn a_day_that_straddles_the_edge_is_kept() {
        // The range is half-open and the day is a whole one, so a range ending
        // one microsecond into a day still needs that day.
        let dir = tempfile::tempdir().unwrap();
        written(dir.path(), vec![payload(1, 20 * DAY + 5, "quotes", "BTC")]);
        assert_eq!(
            partitions_in_range(dir.path(), 19 * DAY, 20 * DAY + 1).len(),
            1
        );
        assert_eq!(partitions_in_range(dir.path(), 19 * DAY, 20 * DAY).len(), 0);
    }

    #[test]
    fn a_partition_level_that_is_not_a_date_is_always_descended_into() {
        // A pruning that guessed would skip data.
        let dir = tempfile::tempdir().unwrap();
        written(dir.path(), vec![payload(1, 20 * DAY, "quotes", "BTC")]);
        let found = partitions_in_range(dir.path(), 20 * DAY, 21 * DAY);
        assert_eq!(found.len(), 1);
        assert!(found[0].to_string_lossy().contains("venue=hyperliquid"));
        assert!(found[0].to_string_lossy().contains("kind=quotes"));
    }
}
