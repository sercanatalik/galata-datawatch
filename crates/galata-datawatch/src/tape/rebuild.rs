//! Archive → the one path → tape.
//!
//! ```text
//!   archive segment ──▶ Payload{origin: Replay} ──▶ ingest ──▶ Envelope ──▶ tape
//!                                                     │
//!                                         archive::append REFUSES Replay,
//!                                         so reading the record cannot grow it
//! ```
//!
//! **Through `ingest`, not around it.** The rebuild could read the archive and
//! normalise for itself; it must not, and that is the whole point. A rebuild
//! with its own normalisation produces a *second derivation* of the bytes, and
//! comparing its tape to what capture published compares two implementations
//! rather than checking one. Going through the one path makes the claim
//! structural: the events a rebuild produces **are** the events capture
//! produced, because the same function produced them.
//!
//! **The sequence is the archive's.** A tape row's `stream_seq` is the `seq`
//! the payload was stored under, read back rather than re-issued — which is
//! what makes segment names stable, and therefore what makes running twice
//! testable.

use std::path::Path;
use std::sync::Arc;

use crate::ingest::ingest;
use crate::record::Archive;
use crate::replay::{self, ReplayError};
use crate::sink::CollectingSink;
use crate::tape::writer::{Row, Tape, TapeError};
use crate::venue::Adapter;

/// Why a rebuild stopped.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum RebuildError {
    /// The archive would not read.
    #[error(transparent)]
    Replay(#[from] ReplayError),
    /// The tape would not write.
    #[error(transparent)]
    Tape(#[from] TapeError),
    /// The record refused.
    #[error(transparent)]
    Record(#[from] crate::record::RecordError),
}

/// What a rebuild did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct Rebuilt {
    /// Payloads read from the archive.
    pub payloads: usize,
    /// Rows handed to the tape.
    pub rows: usize,
    /// Segments the tape committed.
    pub segments: usize,
    /// Payloads that would not normalise.
    ///
    /// **Not an error.** The archive holds them, the tape carries them as
    /// `unparsed` rows, and a rebuild that refused to finish over one would
    /// make a single malformed frame block a whole day.
    pub unparsed: usize,
}

impl Rebuilt {
    /// Whether there was anything to do.
    pub fn is_empty(&self) -> bool {
        self.payloads == 0
    }

    /// A line an operator can act on.
    pub fn report(&self) -> String {
        format!(
            "{} payloads → {} rows in {} segments ({} unparsed)",
            self.payloads, self.rows, self.segments, self.unparsed
        )
    }
}

/// Rebuild the tape for a **half-open** receipt range `[from, to)`.
///
/// `scopes` are archive subtrees — `venue=hyperliquid` — or `None` for all.
///
/// **Whole partitions only.** A rebuild of half a day writes a segment whose
/// sequence range overlaps the one already there, which `check_layout` reports
/// as a double-count — correctly, because it is one.
pub fn rebuild(
    archive_root: &Path,
    tape_root: &Path,
    adapter: &dyn Adapter,
    scopes: Option<&[&str]>,
    from_micros: i64,
    to_micros: i64,
) -> Result<Rebuilt, RebuildError> {
    let payloads = replay::read_range(archive_root, scopes, from_micros, to_micros)?;
    let mut report = Rebuilt {
        payloads: payloads.len(),
        ..Rebuilt::default()
    };
    if payloads.is_empty() {
        return Ok(report);
    }

    // **Rooted at the archive being read**, so a bug that appended anyway would
    // grow that tree and be visible, rather than quietly creating a second one.
    // Nothing is appended: every payload carries `Origin::Replay`.
    let mut archive = Archive::open(archive_root);
    let collected = Arc::new(CollectingSink::default());
    let mut tape = Tape::open(tape_root);

    for payload in payloads {
        let seq = payload.seq;
        let result = ingest(&mut archive, adapter, collected.as_ref(), payload)?;
        if result.unparsed {
            report.unparsed += 1;
        }
        for envelope in collected.drain() {
            tape.take(Row {
                stream_seq: seq,
                envelope,
            });
            report.rows += 1;
        }
    }

    report.segments = tape.commit()?.len();
    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adapters::hyperliquid::{Config as HlConfig, Hyperliquid, Instrument, Market};
    use crate::record::{Payload, PayloadAddress};
    use crate::venue::Construct;
    use galata_wire::Origin;

    const DAY: i64 = 86_400_000_000;

    fn adapter() -> Hyperliquid {
        Hyperliquid::new(
            HlConfig {
                market: Market::Mainnet,
                instruments: vec![Instrument::main("BTC"), Instrument::main("ETH")],
                candle_interval: "1m".into(),
            },
            None,
        )
        .unwrap()
    }

    /// A `bbo` frame in the shape the venue actually sends.
    fn bbo(coin: &str, millis: i64) -> Vec<u8> {
        format!(
            r#"{{"channel":"bbo","data":{{"coin":"{coin}","time":{millis},"bbo":[{{"px":"81213.0","sz":"15.8","n":44}},{{"px":"81214.0","sz":"2.4","n":7}}]}}}}"#
        )
        .into_bytes()
    }

    /// An archive holding a few real frames, plus one that cannot be read.
    fn archive_with_frames() -> (tempfile::TempDir, Hyperliquid) {
        let dir = tempfile::tempdir().unwrap();
        let hl = adapter();
        let mut archive = Archive::open(dir.path().join("archive"));
        for (i, (coin, millis)) in [("BTC", 1_000), ("ETH", 2_000), ("BTC", 3_000)]
            .into_iter()
            .enumerate()
        {
            let seq = archive.next_seq();
            let mut payload = hl.classify(&bbo(coin, millis), DAY + i as i64);
            payload.seq = seq;
            archive.append(payload).unwrap();
        }
        // One the adapter cannot read, under a channel it knows.
        let seq = archive.next_seq();
        archive
            .append(Payload {
                seq,
                recv_micros: DAY + 9,
                address: PayloadAddress::Venue("hyperliquid".into()),
                channel: "bbo".into(),
                kind: "quotes".into(),
                symbol: Some("BTC".into()),
                origin: Origin::Streamed,
                payload: b"{\"channel\":\"bbo\",\"data\":\"not an object\"}".to_vec(),
            })
            .unwrap();
        archive.flush().unwrap();
        (dir, hl)
    }

    #[test]
    fn a_rebuild_writes_what_capture_would_have_published() {
        let (dir, hl) = archive_with_frames();
        let report = rebuild(
            &dir.path().join("archive"),
            &dir.path().join("tape"),
            &hl,
            None,
            i64::MIN,
            i64::MAX,
        )
        .unwrap();

        assert_eq!(report.payloads, 4);
        assert_eq!(report.unparsed, 1, "the malformed frame did not stop it");
        // Three quotes, and the unparsed one published as an anomaly.
        assert_eq!(report.rows, 4);
        assert!(report.segments >= 1);
        assert_eq!(
            crate::tape::check_layout(&dir.path().join("tape")),
            Vec::new()
        );
    }

    #[test]
    fn a_rebuild_does_not_grow_the_archive_it_is_reading() {
        // Replay reads the record and never grows it.
        let (dir, hl) = archive_with_frames();
        let root = dir.path().join("archive");
        let before = galata_segments::partitions(&root)
            .iter()
            .map(|p| galata_segments::list_segments(p).len())
            .sum::<usize>();

        rebuild(
            &root,
            &dir.path().join("tape"),
            &hl,
            None,
            i64::MIN,
            i64::MAX,
        )
        .unwrap();

        let after = galata_segments::partitions(&root)
            .iter()
            .map(|p| galata_segments::list_segments(p).len())
            .sum::<usize>();
        assert_eq!(before, after, "the archive grew");
    }

    #[test]
    fn twice_is_the_same() {
        // The tape is a cache whose justification is that it can be thrown
        // away. A rebuild producing a different tape each time makes that
        // untestable.
        let (dir, hl) = archive_with_frames();
        let root = dir.path().join("archive");

        let names = |tape: &Path| -> Vec<String> {
            let mut out: Vec<String> = galata_segments::partitions(tape)
                .iter()
                .flat_map(|p| galata_segments::list_segments(p))
                .map(|(_, path)| {
                    path.strip_prefix(tape)
                        .unwrap()
                        .to_string_lossy()
                        .to_string()
                })
                .collect();
            out.sort();
            out
        };

        let first = dir.path().join("tape-a");
        let second = dir.path().join("tape-b");
        let a = rebuild(&root, &first, &hl, None, i64::MIN, i64::MAX).unwrap();
        let b = rebuild(&root, &second, &hl, None, i64::MIN, i64::MAX).unwrap();

        assert_eq!(a, b, "the two runs report the same thing");
        assert_eq!(names(&first), names(&second), "the segment names differ");

        // And byte-for-byte, which the names alone do not prove.
        for name in names(&first) {
            assert_eq!(
                std::fs::read(first.join(&name)).unwrap(),
                std::fs::read(second.join(&name)).unwrap(),
                "{name} differs between runs"
            );
        }
    }

    #[test]
    fn an_empty_range_rebuilds_nothing_and_says_so() {
        let (dir, hl) = archive_with_frames();
        let report = rebuild(
            &dir.path().join("archive"),
            &dir.path().join("tape"),
            &hl,
            None,
            500 * DAY,
            501 * DAY,
        )
        .unwrap();
        assert!(report.is_empty());
        assert_eq!(report.segments, 0);
    }
}
