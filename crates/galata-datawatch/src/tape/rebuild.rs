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

use crate::ingest::ingest_replayed;
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
    /// A segment in a partition this run writes holds more than one venue.
    ///
    /// Replacing one venue inside it would mean rewriting the others' rows,
    /// which a cache rebuild does not do behind anyone's back. Refused before
    /// anything is removed.
    #[error(
        "{path} holds more than one venue, so replacing one of them would take the others; \
         the tape is a cache — remove it and rebuild every venue it held"
    )]
    MixedVenues {
        /// The segment.
        path: std::path::PathBuf,
    },
    /// A segment in a partition this run writes does not say whose rows it holds.
    ///
    /// Kept, it would overlap the replacement; removed, it might be another
    /// venue's. Neither is this tool's to guess. Refused before anything is
    /// removed.
    #[error(
        "{path} carries no exact venue statistics, so whose rows it holds is unknown; \
         the tape is a cache — remove it and rebuild every venue it held"
    )]
    UnknownVenue {
        /// The segment.
        path: std::path::PathBuf,
    },
    /// A segment's footer would not read.
    #[error(transparent)]
    Footer(#[from] galata_segments::SegmentError),
    /// A segment could not be removed.
    #[error("{path}: {source}")]
    Replace {
        /// Which.
        path: std::path::PathBuf,
        /// Why.
        #[source]
        source: std::io::Error,
    },
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
    /// Segments removed before writing, where replacement was asked for —
    /// only ever the rebuilt venues' own.
    pub replaced: usize,
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
            "{} payloads → {} rows in {} segments ({} unparsed, {} segments replaced)",
            self.payloads, self.rows, self.segments, self.unparsed, self.replaced
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
    rebuild_with(
        archive_root,
        tape_root,
        adapter,
        scopes,
        from_micros,
        to_micros,
        Replace::Never,
    )
}

/// Whether a rebuild may remove what a previous one wrote.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Replace {
    /// Leave existing segments. An overlap is reported by `check_layout`.
    Never,
    /// Remove, from the partitions this run will write, the segments holding
    /// only the rebuilt venues' rows — **before** writing them. Another venue's
    /// segment in the same partition is left alone.
    Partitions,
}

/// The same, saying whether to replace.
///
/// # Why this exists
///
/// The rebuild is deterministic, so re-running over an **unchanged** archive
/// writes identical filenames and overwrites them harmlessly. The overlap
/// appears when the archive has **grown**:
///
/// ```text
///   first run    252,926 payloads  →  s-0_252926.parquet
///   second run   254,858 payloads  →  s-0_254858.parquet
///   both files claim sequences 0..252,926
/// ```
///
/// Which is exactly what a **scheduled retry** looks like — the first attempt
/// wrote something, capture kept running, the retry sees more. Every scheduler
/// retries, so without this the scheduled use double-counts.
///
/// # The window
///
/// Partitions are removed **before** the write, so for the duration of the
/// rebuild they are empty. That is chosen deliberately: the tape is a cache and
/// the archive is untouched, so the worst outcome of a crash here is a
/// partition that must be rebuilt again — which is what this tool does. The
/// alternative, removing afterwards, needs a delete computed against state that
/// has since changed.
pub fn rebuild_with(
    archive_root: &Path,
    tape_root: &Path,
    adapter: &dyn Adapter,
    scopes: Option<&[&str]>,
    from_micros: i64,
    to_micros: i64,
    replace: Replace,
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
    // Nothing is appended: `ingest_replayed` is the entry point that does not,
    // and a `Replayed` cannot reach the one that does.
    let mut archive = Archive::open(archive_root);
    let collected = Arc::new(CollectingSink::default());
    let mut tape = Tape::open(tape_root);

    for payload in payloads {
        let seq = payload.seq();
        let result = ingest_replayed(&mut archive, adapter, collected.as_ref(), payload)?;
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

    if replace == Replace::Partitions {
        // **Only this run's venues, in only the partitions this run will
        // write.** A dataset with no rows in the range is left alone: *this
        // rebuild produced no trades* and *there are no trades* are different
        // claims. And a partition is `kind=/date=`, shared by every venue that
        // supplies the dataset — so *this venue's quotes* is not *the quotes*,
        // and a segment is kept or removed by the venue its footer names.
        //
        // **Planned in full, then removed.** A refusal therefore means the
        // tape was not touched, never that it was half-replaced.
        let ours = tape.pending_venues();
        let mut doomed = Vec::new();
        for partition in tape.pending_partitions() {
            for (_, segment) in galata_segments::list_segments(&tape_root.join(&partition)) {
                match galata_segments::string_bounds(&segment, "venue")? {
                    Some((low, high)) if low == high => {
                        if ours.contains(&low) {
                            doomed.push(segment);
                        }
                    }
                    Some(_) => return Err(RebuildError::MixedVenues { path: segment }),
                    None => return Err(RebuildError::UnknownVenue { path: segment }),
                }
            }
        }
        for segment in doomed {
            std::fs::remove_file(&segment).map_err(|source| RebuildError::Replace {
                path: segment.clone(),
                source,
            })?;
            report.replaced += 1;
        }
    }

    report.segments = tape.commit()?.len();
    Ok(report)
}

// The tests normalise real frames, so they need an adapter to normalise them
// with. That is the `hyperliquid` feature rather than `capture` — the
// normaliser is pure and needs no runtime, which is the whole point of the
// split.
#[cfg(all(test, feature = "hyperliquid"))]
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

    #[test]
    fn a_generated_gap_rebuilds_as_a_gap() {
        // **The defect this change exists for.** Nine hours of capture, one
        // connection reset, twenty-four gaps recorded — and every one rebuilt
        // as `unparsed`, because the record held `format!("{:?}", event)` and
        // no parser recovers a Debug string.
        //
        // A gap that cannot be rebuilt is an absence again, which is the one
        // thing recording it durably was supposed to prevent.
        use galata_wire::{Clipped, Envelope, Event, Gap, GapCause, Series, Ticker, Venue};

        let dir = tempfile::tempdir().unwrap();
        let hl = adapter();
        let root = dir.path().join("archive");
        let mut archive = Archive::open(&root);
        let sink = crate::sink::NullSink;

        crate::ingest::record_generated(
            &mut archive,
            &sink,
            "hyperliquid",
            Envelope::new(
                Venue::new("hyperliquid").unwrap(),
                Ticker::new("BTC").unwrap(),
                Some(DAY),
                DAY + 5,
                Event::Gap(Gap {
                    series: Series::Quotes,
                    from_micros: DAY,
                    to_micros: DAY + 5,
                    cause: GapCause::SessionLost,
                    clipped: Clipped::Continuous,
                }),
            ),
        )
        .unwrap();
        archive.flush().unwrap();

        let tape = dir.path().join("tape");
        let report = rebuild(&root, &tape, &hl, None, i64::MIN, i64::MAX).unwrap();
        assert_eq!(
            report.unparsed, 0,
            "the gap was handed to the venue adapter"
        );
        assert_eq!(report.rows, 1);

        assert!(
            tape.join("kind=gaps").is_dir(),
            "no gaps dataset: {:?}",
            std::fs::read_dir(&tape)
                .unwrap()
                .flatten()
                .map(|e| e.file_name())
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn a_generated_event_survives_the_round_trip_exactly() {
        // Including its numbers. rust_decimal's ordinary serde goes through
        // f64, which is precision loss in the type chosen because f64 loses
        // precision.
        use galata_wire::{Envelope, Event, Funding, Num, Ticker, Venue};
        use std::str::FromStr;

        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("archive");
        let mut archive = Archive::open(&root);
        // Eighteen decimal places, which an f64 cannot hold.
        let rate = Num::from_str("0.000000000000000123").unwrap();
        let envelope = Envelope::new(
            Venue::new("hyperliquid").unwrap(),
            Ticker::new("BTC").unwrap(),
            Some(DAY),
            DAY,
            Event::Funding(Funding {
                rate,
                next_micros: Some(DAY + 3_600_000_000),
            }),
        );
        crate::ingest::record_generated(
            &mut archive,
            &crate::sink::NullSink,
            "hyperliquid",
            envelope,
        )
        .unwrap();
        archive.flush().unwrap();

        let back = crate::replay::read_all(&root).unwrap();
        let decoded: serde_json::Value =
            serde_json::from_slice(&back[0].payload().payload).unwrap();
        let stored = decoded["envelope"]["event"]["Funding"]["rate"]
            .as_str()
            .unwrap();
        assert_eq!(
            stored, "0.000000000000000123",
            "stored as a string, exactly"
        );
    }

    #[test]
    fn a_malformed_venue_frame_stays_unparsed() {
        // Routing is by the recorded origin, never by trying one and falling
        // back — a fallback would make a genuinely malformed frame look like a
        // generated one on a bad day.
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
        assert_eq!(report.unparsed, 1);
        assert!(dir.path().join("tape").join("kind=unparsed").is_dir());
    }

    /// An archive that grows between two rebuilds — which is exactly what a
    /// scheduled retry sees.
    fn growing_archive() -> (tempfile::TempDir, Hyperliquid) {
        let (dir, hl) = archive_with_frames();
        (dir, hl)
    }

    fn segment_names(tape: &Path) -> Vec<String> {
        let mut out: Vec<String> = galata_segments::partitions(tape)
            .iter()
            .flat_map(|p| galata_segments::list_segments(p))
            .map(|(_, path)| path.file_name().unwrap().to_string_lossy().to_string())
            .collect();
        out.sort();
        out
    }

    /// Add one more frame to an existing archive, as capture would.
    ///
    /// **The sequence is set by hand**, because `Archive::next_seq` restarts at
    /// zero on every `open` — a reopened archive hands out sequences that
    /// collide with the ones already on disk. That is a real defect and it is
    /// recorded in `design/measured.md`; here it would only make the fixture
    /// lie, so the fixture steps around it.
    fn grow(dir: &Path, hl: &Hyperliquid, seq: u64) {
        let mut archive = Archive::open(dir.join("archive"));
        let mut payload = hl.classify(&bbo("BTC", 9_000), DAY + 50);
        payload.seq = seq;
        archive.append(payload).unwrap();
        archive.flush().unwrap();
    }

    #[test]
    fn nothing_is_removed_by_default() {
        // Deleting as a side effect of a rebuild is the kind of thing that
        // should require saying so.
        let (dir, hl) = growing_archive();
        let root = dir.path().join("archive");
        let tape = dir.path().join("tape");

        rebuild(&root, &tape, &hl, None, i64::MIN, i64::MAX).unwrap();
        let first = segment_names(&tape);
        grow(dir.path(), &hl, 100);
        let report = rebuild(&root, &tape, &hl, None, i64::MIN, i64::MAX).unwrap();

        assert_eq!(report.replaced, 0);
        let after = segment_names(&tape);
        assert!(
            after.len() > first.len(),
            "the old segments should still be there: {after:?}"
        );
        // And the overlap is REPORTED rather than hidden.
        assert!(
            !crate::tape::check_layout(&tape).is_empty(),
            "an overlap went unreported"
        );
    }

    #[test]
    fn a_retry_after_growth_leaves_one_copy() {
        // The scheduled case. Without this the second run's rows are counted
        // twice by anyone summing the partition.
        let (dir, hl) = growing_archive();
        let root = dir.path().join("archive");
        let tape = dir.path().join("tape");

        rebuild_with(
            &root,
            &tape,
            &hl,
            None,
            i64::MIN,
            i64::MAX,
            Replace::Partitions,
        )
        .unwrap();
        grow(dir.path(), &hl, 100);
        let report = rebuild_with(
            &root,
            &tape,
            &hl,
            None,
            i64::MIN,
            i64::MAX,
            Replace::Partitions,
        )
        .unwrap();

        assert!(report.replaced > 0, "nothing was replaced");
        assert_eq!(
            crate::tape::check_layout(&tape),
            Vec::new(),
            "an overlap survived replacement"
        );
    }

    /// Commit `venue`'s quotes for the fixture's day straight onto the tape,
    /// as that venue's own rebuild would have: a segment holding its rows only.
    fn another_venues_quotes(tape: &Path, venue: &str) -> std::path::PathBuf {
        venues_quotes(tape, &[venue, venue])
    }

    /// One committed segment holding one quote per venue named, in order.
    fn venues_quotes(tape: &Path, venues: &[&str]) -> std::path::PathBuf {
        use galata_wire::{Envelope, Event, Num, Quote, Ticker, Venue};
        use std::str::FromStr;
        let mut writer = Tape::open(tape);
        for (i, venue) in venues.iter().enumerate() {
            let (seq, millis) = (900_000 + i as u64, 1_500 + 1_000 * i as i64);
            writer.take(Row {
                stream_seq: seq,
                envelope: Envelope::new(
                    Venue::new(*venue).unwrap(),
                    Ticker::new("BTC").unwrap(),
                    Some(millis * 1_000),
                    millis * 1_000 + 7,
                    Event::Quote(Quote {
                        bid_px: Some(Num::from_str("81210.0").unwrap()),
                        ask_px: Some(Num::from_str("81216.0").unwrap()),
                        bid_sz: None,
                        ask_sz: None,
                        bid_spread: None,
                        ask_spread: None,
                    }),
                ),
            });
        }
        let mut written = writer.commit().unwrap();
        assert_eq!(written.len(), 1);
        written.remove(0)
    }

    #[test]
    fn another_venues_rows_survive_replacement() {
        // The tape partitions by kind and date; venue is a column. So a
        // partition is shared by every venue that supplies the dataset, and
        // replacing one venue's day must not take the others' with it.
        let (dir, hl) = growing_archive();
        let root = dir.path().join("archive");
        let tape = dir.path().join("tape");

        let theirs = another_venues_quotes(&tape, "rh-crypto");
        let before = std::fs::read(&theirs).unwrap();

        rebuild_with(
            &root,
            &tape,
            &hl,
            None,
            i64::MIN,
            i64::MAX,
            Replace::Partitions,
        )
        .unwrap();
        grow(dir.path(), &hl, 100);
        let report = rebuild_with(
            &root,
            &tape,
            &hl,
            None,
            i64::MIN,
            i64::MAX,
            Replace::Partitions,
        )
        .unwrap();

        assert!(report.replaced > 0, "nothing of hyperliquid's was replaced");
        assert_eq!(
            std::fs::read(&theirs).ok(),
            Some(before),
            "another venue's segment did not survive a replacement of hyperliquid"
        );
        assert_eq!(crate::tape::check_layout(&tape), Vec::new());
    }

    fn quotes_segments(tape: &Path) -> Vec<std::path::PathBuf> {
        galata_segments::list_segments(&tape.join("kind=quotes/date=1970-01-01"))
            .into_iter()
            .map(|(_, path)| path)
            .collect()
    }

    #[test]
    fn a_mixed_venue_segment_is_refused_before_anything_is_removed() {
        let (dir, hl) = growing_archive();
        let root = dir.path().join("archive");
        let tape = dir.path().join("tape");

        // hyperliquid's own segment, which a replacement WOULD remove…
        rebuild(&root, &tape, &hl, None, i64::MIN, i64::MAX).unwrap();
        // …beside one holding two venues, which it cannot.
        let mixed = venues_quotes(&tape, &["hyperliquid", "rh-crypto"]);
        let before = quotes_segments(&tape);
        assert_eq!(before.len(), 2);

        grow(dir.path(), &hl, 100);
        let refused = rebuild_with(
            &root,
            &tape,
            &hl,
            None,
            i64::MIN,
            i64::MAX,
            Replace::Partitions,
        );

        match refused {
            Err(RebuildError::MixedVenues { path }) => assert_eq!(path, mixed),
            other => panic!("expected MixedVenues, got {other:?}"),
        }
        assert_eq!(
            quotes_segments(&tape),
            before,
            "a refusal removed something"
        );
    }

    #[test]
    fn a_segment_without_venue_statistics_is_refused() {
        use arrow::array::StringArray;
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;

        let (dir, hl) = growing_archive();
        let root = dir.path().join("archive");
        let tape = dir.path().join("tape");

        // A segment written without statistics on `venue`, as a tape from
        // before `venue` joined PRUNE_ON would be.
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "venue",
                DataType::Utf8,
                false,
            )])),
            vec![Arc::new(StringArray::from(vec!["rh-crypto"]))],
        )
        .unwrap();
        let silent = galata_segments::write_segment(
            &tape.join("kind=quotes/date=1970-01-01"),
            galata_segments::Cursor::Seq { first: 5, last: 5 },
            &batch,
            galata_segments::Codec::Zstd,
        )
        .unwrap();

        let refused = rebuild_with(
            &root,
            &tape,
            &hl,
            None,
            i64::MIN,
            i64::MAX,
            Replace::Partitions,
        );
        match refused {
            Err(RebuildError::UnknownVenue { path }) => assert_eq!(path, silent),
            other => panic!("expected UnknownVenue, got {other:?}"),
        }
        assert!(silent.exists(), "an unknown segment was removed");
    }

    #[test]
    fn the_longest_valid_venue_is_read_exactly() {
        // Parquet may truncate string statistics. A venue is a token of at most
        // 64 bytes, and a truncated bound would make its segment unknown.
        let dir = tempfile::tempdir().unwrap();
        let longest = "v".repeat(galata_wire::MAX_TOKEN);
        let segment = venues_quotes(dir.path(), &[&longest, &longest]);
        assert_eq!(
            galata_segments::string_bounds(&segment, "venue").unwrap(),
            Some((longest.clone(), longest))
        );
    }

    #[test]
    fn a_neighbouring_date_survives_replacement() {
        // Replacement removes only the partitions this run will write.
        let (dir, hl) = growing_archive();
        let root = dir.path().join("archive");
        let tape = dir.path().join("tape");

        // A partition from some other day, which this rebuild has no rows for.
        let elsewhere = tape.join("kind=quotes/date=2020-01-01");
        std::fs::create_dir_all(&elsewhere).unwrap();
        std::fs::write(elsewhere.join("s-1_2.parquet"), b"not ours").unwrap();

        rebuild_with(
            &root,
            &tape,
            &hl,
            None,
            i64::MIN,
            i64::MAX,
            Replace::Partitions,
        )
        .unwrap();

        assert!(
            elsewhere.join("s-1_2.parquet").exists(),
            "a date this run never touched was removed"
        );
    }
}
