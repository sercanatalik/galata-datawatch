//! What the record and the one path claim, asserted against a real filesystem.
//!
//! These live **inside** the crate rather than under `tests/`, because
//! `Archive::append` is `pub(crate)` and an integration test genuinely cannot
//! reach it. That is the wall working: the only way in from outside is
//! [`crate::ingest`].

use std::sync::Mutex;

use galata_segments::{list_segments, read_segment};
use galata_wire::{Envelope, Event, GapCause, Origin, Quote, Ticker, Venue};

use crate::ingest::ingest;
use crate::normalise::{Normalise, NormaliseError};
use crate::record::{Archive, Payload, PayloadAddress, RecordError, venue_partition_of};
use crate::sink::testing::{RecordingSink, RefusingSink};

// ---- fixtures -------------------------------------------------------------

fn venue() -> Venue {
    Venue::new("hyperliquid").expect("a legal venue")
}

fn payload(origin: Origin, recv_micros: i64, bytes: &[u8]) -> Payload {
    Payload {
        seq: 0,
        recv_micros,
        address: PayloadAddress::Venue("hyperliquid".into()),
        channel: "bbo".into(),
        kind: "quotes".into(),
        symbol: Some("BTC".into()),
        origin,
        payload: bytes.to_vec(),
    }
}

/// A normaliser that records when it ran, so the ORDER can be asserted rather
/// than assumed.
#[derive(Default)]
struct Witness {
    ran: Mutex<Vec<Vec<u8>>>,
}

impl Normalise for Witness {
    fn normalise(&self, p: &Payload) -> Result<Vec<Envelope>, NormaliseError> {
        self.ran.lock().unwrap().push(p.payload.clone());
        Ok(vec![Envelope::new(
            venue(),
            Ticker::new("BTC").unwrap(),
            Some(p.recv_micros - 1_000),
            p.recv_micros,
            Event::Quote(Quote::default()),
        )])
    }
    fn venue(&self) -> &Venue {
        VENUE.get_or_init(venue)
    }
}

static VENUE: std::sync::OnceLock<Venue> = std::sync::OnceLock::new();

/// A normaliser that panics, which an adapter meeting an unfamiliar shape may.
struct Panicking;

impl Normalise for Panicking {
    fn normalise(&self, _p: &Payload) -> Result<Vec<Envelope>, NormaliseError> {
        panic!("a shape this adapter was not written for");
    }
    fn venue(&self) -> &Venue {
        VENUE.get_or_init(venue)
    }
}

/// A normaliser that fails cleanly.
struct Failing;

impl Normalise for Failing {
    fn normalise(&self, _p: &Payload) -> Result<Vec<Envelope>, NormaliseError> {
        Err(NormaliseError::UnknownChannel("bbo".into()))
    }
    fn venue(&self) -> &Venue {
        VENUE.get_or_init(venue)
    }
}

/// Run `f` with the panic hook silenced.
///
/// The default hook prints to stderr **before** `catch_unwind` catches, so a
/// test that deliberately panics would otherwise spew a backtrace into a green
/// run. Suppressed here and **not** in the library: in production a panicking
/// adapter should be loud, because it is a defect.
fn quietly<T>(f: impl FnOnce() -> T) -> T {
    let previous = std::panic::take_hook();
    std::panic::set_hook(Box::new(|_| {}));
    let out = f();
    std::panic::set_hook(previous);
    out
}

fn rows_under(dir: &std::path::Path) -> usize {
    list_segments(dir)
        .iter()
        .map(|(_, path)| {
            read_segment(path)
                .unwrap()
                .iter()
                .map(|b| b.num_rows())
                .sum::<usize>()
        })
        .sum()
}

// ---- durability follows origin --------------------------------------------

#[test]
fn a_fetched_payload_is_durable_when_append_returns() {
    // It covers a range nothing will fetch again, so the walk must not advance
    // past it until it is on disk.
    let root = tempfile::tempdir().unwrap();
    let mut archive = Archive::open(root.path()).scoped_to("hyperliquid");
    let sink = RecordingSink::default();

    ingest(
        &mut archive,
        &Witness::default(),
        &sink,
        payload(Origin::Fetched, 1_758_326_400_000_000, b"{}"),
    )
    .unwrap();

    let dir = root.path().join(venue_partition_of(
        "hyperliquid",
        "quotes",
        1_758_326_400_000_000,
    ));
    assert_eq!(rows_under(&dir), 1, "durable before the call returned");
    assert_eq!(archive.buffered(), 0);
}

#[test]
fn a_streamed_payload_waits_for_the_flush() {
    let root = tempfile::tempdir().unwrap();
    let mut archive = Archive::open(root.path()).scoped_to("hyperliquid");
    let sink = RecordingSink::default();
    let at = 1_758_326_400_000_000;

    ingest(
        &mut archive,
        &Witness::default(),
        &sink,
        payload(Origin::Streamed, at, b"{}"),
    )
    .unwrap();

    let dir = root
        .path()
        .join(venue_partition_of("hyperliquid", "quotes", at));
    assert_eq!(
        archive.buffered(),
        1,
        "the standing risk a crash would cost"
    );
    assert_eq!(rows_under(&dir), 0, "nothing written yet");

    archive.flush().unwrap();
    assert_eq!(archive.buffered(), 0);
    assert_eq!(rows_under(&dir), 1);
}

#[test]
fn a_replayed_payload_is_refused_by_name() {
    // Replay reads the record back out; writing it would grow the thing it is
    // reading.
    let root = tempfile::tempdir().unwrap();
    let mut archive = Archive::open(root.path());
    let err = archive
        .append(payload(Origin::Replay, 0, b"{}"))
        .unwrap_err();
    match err {
        RecordError::ReplayIsNotWritten { address, channel } => {
            assert_eq!(address, "venue=hyperliquid");
            assert_eq!(channel, "bbo");
        }
        other => panic!("expected a named refusal, got {other:?}"),
    }
}

#[test]
fn a_replayed_payload_is_not_written_back() {
    // Through the one path: normalised and emitted, archived not at all, and
    // keeping the sequence it came out with.
    let root = tempfile::tempdir().unwrap();
    let mut archive = Archive::open(root.path());
    let sink = RecordingSink::default();
    let mut p = payload(Origin::Replay, 1_758_326_400_000_000, b"{}");
    p.seq = 99;

    let result = ingest(&mut archive, &Witness::default(), &sink, p).unwrap();

    assert_eq!(
        result.seq, 99,
        "the sequence it came out of the record with"
    );
    assert_eq!(result.emitted, 1);
    assert!(list_segments(root.path()).is_empty());
    assert_eq!(sink.emitted()[0].seq, 99);
}

// ---- the one path ---------------------------------------------------------

#[test]
fn the_path_archives_then_normalises_then_publishes() {
    let root = tempfile::tempdir().unwrap();
    let mut archive = Archive::open(root.path()).scoped_to("hyperliquid");
    let sink = RecordingSink::default();
    let witness = Witness::default();
    let at = 1_758_326_400_000_000;

    // A FETCHED payload, so the record is on disk rather than buffered — which
    // is what lets the ordering be observed rather than argued.
    ingest(
        &mut archive,
        &witness,
        &sink,
        payload(Origin::Fetched, at, b"{\"px\":1}"),
    )
    .unwrap();

    let dir = root
        .path()
        .join(venue_partition_of("hyperliquid", "quotes", at));
    assert_eq!(rows_under(&dir), 1, "archived");
    assert_eq!(witness.ran.lock().unwrap().len(), 1, "normalised");
    assert_eq!(sink.emitted().len(), 1, "emitted");
    assert_eq!(sink.emitted()[0].seq, 0, "stamped with the record sequence");
}

#[test]
fn a_panicking_normaliser_costs_a_parse_and_never_the_bytes() {
    let root = tempfile::tempdir().unwrap();
    let mut archive = Archive::open(root.path()).scoped_to("hyperliquid");
    let sink = RecordingSink::default();
    let at = 1_758_326_400_000_000;
    let bytes = b"{\"shape\":\"unfamiliar\"}";

    let result = quietly(|| {
        ingest(
            &mut archive,
            &Panicking,
            &sink,
            payload(Origin::Fetched, at, bytes),
        )
    })
    .expect("a panic must not propagate out of the one path");

    assert!(result.unparsed);

    // The bytes are in the record, unchanged.
    let dir = root
        .path()
        .join(venue_partition_of("hyperliquid", "quotes", at));
    let (_, path) = list_segments(&dir).pop().unwrap();
    let batches = read_segment(&path).unwrap();
    let stored = batches[0]
        .column_by_name("payload")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow::array::BinaryArray>()
        .unwrap()
        .value(0);
    assert_eq!(stored, bytes, "the bytes survived the panic");
}

#[test]
fn a_panic_is_reported_as_a_failure_that_names_it() {
    let root = tempfile::tempdir().unwrap();
    let mut archive = Archive::open(root.path()).scoped_to("hyperliquid");
    let sink = RecordingSink::default();
    let at = 1_758_326_400_000_000;

    quietly(|| {
        ingest(
            &mut archive,
            &Panicking,
            &sink,
            payload(Origin::Streamed, at, b"{}"),
        )
    })
    .unwrap();
    archive.flush().unwrap();

    let failures = root
        .path()
        .join(venue_partition_of("hyperliquid", "quotes", at))
        .join("failures");
    let (_, path) = list_segments(&failures).pop().expect("a failure row");
    let batches = read_segment(&path).unwrap();
    let error = batches[0]
        .column_by_name("error")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .unwrap()
        .value(0);
    assert!(error.contains("panicked"), "{error}");
    assert!(
        error.contains("a shape this adapter was not written for"),
        "{error}"
    );
}

#[test]
fn a_failure_and_its_payload_join_on_seq() {
    let root = tempfile::tempdir().unwrap();
    let mut archive = Archive::open(root.path()).scoped_to("hyperliquid");
    let sink = RecordingSink::default();
    let at = 1_758_326_400_000_000;

    // Two payloads, so the second's sequence is not trivially zero.
    ingest(
        &mut archive,
        &Witness::default(),
        &sink,
        payload(Origin::Streamed, at, b"{}"),
    )
    .unwrap();
    let result = ingest(
        &mut archive,
        &Failing,
        &sink,
        payload(Origin::Streamed, at, b"bad"),
    )
    .unwrap();
    archive.flush().unwrap();

    assert_eq!(result.seq, 1);

    let failures = root
        .path()
        .join(venue_partition_of("hyperliquid", "quotes", at))
        .join("failures");
    let (_, path) = list_segments(&failures).pop().unwrap();
    let batches = read_segment(&path).unwrap();
    let seq = batches[0]
        .column_by_name("seq")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow::array::UInt64Array>()
        .unwrap()
        .value(0);
    assert_eq!(
        seq, 1,
        "the failure names the payload by number, not by order"
    );
}

#[test]
fn the_payload_survives_its_own_failure_row() {
    // Filtering a record by parse success would discard exactly the evidence a
    // normalisation defect is diagnosed from.
    let root = tempfile::tempdir().unwrap();
    let mut archive = Archive::open(root.path()).scoped_to("hyperliquid");
    let sink = RecordingSink::default();
    let at = 1_758_326_400_000_000;

    ingest(
        &mut archive,
        &Failing,
        &sink,
        payload(Origin::Streamed, at, b"bad"),
    )
    .unwrap();
    archive.flush().unwrap();

    let main = root
        .path()
        .join(venue_partition_of("hyperliquid", "quotes", at));
    assert_eq!(
        rows_under(&main),
        1,
        "the bytes are still in the main segment"
    );
}

#[test]
fn an_unparsed_payload_is_never_silent() {
    let root = tempfile::tempdir().unwrap();
    let mut archive = Archive::open(root.path()).scoped_to("hyperliquid");
    let sink = RecordingSink::default();

    let result = ingest(
        &mut archive,
        &Failing,
        &sink,
        payload(Origin::Streamed, 1_758_326_400_000_000, b"bad"),
    )
    .unwrap();

    assert!(result.unparsed);
    assert_eq!(result.emitted, 1, "an anomaly is an event like any other");
    match &sink.emitted()[0].event {
        Event::Unparsed(u) => {
            assert_eq!(u.channel, "bbo");
            assert_eq!(
                u.archive_seq, result.seq,
                "it names the row holding the bytes"
            );
        }
        other => panic!("expected an anomaly, got {other:?}"),
    }
}

#[test]
fn the_record_does_not_depend_on_the_sink() {
    let root = tempfile::tempdir().unwrap();
    let mut archive = Archive::open(root.path()).scoped_to("hyperliquid");
    let at = 1_758_326_400_000_000;

    let result = ingest(
        &mut archive,
        &Witness::default(),
        &RefusingSink,
        payload(Origin::Fetched, at, b"{}"),
    )
    .expect("a refusing sink must not fail ingestion");

    assert!(result.emit_failed, "and it is reported");
    assert_eq!(result.emitted, 0);
    let dir = root
        .path()
        .join(venue_partition_of("hyperliquid", "quotes", at));
    assert_eq!(rows_under(&dir), 1, "the bytes are recorded regardless");
}

// ---- restart --------------------------------------------------------------

const DAY: i64 = 86_400_000_000;

fn wrote_at(root: &std::path::Path, venue: &str, at: i64) {
    let mut archive = Archive::open(root).scoped_to(venue);
    let sink = RecordingSink::default();
    let mut p = payload(Origin::Fetched, at, b"{}");
    p.address = PayloadAddress::Venue(venue.into());
    ingest(&mut archive, &Witness::default(), &sink, p).unwrap();
}

#[test]
fn a_first_ever_start_reports_no_window() {
    // A gap back to the beginning of time is not a fact.
    let root = tempfile::tempdir().unwrap();
    let archive = Archive::open(root.path()).scoped_to("hyperliquid");
    assert_eq!(archive.restart_window(DAY), None);
}

#[test]
fn a_clean_stop_reports_downtime() {
    let root = tempfile::tempdir().unwrap();
    wrote_at(root.path(), "hyperliquid", DAY);

    let archive = Archive::open(root.path()).scoped_to("hyperliquid");
    archive.mark_clean_shutdown(DAY);

    let reopened = Archive::open(root.path()).scoped_to("hyperliquid");
    let (_, _, cause) = reopened.restart_window(DAY * 2).expect("a window");
    assert_eq!(cause, GapCause::Downtime);
}

#[test]
fn a_kill_reports_crash_unflushed() {
    // No marker: what was received and not yet durable is OUR loss, and the
    // discipline that makes a gap trustworthy applies to our loss too.
    let root = tempfile::tempdir().unwrap();
    wrote_at(root.path(), "hyperliquid", DAY);

    let reopened = Archive::open(root.path()).scoped_to("hyperliquid");
    let (_, _, cause) = reopened.restart_window(DAY * 2).expect("a window");
    assert_eq!(cause, GapCause::CrashUnflushed);
}

#[test]
fn the_window_begins_at_the_last_durable_receipt() {
    // Not at the moment the loss was noticed, which would understate it by
    // exactly the buffer that was outstanding.
    let root = tempfile::tempdir().unwrap();
    wrote_at(root.path(), "hyperliquid", DAY);

    let archive = Archive::open(root.path()).scoped_to("hyperliquid");
    let (from, to, _) = archive.restart_window(DAY * 2).expect("a window");
    assert_eq!(from, DAY, "the last moment actually covered");
    assert_eq!(to, DAY * 2);
}

#[test]
fn two_venues_do_not_read_each_others_shutdown() {
    // One process per venue, one root between them. A handle scoped to one
    // must not report a colleague's clean stop as its own.
    let root = tempfile::tempdir().unwrap();
    wrote_at(root.path(), "hyperliquid", DAY);
    wrote_at(root.path(), "rh-crypto", DAY);

    Archive::open(root.path())
        .scoped_to("hyperliquid")
        .mark_clean_shutdown(DAY);

    let clean = Archive::open(root.path()).scoped_to("hyperliquid");
    let killed = Archive::open(root.path()).scoped_to("rh-crypto");

    assert_eq!(clean.restart_window(DAY * 2).unwrap().2, GapCause::Downtime);
    assert_eq!(
        killed.restart_window(DAY * 2).unwrap().2,
        GapCause::CrashUnflushed,
        "the unmarked venue must report its own loss"
    );
}

#[test]
fn clearing_the_marker_makes_the_next_kill_report_as_one() {
    let root = tempfile::tempdir().unwrap();
    wrote_at(root.path(), "hyperliquid", DAY);
    let archive = Archive::open(root.path()).scoped_to("hyperliquid");
    archive.mark_clean_shutdown(DAY);
    assert_eq!(
        archive.restart_window(DAY * 2).unwrap().2,
        GapCause::Downtime
    );

    archive.clear_clean_shutdown();
    assert_eq!(
        archive.restart_window(DAY * 2).unwrap().2,
        GapCause::CrashUnflushed
    );
}

// ---- layout ---------------------------------------------------------------

#[test]
fn a_venue_subtree_holds_every_kind_it_sent() {
    let root = tempfile::tempdir().unwrap();
    let mut archive = Archive::open(root.path()).scoped_to("hyperliquid");
    let sink = RecordingSink::default();
    let at = 1_758_326_400_000_000;

    for kind in ["quotes", "trades", "candles"] {
        let mut p = payload(Origin::Fetched, at, b"{}");
        p.kind = kind.into();
        ingest(&mut archive, &Witness::default(), &sink, p).unwrap();
    }

    let venue_dir = root.path().join("venue=hyperliquid");
    let kinds: Vec<String> = std::fs::read_dir(&venue_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().to_string())
        .filter(|n| n.starts_with("kind="))
        .collect();
    assert_eq!(kinds.len(), 3, "one venue's bytes are a single subtree");
}
