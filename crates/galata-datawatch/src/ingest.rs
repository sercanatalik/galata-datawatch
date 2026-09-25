//! **The one path.** Archive → normalise → emit, implemented once.
//!
//! ```text
//!   live frames   ─┐
//!   a walk        ─┤
//!   a metadata fetch ┼──▶  ingest  ──▶  archive → normalise → emit
//!   replay        ─┘   replay READS the record and must not write back
//! ```
//!
//! Archive-before-normalise holds **by construction** rather than by convention
//! at six call sites, because there is one function and everything calls it. An
//! invariant enforced in one place cannot be relaxed in another.
//!
//! Two things hold that. `Archive::append` is `pub(crate)`, so no other crate
//! can reach past this; and `scripts/check-ingest-callers.sh` asserts no other
//! *module* in this crate does either — which is the half the compiler cannot
//! see.
//!
//! The ordering is the whole point. A payload is durable *before* anything
//! tries to parse it, so an adapter meeting a shape it was not written for
//! costs a parse and never the bytes.

use galata_wire::{Envelope, Event, Origin, Ticker, Unparsed};

use crate::normalise::Normalise;
use crate::record::{Archive, Failure, Payload, RecordError};
use crate::sink::Sink;

/// What one payload produced.
#[derive(Debug, Default, PartialEq)]
#[non_exhaustive]
pub struct Ingested {
    /// The record sequence the payload was stored under. The road back to the
    /// bytes from any row derived from them.
    pub seq: u64,
    /// Events handed to the sink.
    pub emitted: usize,
    /// Whether normalisation failed. The payload is recorded either way.
    pub unparsed: bool,
    /// Whether an emit failed. The payload is recorded either way, and capture
    /// does not stop.
    pub emit_failed: bool,
    /// The ticker and **venue time** of each event this payload carried.
    ///
    /// Reported rather than re-derived: a caller wants the venue's clock for
    /// the latency it publishes, and normalising a second time to get it would
    /// double the parse cost of every frame.
    pub venue_times: Vec<(Ticker, i64)>,
}

/// The one ingestion function.
///
/// Three steps, in this order, once:
///
/// 1. **Archive** the bytes verbatim, before any parse is attempted.
/// 2. **Normalise** them, inside a panic boundary.
/// 3. **Emit** the events.
///
/// A payload read back out of the record takes [`ingest_replayed`] instead,
/// which is the same three steps without the first. The two are separate
/// **entry points and one implementation**, so the ordering still lives in one
/// place while the route cannot be got wrong — see [`crate::replay::Replayed`].
pub fn ingest(
    archive: &mut Archive,
    normaliser: &dyn Normalise,
    sink: &dyn Sink,
    payload: Payload,
) -> Result<Ingested, RecordError> {
    one_path(archive, normaliser, sink, payload, false)
}

/// The same path, for a payload that came **out of** the record.
///
/// Nothing is appended: replay reads the record back out, and writing it would
/// grow the thing it is reading. The normalise and emit steps are identical, so
/// the events a replay produces *are* the events live capture produced rather
/// than a second derivation.
///
/// It takes a [`Replayed`](crate::replay::Replayed), which only `replay`
/// constructs and which cannot be turned back into a [`Payload`]. The previous
/// form asked a caller to set an origin field correctly; this one cannot be got
/// wrong.
pub fn ingest_replayed(
    archive: &mut Archive,
    normaliser: &dyn Normalise,
    sink: &dyn Sink,
    replayed: crate::replay::Replayed,
) -> Result<Ingested, RecordError> {
    one_path(archive, normaliser, sink, replayed.into_payload(), true)
}

fn one_path(
    archive: &mut Archive,
    normaliser: &dyn Normalise,
    sink: &dyn Sink,
    mut payload: Payload,
    replaying: bool,
) -> Result<Ingested, RecordError> {
    let seq = if replaying {
        // The sequence a replayed payload already carries: it is the record
        // row it came out of, and every row derived from it names that.
        payload.seq
    } else {
        let seq = archive.next_seq();
        payload.seq = seq;
        seq
    };

    let mut result = Ingested {
        seq,
        ..Ingested::default()
    };

    // 1. The bytes are durable — or buffered against the flush timer — before
    //    anything looks at them.
    if !replaying {
        archive.append(payload.clone())?;
    }

    // 2. Normalisation is the adapter's, and it is allowed to fail. An adapter
    //    that panics here would take capture with it, so it is caught: the
    //    payload is already recorded, and a panic is a defect to report rather
    //    than a reason to stop recording.
    let normalised = match interpret(normaliser, &payload) {
        Ok(events) => events,
        Err(error) => {
            result.unparsed = true;
            if !replaying {
                archive.append_failure(Failure {
                    seq,
                    recv_micros: payload.recv_micros,
                    venue: payload.address.value().to_string(),
                    account: match &payload.address {
                        crate::record::PayloadAddress::Account(a) => Some(a.clone()),
                        _ => None,
                    },
                    channel: payload.channel.clone(),
                    kind: payload.kind.clone(),
                    error: error.clone(),
                });
            }
            // An anomaly is an event like any other, and a row here always has
            // bytes behind it. The one thing that must not happen is silence.
            emit_unparsed(normaliser, sink, &payload, seq, &error, &mut result);
            return Ok(result);
        }
    };

    // 3. Emitting is the only step that may fail without costing anything: the
    //    record does not depend on the sink.
    for envelope in normalised {
        // The stream position, stamped here because this is the only place
        // that knows it: an adapter normalising bytes does not know the
        // sequence they were recorded under.
        let envelope = envelope.stamped(seq);
        if let (Some(at), Some(ticker)) = (envelope.at_micros, envelope.ticker()) {
            result.venue_times.push((ticker.clone(), at));
        }
        match sink.emit(&envelope) {
            Ok(()) => result.emitted += 1,
            Err(_) => result.emit_failed = true,
        }
    }

    Ok(result)
}

/// Record an event this process **generated**, then emit it.
///
/// A gap has no bytes behind it: nothing arrived, which is the whole point of
/// it. So there is no payload to archive before a parse — but *durable before
/// emitted* still applies, and for a sharper reason than usual.
///
/// **A gap emitted straight to the sink exists only if the sink was up.** And a
/// gap is precisely what a consumer needs after an outage, which is exactly
/// when the sink is most likely to have been down. The predecessor publishes
/// its gaps and does not record them; an outage therefore erases the evidence
/// of itself.
///
/// So the envelope is rendered to JSON, stored as a payload of its own under
/// the dataset it belongs to, and emitted only once that has committed. This
/// lives in `ingest.rs` beside [`ingest`] because it is the same ordering rule,
/// and the rule has one home.
pub fn record_generated(
    archive: &mut Archive,
    sink: &dyn Sink,
    venue: &str,
    envelope: Envelope,
) -> Result<Ingested, RecordError> {
    record_generated_at(
        archive,
        sink,
        crate::record::PayloadAddress::Venue(venue.to_string()),
        envelope,
    )
}

/// The same rule, for an event whose place in the record is not a venue's own
/// subtree: the ledger's gaps and bindings, which belong under their account.
pub fn record_generated_at(
    archive: &mut Archive,
    sink: &dyn Sink,
    address: crate::record::PayloadAddress,
    envelope: Envelope,
) -> Result<Ingested, RecordError> {
    let seq = archive.next_seq();
    let kind = envelope.kind();
    let envelope = envelope.stamped(seq);
    let rendered = serde_json::to_vec(&GeneratedPayload {
        envelope: envelope.clone(),
    })
    .unwrap_or_default();

    archive.append(Payload {
        seq,
        recv_micros: envelope.recv_micros,
        address,
        channel: kind.as_str().to_string(),
        kind: kind.as_str().to_string(),
        symbol: envelope.ticker().map(|t| t.as_str().to_string()),
        // **Generated**, which is both why it is durable before it is emitted —
        // a crash leaves the record ahead of the stream and never behind it —
        // and how the one path knows to decode it rather than hand it to a
        // venue adapter that would rightly refuse it.
        origin: Origin::Generated,
        payload: rendered,
    })?;

    let mut result = Ingested {
        seq,
        ..Ingested::default()
    };
    match sink.emit(&envelope) {
        Ok(()) => result.emitted += 1,
        Err(_) => result.emit_failed = true,
    }
    Ok(result)
}

/// How a generated event is stored.
///
/// **The whole envelope, encoded so it decodes.** An earlier version stored
/// `format!("{:?}", event)` — a *readable rendering*, chosen deliberately, and
/// not round-trippable. Nine hours of capture and one connection reset showed
/// what that cost: twenty-four gaps recorded, and every one of them rebuilt as
/// `unparsed`, because no parser will ever recover a Debug string.
///
/// A gap that cannot be rebuilt is an absence again, which is the one thing
/// recording it durably was supposed to prevent. Readable was the wrong thing
/// to optimise for; this is still readable, and it is also a record.
#[derive(serde::Serialize, serde::Deserialize)]
struct GeneratedPayload {
    /// The event, whole.
    envelope: Envelope,
}

/// Turn a payload into events, by **how the bytes came to exist**.
///
/// ```text
///   Streamed │ Fetched   ──▶  the venue adapter normalises
///   Generated            ──▶  decode: the payload IS the event
/// ```
///
/// Chosen by a fact the record stores in a column — never by trying one and
/// falling back to the other. A fallback would make a genuinely malformed venue
/// frame look like a generated one on a bad day, which is the failure this
/// routing exists to avoid rather than to cause.
fn interpret(normaliser: &dyn Normalise, payload: &Payload) -> Result<Vec<Envelope>, String> {
    match payload.origin {
        Origin::Generated => decode(payload),
        _ => catch_normalise(normaliser, payload),
    }
}

/// The event a **generated** payload holds, for a reader that needs the event
/// without re-ingesting it — the ledger rebuilding its bindings from the
/// record at boot.
///
/// The same decoding the one path uses, exposed rather than copied: a second
/// decoder would be free to disagree about what the record says.
pub fn generated_envelope(bytes: &[u8]) -> Result<Envelope, String> {
    serde_json::from_slice::<GeneratedPayload>(bytes)
        .map(|stored| stored.envelope)
        .map_err(|e| format!("a generated payload would not decode: {e}"))
}

/// A generated payload, read back into the event it holds.
///
/// No panic boundary: this is **our own encoding**, and a failure here is a
/// defect in this crate rather than a venue sending an unexpected shape. It
/// still returns an error rather than panicking, so a record written by an
/// older build reports itself instead of stopping a rebuild.
fn decode(payload: &Payload) -> Result<Vec<Envelope>, String> {
    generated_envelope(&payload.payload).map(|envelope| vec![envelope])
}

/// Normalise, converting a panic into an error.
///
/// An adapter that panics on an unrecognised shape costs a parse, never the
/// bytes and never the process — the payload is already recorded by the time
/// this runs.
///
/// **This sits on the hottest path in the system**, and it was an open question
/// for five tiers on the strength of a Servo profile showing half a hot loop
/// inside `__rust_try` and `PANIC_COUNT`.
///
/// # Measured 2026-09-21: between −7 and +17 ns per call
///
/// Two million iterations of a virtual call returning a `Vec`, with and
/// without the boundary, put the cost **under 20 ns per call and within its own
/// noise**. Against a normalise that takes hundreds of nanoseconds at minimum
/// that is a fraction of a percent — which is why the end-to-end instrument,
/// `cargo run --release --example unwind-cost`, *cannot resolve it at all*:
/// its run-to-run variance is ±35%, orders of magnitude larger than the effect.
///
/// The Servo figure is superseded. `catch_unwind` was since refactored so LLVM
/// inlines the try closure into the happy path, making it zero-cost unless a
/// panic is actually thrown.
///
/// **So the mitigation this note used to hold in reserve — wrapping a *batch*
/// of frames rather than each one — is not taken.** It would mean buffering
/// payloads before normalising, which delays the anomaly a failed parse
/// publishes and makes one bad payload cost the batch's parses: real
/// complexity and a real behavioural change, bought for an effect no
/// instrument here can detect. Recorded as *measured unnecessary* rather than
/// deleted, so the next person to wonder finds the answer.
fn catch_normalise(normaliser: &dyn Normalise, payload: &Payload) -> Result<Vec<Envelope>, String> {
    let caught = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        normaliser.normalise(payload)
    }));
    match caught {
        Ok(Ok(events)) => Ok(events),
        Ok(Err(error)) => Err(error.to_string()),
        Err(panic) => Err(format!("normalise panicked: {}", panic_message(&panic))),
    }
}

fn panic_message(panic: &Box<dyn std::any::Any + Send>) -> String {
    if let Some(s) = panic.downcast_ref::<&str>() {
        (*s).to_string()
    } else if let Some(s) = panic.downcast_ref::<String>() {
        s.clone()
    } else {
        "a panic carrying no message".to_string()
    }
}

fn emit_unparsed(
    normaliser: &dyn Normalise,
    sink: &dyn Sink,
    payload: &Payload,
    seq: u64,
    error: &str,
    result: &mut Ingested,
) {
    // An unparsed payload has no ticker — resolving one is the parse that just
    // failed — so it is addressed by the venue's own symbol where the payload
    // carried one, and by the channel where it did not. A channel may hold
    // characters a ticker may not, so it is sanitised rather than refused.
    let addressable = payload
        .symbol
        .clone()
        .unwrap_or_else(|| sanitise(&payload.channel));

    let Ok(ticker) = Ticker::new(addressable) else {
        // Nothing addressable at all. The bytes are still recorded, and a
        // failure row still names them; only the anomaly EVENT is lost, which
        // is the one part of this that has a durable substitute.
        return;
    };

    let envelope = Envelope::new(
        normaliser.venue().clone(),
        ticker,
        None,
        payload.recv_micros,
        Event::Unparsed(Unparsed {
            channel: payload.channel.clone(),
            archive_seq: seq,
            error: error.to_string(),
        }),
    )
    .stamped(seq);

    match sink.emit(&envelope) {
        Ok(()) => result.emitted += 1,
        Err(_) => result.emit_failed = true,
    }
}

/// A channel name, made addressable.
///
/// Channels are the venue's own strings and may hold anything; a ticker may
/// hold `a-zA-Z0-9_-`. Replacing rather than refusing keeps the anomaly
/// addressable for the case it exists to serve.
fn sanitise(channel: &str) -> String {
    channel
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' || c == '-' {
                c
            } else {
                '-'
            }
        })
        .take(64)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_channel_becomes_addressable_rather_than_unaddressable() {
        assert_eq!(sanitise("l2Book"), "l2Book");
        assert_eq!(sanitise("spot:PURR/USDC"), "spot-PURR-USDC");
        assert_eq!(sanitise(&"x".repeat(100)).len(), 64);
    }
}
