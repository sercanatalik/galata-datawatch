//! Where normalised events go, and why that is not the record's problem.
//!
//! # `emit` is synchronous, and must not block
//!
//! The predecessor's publish is `async` and is awaited **inside** the one path.
//! The bytes are already durable by then, so no payload is lost to a slow
//! broker — but the loop cannot take the next frame until the publish returns,
//! so a slow broker does slow capture. *The record does not depend on the
//! broker* then holds only in the weak sense: nothing is lost, and everything
//! is late.
//!
//! Here it holds in the strong sense. `emit` takes `&self`, returns
//! immediately, and an implementation that talks to a network **hands off to a
//! queue it owns** rather than waiting. A broker that is down, slow, or
//! unreachable costs a reported failure and no wall-clock time on the path
//! every payload crosses.
//!
//! This also keeps a runtime out of this crate entirely: there is no future to
//! await, so there is nothing to await it with.

use galata_wire::Envelope;

/// Why an event could not be handed off.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum SinkError {
    /// The sink's consumer is gone, or its queue is full.
    ///
    /// Reported, never fatal. A full queue is a fact about the consumer, and
    /// the right response is to say so on the status surface — not to stop
    /// recording.
    #[error("the sink would not take it: {0}")]
    Refused(String),
}

/// Somewhere normalised events go.
///
/// **Implementations must not block.** See the module documentation: the one
/// path crosses this for every event of every payload, and a sink that waits
/// on a network turns a broker outage into a capture slowdown.
pub trait Sink: Send + Sync {
    /// How many events this sink has dropped because it could not keep up.
    ///
    /// **Zero for a sink that cannot drop**, which is most of them. It is on
    /// the trait rather than on the one implementation that can, because the
    /// status surface reports it and the loop holds a `dyn Sink` — and a
    /// drop that is counted but never published is a drop nobody sees, which
    /// is the failure the counter exists to prevent.
    fn dropped(&self) -> u64 {
        0
    }

    /// A status snapshot, on its own root.
    ///
    /// **Defaulted to doing nothing**, so a sink that has no use for one is not
    /// forced to pretend — and so adding this did not break any sink already
    /// written, including one written outside this crate.
    fn emit_status(&self, _venue: &galata_wire::Venue, _json: &[u8]) -> Result<(), SinkError> {
        Ok(())
    }

    /// Hand off one event. Returns immediately.
    fn emit(&self, envelope: &Envelope) -> Result<(), SinkError>;
}

/// A sink that keeps what it is given.
///
/// **Not a test double.** The rebuild uses it in earnest: the one path emits
/// into a sink, so the only way to take the envelopes it produced — rather than
/// deriving them a second time — is to be the sink it emits into.
///
/// `Sink::emit` takes `&self`, because a sink must not need a mutable borrow on
/// the hot path; keeping anything therefore needs interior mutability, and a
/// `Mutex` is what makes this usable behind the `Arc<dyn Sink>` the loop holds.
#[derive(Debug, Default)]
pub struct CollectingSink {
    taken: std::sync::Mutex<Vec<Envelope>>,
}

impl CollectingSink {
    /// Everything emitted so far, leaving the sink empty.
    ///
    /// Drained rather than cloned: a rebuild takes a batch, writes it, and must
    /// not write it again on the next batch.
    pub fn drain(&self) -> Vec<Envelope> {
        std::mem::take(&mut self.taken.lock().expect("not poisoned"))
    }

    /// How many are held.
    pub fn len(&self) -> usize {
        self.taken.lock().expect("not poisoned").len()
    }

    /// Whether any are.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl Sink for CollectingSink {
    fn emit(&self, envelope: &Envelope) -> Result<(), SinkError> {
        self.taken
            .lock()
            .expect("not poisoned")
            .push(envelope.clone());
        Ok(())
    }
}

/// The bridge from a **synchronous** sink to an **asynchronous** broker.
///
/// What goes down the one channel to the broker.
///
/// **One channel rather than two**, because backpressure is one decision. Two
/// would mean two capacities, two drop counters and two answers to *what
/// happens when the broker is behind* — and the interesting case is exactly
/// when both are backed up at once.
///
/// # Why the large variant is not boxed
///
/// Clippy notices that `Event` is much bigger than `Status` and suggests a
/// `Box`. **The large variant is the common one**: events arrive continuously
/// and a snapshot once a second, so boxing would add an allocation to the
/// hottest path in the system in order to save memory on the rare one. At the
/// shipped queue depth the whole channel is a couple of megabytes, which is not
/// a number worth trading a per-event allocation for.
#[allow(clippy::large_enum_variant)]
#[cfg(feature = "capture")]
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum Outbound {
    /// A normalised event.
    Event(Envelope),
    /// A status snapshot, and whose it is.
    Status {
        /// Which venue's process.
        venue: galata_wire::Venue,
        /// The snapshot, as JSON.
        json: Vec<u8>,
    },
}

/// **Behind the `capture` feature**, because the channel it hands over is
/// tokio's. A consumer that only reads the tape has nothing to publish.
///
/// ```text
///   ingest ──▶ NatsSink::emit ──try_send──▶ [bounded] ──▶ task ──▶ NATS
///              synchronous,                  capacity N   async
///              never blocks
/// ```
///
/// **`try_send`, never `send`.** A full channel means the broker is slower than
/// capture, and the answer to that is to drop and count — not to block the
/// thread that is archiving. The record is the thing that must not stall; the
/// stream is a cache of it. This is the whole reason [`Sink::emit`] is
/// synchronous, and the predecessor's is not: there, a slow broker slows
/// capture.
///
/// **Drops are counted, never silent.** A publish that quietly did nothing is
/// indistinguishable from one that worked.
///
/// # Why the large variant is not boxed
///
/// Clippy notices that [`Outbound::Event`] is much bigger than
/// [`Outbound::Status`] and suggests a `Box`. **The large variant is the common
/// one**: events arrive continuously and a snapshot once a second, so boxing
/// would add an allocation to the hottest path in the system in order to save
/// memory on the rare one. At the shipped queue depth the whole channel is a
/// couple of megabytes, which is not a number worth trading a per-event
/// allocation for.
#[cfg(feature = "capture")]
#[derive(Debug)]
pub struct NatsSink {
    tx: tokio::sync::mpsc::Sender<Outbound>,
    dropped: std::sync::atomic::AtomicU64,
}

#[cfg(feature = "capture")]
impl NatsSink {
    /// Wrap a sender, and say how many events may be outstanding.
    ///
    /// The capacity is **exactly the number of events a broker stall can
    /// swallow before they start being dropped**, so it is declared by an
    /// operator rather than defaulted by us.
    pub fn new(tx: tokio::sync::mpsc::Sender<Outbound>) -> NatsSink {
        NatsSink {
            tx,
            dropped: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// How many events have been dropped because the broker was behind.
    pub fn dropped(&self) -> u64 {
        self.dropped.load(std::sync::atomic::Ordering::Relaxed)
    }
}

#[cfg(feature = "capture")]
impl NatsSink {
    /// Hand anything over, or say why not.
    fn offer(&self, outbound: Outbound) -> Result<(), SinkError> {
        match self.tx.try_send(outbound) {
            Ok(()) => Ok(()),
            Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                let n = self
                    .dropped
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
                    + 1;
                Err(SinkError::Refused(format!(
                    "the broker is behind capture; {n} events dropped so far"
                )))
            }
            Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => Err(SinkError::Refused(
                "the broker task has stopped; events are recorded but not published".into(),
            )),
        }
    }
}

#[cfg(feature = "capture")]
impl Sink for NatsSink {
    fn dropped(&self) -> u64 {
        NatsSink::dropped(self)
    }

    fn emit(&self, envelope: &Envelope) -> Result<(), SinkError> {
        self.offer(Outbound::Event(envelope.clone()))
    }

    /// **The same queue and the same drop rule as an event.** A dropped
    /// snapshot is the least costly thing in it, because the file on disk still
    /// has one.
    fn emit_status(&self, venue: &galata_wire::Venue, json: &[u8]) -> Result<(), SinkError> {
        self.offer(Outbound::Status {
            venue: venue.clone(),
            json: json.to_vec(),
        })
    }
}

/// A sink that takes everything and does nothing.
///
/// What a process runs on when no broker is configured, or when one is
/// configured and absent. The record does not depend on the broker, so running
/// without one is a supported state rather than a degraded one.
#[derive(Debug, Default, Clone, Copy)]
pub struct NullSink;

impl Sink for NullSink {
    fn emit(&self, _envelope: &Envelope) -> Result<(), SinkError> {
        Ok(())
    }
}

/// Sinks for testing an adapter — **including one written out of tree**.
///
/// Behind the `testing` feature rather than `#[cfg(test)]`, because
/// `#[cfg(test)]` is invisible outside this crate: an adapter author writing
/// their own venue had **no test sink at all** and would have had to write one
/// to exercise the one path.
///
/// Found by `tests/out_of_tree_venue.rs`, which is compiled as its own crate
/// and therefore sees exactly what a stranger sees. That is what the test is
/// for, and it found this on its first run.
#[cfg(any(test, feature = "testing"))]
pub mod testing {
    use super::*;
    use std::sync::Mutex;

    /// A sink that remembers what it was given.
    ///
    /// What an adapter is tested against: hand it to
    /// [`ingest`](crate::ingest::ingest) and assert on what came out, rather
    /// than on what the adapter returned — which checks the *path* as well as
    /// the normaliser.
    #[derive(Debug, Default)]
    pub struct RecordingSink {
        emitted: Mutex<Vec<Envelope>>,
    }

    impl RecordingSink {
        /// Everything emitted so far, in order.
        ///
        /// Cloned rather than drained, so a test may assert about it more than
        /// once without the second assertion seeing an empty list.
        pub fn emitted(&self) -> Vec<Envelope> {
            self.emitted.lock().expect("not poisoned").clone()
        }
    }

    impl Sink for RecordingSink {
        fn emit(&self, envelope: &Envelope) -> Result<(), SinkError> {
            self.emitted
                .lock()
                .expect("not poisoned")
                .push(envelope.clone());
            Ok(())
        }
    }

    /// A sink that refuses everything.
    ///
    /// For asserting the property the whole design rests on: **the record does
    /// not depend on the broker.** Ingest through this and the payload is still
    /// archived, which is what makes running with no broker a supported state
    /// rather than a degraded one.
    #[derive(Debug, Default, Clone, Copy)]
    pub struct RefusingSink;

    impl Sink for RefusingSink {
        fn emit(&self, _envelope: &Envelope) -> Result<(), SinkError> {
            Err(SinkError::Refused("this sink refuses everything".into()))
        }
    }
}

#[cfg(all(test, feature = "capture"))]
mod sink_tests {
    use super::*;

    /// A sink whose channel is full from the first message.
    fn full_sink() -> NatsSink {
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        // One slot, filled, and the receiver held so the channel stays open.
        tx.try_send(Outbound::Event(envelope())).unwrap();
        std::mem::forget(rx);
        NatsSink::new(tx)
    }

    fn envelope() -> Envelope {
        use galata_wire::{Clipped, Event, Gap, GapCause, Series, Ticker, Venue};
        Envelope::new(
            Venue::new("hyperliquid").unwrap(),
            Ticker::new("BTC").unwrap(),
            Some(1),
            2,
            Event::Gap(Gap {
                series: Series::Quotes,
                from_micros: 0,
                to_micros: 1,
                cause: GapCause::SessionLost,
                clipped: Clipped::Continuous,
                dex: None,
            }),
        )
    }

    #[test]
    fn a_full_channel_drops_and_does_not_block() {
        // The record is the thing that must not stall. This test returning at
        // all is the assertion — `send` would deadlock here, and `try_send`
        // does not.
        let sink = full_sink();
        assert!(sink.emit(&envelope()).is_err());
        assert_eq!(sink.dropped(), 1);
    }

    #[test]
    fn a_drop_is_counted_and_named() {
        // A publish that silently did nothing is indistinguishable from one
        // that worked.
        let sink = full_sink();
        for _ in 0..3 {
            let _ = sink.emit(&envelope());
        }
        assert_eq!(sink.dropped(), 3);
        let error = sink.emit(&envelope()).unwrap_err().to_string();
        assert!(error.contains("4 events dropped"), "{error}");
        assert!(error.contains("behind capture"), "{error}");
    }

    #[test]
    fn a_stopped_broker_task_is_told_apart_from_a_full_one() {
        // Different facts: one is the broker being slow, the other is the
        // publishing half being gone entirely.
        let (tx, rx) = tokio::sync::mpsc::channel(4);
        drop(rx);
        let sink = NatsSink::new(tx);
        let error = sink.emit(&envelope()).unwrap_err().to_string();
        assert!(error.contains("stopped"), "{error}");
        assert!(error.contains("recorded but not published"), "{error}");
        assert_eq!(sink.dropped(), 0, "a closed channel is not a drop count");
    }

    #[test]
    fn an_event_reaches_the_channel_when_there_is_room() {
        let (tx, mut rx) = tokio::sync::mpsc::channel(4);
        let sink = NatsSink::new(tx);
        assert!(sink.emit(&envelope()).is_ok());
        assert_eq!(rx.try_recv().unwrap(), Outbound::Event(envelope()));
        assert_eq!(sink.dropped(), 0);
    }

    #[test]
    fn a_status_snapshot_shares_the_queue_and_its_drop_rule() {
        // One channel rather than two, because backpressure is one decision —
        // and the interesting case is when events and snapshots are backed up
        // at once.
        let venue = galata_wire::Venue::new("hyperliquid").unwrap();

        let (tx, mut rx) = tokio::sync::mpsc::channel(4);
        let sink = NatsSink::new(tx);
        assert!(sink.emit_status(&venue, b"{}").is_ok());
        assert_eq!(
            rx.try_recv().unwrap(),
            Outbound::Status {
                venue: venue.clone(),
                json: b"{}".to_vec()
            }
        );

        // And a full queue drops it exactly as it drops an event.
        let full = full_sink();
        assert!(full.emit_status(&venue, b"{}").is_err());
        assert_eq!(full.dropped(), 1);
    }

    #[test]
    fn a_sink_with_no_use_for_a_snapshot_is_not_forced_to_pretend() {
        // Defaulted to doing nothing, so adding this broke no sink already
        // written — including one written outside this crate.
        let venue = galata_wire::Venue::new("hyperliquid").unwrap();
        assert!(NullSink.emit_status(&venue, b"{}").is_ok());
    }
}
