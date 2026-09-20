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
    /// Hand off one event. Returns immediately.
    fn emit(&self, envelope: &Envelope) -> Result<(), SinkError>;
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

#[cfg(test)]
pub(crate) mod testing {
    use super::*;
    use std::sync::Mutex;

    /// A sink that remembers what it was given.
    #[derive(Debug, Default)]
    pub struct RecordingSink {
        emitted: Mutex<Vec<Envelope>>,
    }

    impl RecordingSink {
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

    /// A sink that refuses everything, for asserting that the record does not
    /// depend on it.
    #[derive(Debug, Default, Clone, Copy)]
    pub struct RefusingSink;

    impl Sink for RefusingSink {
        fn emit(&self, _envelope: &Envelope) -> Result<(), SinkError> {
            Err(SinkError::Refused("this sink refuses everything".into()))
        }
    }
}
