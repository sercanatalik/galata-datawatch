//! Bytes to events: the seam, and why it is pure.

use galata_wire::Envelope;

use crate::record::Payload;

/// Why a payload could not be turned into events.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum NormaliseError {
    /// The bytes are not what they claimed to be.
    #[error("payload is not valid json: {0}")]
    Json(String),
    /// The adapter does not know this channel.
    #[error("unrecognised channel {0:?}")]
    UnknownChannel(String),
    /// A field was missing or of the wrong shape.
    #[error("{kind}: {detail}")]
    Shape {
        /// Which message shape was being read.
        kind: &'static str,
        /// What was wrong with it.
        detail: String,
    },
    /// A number would not parse.
    #[error("{0}")]
    Num(#[from] galata_wire::NumError),
    /// A name would not survive validation.
    #[error("{0}")]
    Token(#[from] galata_wire::TokenError),
}

/// One raw payload to the events it carries.
///
/// **Pure and stateless.** No transport, no clock: every timestamp it emits
/// came from the venue or from the payload's receipt time. That is what lets
/// the live path, a historical walk and replay share one implementation — and
/// what makes a replayed frame and a live frame provably identical rather than
/// conventionally so.
///
/// A stateful normaliser would make replay's output depend on the order the
/// record was read in, which is precisely the property replay exists to
/// provide.
///
/// It is also allowed to **fail, and to panic**. The payload is durable before
/// this is called, so either costs a parse and never the bytes.
pub trait Normalise: Send + Sync {
    /// Read the payload's bytes into the events they carry.
    fn normalise(&self, payload: &Payload) -> Result<Vec<Envelope>, NormaliseError>;

    /// The venue this normaliser speaks for, for addressing an anomaly that
    /// could not be parsed well enough to address itself.
    fn venue(&self) -> &galata_wire::Venue;
}
