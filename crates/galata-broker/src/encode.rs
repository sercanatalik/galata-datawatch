//! Envelope to bytes, and back.
//!
//! **JSON**, because [`Envelope`] serialises exactly — including its numbers,
//! which round-trip as strings rather than through `f64`.
//!
//! A binary encoding is a later optimisation with a measurement attached. There
//! is none yet, and a wire format chosen without one is a guess that becomes
//! hard to change the moment a subscriber exists.

use galata_wire::Envelope;

/// Why a message could not be read.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum DecodeError {
    /// The bytes are not an envelope.
    #[error("a message on the bus would not decode: {0}")]
    NotAnEnvelope(String),
}

/// Why an envelope could not be put on the wire.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum EncodeError {
    /// Serialisation refused it.
    #[error("an envelope would not encode: {0}")]
    NotEncodable(String),
}

/// What an encoded envelope usually takes: a quote or a trade is a few
/// hundred bytes. Starting there skips the doubling from empty that most
/// messages went through on the way to their size.
const TYPICAL_BYTES: usize = 512;

/// An envelope, as it goes on the wire.
///
/// **A failure is said, with serde's reason.** Infallible in practice — every
/// field is a type that serialises — but it used to become an empty body
/// published to the bus, whose only trace was a generic decode error at some
/// distant consumer.
pub fn encode(envelope: &Envelope) -> Result<Vec<u8>, EncodeError> {
    let mut out = Vec::with_capacity(TYPICAL_BYTES);
    serde_json::to_writer(&mut out, envelope)
        .map_err(|e| EncodeError::NotEncodable(e.to_string()))?;
    Ok(out)
}

/// An envelope, back off the wire.
///
/// **A message that will not decode is an error, never a skip.** A consumer
/// silently dropping what it could not read is the failure a named error exists
/// to prevent.
pub fn decode(bytes: &[u8]) -> Result<Envelope, DecodeError> {
    serde_json::from_slice(bytes).map_err(|e| DecodeError::NotAnEnvelope(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use galata_wire::{Event, Num, Quote, Ticker, Venue};
    use std::str::FromStr;

    fn envelope() -> Envelope {
        Envelope::new(
            Venue::new("hyperliquid").unwrap(),
            Ticker::new("BTC").unwrap(),
            Some(1_789_941_180_000_000),
            1_789_941_180_000_320,
            Event::Quote(Quote {
                bid_px: Some(Num::from_str("81213.000000000000000001").unwrap()),
                ask_px: Some(Num::from_str("81214.5").unwrap()),
                bid_sz: None,
                ask_sz: None,
                bid_spread: None,
                ask_spread: None,
            }),
        )
        .stamped(42)
    }

    #[test]
    fn an_envelope_survives_the_wire_exactly() {
        let original = envelope();
        assert_eq!(decode(&encode(&original).unwrap()).unwrap(), original);
    }

    #[test]
    fn a_number_crosses_as_a_string_and_loses_nothing() {
        // Eighteen decimal places. `f64` holds about fifteen significant
        // digits, so a float encoding would round this and nothing would say
        // so.
        let bytes = encode(&envelope()).unwrap();
        let text = String::from_utf8(bytes.clone()).unwrap();
        assert!(
            text.contains("\"81213.000000000000000001\""),
            "the number was not written as a string: {text}"
        );
        let back = decode(&bytes).unwrap();
        match back.event {
            Event::Quote(q) => assert_eq!(
                q.bid_px.unwrap(),
                Num::from_str("81213.000000000000000001").unwrap()
            ),
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn the_stream_position_crosses_too() {
        // It is the road back from any message to the bytes it came from.
        assert_eq!(decode(&encode(&envelope()).unwrap()).unwrap().seq, 42);
    }

    #[test]
    fn a_message_that_will_not_decode_is_an_error_not_a_skip() {
        assert!(decode(b"").is_err());
        assert!(decode(b"{\"not\":\"an envelope\"}").is_err());
    }
}
