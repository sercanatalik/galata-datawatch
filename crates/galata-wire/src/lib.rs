//! The shared vocabulary of [galata-datawatch]: how an instrument is named,
//! what a dataset is, and what a normalised market-data event carries.
//!
//! Everything above the venue seam speaks this and nothing else. A
//! venue-specific field that reaches here has escaped the seam, which is the
//! one thing the adapter boundary exists to prevent.
//!
//! # The dependency wall
//!
//! This crate links `serde`, `serde_json`, `rust_decimal` and `thiserror`, and
//! **nothing else** — no columnar format, no runtime, no broker, no HTTP. Every
//! future consumer of the vocabulary links it, and a process that only needs to
//! name an event must not inherit `arrow` to do it. The build enforces this
//! rather than intending it.
//!
//! # What is here
//!
//! ```text
//!   identity   Venue · Ticker · Market · Account   validated once, at construction
//!   dataset    Series · Kind · Addressing   what may be declared, what is written
//!   event      Envelope · Event · Gap       two clocks, and a stream position
//!   token      Token · Num                  numbers as the venue spelled them
//! ```
//!
//! [galata-datawatch]: https://github.com/sercanatalik/galata-datawatch

#![cfg_attr(docsrs, feature(doc_cfg))]

pub mod dataset;
pub mod event;
pub mod identity;
pub mod token;

pub use dataset::{Addressing, Kind, Series};
pub use event::{
    AccountMode, AccountSeen, Address, Book, BookLevel, Candle, Clipped, Envelope, Event, Funding,
    Gap, GapCause, Instrument, Margin, Mark, Mint, Origin, Position, Quote, Reorg, Session,
    SessionKind, Side, Trade, Transfer, Unparsed,
};
pub use identity::{Account, MAX_TOKEN, Market, Ticker, TokenError, Venue};
pub use token::{Num, NumError, Token, require};

/// The version every stored row carries.
///
/// **Additive only.** A schema gains columns; it does not repurpose one. Rows
/// written under an earlier version stay readable by every later release, which
/// is what lets a column set that does not exist yet re-project months of
/// history without recomputing anything.
pub const SCHEMA_VERSION: u16 = 1;

/// Venue milliseconds to the microseconds everything downstream carries.
///
/// Saturating, because a venue that sends a nonsense magnitude should produce a
/// clamped timestamp rather than a panic in the one path every payload takes.
pub fn millis_to_micros(ms: i64) -> i64 {
    ms.saturating_mul(1_000)
}

/// Venue seconds to microseconds. Saturating, for the same reason.
pub fn secs_to_micros(s: i64) -> i64 {
    s.saturating_mul(1_000_000)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_nonsense_magnitude_clamps_rather_than_panicking() {
        // This runs inside the one path every payload takes. A panic here
        // would cost the process; a clamp costs one wrong timestamp on a
        // payload whose bytes are already durable.
        assert_eq!(millis_to_micros(i64::MAX), i64::MAX);
        assert_eq!(secs_to_micros(i64::MIN), i64::MIN);
        assert_eq!(millis_to_micros(1_758_326_400_000), 1_758_326_400_000_000);
    }
}
