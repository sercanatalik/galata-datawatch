//! The seam every venue crosses.
//!
//! Raw payloads go in; shared model types come out. A venue-specific field that
//! leaks above this boundary becomes load-bearing somewhere that cannot see the
//! venue — so the trait's outputs are [`galata_wire`] types and nothing else.
//!
//! Three differences break a single-venue design, and all three are known in
//! advance, so all three are in the trait rather than in a caller:
//!
//! ```text
//!   a venue may not push a series   → subscriptions are a DECLARED SET
//!   a venue may page differently    → a fetch takes a RANGE and returns WHAT
//!                                     IT GOT; the caller does the walking
//!   a venue may name an instrument  → symbols cross as the VENUE'S OWN
//!   differently per channel           STRINGS, resolved once, at the seam
//! ```
//!
//! # Why none of this is async
//!
//! `declaration` and `normalise` are **facts about a venue**, and a fact that
//! needs a runtime to state is a fact that cannot be asserted in a test.
//!
//! It is also what keeps transport out. The predecessor's seam abstracts
//! framing but not transport — its capture loop opens a websocket directly, so
//! `subscribe_frames` and a keepalive are baked in as assumptions. Two of the
//! three venues on this roadmap are polled or cursor-driven, for which both are
//! meaningless. Here the transport lives above this trait, not inside it.
//!
//! **The trait defines no method that places, cancels or amends an order.**

pub mod declaration;
pub mod symbols;

pub use declaration::{
    Budget, ConnectionPolicy, Declaration, DeclarationError, PageDirection, PageEnd, Paging,
};
pub use symbols::Symbols;

use galata_wire::{Series, Ticker};

use crate::normalise::Normalise;
use crate::record::Payload;

/// One subscription: a ticker and a series, on one venue.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Subscription {
    /// Which instrument.
    pub ticker: Ticker,
    /// Which series.
    pub series: Series,
}

/// How a venue wants to be kept alive.
///
/// **Not a `String`**, because one venue answers a protocol-level WebSocket
/// Ping with a Pong and rejects `{"event":"ping"}` as a bad message, while
/// another wants a JSON frame. A `String` return would have forced the first
/// venue to send something the second refuses.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Keepalive {
    /// The venue wants nothing.
    None,
    /// A WebSocket protocol Ping.
    Protocol,
    /// A frame of the venue's own.
    Frame(String),
}

/// What a subscription convergence attempt produced.
///
/// **Three outcomes, not two.** A venue may refuse a subscription for reasons
/// that have nothing to do with the request, and a component that collapses
/// *refused* into *pending* claims coverage it does not have.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Subscribed {
    /// The venue confirmed it.
    Held,
    /// Sent, not yet confirmed.
    Pending,
    /// The venue said no.
    Refused {
        /// What it said.
        reason: String,
    },
}

/// Why an adapter could not be built.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ConstructError {
    /// The venue refused the configuration.
    #[error("{venue}: {detail}")]
    Rejected {
        /// Which venue.
        venue: String,
        /// Why.
        detail: String,
    },
    /// A name would not survive validation.
    #[error("{0}")]
    Token(#[from] galata_wire::TokenError),
    /// The declaration is not usable.
    #[error("{0}")]
    Declaration(#[from] DeclarationError),
}

/// The seam.
///
/// Every method is synchronous and pure. See the module documentation for why.
pub trait Adapter: Normalise {
    /// What this venue serves, pages and permits. Callers derive behaviour from
    /// this rather than from constants of their own.
    fn declaration(&self) -> &Declaration;

    /// The venue's own channel name for a subscription.
    fn channel_of(&self, subscription: &Subscription) -> String;

    /// Which series a channel's payloads belong to, for partitioning and for
    /// coverage.
    ///
    /// `None` for a channel carrying no observation and for one never seen. The
    /// two are distinguished in `normalise`, not here: a channel that carries
    /// nothing is not an anomaly, and one we do not understand is.
    fn series_of_channel(&self, channel: &str) -> Option<Series>;

    /// The frames that subscribe the given set.
    ///
    /// Plural because a venue may carry many instruments in one frame and
    /// another may want one frame each. The caller sends what it is given and
    /// knows neither shape.
    fn subscribe_frames(&self, subscriptions: &[Subscription]) -> Vec<String>;

    /// The venue's keepalive, where it wants one.
    fn keepalive(&self) -> Keepalive;

    /// Read a live frame's envelope — its channel and the venue's own symbol —
    /// **without altering the bytes**.
    ///
    /// The payload it returns carries the frame verbatim: the record stores what
    /// arrived, and this reads only enough of it to decide which partition it
    /// belongs in. A frame whose envelope cannot be read still becomes a
    /// payload, under whatever channel the adapter can say, because the bytes
    /// are the thing that must not be lost.
    fn classify(&self, bytes: &[u8], recv_micros: i64) -> Payload;

    /// The ticker a venue's own symbol means on a channel.
    ///
    /// The loop needs this to credit coverage to the right pair, and it must
    /// not resolve the symbol itself — a venue's spelling is the venue's
    /// business, and a loop that mapped one would be the venue boundary
    /// leaking upward.
    fn venue_ticker(&self, channel: &str, venue_symbol: &str) -> Option<Ticker>;
}

/// How an adapter is constructed.
///
/// Every adapter takes an optional credential, and a venue whose market data is
/// public refuses a populated one rather than ignoring it — being handed a key
/// would mean somebody believed otherwise.
pub trait Construct: Sized {
    /// What this adapter is configured with.
    type Config;

    /// Build it, or refuse by name.
    fn new(config: Self::Config, credential: Option<Credential>) -> Result<Self, ConstructError>;
}

/// A credential an adapter may need.
///
/// Opaque here on purpose: this crate can name the concept without being able
/// to read a secret. The type that *reads* one lives elsewhere, and most
/// binaries do not link it.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum Credential {
    /// A key and a secret, for a venue that signs its reads.
    Signed {
        /// The key identity.
        key: String,
        /// The secret material.
        secret: String,
    },
    /// An endpoint whose URL itself carries authority.
    Endpoint {
        /// The URL.
        url: String,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_keepalive_is_not_a_string() {
        // One venue answers a protocol Ping and REJECTS a JSON one. A String
        // return would force the first venue to send what the second refuses.
        assert_ne!(Keepalive::Protocol, Keepalive::Frame("ping".into()));
        assert_ne!(Keepalive::None, Keepalive::Protocol);
    }

    #[test]
    fn refused_is_not_pending() {
        // Collapsing them would claim coverage we do not have: a pending
        // subscription may yet arrive, and a refused one never will.
        assert_ne!(
            Subscribed::Pending,
            Subscribed::Refused {
                reason: "unknown coin".into()
            }
        );
    }
}
