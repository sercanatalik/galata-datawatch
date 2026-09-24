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

pub mod chain;
pub mod declaration;
pub mod poll;
pub mod symbols;
pub mod transport;
pub mod universe;

pub use chain::{BlockPaging, BlockStep, Frontier, PlanError};
pub use declaration::{
    Budget, ConnectionPolicy, Declaration, DeclarationError, PageDirection, PageEnd, Paging,
};
pub use poll::{Cadence, Polled};
pub use symbols::Symbols;
pub use transport::{Endpoint, RequestSigner, Signer, Streaming, Transport};

pub use universe::UniverseError;

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

/// Reference data a venue will only answer when asked.
///
/// **Measured 2026-09-21:** fifty thousand blocks of NVDA carry no multiplier
/// update log under any candidate signature, so on Robinhood Chain a corporate
/// action is learned by polling or not at all. That is what this exists for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Reference {
    /// How often to re-ask. Bounds the staleness; it does not detect a change
    /// any sooner than that.
    pub interval_micros: i64,
    /// The venue's own symbols to ask about — contract addresses on a chain.
    pub symbols: Vec<String>,
}

/// The seam.
///
/// Every method is synchronous and pure. See the module documentation for why.
///
/// # Adding a venue from outside this crate
///
/// Everything below is `pub`, so a venue can live in your own crate and never
/// touch this one. The shape is small — the trait answers *what this venue is*,
/// and [`Normalise`] turns its bytes into events:
///
/// ```
/// use std::collections::BTreeMap;
/// use galata_datawatch::normalise::{Normalise, NormaliseError};
/// use galata_datawatch::record::{Payload, PayloadAddress};
/// use galata_datawatch::venue::{
///     Adapter, Budget, ConnectionPolicy, Declaration, Endpoint, Keepalive, Paging, Transport,
/// };
/// use galata_wire::{Envelope, Origin, Series, Ticker, Venue};
///
/// struct MyVenue {
///     venue: Venue,
///     declaration: Declaration,
/// }
///
/// impl MyVenue {
///     fn new() -> MyVenue {
///         MyVenue {
///             venue: Venue::new("my-venue").expect("a legal venue name"),
///             declaration: Declaration {
///                 // What it pushes, what it serves historically, and how it
///                 // pages — DECLARED, so a walk never discovers it by
///                 // failing.
///                 streams: vec![Series::Trades],
///                 historical: vec![],
///                 paging: BTreeMap::new(),
///                 budget: Budget {
///                     requests_per_minute: 60.0,
///                     min_historical_interval_ms: 1_000,
///                 },
///                 connection: ConnectionPolicy::KeepAliveOnly { keepalive_secs: 30 },
///                 ws_url: "wss://my-venue.invalid/stream",
///                 rest_url: "https://my-venue.invalid",
///             },
///         }
///     }
/// }
///
/// impl Normalise for MyVenue {
///     fn venue(&self) -> &Venue {
///         &self.venue
///     }
///
///     /// **Runs after the bytes are already durable**, so a shape you did
///     /// not expect costs a parse and never the record.
///     fn normalise(&self, _payload: &Payload) -> Result<Vec<Envelope>, NormaliseError> {
///         Ok(Vec::new())
///     }
/// }
///
/// impl Adapter for MyVenue {
///     fn declaration(&self) -> &Declaration {
///         &self.declaration
///     }
///
///     /// Which of the three loops drives this venue. No `streaming()`
///     /// override means nothing is pushed, and the default says so rather
///     /// than returning an empty frame list.
///     fn transport(&self) -> Transport {
///         Transport::Stream {
///             ws_url: Endpoint::public("wss://my-venue.invalid/stream"),
///             keepalive: Keepalive::None,
///         }
///     }
///
///     fn series_of_channel(&self, channel: &str) -> Option<Series> {
///         (channel == "trades").then_some(Series::Trades)
///     }
///
///     /// Reads only enough of a frame to decide which partition it belongs
///     /// in. A frame whose envelope cannot be read still becomes a payload.
///     fn classify(&self, bytes: &[u8], recv_micros: i64) -> Payload {
///         Payload {
///             seq: 0,
///             recv_micros,
///             address: PayloadAddress::Venue("my-venue".into()),
///             channel: "trades".into(),
///             kind: Series::Trades.as_str().to_string(),
///             symbol: None,
///             origin: Origin::Streamed,
///             payload: bytes.to_vec(),
///         }
///     }
///
///     fn venue_symbol(&self, ticker: &Ticker) -> Option<String> {
///         Some(ticker.as_str().to_string())
///     }
///
///     /// A venue with no bar widths says `None` rather than inventing one.
///     fn interval_label(&self, _interval_micros: i64) -> Option<String> {
///         None
///     }
///
///     fn venue_ticker(&self, _channel: &str, venue_symbol: &str) -> Option<Ticker> {
///         Ticker::new(venue_symbol).ok()
///     }
/// }
///
/// let adapter = MyVenue::new();
/// assert_eq!(adapter.venue().as_str(), "my-venue");
/// assert!(adapter.transport().is_stream());
/// // Nothing is pushed unless `streaming()` is overridden.
/// assert!(adapter.streaming().is_none());
/// ```
///
/// **This example is a doctest, so it compiles against this crate — which is
/// not quite the claim.** A doctest can reach anything the crate can. The
/// stronger claim is checked by `tests/out_of_tree_venue.rs`, compiled by cargo
/// as its own crate, so it sees exactly what a stranger sees: if it needs
/// something private, the compiler says which thing *there* rather than in
/// somebody's repository. That test implements a deliberately fictional venue,
/// because one resembling an in-tree venue would tempt reuse of its helpers,
/// and reuse is what makes a test pass for the wrong reason.
pub trait Adapter: Normalise {
    /// What this venue serves, pages and permits. Callers derive behaviour from
    /// this rather than from constants of their own.
    fn declaration(&self) -> &Declaration;

    /// **What carries this venue's bytes.**
    ///
    /// Declared rather than assumed. A venue read by polling has no
    /// subscription and no keepalive, and before this existed it had to answer
    /// an empty frame list and a `Keepalive::None` — values that are not false
    /// but meaningless.
    fn transport(&self) -> Transport;

    /// The subscribing half, **where there is one**.
    ///
    /// `None` is the default and the honest answer for a venue that does not
    /// subscribe. A caller must handle it, which is the difference between this
    /// and defaulted methods returning an empty frame list: an empty list is an
    /// answer a loop will act on, and `None` is not.
    fn streaming(&self) -> Option<&dyn Streaming> {
        None
    }

    /// Which series a channel's payloads belong to, for partitioning and for
    /// coverage.
    ///
    /// `None` for a channel carrying no observation and for one never seen. The
    /// two are distinguished in `normalise`, not here: a channel that carries
    /// nothing is not an anomaly, and one we do not understand is.
    fn series_of_channel(&self, channel: &str) -> Option<Series>;

    /// Read a live frame's envelope — its channel and the venue's own symbol —
    /// **without altering the bytes**.
    ///
    /// The payload it returns carries the frame verbatim: the record stores what
    /// arrived, and this reads only enough of it to decide which partition it
    /// belongs in. A frame whose envelope cannot be read still becomes a
    /// payload, under whatever channel the adapter can say, because the bytes
    /// are the thing that must not be lost.
    fn classify(&self, bytes: &[u8], recv_micros: i64) -> Payload;

    /// Reference data this venue must be **asked** for, on a cadence.
    ///
    /// `None` by default, which is the ordinary case: most venues state
    /// reference data on a channel you subscribe to, and one that does not is
    /// the exception. A chain is that exception — a token contract answers
    /// questions and announces nothing.
    ///
    /// On the seam rather than in the loop because *which symbols* and *how
    /// often* are both venue knowledge, and a loop that hard-coded either
    /// would apply one venue's answer to the next one.
    fn reference(&self) -> Option<Reference> {
        None
    }

    /// The venue's own string for a ticker — the inverse of
    /// [`Adapter::venue_ticker`], and what a historical request carries.
    ///
    /// On the seam for the same reason as its inverse: a venue's spelling is
    /// the venue's business, and a walk composing `xyz:XYZ100` for itself would
    /// be the venue boundary leaking upward.
    fn venue_symbol(&self, ticker: &Ticker) -> Option<String>;

    /// The venue's own label for a bar width — `1m`, `4h`.
    ///
    /// The walk plans in microseconds because arithmetic over a range needs a
    /// number; the venue is asked in its own units. `None` for a width the
    /// venue does not serve, which is refused rather than rounded: a walk that
    /// silently asked for hourly bars where minute ones were wanted would
    /// report success over sixty times too little.
    fn interval_label(&self, interval_micros: i64) -> Option<String>;

    /// The bar width the venue **pushes**, where it pushes one.
    ///
    /// The walk needs this to know which width may resume from the record.
    /// Every other width must state what it needs, because the record is dated
    /// by receipt — see [`crate::capture::walk`].
    fn live_interval_micros(&self) -> Option<i64> {
        None
    }

    /// Where a forward-paged page ended, so the walk can issue the next or know
    /// it has had the last.
    ///
    /// `None` for a payload that is not a forward-paged page, and for a page
    /// the adapter cannot read — which stops the walk rather than paging
    /// forever from a time it invented.
    fn page_end(&self, _payload: &Payload) -> Option<PageEnd> {
        None
    }

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
