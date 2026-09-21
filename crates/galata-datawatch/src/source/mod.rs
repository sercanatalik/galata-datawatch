//! How bytes arrive.
//!
//! **This is the seam the predecessor did not have.** Its adapter abstracts
//! *framing* and its loop calls `connect_async` directly, so a subscribe frame
//! and a keepalive are baked in as assumptions about how bytes arrive. Two of
//! the three venues on this roadmap have neither:
//!
//! ```text
//!   Stream   the venue pushes            hyperliquid    — this change
//!   Poll     we ask on a cadence         rh-crypto      — a later change
//!   Cursor   we advance through blocks   rh-chain       — a later change
//! ```
//!
//! # Why an enum and not a trait
//!
//! The three have genuinely different *shapes* — one is driven by the venue,
//! one by a timer, one by a monotonic cursor — and the loop's handling of them
//! differs in more than a method call. An enum makes that match exhaustive, so
//! a fourth cannot be quietly forgotten in one arm of the loop.
//!
//! The extension point that matters for out-of-tree work is
//! [`Adapter`], which is a trait.

pub mod backoff;
pub mod stream;

pub use backoff::Backoff;
pub use stream::StreamSource;

/// What a source hands the loop.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Frame {
    /// Bytes the venue sent.
    Bytes(Vec<u8>),
    /// The connection ended. The loop decides what that means.
    Closed,
    /// Nothing arrived inside the poll window.
    ///
    /// **This is not a gap.** A quiet market and a silently dead connection are
    /// the same shape from here, so nothing is inferred from it. It exists so
    /// the loop can service its timers rather than block forever on a socket.
    Idle,
}

/// Why a source could not deliver.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum SourceError {
    /// The connection could not be established.
    #[error("connecting to {endpoint}: {reason}")]
    Connect {
        /// **The endpoint's safe label**, not its URL. A websocket venue's
        /// address is a compiled-in identity today, but the rule this tree
        /// holds is that *no* error message carries an endpoint — a rule with
        /// an exception is a rule nobody can check.
        endpoint: String,
        /// Why.
        reason: String,
    },
    /// The connection failed while delivering.
    #[error("the session failed: {0}")]
    Session(String),
}

/// How a venue's bytes reach the loop.
///
/// One variant implemented in this change; the others are named so the loop's
/// match is written against all three from the start.
#[derive(Debug)]
#[non_exhaustive]
pub enum Source {
    /// The venue pushes over a long-lived connection.
    Stream(StreamSource),
}
