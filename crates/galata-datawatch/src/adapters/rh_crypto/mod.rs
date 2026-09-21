//! Robinhood Crypto: a venue that is **asked**, and that authenticates every
//! ask.
//!
//! The cheapest possible proof of `Source::Poll`, and the venue where this
//! system gives its third different answer to the same question:
//!
//! ```text
//!   stream   a quiet market and a dead socket look identical
//!            → NEVER infer a gap
//!   chain    the chain hands back a different block
//!            → PROVE the gap
//!   poll     we asked at a known moment and nothing came back
//!            → BOUND the gap, exactly one interval wide
//! ```
//!
//! The poll is the case where **our own action supplies the missing half**. A
//! stream cannot tell silence from absence because nothing happened either way.
//! A poll can, because we did something, and its failure is an event we
//! witnessed.

pub mod sign;
pub mod wire;

/// The venue's name, as it appears in a partition and on a subject.
pub const VENUE: &str = "rh-crypto";

/// Where the market-data endpoints live.
pub const REST_URL: &str = "https://trading.robinhood.com";

/// The best bid and ask, for many symbols in one request.
///
/// **Repeated `?symbol=`**, which is what makes one poll cover every instrument
/// rather than one request each.
pub const BEST_BID_ASK_PATH: &str = "/api/v1/crypto/marketdata/best_bid_ask/";
