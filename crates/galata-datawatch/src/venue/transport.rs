//! **What carries a venue's bytes** — declared, not assumed.
//!
//! The seam abstracted framing and not transport, which was diagnosed in the
//! predecessor, written down as the thing to avoid, and then reproduced here:
//!
//! ```text
//!   what the seam abstracted     what it still assumed
//!   ────────────────────────     ─────────────────────
//!   bytes → events               there is a websocket
//!   a venue's symbols            there are subscriptions
//!   a venue's declaration        there is a keepalive
//! ```
//!
//! A venue read by polling a JSON-RPC endpoint has none of those three.
//! Expressed through a stream-shaped trait it must return an empty frame list
//! and a `Keepalive::None` — values that are not *false*, they are
//! **meaningless** — and a loop acting on them opens a socket that should never
//! have been opened.

use super::chain::BlockPaging;

/// What moves a venue's bytes.
///
/// **An enum rather than a trait object**, because the set is closed and small
/// and the capture loop has to match on it anyway: a stream loop and a cursor
/// loop are genuinely different programs, not two implementations of one
/// interface. Pretending otherwise is how `subscribe_frames` would end up on a
/// chain adapter.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Transport {
    /// The venue pushes, over a socket held open.
    Stream {
        /// The websocket endpoint.
        ///
        /// **A code identity, never configuration** — a service running against
        /// a venue its configuration did not name is the failure that prevents.
        /// Unchanged by this move; it simply lives inside the variant that has
        /// one.
        ws_url: &'static str,
        /// How the venue wants to be kept alive.
        keepalive: super::Keepalive,
    },
    /// We ask, on a timer, and the answer is the whole current state.
    ///
    /// **The shape where silence is a gap.** A stream cannot tell a quiet
    /// market from a dead socket, because nothing happened either way. A poll
    /// can: we asked at a known moment, so its failure is an event we
    /// witnessed and the interval is exactly the cadence.
    Poll {
        /// Where to ask.
        rest_url: &'static str,
        /// The path asked for.
        path: &'static str,
        /// How often, in microseconds.
        ///
        /// **Also the width of a gap a single failure produces**, which is why
        /// it is a declaration rather than a tuning knob.
        interval_micros: i64,
    },
    /// We ask, by position, and the position is a block number.
    Cursor {
        /// The JSON-RPC endpoint.
        ///
        /// A **default** here: unlike a websocket venue, a provider is
        /// substitutable, and the public node is the one every reader can
        /// reach. A configured provider replaces it, and a provider URL is a
        /// **secret** rather than configuration because it usually carries a
        /// key.
        rpc_url: &'static str,
        /// The chain's own identifier, for refusing a provider pointed
        /// elsewhere. A provider silently serving a different chain would
        /// produce blocks that are real and not ours.
        chain_id: u64,
        /// How the provider serves ranges.
        paging: BlockPaging,
        /// How far behind the head finality runs, in blocks.
        ///
        /// Measured, not documented: on Robinhood Chain, 11,678 blocks and 19.6
        /// minutes. It bounds the hash trail, because a block at or below
        /// finality cannot be reorganised.
        finality_lag: u64,
    },
}

impl Transport {
    /// Whether this venue pushes.
    pub fn is_stream(&self) -> bool {
        matches!(self, Transport::Stream { .. })
    }

    /// The endpoint, whichever kind it is — for a log line that should not care.
    pub fn endpoint(&self) -> &str {
        match self {
            Transport::Stream { ws_url, .. } => ws_url,
            Transport::Poll { rest_url, .. } => rest_url,
            Transport::Cursor { rpc_url, .. } => rpc_url,
        }
    }
}

/// The half of a seam that only a **subscribing** transport has.
///
/// Separate from [`Adapter`](super::Adapter) so that an adapter which cannot
/// subscribe is **unable to be asked**, rather than answering emptily. The
/// frames stay a method rather than a field because they depend on the
/// subscription set.
pub trait Streaming {
    /// The venue's own channel name for a subscription.
    fn channel_of(&self, subscription: &super::Subscription) -> String;

    /// The frames that subscribe the given set.
    ///
    /// Plural because a venue may carry many instruments in one frame and
    /// another may want one frame each. The caller sends what it is given and
    /// knows neither shape.
    fn subscribe_frames(&self, subscriptions: &[super::Subscription]) -> Vec<String>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::venue::Keepalive;

    #[test]
    fn a_cursor_venue_declares_no_keepalive_because_it_has_none() {
        // The point of the whole change: not `Keepalive::None`, which is an
        // answer, but no field at all, which is the truth.
        let cursor = Transport::Cursor {
            rpc_url: "https://rpc.mainnet.chain.robinhood.com",
            chain_id: 4663,
            paging: BlockPaging {
                max_span: 1_000,
                earliest: None,
            },
            finality_lag: 11_678,
        };
        assert!(!cursor.is_stream());
        assert_eq!(cursor.endpoint(), "https://rpc.mainnet.chain.robinhood.com");
    }

    #[test]
    fn a_stream_venue_carries_its_keepalive() {
        let stream = Transport::Stream {
            ws_url: "wss://api.hyperliquid.xyz/ws",
            keepalive: Keepalive::Frame(r#"{"method":"ping"}"#.into()),
        };
        assert!(stream.is_stream());
        assert_eq!(stream.endpoint(), "wss://api.hyperliquid.xyz/ws");
    }

    #[test]
    fn the_finality_lag_is_a_measurement_and_is_carried_as_one() {
        // 11,678 blocks and 19.6 minutes, read off the chain. It bounds the
        // hash trail: a block at or below finality cannot be reorganised.
        let Transport::Cursor { finality_lag, .. } = (Transport::Cursor {
            rpc_url: "x",
            chain_id: 4663,
            paging: BlockPaging {
                max_span: 10,
                earliest: None,
            },
            finality_lag: 11_678,
        }) else {
            panic!("not a cursor")
        };
        assert_eq!(finality_lag, 11_678);
    }
}
