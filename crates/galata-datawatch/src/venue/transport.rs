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
use crate::config::Secret;

/// Where a venue is reached — **and whether that can be said out loud.**
///
/// A keyed provider carries its key **in the path**:
///
/// ```text
///   https://arb-mainnet.g.alchemy.com/v2/<KEY>
///   https://<slug>.arbitrum-mainnet.quiknode.pro/<TOKEN>/
/// ```
///
/// so the URL *is* the credential, and stripping a query string — the remedy
/// most projects reach for — protects nothing here.
///
/// **Not a [`Secret`]**, because a public endpoint *should* print.
/// `wss://api.hyperliquid.xyz/ws` in a log is useful and is not a credential,
/// and a type that withheld it would make every venue's logs worse to protect
/// the one venue that needs it.
#[derive(Clone, PartialEq, Eq)]
pub enum Endpoint {
    /// A code identity, compiled in. Prints.
    ///
    /// A service running against a venue its configuration did not name is the
    /// failure this prevents.
    Public(&'static str),
    /// Supplied by a [`crate::config::SecretSource`]. **Prints the variable
    /// name.**
    Held {
        /// The variable that supplied it — committed in the configuration
        /// file, and what an operator needs in order to fix a bad provider.
        var: String,
        /// The URL itself.
        url: Secret,
    },
}

impl Endpoint {
    /// A compiled-in endpoint.
    pub fn public(url: &'static str) -> Endpoint {
        Endpoint::Public(url)
    }

    /// One that came from a secret source.
    pub fn held(var: impl Into<String>, url: Secret) -> Endpoint {
        Endpoint::Held {
            var: var.into(),
            url,
        }
    }

    /// Hand it to the client that must connect.
    ///
    /// Named `expose` rather than `as_str` so that every use reads, at the call
    /// site, like the decision it is — the convention [`Secret`] already sets.
    pub fn expose(&self) -> &str {
        match self {
            Endpoint::Public(url) => url,
            Endpoint::Held { url, .. } => url.expose(),
        }
    }

    /// Whether this one came from a secret.
    pub fn is_held(&self) -> bool {
        matches!(self, Endpoint::Held { .. })
    }
}

/// **The safe rendering, and the only one.** There is no second formatting
/// path to forget about: `Debug` defers to this too.
impl std::fmt::Display for Endpoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Endpoint::Public(url) => f.write_str(url),
            // **Not the host.** QuickNode puts an identifying slug in the
            // hostname, so "scheme + host" is a rule that is right for Alchemy
            // and wrong for QuickNode — the kind of rule that ships.
            Endpoint::Held { var, .. } => write!(f, "the provider named by {var}"),
        }
    }
}

impl std::fmt::Debug for Endpoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, f)
    }
}

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
        /// Always [`Endpoint::Public`]: a venue that pushes is reached at an
        /// address this crate compiles in, and no key goes in it.
        ws_url: Endpoint,
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
        /// Where to ask. Always [`Endpoint::Public`] today.
        rest_url: Endpoint,
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
        /// **The one that can be held.** Unlike a websocket venue, a provider
        /// is substitutable: the public node is the default every reader can
        /// reach, and a configured provider replaces it — carrying a key in
        /// its path, which is why this is an [`Endpoint`] and not a `&str`.
        rpc_url: Endpoint,
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

    /// The endpoint, whichever kind it is — for a log line that should not
    /// care, and **which cannot leak one that is held** because there is only
    /// the one rendering.
    pub fn endpoint(&self) -> &Endpoint {
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

    #[test]
    fn a_held_endpoint_cannot_say_its_url() {
        // A real Alchemy shape: the key is in the PATH, so stripping a query
        // string — the remedy most projects reach for — protects nothing.
        let url = "https://arb-mainnet.g.alchemy.com/v2/SUPERSECRETKEY";
        let held = Endpoint::held("GALATA_RHCHAIN_RPC_URL", Secret::new(url));

        for rendered in [format!("{held}"), format!("{held:?}")] {
            assert!(!rendered.contains("SUPERSECRETKEY"), "{rendered}");
            assert!(!rendered.contains("alchemy"), "{rendered}");
            // What an operator needs in order to FIX it.
            assert!(rendered.contains("GALATA_RHCHAIN_RPC_URL"), "{rendered}");
        }
        // And it is handed over only by a name that reads like a decision.
        assert_eq!(held.expose(), url);
        assert!(held.is_held());
    }

    #[test]
    fn a_public_endpoint_still_prints() {
        // A type that withheld this would make every venue's logs worse to
        // protect the one venue that needs it.
        let public = Endpoint::public("wss://api.hyperliquid.xyz/ws");
        assert_eq!(public.to_string(), "wss://api.hyperliquid.xyz/ws");
        assert_eq!(format!("{public:?}"), "wss://api.hyperliquid.xyz/ws");
        assert!(!public.is_held());
    }

    #[test]
    fn a_transport_holding_a_held_endpoint_leaks_nothing_through_debug() {
        // `Transport` derives `Debug`, and a derived `Debug` prints every
        // field — so the protection has to live in the field's own type.
        let cursor = Transport::Cursor {
            rpc_url: Endpoint::held("GALATA_RHCHAIN_RPC_URL", Secret::new("https://x/v2/KEY")),
            chain_id: 4663,
            paging: BlockPaging {
                max_span: 1_000,
                earliest: None,
            },
            finality_lag: 1,
        };
        let rendered = format!("{cursor:?}");
        assert!(!rendered.contains("KEY"), "{rendered}");
        assert!(rendered.contains("GALATA_RHCHAIN_RPC_URL"), "{rendered}");
        assert!(!cursor.endpoint().to_string().contains("KEY"));
    }
    use super::*;
    use crate::venue::Keepalive;

    #[test]
    fn a_cursor_venue_declares_no_keepalive_because_it_has_none() {
        // The point of the whole change: not `Keepalive::None`, which is an
        // answer, but no field at all, which is the truth.
        let cursor = Transport::Cursor {
            rpc_url: Endpoint::public("https://rpc.mainnet.chain.robinhood.com"),
            chain_id: 4663,
            paging: BlockPaging {
                max_span: 1_000,
                earliest: None,
            },
            finality_lag: 11_678,
        };
        assert!(!cursor.is_stream());
        assert_eq!(
            cursor.endpoint().to_string(),
            "https://rpc.mainnet.chain.robinhood.com"
        );
    }

    #[test]
    fn a_stream_venue_carries_its_keepalive() {
        let stream = Transport::Stream {
            ws_url: Endpoint::public("wss://api.hyperliquid.xyz/ws"),
            keepalive: Keepalive::Frame(r#"{"method":"ping"}"#.into()),
        };
        assert!(stream.is_stream());
        assert_eq!(
            stream.endpoint().to_string(),
            "wss://api.hyperliquid.xyz/ws"
        );
    }

    #[test]
    fn the_finality_lag_is_a_measurement_and_is_carried_as_one() {
        // 11,678 blocks and 19.6 minutes, read off the chain. It bounds the
        // hash trail: a block at or below finality cannot be reorganised.
        let Transport::Cursor { finality_lag, .. } = (Transport::Cursor {
            rpc_url: Endpoint::public("x"),
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
