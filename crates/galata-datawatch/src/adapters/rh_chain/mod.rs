//! Robinhood Chain: an Arbitrum Orbit rollup carrying tokenised equities.
//!
//! **Chain 4663**, settling to Ethereum, ETH for gas. The public endpoint needs
//! no key and keeps no archive, which is why a provider's earliest block is a
//! declared fact rather than an assumption.
//!
//! The two things that make this venue different from an exchange:
//!
//! 1. **History pages by block, not by time.** Twenty consecutive blocks carry
//!    four distinct timestamps. See [`crate::venue::chain`].
//! 2. **There are two frontiers.** What arrived, and what cannot be taken back.

/// **Behind the `capture` feature**: it makes requests.
#[cfg(feature = "capture")]
pub mod client;
pub mod normalise;
pub mod trail;
pub mod wire;

/// The venue's name, as it appears in a partition and on a subject.
pub const VENUE: &str = "rh-chain";

/// The chain's own identifier, for refusing a provider pointed elsewhere.
pub const CHAIN_ID: u64 = 4663;

/// The public endpoint. **No key, no archive, no SLA.**
pub const PUBLIC_RPC: &str = "https://rpc.mainnet.chain.robinhood.com";

use std::collections::BTreeMap;

use galata_wire::{Origin, Series, Ticker, Venue};

use crate::normalise::{Normalise, NormaliseError};
use crate::record::{Payload, PayloadAddress};

use crate::venue::{
    Adapter, BlockPaging, Budget, ConnectionPolicy, Declaration, Paging, Transport,
};

/// How far behind the head finality runs, in blocks.
///
/// **Measured 2026-09-21**: `finalized` was 11,678 blocks and 19.6 minutes
/// behind `latest`. Not the ~13 minutes usually quoted — that is `safe`, which
/// can still be reorganised under a fault.
pub const FINALITY_LAG_BLOCKS: u64 = 11_678;

/// The most blocks one `eth_getLogs` may span.
///
/// Conservative against a public node that states no limit and enforces one by
/// timing out. A range that comes back empty because it was too wide is
/// indistinguishable from a range with nothing in it.
pub const MAX_BLOCK_SPAN: u64 = 1_000;

/// One instrument on the chain: a contract, what it is, and how it counts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Instrument {
    /// Our name for it.
    pub ticker: String,
    /// The contract that emits its transfers.
    pub contract: String,
    /// **Per contract, with no default** — stock tokens carry 18 and USDG 6.
    pub decimals: u32,
}

/// What this adapter is built from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Config {
    /// What to capture.
    pub instruments: Vec<Instrument>,
}

/// The chain, across the seam.
#[derive(Debug)]
pub struct RhChain {
    declaration: Declaration,
    tickers: BTreeMap<String, Ticker>,
    decimals: BTreeMap<String, u32>,
}

impl RhChain {
    /// Build one.
    pub fn new(config: Config) -> Result<RhChain, crate::venue::ConstructError> {
        let mut tickers = BTreeMap::new();
        let mut decimals = BTreeMap::new();
        for instrument in &config.instruments {
            let contract = instrument.contract.to_ascii_lowercase();
            tickers.insert(contract.clone(), Ticker::new(&instrument.ticker)?);
            decimals.insert(contract, instrument.decimals);
        }
        Ok(RhChain {
            declaration: Declaration {
                // **Nothing is pushed.** The chain is asked.
                streams: Vec::new(),
                historical: vec![Series::Transfers, Series::Mints],
                paging: BTreeMap::from([
                    (Series::Transfers, Paging::forward_from_start(0)),
                    (Series::Mints, Paging::forward_from_start(0)),
                ]),
                budget: Budget {
                    // The public node states no rate and enforces one by
                    // refusing. Declared low rather than discovered.
                    requests_per_minute: 120.0,
                    min_historical_interval_ms: 250,
                },
                // No socket, so no lifetime and no rotation.
                connection: ConnectionPolicy::KeepAliveOnly { keepalive_secs: 0 },
                ws_url: "",
                rest_url: PUBLIC_RPC,
            },
            tickers,
            decimals,
        })
    }

    /// The instruments this captures, by contract.
    pub fn tickers(&self) -> &BTreeMap<String, Ticker> {
        &self.tickers
    }

    /// Their decimals, by contract.
    pub fn decimals(&self) -> &BTreeMap<String, u32> {
        &self.decimals
    }
}

impl Normalise for RhChain {
    fn venue(&self) -> &Venue {
        static VENUE_NAME: std::sync::OnceLock<Venue> = std::sync::OnceLock::new();
        VENUE_NAME.get_or_init(|| Venue::new(VENUE).expect("a legal venue name"))
    }

    fn normalise(&self, payload: &Payload) -> Result<Vec<galata_wire::Envelope>, NormaliseError> {
        let logs: Vec<wire::Log> =
            serde_json::from_slice(&payload.payload).map_err(|e| NormaliseError::Shape {
                kind: "eth_getLogs response",
                detail: e.to_string(),
            })?;
        Ok(normalise::read(
            self.venue(),
            &logs,
            &self.tickers,
            &self.decimals,
            payload.recv_micros,
            None,
        )
        .events)
    }
}

impl Adapter for RhChain {
    fn declaration(&self) -> &Declaration {
        &self.declaration
    }

    /// **A cursor, and no keepalive field to fill in emptily.**
    fn transport(&self) -> Transport {
        Transport::Cursor {
            rpc_url: PUBLIC_RPC,
            chain_id: CHAIN_ID,
            paging: BlockPaging {
                max_span: MAX_BLOCK_SPAN,
                // The public node keeps no archive and states no earliest
                // block. `None` means nothing is refused for being too old,
                // which is honest: it will answer, and answer empty.
                earliest: None,
            },
            finality_lag: FINALITY_LAG_BLOCKS,
        }
    }

    // **No `streaming()`.** The default is `None`, which is the truth.

    fn series_of_channel(&self, channel: &str) -> Option<Series> {
        match channel {
            "eth_getLogs" => Some(Series::Transfers),
            _ => None,
        }
    }

    fn classify(&self, bytes: &[u8], recv_micros: i64) -> Payload {
        Payload {
            seq: 0,
            recv_micros,
            address: PayloadAddress::Venue(VENUE.into()),
            channel: "eth_getLogs".into(),
            kind: Series::Transfers.as_str().to_string(),
            // **A response is about many instruments**, so it names none.
            symbol: None,
            origin: Origin::Fetched,
            payload: bytes.to_vec(),
        }
    }

    fn venue_symbol(&self, ticker: &Ticker) -> Option<String> {
        // The venue's own name for an instrument is its contract address.
        self.tickers
            .iter()
            .find(|(_, t)| *t == ticker)
            .map(|(contract, _)| contract.clone())
    }

    fn interval_label(&self, _interval_micros: i64) -> Option<String> {
        // A chain has no bar widths. `None` refuses rather than inventing one.
        None
    }

    fn venue_ticker(&self, _channel: &str, venue_symbol: &str) -> Option<Ticker> {
        self.tickers
            .get(&venue_symbol.to_ascii_lowercase())
            .cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn chain() -> RhChain {
        RhChain::new(Config {
            instruments: vec![Instrument {
                ticker: "NVDA".into(),
                contract: "0x0BD7D308F8E1639FAB988DF18A8011F41EACAD73".into(),
                decimals: 18,
            }],
        })
        .unwrap()
    }

    #[test]
    fn a_cursor_venue_can_be_an_adapter_at_all() {
        // The whole point of the transport split. Before it, this type had to
        // answer `subscribe_frames` with an empty Vec and `keepalive` with
        // None — values that are not false but MEANINGLESS.
        let chain = chain();
        assert!(matches!(chain.transport(), Transport::Cursor { .. }));
        assert!(chain.streaming().is_none(), "a chain offered a subscriber");
    }

    #[test]
    fn it_declares_that_nothing_is_pushed() {
        assert!(chain().declaration().streams.is_empty());
        assert!(chain().declaration().serves_historically(Series::Transfers));
    }

    #[test]
    fn the_transport_carries_the_measured_finality_lag() {
        let Transport::Cursor {
            finality_lag,
            chain_id,
            ..
        } = chain().transport()
        else {
            panic!("not a cursor")
        };
        assert_eq!(finality_lag, 11_678, "measured, not documented");
        assert_eq!(chain_id, 4663);
    }

    #[test]
    fn a_contract_resolves_to_its_instrument_whatever_its_case() {
        // Nodes disagree about hex case, and a lookup that did not fold it
        // would silently capture nothing.
        let chain = chain();
        let ticker = Ticker::new("NVDA").unwrap();
        assert_eq!(
            chain.venue_ticker("eth_getLogs", "0x0bd7d308f8e1639fab988df18a8011f41eacad73"),
            Some(ticker.clone())
        );
        assert_eq!(
            chain.venue_ticker("eth_getLogs", "0x0BD7D308F8E1639FAB988DF18A8011F41EACAD73"),
            Some(ticker.clone())
        );
        assert_eq!(chain.venue_symbol(&ticker).unwrap(), contract_lowercase());
    }

    fn contract_lowercase() -> String {
        "0x0bd7d308f8e1639fab988df18a8011f41eacad73".into()
    }

    #[test]
    fn a_response_is_classified_as_being_about_no_one_instrument() {
        // It carries many, which is why the payload unit is the response.
        let payload = chain().classify(b"[]", 100);
        assert_eq!(payload.symbol, None);
        assert_eq!(payload.channel, "eth_getLogs");
        assert_eq!(payload.origin, Origin::Fetched);
    }

    #[test]
    fn a_real_response_normalises_through_the_seam() {
        let payload = chain().classify(
            br#"[{"address":"0x0bd7d308f8e1639fab988df18a8011f41eacad73",
              "topics":["0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
                "0x0000000000000000000000000000000000000000000000000000000000000000",
                "0x00000000000000000000000065050a9b7e5075a2ba5ced7b1b64ee66262c40dc"],
              "data":"0x000000000000000000000000000000000000000000000000015fb7f9b8c38000",
              "blockNumber":"0x4176ed2",
              "transactionHash":"0x08dd4d916d95830ae4a8889818a5fc27e2deef7e0fad9298f2d6dde52f24aa63",
              "logIndex":"0x1"}]"#,
            100,
        );
        let events = chain().normalise(&payload).unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].kind(), galata_wire::Kind::Mints);
    }

    #[test]
    fn a_chain_has_no_bar_widths_and_says_so() {
        assert_eq!(chain().interval_label(60_000_000), None);
    }
}
