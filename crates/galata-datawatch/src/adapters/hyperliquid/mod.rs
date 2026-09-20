//! Hyperliquid.
//!
//! Market data is public: no secret is loaded to construct this adapter, and a
//! credential is refused rather than ignored.
//!
//! The load-bearing measured fact is the **session lifetime**. With pings every
//! twenty seconds and pongs confirmed, sessions still closed at around ten and a
//! half minutes. A reconnect is therefore not a defect to eliminate; it is a
//! condition to handle, and the way to handle it is to go first — open and
//! subscribe the replacement, *then* close the current one, on a timer set
//! inside the observed lifetime. That is why a rotation publishes no gap: none
//! occurred.
//!
//! **HIP-3.** A builder-deployed perp is spelled `<dex>:<coin>` in every API
//! call. A [`Ticker`](galata_wire::Ticker) may not hold a `:`, so the ticker is
//! the bare coin and the prefix is composed at the seam.

pub mod normalise;
pub mod wire;

use std::collections::BTreeMap;

use galata_wire::{Origin, Series, Venue};

use crate::normalise::{Normalise, NormaliseError};
use crate::record::{Payload, PayloadAddress};
use crate::venue::{
    Adapter, Budget, ConnectionPolicy, Construct, ConstructError, Credential, Declaration,
    Keepalive, Paging, Subscription, Symbols,
};

/// The venue's name, as it appears in a partition and on a subject.
pub const VENUE: &str = "hyperliquid";

/// The connection lifetime observed at the venue: 10.4 min without a keepalive
/// and 10.73 min with one. **The tighter figure is used**, and the venue
/// documents neither.
pub const OBSERVED_LIFETIME_SECS: u64 = 10 * 60 + 24;

/// When the handover begins, inside that lifetime.
pub const ROTATE_AFTER_SECS: u64 = 8 * 60;

/// Three messages a minute against a limit of two thousand.
///
/// The keepalive is kept because it removes idle timeout as a variable. It is
/// **not** claimed to fix the session close — sessions closed at the same
/// lifetime with it as without.
pub const PING_INTERVAL_SECS: u64 = 20;

// Relationships between the constants, checked at COMPILE time. A test
// comparing them asserts nothing at runtime and would pass whatever the values
// became.
const _: () = assert!(
    ROTATE_AFTER_SECS < OBSERVED_LIFETIME_SECS,
    "rotation is later than the venue closes; the handover would not be free"
);
const _: () = assert!(
    OBSERVED_LIFETIME_SECS - ROTATE_AFTER_SECS >= 60,
    "less than a minute of margin inside the venue's observed lifetime"
);

/// Which Hyperliquid.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Market {
    /// The live one.
    Mainnet,
    /// The test one.
    Testnet,
}

impl Market {
    /// Parse a market name, refusing an unknown one by listing the known.
    pub fn parse(name: &str) -> Result<Market, ConstructError> {
        match name {
            "mainnet" => Ok(Market::Mainnet),
            "testnet" => Ok(Market::Testnet),
            other => Err(ConstructError::Rejected {
                venue: VENUE.into(),
                detail: format!("{other:?} is not a Hyperliquid market. Known: mainnet, testnet"),
            }),
        }
    }

    /// The websocket endpoint. A **code identity**, never configurable.
    fn ws_url(&self) -> &'static str {
        match self {
            Market::Mainnet => "wss://api.hyperliquid.xyz/ws",
            Market::Testnet => "wss://api.hyperliquid-testnet.xyz/ws",
        }
    }

    /// The REST root. Same rule.
    fn rest_url(&self) -> &'static str {
        match self {
            Market::Mainnet => "https://api.hyperliquid.xyz",
            Market::Testnet => "https://api.hyperliquid-testnet.xyz",
        }
    }
}

/// One instrument to capture.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Instrument {
    /// The ticker, which is the venue's bare coin. May not hold a `:`.
    pub ticker: String,
    /// The builder-deployed dex this instrument lives on, where it is not on
    /// the main one. `Some("xyz")` makes the venue symbol `xyz:<ticker>`.
    pub dex: Option<String>,
}

impl Instrument {
    /// An instrument on the main perp dex.
    pub fn main(ticker: &str) -> Instrument {
        Instrument {
            ticker: ticker.to_string(),
            dex: None,
        }
    }

    /// An instrument on a builder-deployed dex.
    pub fn on_dex(ticker: &str, dex: &str) -> Instrument {
        Instrument {
            ticker: ticker.to_string(),
            dex: Some(dex.to_string()),
        }
    }

    /// The venue's own spelling: `<dex>:<coin>`, or the bare coin.
    pub fn venue_symbol(&self) -> String {
        match &self.dex {
            Some(dex) => format!("{dex}:{}", self.ticker),
            None => self.ticker.clone(),
        }
    }
}

/// How the adapter is configured. Nothing here could hold a secret.
#[derive(Debug, Clone)]
pub struct Config {
    /// Which network.
    pub market: Market,
    /// What to capture.
    pub instruments: Vec<Instrument>,
    /// The bar width subscribed live.
    pub candle_interval: String,
}

/// The adapter.
#[derive(Debug)]
pub struct Hyperliquid {
    venue: Venue,
    market: Market,
    declaration: Declaration,
    symbols: Symbols,
    candle_interval: String,
}

impl Construct for Hyperliquid {
    type Config = Config;

    fn new(config: Config, credential: Option<Credential>) -> Result<Hyperliquid, ConstructError> {
        // Refused rather than ignored: this venue's market data is public, and
        // being handed a key would mean somebody believed otherwise.
        if credential.is_some() {
            return Err(ConstructError::Rejected {
                venue: VENUE.into(),
                detail: "market data here is public; a credential was supplied and nothing would \
                         use it"
                    .into(),
            });
        }

        // HIP-3 is permissionless, so two dexes may each list `GOLD`. Within
        // one venue the ticker is a column rather than a partition, so the two
        // would collide — and choosing silently would make that name mean
        // whichever the iteration order picked.
        let mut seen: BTreeMap<&str, Option<&str>> = BTreeMap::new();
        for instrument in &config.instruments {
            if let Some(first) = seen.insert(&instrument.ticker, instrument.dex.as_deref()) {
                return Err(ConstructError::Rejected {
                    venue: VENUE.into(),
                    detail: format!(
                        "{} is declared twice, on dex {:?} and {:?}. Within one venue a ticker is \
                         a column rather than a partition, so the two would collide",
                        instrument.ticker, first, instrument.dex
                    ),
                });
            }
        }

        let mut symbols = Symbols::new();
        for instrument in &config.instruments {
            symbols.everywhere(&instrument.venue_symbol(), &instrument.ticker)?;
        }

        let declaration = Declaration {
            // The venue pushes all of these.
            streams: vec![
                Series::Trades,
                Series::Quotes,
                Series::Candles,
                Series::Funding,
            ],
            // It hands back candles and funding on request. It will not hand
            // back the book as it stood, at any price.
            historical: vec![Series::Candles, Series::Funding],
            paging: BTreeMap::from([
                (
                    Series::Candles,
                    // **Measured against mainnet and testnet, `candleSnapshot`:**
                    // the venue returns the most recent 5,000 bars per
                    // (coin, interval) whatever range is asked — a 400-day ask
                    // at 1m came back reaching 3.5 days, at 1h 208 days. Not a
                    // documented figure; if the reach the walk reports stops
                    // matching this, re-measure.
                    Paging::most_recent(5_000, Some(5_000), None),
                ),
                (
                    Series::Funding,
                    // **Measured, `fundingHistory`:** the oldest 500 hourly rows
                    // at or after `startTime`, paged forward, the venue's whole
                    // history in reach — a 1,500-day ask answered from 2023.
                    // The OPPOSITE direction to candles, on the same venue.
                    Paging::forward_from_start(500),
                ),
            ]),
            budget: Budget {
                // The venue's stated weight allowance, as requests a minute for
                // an info call. Per IP, and shared across every dex.
                requests_per_minute: 1_200.0,
                min_historical_interval_ms: 100,
            },
            connection: ConnectionPolicy::RotateAhead {
                observed_lifetime_secs: OBSERVED_LIFETIME_SECS,
                rotate_after_secs: ROTATE_AFTER_SECS,
                keepalive_secs: PING_INTERVAL_SECS,
            },
            ws_url: config.market.ws_url(),
            rest_url: config.market.rest_url(),
        };
        declaration.validate()?;

        Ok(Hyperliquid {
            venue: Venue::new(VENUE)?,
            market: config.market,
            declaration,
            symbols,
            candle_interval: config.candle_interval,
        })
    }
}

impl Hyperliquid {
    /// Which network this adapter speaks to.
    pub fn market(&self) -> Market {
        self.market
    }

    /// The symbol resolver.
    pub fn symbols(&self) -> &Symbols {
        &self.symbols
    }
}

impl Normalise for Hyperliquid {
    fn normalise(&self, payload: &Payload) -> Result<Vec<galata_wire::Envelope>, NormaliseError> {
        normalise::normalise(&self.venue, &self.symbols, payload)
    }

    fn venue(&self) -> &Venue {
        &self.venue
    }
}

impl Adapter for Hyperliquid {
    fn declaration(&self) -> &Declaration {
        &self.declaration
    }

    fn channel_of(&self, subscription: &Subscription) -> String {
        wire::channel_of(subscription.series)
            .unwrap_or("unknown")
            .to_string()
    }

    fn series_of_channel(&self, channel: &str) -> Option<Series> {
        wire::series_of_channel(channel)
    }

    /// One frame per subscription: this venue takes a single `coin` per
    /// subscription message.
    fn subscribe_frames(&self, subscriptions: &[Subscription]) -> Vec<String> {
        subscriptions
            .iter()
            .filter_map(|s| {
                let symbol = self.symbols.venue_symbol_for(&s.ticker)?;
                wire::subscribe_frame(s.series, symbol, &self.candle_interval)
            })
            .collect()
    }

    fn keepalive(&self) -> Keepalive {
        Keepalive::Frame(wire::ping_frame().to_string())
    }

    fn classify(&self, bytes: &[u8], recv_micros: i64) -> Payload {
        let (channel, symbol) = wire::envelope_of(bytes);
        Payload {
            seq: 0,
            recv_micros,
            address: PayloadAddress::Venue(VENUE.into()),
            kind: wire::series_of_channel(&channel)
                .map(|s| s.as_str().to_string())
                // A frame carrying no observation still arrived, and the record
                // records arrivals. It partitions under the venue's own channel
                // name rather than being dropped.
                .unwrap_or_else(|| channel.clone()),
            channel,
            symbol,
            origin: Origin::Streamed,
            payload: bytes.to_vec(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(instruments: Vec<Instrument>) -> Config {
        Config {
            market: Market::Mainnet,
            instruments,
            candle_interval: "1m".into(),
        }
    }

    fn shipped() -> Hyperliquid {
        Hyperliquid::new(
            config(vec![
                Instrument::main("BTC"),
                Instrument::main("ETH"),
                Instrument::main("HYPE"),
                Instrument::on_dex("WTIOIL", "xyz"),
                Instrument::on_dex("XYZ100", "xyz"),
                Instrument::on_dex("GOLD", "xyz"),
            ]),
            None,
        )
        .expect("the shipped configuration must construct")
    }

    #[test]
    fn a_credential_is_refused_rather_than_ignored() {
        let refused = Hyperliquid::new(
            config(vec![Instrument::main("BTC")]),
            Some(Credential::Endpoint {
                url: "https://example.invalid".into(),
            }),
        )
        .unwrap_err();
        assert!(refused.to_string().contains("public"), "{refused}");
    }

    #[test]
    fn a_dex_prefixed_symbol_yields_an_unprefixed_ticker() {
        let hl = shipped();
        let ticker = hl.symbols().resolve("bbo", "xyz:XYZ100").unwrap();
        assert_eq!(ticker.as_str(), "XYZ100");
        assert_eq!(hl.symbols().venue_symbol_for(ticker), Some("xyz:XYZ100"));
    }

    #[test]
    fn an_instrument_with_no_dex_is_unprefixed() {
        let hl = shipped();
        let ticker = hl.symbols().resolve("bbo", "BTC").unwrap();
        assert_eq!(hl.symbols().venue_symbol_for(ticker), Some("BTC"));
    }

    #[test]
    fn two_dexes_listing_one_ticker_are_refused() {
        // HIP-3 is permissionless. Both would answer to `GOLD`, and choosing
        // silently would make it mean whichever the iteration order picked.
        let refused = Hyperliquid::new(
            config(vec![
                Instrument::on_dex("GOLD", "xyz"),
                Instrument::on_dex("GOLD", "other"),
            ]),
            None,
        )
        .unwrap_err();
        let message = refused.to_string();
        assert!(message.contains("GOLD"), "{message}");
        assert!(
            message.contains("xyz") && message.contains("other"),
            "{message}"
        );
    }

    #[test]
    fn the_subscribe_frame_carries_the_prefix() {
        use galata_wire::Ticker;
        let hl = shipped();
        let frames = hl.subscribe_frames(&[
            Subscription {
                ticker: Ticker::new("XYZ100").unwrap(),
                series: Series::Quotes,
            },
            Subscription {
                ticker: Ticker::new("BTC").unwrap(),
                series: Series::Quotes,
            },
        ]);
        assert!(
            frames[0].contains(r#""coin":"xyz:XYZ100""#),
            "{}",
            frames[0]
        );
        assert!(frames[1].contains(r#""coin":"BTC""#), "{}", frames[1]);
    }

    #[test]
    fn the_bytes_are_unchanged_by_classification() {
        let hl = shipped();
        let frame = br#"{"channel":"bbo","data":{"coin":"BTC","time":1,"levels":[[],[]]}}"#;
        let payload = hl.classify(frame, 42);
        assert_eq!(payload.payload, frame.to_vec());
        assert_eq!(payload.channel, "bbo");
        assert_eq!(payload.kind, "quotes");
        assert_eq!(payload.recv_micros, 42);
    }

    #[test]
    fn an_unrecognised_channel_partitions_under_its_own_name() {
        let hl = shipped();
        let payload = hl.classify(br#"{"channel":"somethingNew","data":{}}"#, 0);
        assert_eq!(payload.kind, "somethingNew");
    }

    #[test]
    fn the_book_is_not_served_historically() {
        // The venue will hand back candles. It will not hand back the book as
        // it stood, at any price — so a walk attempts only what it serves and
        // the rest stay gaps.
        let hl = shipped();
        assert!(!hl.declaration().serves_historically(Series::Book));
        assert!(hl.declaration().serves_historically(Series::Candles));
    }

    #[test]
    fn the_candle_reach_is_a_row_count() {
        const MINUTE: i64 = 60_000_000;
        let hl = shipped();
        let at_1m = hl
            .declaration()
            .reach_micros(Series::Candles, MINUTE)
            .unwrap();
        let at_1h = hl
            .declaration()
            .reach_micros(Series::Candles, 60 * MINUTE)
            .unwrap();
        assert_eq!(at_1h, at_1m * 60, "the reach scales with the bar width");
    }

    #[test]
    fn funding_pages_the_other_way() {
        use crate::venue::PageDirection;
        let hl = shipped();
        assert_eq!(
            hl.declaration().paging(Series::Funding).unwrap().direction,
            PageDirection::ForwardFromStart
        );
        assert_eq!(
            hl.declaration().paging(Series::Candles).unwrap().direction,
            PageDirection::MostRecent
        );
    }

    #[test]
    fn an_unknown_market_is_refused_by_listing_the_known() {
        let err = Market::parse("devnet").unwrap_err().to_string();
        assert!(err.contains("mainnet") && err.contains("testnet"), "{err}");
    }
}
