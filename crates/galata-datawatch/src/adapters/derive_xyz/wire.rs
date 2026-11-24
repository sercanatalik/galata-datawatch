//! The venue's own shapes, and nothing that interprets them.
//!
//! Derive speaks JSON-RPC 2.0 over one socket and over HTTP POST, with the
//! same method names on both. Three shapes reach this adapter:
//!
//! ```text
//!   {"method":"subscription","params":{"channel":"ticker_slim.<name>.1000","data":{…}}}
//!   {"method":"subscription","params":{"channel":"spot_feed.<ccy>","data":{…}}}
//!   {"id":…,"result":{…}} | {"id":…,"error":{…}}        an answer to a request
//! ```
//!
//! **Every price, size and greek arrives as a JSON string** (read 2026-11-24),
//! so nothing here passes through a double. [`Token`] takes a number either
//! way, because a venue that documents strings has been known to send the
//! other.

use std::borrow::Cow;

use galata_wire::Token;
use serde::Deserialize;

/// The channel an option's ticker arrives on, as this adapter names it.
pub const TICKER: &str = "ticker_slim";
/// The channel an underlying's index arrives on.
pub const INDEX: &str = "spot_feed";
/// An answer to a request this adapter made on the socket: the subscribe
/// acknowledgement. Carries no observation.
pub const ANSWER: &str = "answer";
/// Something that is not one of the shapes above.
pub const UNKNOWN: &str = "unknown";

/// The interval, in milliseconds, the ticker is subscribed at.
///
/// The venue offers `100` and `1000`. One second, because the set is a list of
/// named options the owner reads, not a book being traded against.
pub const TICKER_INTERVAL_MS: u32 = 1_000;

/// The venue's code for a name it does not know.
///
/// Measured 2026-11-24: HTTP 200 with
/// `{"error":{"code":12001,"message":"Instrument not found"}}`, for a name
/// that never existed and for one that is not shaped like a name at all.
pub const INSTRUMENT_NOT_FOUND: i64 = 12_001;

/// A frame, peeked: only what says which shape it is.
#[derive(Debug, Deserialize)]
pub struct Peek<'a> {
    /// `subscription` on a pushed frame.
    #[serde(borrow, default)]
    pub method: Option<Cow<'a, str>>,
    /// The pushed frame's body.
    #[serde(borrow, default)]
    pub params: Option<PeekParams<'a>>,
    /// Present on an answer to a request.
    #[serde(borrow, default)]
    pub id: Option<&'a serde_json::value::RawValue>,
}

/// A pushed frame's `params`, peeked.
#[derive(Debug, Deserialize)]
pub struct PeekParams<'a> {
    /// The venue's channel, in full: `ticker_slim.<name>.1000`.
    #[serde(borrow, default)]
    pub channel: Option<Cow<'a, str>>,
}

/// A venue channel, split into the kind this adapter names and the symbol.
///
/// `ticker_slim.ETH-20261225-2200-C.1000` is `(ticker_slim, ETH-20261225-2200-C)`
/// and `spot_feed.BTC` is `(spot_feed, BTC)`. A channel of any other kind keeps
/// its own first segment and names no symbol: it still arrived, and the record
/// records arrivals.
pub fn split_channel(channel: &str) -> (String, Option<String>) {
    let mut parts = channel.splitn(2, '.');
    let kind = parts.next().unwrap_or_default();
    let rest = parts.next();
    match (kind, rest) {
        (TICKER, Some(rest)) => {
            // The interval is the last segment. An instrument's name holds no
            // `.`: a fractional strike is spelled with `_`.
            let symbol = rest.rsplit_once('.').map(|(name, _)| name).unwrap_or(rest);
            (TICKER.to_string(), Some(symbol.to_string()))
        }
        (INDEX, Some(symbol)) => (INDEX.to_string(), Some(symbol.to_string())),
        ("", _) => (UNKNOWN.to_string(), None),
        (other, _) => (other.to_string(), None),
    }
}

/// Which shape a frame is, and whose it is: `(channel, symbol)`.
pub fn envelope_of(bytes: &[u8]) -> (String, Option<String>) {
    let Ok(peek) = serde_json::from_slice::<Peek<'_>>(bytes) else {
        return (UNKNOWN.to_string(), None);
    };
    if peek.method.as_deref() == Some("subscription") {
        return match peek.params.and_then(|p| p.channel) {
            Some(channel) => split_channel(&channel),
            None => (UNKNOWN.to_string(), None),
        };
    }
    if peek.id.is_some() {
        return (ANSWER.to_string(), None);
    }
    (UNKNOWN.to_string(), None)
}

/// A pushed frame, typed down to its `data`.
#[derive(Debug, Deserialize)]
pub struct Pushed<T> {
    /// The body.
    pub params: PushedParams<T>,
}

/// A pushed frame's body.
#[derive(Debug, Deserialize)]
pub struct PushedParams<T> {
    /// The channel's payload.
    pub data: T,
}

/// A ticker frame's `data`.
#[derive(Debug, Deserialize)]
pub struct TickerData {
    /// The ticker itself.
    pub instrument_ticker: Ticker,
}

/// One instrument's ticker, in the venue's own one-letter keys.
///
/// Every number optional: a key the venue stops sending is a field that reads
/// absent, not a payload that stops parsing.
#[derive(Debug, Deserialize)]
pub struct Ticker {
    /// The snapshot's time, in venue milliseconds.
    pub t: i64,
    /// Best ask price.
    #[serde(default)]
    pub a: Option<Token>,
    /// Amount at the best ask.
    #[serde(rename = "A", default)]
    pub ask_amount: Option<Token>,
    /// Best bid price.
    #[serde(default)]
    pub b: Option<Token>,
    /// Amount at the best bid.
    #[serde(rename = "B", default)]
    pub bid_amount: Option<Token>,
    /// Mark price.
    #[serde(rename = "M", default)]
    pub mark: Option<Token>,
    /// Index price.
    #[serde(rename = "I", default)]
    pub index: Option<Token>,
    /// 24-hour statistics. Only the open interest is normalised.
    #[serde(default)]
    pub stats: Option<Stats>,
    /// The venue's own pricing of an option. `null` for anything else.
    #[serde(default)]
    pub option_pricing: Option<Pricing>,
}

/// The ticker's statistics. The archive holds the rest.
#[derive(Debug, Deserialize)]
pub struct Stats {
    /// Open interest.
    #[serde(default)]
    pub oi: Option<Token>,
}

/// The venue's pricing of one option.
#[derive(Debug, Deserialize)]
pub struct Pricing {
    /// Implied volatility at the mark.
    #[serde(default)]
    pub i: Option<Token>,
    /// Implied volatility at the best bid.
    #[serde(default)]
    pub bi: Option<Token>,
    /// Implied volatility at the best ask.
    #[serde(default)]
    pub ai: Option<Token>,
    /// Delta.
    #[serde(default)]
    pub d: Option<Token>,
    /// Gamma.
    #[serde(default)]
    pub g: Option<Token>,
    /// Vega.
    #[serde(default)]
    pub v: Option<Token>,
    /// Theta.
    #[serde(default)]
    pub t: Option<Token>,
    /// Rho.
    #[serde(default)]
    pub r: Option<Token>,
    /// The forward priced against.
    #[serde(default)]
    pub f: Option<Token>,
    /// The discount factor priced with.
    #[serde(default)]
    pub df: Option<Token>,
}

/// An index frame's `data`.
#[derive(Debug, Deserialize)]
pub struct IndexData {
    /// The feed's time, in venue milliseconds.
    pub timestamp: i64,
    /// One entry per currency. A subscription to one currency carries one.
    pub feeds: std::collections::BTreeMap<String, IndexFeed>,
}

/// One currency's index.
#[derive(Debug, Deserialize)]
pub struct IndexFeed {
    /// The index price.
    pub price: Token,
}

/// An answer to `public/get_instrument`.
#[derive(Debug, Deserialize)]
pub struct InstrumentAnswer {
    /// The instrument, where the venue knows the name.
    #[serde(default)]
    pub result: Option<InstrumentSpec>,
    /// Why not, where it does not.
    #[serde(default)]
    pub error: Option<RpcError>,
}

/// A JSON-RPC error.
#[derive(Debug, Deserialize)]
pub struct RpcError {
    /// The venue's code.
    pub code: i64,
    /// The venue's words.
    #[serde(default)]
    pub message: String,
}

/// One instrument's reference data.
#[derive(Debug, Deserialize)]
pub struct InstrumentSpec {
    /// The venue's name for it.
    pub instrument_name: String,
    /// `option`, `perp` or `erc20`.
    pub instrument_type: String,
    /// Whether the venue lists it now. **False once it has expired.**
    pub is_active: bool,
    /// The smallest price increment.
    pub tick_size: Token,
    /// The smallest order.
    pub minimum_amount: Token,
    /// The increment an order's size moves in.
    pub amount_step: Token,
    /// What is being traded.
    pub base_currency: String,
    /// What it is priced in.
    pub quote_currency: String,
    /// The option's own terms. `null` for anything that is not one.
    #[serde(default)]
    pub option_details: Option<OptionDetails>,
}

/// An option's terms.
#[derive(Debug, Deserialize)]
pub struct OptionDetails {
    /// The index it settles against, as the venue names it: `BTC-USD`.
    pub index: String,
    /// When it expires, in **seconds**. The venue's own, and the only source
    /// of the hour: the name carries a date.
    pub expiry: i64,
    /// The strike.
    pub strike: Token,
    /// `C` or `P`.
    pub option_type: String,
}

/// The one request this adapter sends on the socket.
///
/// **One frame for the whole set.** The venue takes up to 10,000 channels in a
/// single `subscribe` and answers with a status per channel.
pub fn subscribe_frame(channels: &[String]) -> String {
    serde_json::json!({
        "method": "subscribe",
        "params": { "channels": channels },
        "id": 1,
    })
    .to_string()
}

/// The venue's channel for one option's ticker.
pub fn ticker_channel(name: &str) -> String {
    format!("{TICKER}.{name}.{TICKER_INTERVAL_MS}")
}

/// The venue's channel for one currency's index.
pub fn index_channel(currency: &str) -> String {
    format!("{INDEX}.{currency}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_ticker_channel_names_its_instrument_and_not_its_interval() {
        assert_eq!(
            split_channel("ticker_slim.ETH-20261225-2200-C.1000"),
            (TICKER.to_string(), Some("ETH-20261225-2200-C".to_string()))
        );
        // A fractional strike is spelled with `_`, so the last `.` is still
        // the interval's.
        assert_eq!(
            split_channel("ticker_slim.ADA-20261127-0_15-C.100"),
            (TICKER.to_string(), Some("ADA-20261127-0_15-C".to_string()))
        );
    }

    #[test]
    fn an_index_channel_names_its_currency() {
        assert_eq!(
            split_channel("spot_feed.BTC"),
            (INDEX.to_string(), Some("BTC".to_string()))
        );
    }

    #[test]
    fn a_channel_this_adapter_does_not_use_keeps_its_own_name() {
        // It arrived, so it is recorded under what the venue called it.
        assert_eq!(
            split_channel("trades.BTC-20261225-60000-P"),
            ("trades".to_string(), None)
        );
        assert_eq!(split_channel(""), (UNKNOWN.to_string(), None));
    }

    #[test]
    fn an_answer_is_not_an_observation() {
        let ack = br#"{"id":1,"result":{"status":{"spot_feed.BTC":"ok"},"current_subscriptions":["spot_feed.BTC"]}}"#;
        assert_eq!(envelope_of(ack), (ANSWER.to_string(), None));
        // A text `pong` is not JSON at all.
        assert_eq!(envelope_of(b"pong"), (UNKNOWN.to_string(), None));
    }

    #[test]
    fn the_subscribe_frame_carries_every_channel_once() {
        let frame =
            subscribe_frame(&[ticker_channel("BTC-20261225-60000-P"), index_channel("BTC")]);
        let json: serde_json::Value = serde_json::from_str(&frame).unwrap();
        assert_eq!(json["method"], "subscribe");
        assert_eq!(
            json["params"]["channels"],
            serde_json::json!(["ticker_slim.BTC-20261225-60000-P.1000", "spot_feed.BTC"])
        );
    }
}
