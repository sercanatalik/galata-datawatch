//! Hyperliquid's frame shapes, and the borrowed peek that routes them.

use galata_wire::{Series, Token};
use serde::Deserialize;

/// The venue's channel name for a series.
pub fn channel_of(series: Series) -> Option<&'static str> {
    match series {
        Series::Trades => Some("trades"),
        // `bbo` rather than `l2Book`: it is functionally `l2Book` with
        // `nLevels: 1, strict: true`, and it is emitted **only when the top of
        // book changes on a block** — so the rate is bounded by block cadence
        // and by change, where the public `l2Book` is a throttled snapshot on a
        // timer whatever happened.
        Series::Quotes => Some("bbo"),
        Series::Book => Some("l2Book"),
        Series::Candles => Some("candle"),
        Series::Funding => Some("activeAssetCtx"),
        // Transfers and mints are a chain's, not a perp venue's. And the
        // catch-all is required rather than tidy: `Series` is
        // `#[non_exhaustive]`, so a series added later must not break an
        // adapter written outside this tree — it answers "I do not carry that"
        // instead of failing to compile.
        Series::Transfers | Series::Mints | _ => None,
    }
}

/// Which series a channel's payloads belong to.
///
/// `None` for a channel that carries no observation — the control frames — and
/// for one never seen. The two are distinguished in `normalise`, not here: a
/// channel that carries nothing is not an anomaly, and one we do not understand
/// is.
pub fn series_of_channel(channel: &str) -> Option<Series> {
    match channel {
        "trades" => Some(Series::Trades),
        "bbo" => Some(Series::Quotes),
        "l2Book" => Some(Series::Book),
        "candle" | "candleSnapshot" => Some(Series::Candles),
        "activeAssetCtx" | "fundingHistory" => Some(Series::Funding),
        _ => None,
    }
}

/// Frames the venue sends as a matter of course: a subscription
/// acknowledgement, a heartbeat reply, a post response.
///
/// They carry no observation, so they normalise to nothing — and they are
/// **not** anomalies. Reporting them as such would fire the anomaly path on
/// every subscribe, and an anomaly that fires constantly is one nobody reads.
pub fn is_control_frame(channel: &str) -> bool {
    matches!(channel, "subscriptionResponse" | "pong" | "post" | "error")
}

/// The keepalive.
pub fn ping_frame() -> &'static str {
    r#"{"method":"ping"}"#
}

/// The subscribe frame for one series and venue symbol.
///
/// `symbol` is the venue's own spelling, already carrying a dex prefix where the
/// instrument has one — `xyz:XYZ100` rather than `XYZ100`.
pub fn subscribe_frame(series: Series, symbol: &str, candle_interval: &str) -> Option<String> {
    let inner = match series {
        Series::Trades => format!(r#"{{"type":"trades","coin":"{symbol}"}}"#),
        Series::Quotes => format!(r#"{{"type":"bbo","coin":"{symbol}"}}"#),
        Series::Book => format!(r#"{{"type":"l2Book","coin":"{symbol}"}}"#),
        Series::Candles => {
            format!(r#"{{"type":"candle","coin":"{symbol}","interval":"{candle_interval}"}}"#)
        }
        Series::Funding => format!(r#"{{"type":"activeAssetCtx","coin":"{symbol}"}}"#),
        // As above: non-exhaustive, so an unknown series is one this venue
        // does not carry rather than a compile error in somebody else's crate.
        Series::Transfers | Series::Mints | _ => return None,
    };
    Some(format!(
        r#"{{"method":"subscribe","subscription":{inner}}}"#
    ))
}

/// A frame's envelope, **borrowed** from the bytes: the channel, and `data` as
/// the raw span it occupies — read once, interpreted by the normaliser alone.
///
/// `Cow` rather than `&str` so an escaped string still reads; the common case
/// borrows and allocates nothing.
#[derive(Debug, Deserialize)]
pub struct Peek<'a> {
    /// The channel, where the frame names one.
    #[serde(default, borrow)]
    pub channel: Option<std::borrow::Cow<'a, str>>,
    /// The payload span, uninterpreted.
    #[serde(default, borrow)]
    pub data: Option<&'a serde_json::value::RawValue>,
}

/// The one field the peek wants from `data`: the venue's own symbol, as `coin`
/// on most channels and `s` on a candle. Every other field is skipped by the
/// deserializer without being allocated.
#[derive(Debug, Deserialize)]
struct Named<'a> {
    #[serde(default, borrow, alias = "s")]
    coin: Option<std::borrow::Cow<'a, str>>,
}

/// A frame's channel and the venue's own symbol, read without altering the
/// bytes — one borrowed pass over the frame, one over `data`, no tree built.
///
/// A frame whose envelope cannot be read is not discarded: it arrived, and the
/// record records arrivals. It is reported under `unknown`, which `normalise`
/// refuses — so the bytes are kept and the anomaly is emitted.
pub fn envelope_of(bytes: &[u8]) -> (String, Option<String>) {
    let Ok(peek) = serde_json::from_slice::<Peek<'_>>(bytes) else {
        return ("unknown".to_string(), None);
    };
    let channel = peek
        .channel
        .map(|c| c.into_owned())
        .unwrap_or_else(|| "unknown".to_string());
    let symbol = peek.data.and_then(|d| symbol_of(d.get()));
    (channel, symbol)
}

/// The symbol in a `data` span: an object's `coin` or `s`, or — a trades frame
/// is an array, and every element carries the same coin — the first element's.
fn symbol_of(data: &str) -> Option<String> {
    if let Ok(named) = serde_json::from_str::<Named<'_>>(data) {
        return named.coin.map(|c| c.into_owned());
    }
    serde_json::from_str::<Vec<Named<'_>>>(data)
        .ok()
        .and_then(|all| all.into_iter().next())
        .and_then(|first| first.coin.map(|c| c.into_owned()))
}

// ---- the typed shapes -----------------------------------------------------

/// One printed execution.
#[derive(Debug, Deserialize)]
pub struct WsTrade {
    /// The venue's own symbol.
    pub coin: String,
    /// `"B"` or `"A"` — the venue's own convention for which side crossed, not
    /// one we invent.
    pub side: String,
    /// Price.
    pub px: Token,
    /// Size.
    pub sz: Token,
    /// Venue milliseconds.
    pub time: i64,
    /// The venue's trade identity, where it states one.
    #[serde(default)]
    pub tid: Option<u64>,
}

/// One price level.
#[derive(Debug, Deserialize)]
pub struct WsLevel {
    /// Price.
    pub px: Token,
    /// Size.
    pub sz: Token,
    /// How many orders rest there, where the venue states it.
    #[serde(default)]
    pub n: Option<u32>,
}

/// A book frame. `bbo` and `l2Book` share this shape — the former is the latter
/// at one level a side.
#[derive(Debug, Deserialize)]
pub struct WsBook {
    /// The venue's own symbol.
    pub coin: String,
    /// `[bids, asks]`, in that order. A side may be empty.
    pub levels: Vec<Vec<Option<WsLevel>>>,
    /// Venue milliseconds.
    pub time: i64,
}

/// One bar.
#[derive(Debug, Deserialize)]
pub struct WsCandle {
    /// Open, venue milliseconds.
    pub t: i64,
    /// The venue's own symbol.
    pub s: String,
    /// The bar width, as the venue spells it.
    pub i: String,
    /// Open.
    pub o: Token,
    /// Close.
    pub c: Token,
    /// High.
    pub h: Token,
    /// Low.
    pub l: Token,
    /// Volume.
    pub v: Token,
    /// Trade count, where stated.
    #[serde(default)]
    pub n: Option<u32>,
}

/// One asset's context, pushed as it changes.
#[derive(Debug, Deserialize)]
pub struct WsActiveAssetCtx {
    /// The venue's own symbol.
    pub coin: String,
    /// The context.
    pub ctx: AssetCtx,
}

/// Four different prices and an open interest, none derivable from another.
#[derive(Debug, Deserialize, Default)]
pub struct AssetCtx {
    /// What the venue marks positions at — margin and liquidation.
    #[serde(default, rename = "markPx")]
    pub mark_px: Option<Token>,
    /// The externally-sourced price. On this venue it comes from validators and
    /// drives funding, while the mark is derived from it together with the book
    /// and drives liquidation — a position can be liquidated at a price no
    /// trade ever printed.
    #[serde(default, rename = "oraclePx")]
    pub oracle_px: Option<Token>,
    /// The midpoint.
    #[serde(default, rename = "midPx")]
    pub mid_px: Option<Token>,
    /// Open interest.
    #[serde(default, rename = "openInterest")]
    pub open_interest: Option<Token>,
    /// The current funding rate.
    #[serde(default)]
    pub funding: Option<Token>,
}

/// One row of a funding history page: the rate that settled at `time`.
///
/// `premium` is on the wire and not carried — the event has no field for it,
/// and the record keeps the bytes.
#[derive(Debug, Deserialize)]
pub struct FundingRow {
    /// The venue's own symbol.
    pub coin: String,
    /// The rate.
    #[serde(rename = "fundingRate")]
    pub funding_rate: Token,
    /// Venue milliseconds.
    pub time: i64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_control_frame_is_recognised_as_one() {
        // Not an anomaly. An anomaly that fires on every subscribe is one
        // nobody reads.
        for channel in ["subscriptionResponse", "pong", "post", "error"] {
            assert!(is_control_frame(channel), "{channel}");
            assert_eq!(series_of_channel(channel), None);
        }
        assert!(!is_control_frame("trades"));
    }

    #[test]
    fn a_frame_is_peeked_without_building_a_tree() {
        let frame = br#"{"channel":"bbo","data":{"coin":"BTC","time":1,"levels":[[],[]]}}"#;
        let (channel, symbol) = envelope_of(frame);
        assert_eq!(channel, "bbo");
        assert_eq!(symbol.as_deref(), Some("BTC"));
    }

    #[test]
    fn a_candle_names_its_symbol_under_s() {
        let frame = br#"{"channel":"candle","data":{"s":"ETH","i":"1m","t":0}}"#;
        assert_eq!(envelope_of(frame).1.as_deref(), Some("ETH"));
    }

    #[test]
    fn a_trades_array_names_the_first_elements_coin() {
        let frame = br#"{"channel":"trades","data":[{"coin":"SOL","px":"1","sz":"2"}]}"#;
        assert_eq!(envelope_of(frame).1.as_deref(), Some("SOL"));
    }

    #[test]
    fn a_dex_prefixed_symbol_survives_the_peek() {
        let frame = br#"{"channel":"bbo","data":{"coin":"xyz:XYZ100","time":1,"levels":[[],[]]}}"#;
        assert_eq!(envelope_of(frame).1.as_deref(), Some("xyz:XYZ100"));
    }

    #[test]
    fn an_unreadable_frame_is_still_addressable() {
        // It arrived, and the record records arrivals. `unknown` is a channel
        // normalise refuses, so the bytes are kept and the anomaly is emitted.
        let (channel, symbol) = envelope_of(b"not json at all");
        assert_eq!(channel, "unknown");
        assert_eq!(symbol, None);
    }

    #[test]
    fn a_subscribe_frame_carries_the_venues_own_spelling() {
        let frame = subscribe_frame(Series::Quotes, "xyz:XYZ100", "1m").unwrap();
        assert!(frame.contains(r#""type":"bbo""#));
        assert!(frame.contains(r#""coin":"xyz:XYZ100""#));
    }

    #[test]
    fn a_series_this_venue_does_not_carry_has_no_frame() {
        // Transfers and mints are a chain's, not a perp venue's.
        assert_eq!(subscribe_frame(Series::Transfers, "BTC", "1m"), None);
        assert_eq!(channel_of(Series::Mints), None);
    }
}
