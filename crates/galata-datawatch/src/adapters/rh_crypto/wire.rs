//! The `best_bid_ask` response, and the fields that actually matter.
//!
//! ```text
//!   results: [{
//!     symbol, side, price, quantity,
//!     bid_inclusive_of_sell_spread, sell_spread,
//!     ask_inclusive_of_buy_spread,  buy_spread,
//!     timestamp
//!   }]
//! ```
//!
//! # A documented disagreement, unresolved here
//!
//! One published description of this endpoint lists a top-level `price`.
//! A bug report against a client library says the opposite — that
//! `best_bid_ask` *structurally has no `price` field*, and that reading one
//! yields nothing.
//!
//! **This tree has been here before.** Hyperliquid's `bbo` was documented as
//! "functionally equivalent to `l2Book` with `nLevels: 1`" — true of meaning,
//! false of shape — and every frame failed to normalise until a live run showed
//! the real form. The lesson taken then applies now: *the record holds what
//! arrived, and the shape is read off disk rather than off a document.*
//!
//! So this reads `price` **where it is present** and never requires it. The
//! prices it relies on are the two spread-inclusive ones, which are the
//! tradeable numbers anyway — what you would actually pay.
//!
//! **This has not been checked against the live endpoint**, because that needs
//! credentials. Until it is, the shape below is a hypothesis with a test, not a
//! measurement.

use galata_wire::{Envelope, Event, Num, Quote, Ticker, Venue};

use crate::normalise::NormaliseError;

/// One symbol's top of book.
#[derive(Debug, Clone, PartialEq, serde::Deserialize)]
pub struct BestBidAsk {
    /// The pair, as the venue spells it: `BTC-USD`.
    pub symbol: String,
    /// The bid **a seller would actually receive**, spread included.
    ///
    /// This rather than a raw mid or a documented `price`: a broker's quote is
    /// what you would get, and the spread is how it is paid.
    #[serde(default)]
    pub bid_inclusive_of_sell_spread: Option<String>,
    /// The ask **a buyer would actually pay**, spread included.
    #[serde(default)]
    pub ask_inclusive_of_buy_spread: Option<String>,
    /// The spread taken on a sell.
    #[serde(default)]
    pub sell_spread: Option<String>,
    /// The spread taken on a buy.
    #[serde(default)]
    pub buy_spread: Option<String>,
    /// Where a quantity is stated.
    #[serde(default)]
    pub quantity: Option<String>,
    /// **Present in one description of this endpoint and reported absent by a
    /// client library's bug tracker.** Read where present, never required.
    #[serde(default)]
    pub price: Option<String>,
    /// The venue's own clock, ISO-8601.
    #[serde(default)]
    pub timestamp: Option<String>,
}

/// The envelope the endpoint answers with.
#[derive(Debug, Clone, PartialEq, serde::Deserialize)]
pub struct Response {
    /// One entry per symbol asked for.
    pub results: Vec<BestBidAsk>,
}

/// Turn one response into quotes.
///
/// **A symbol the caller did not ask about is skipped**, and an entry that
/// carries no prices at all becomes a quote with none rather than being
/// dropped: *this venue stated nothing* is a fact, and it is the fact a
/// cross-venue comparison needs in order to show a hole rather than a shorter
/// list.
pub fn read(
    venue: &Venue,
    response: &Response,
    tickers: &std::collections::BTreeMap<String, Ticker>,
    recv_micros: i64,
) -> Vec<Envelope> {
    let mut out = Vec::new();
    for entry in &response.results {
        let Some(ticker) = tickers.get(&entry.symbol) else {
            continue;
        };
        out.push(Envelope::new(
            venue.clone(),
            ticker.clone(),
            entry.at_micros(),
            recv_micros,
            Event::Quote(Quote {
                bid_px: num(&entry.bid_inclusive_of_sell_spread),
                ask_px: num(&entry.ask_inclusive_of_buy_spread),
                // This venue states one quantity, not one per side. Putting it
                // on both would claim a symmetry it did not state.
                bid_sz: num(&entry.quantity),
                ask_sz: num(&entry.quantity),
                bid_spread: num(&entry.sell_spread),
                ask_spread: num(&entry.buy_spread),
            }),
        ));
    }
    out
}

/// Parse a response, refusing a shape that is not one.
pub fn response(bytes: &[u8]) -> Result<Response, NormaliseError> {
    serde_json::from_slice(bytes).map_err(|e| NormaliseError::Shape {
        kind: "best_bid_ask",
        detail: e.to_string(),
    })
}

fn num(value: &Option<String>) -> Option<Num> {
    // A field the venue did not state is **absent**, not zero. Zero is a price.
    value.as_ref().and_then(|v| v.parse::<Num>().ok())
}

impl BestBidAsk {
    /// The venue's own time, where it stated one.
    ///
    /// **`None` rather than ours.** An entry with no timestamp is not *at* our
    /// receipt time; it is at a time the venue did not say.
    pub fn at_micros(&self) -> Option<i64> {
        let text = self.timestamp.as_ref()?;
        // `2026-09-21T10:15:30.123456Z`, parsed without a date library: the
        // fields are fixed-width and the alternative is a dependency for one
        // format.
        let (date, rest) = text.split_once('T')?;
        let time = rest.trim_end_matches('Z');
        let (hms, fraction) = match time.split_once('.') {
            Some((hms, fraction)) => (hms, fraction),
            None => (time, ""),
        };
        let mut ymd = date.split('-');
        let year: i64 = ymd.next()?.parse().ok()?;
        let month: u32 = ymd.next()?.parse().ok()?;
        let day: u32 = ymd.next()?.parse().ok()?;
        let mut parts = hms.split(':');
        let hour: i64 = parts.next()?.parse().ok()?;
        let minute: i64 = parts.next()?.parse().ok()?;
        let second: i64 = parts.next()?.parse().ok()?;

        let midnight = crate::calendar::midnight_of(&format!("{year:04}-{month:02}-{day:02}"))?;
        let micros: i64 = format!("{fraction:0<6}")
            .get(..6)
            .and_then(|f| f.parse().ok())
            .unwrap_or(0);
        Some(midnight + (hour * 3_600 + minute * 60 + second) * 1_000_000 + micros)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use std::str::FromStr;

    /// The shape as documented. **Not captured from the venue** — no
    /// credentials — so this is a hypothesis with a test rather than a
    /// measurement, and it says so.
    const RESPONSE: &str = r#"{
      "results": [{
        "symbol": "BTC-USD",
        "bid_inclusive_of_sell_spread": "81190.50",
        "sell_spread": "22.50",
        "ask_inclusive_of_buy_spread": "81235.50",
        "buy_spread": "22.50",
        "quantity": "0.5",
        "timestamp": "2026-09-21T10:15:30.123456Z"
      }]
    }"#;

    fn tickers() -> BTreeMap<String, Ticker> {
        BTreeMap::from([("BTC-USD".into(), Ticker::new("BTC").unwrap())])
    }

    fn venue() -> Venue {
        Venue::new("rh-crypto").unwrap()
    }

    #[test]
    fn the_prices_are_the_ones_you_would_actually_pay() {
        // A broker's quote is what you would GET, and the spread is how it is
        // paid. Using a raw mid would state a price nobody can trade at.
        let parsed = response(RESPONSE.as_bytes()).unwrap();
        let quotes = read(&venue(), &parsed, &tickers(), 100);
        assert_eq!(quotes.len(), 1);
        match &quotes[0].event {
            Event::Quote(q) => {
                assert_eq!(q.bid_px, Some(Num::from_str("81190.50").unwrap()));
                assert_eq!(q.ask_px, Some(Num::from_str("81235.50").unwrap()));
                assert_eq!(q.bid_spread, Some(Num::from_str("22.50").unwrap()));
                assert_eq!(q.ask_spread, Some(Num::from_str("22.50").unwrap()));
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn a_missing_price_is_absent_and_never_zero() {
        // **The documented disagreement.** One description of this endpoint
        // lists a top-level `price`; a client library's bug tracker says it has
        // none. So nothing here requires it, and a field the venue did not
        // state is absent rather than zero — zero is a price.
        let parsed =
            response(br#"{"results":[{"symbol":"BTC-USD","timestamp":"2026-09-21T10:15:30Z"}]}"#)
                .unwrap();
        let quotes = read(&venue(), &parsed, &tickers(), 100);
        assert_eq!(quotes.len(), 1, "an entry with no prices is still a fact");
        match &quotes[0].event {
            Event::Quote(q) => {
                assert_eq!(q.bid_px, None);
                assert_eq!(q.ask_px, None);
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn the_venue_clock_is_read_and_not_replaced_with_ours() {
        let parsed = response(RESPONSE.as_bytes()).unwrap();
        let quotes = read(&venue(), &parsed, &tickers(), 999);
        let at = quotes[0].at_micros.expect("the venue stated a time");
        assert_ne!(at, 999, "our clock was used as the venue's");
        // 2026-09-21T10:15:30.123456Z
        assert_eq!(
            at,
            crate::calendar::midnight_of("2026-09-21").unwrap()
                + (10 * 3_600 + 15 * 60 + 30) * 1_000_000
                + 123_456
        );
    }

    #[test]
    fn an_entry_with_no_timestamp_is_at_no_venue_time() {
        let parsed = response(br#"{"results":[{"symbol":"BTC-USD","quantity":"1"}]}"#).unwrap();
        let quotes = read(&venue(), &parsed, &tickers(), 500);
        assert_eq!(quotes[0].at_micros, None, "our clock was borrowed");
        assert_eq!(quotes[0].recv_micros, 500);
    }

    #[test]
    fn a_symbol_nobody_asked_about_is_skipped() {
        let parsed = response(br#"{"results":[{"symbol":"DOGE-USD","quantity":"1"}]}"#).unwrap();
        assert!(read(&venue(), &parsed, &tickers(), 100).is_empty());
    }

    #[test]
    fn a_response_that_is_not_one_is_refused() {
        assert!(response(b"{\"nope\":1}").is_err());
        assert!(response(b"not json").is_err());
    }
}
