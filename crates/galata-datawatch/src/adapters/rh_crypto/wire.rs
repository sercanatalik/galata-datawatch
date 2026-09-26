//! The `best_bid_ask` response, and the fields that actually matter.
//!
//! ```text
//!   v1  results: [{
//!         symbol, price,
//!         bid_inclusive_of_sell_spread, sell_spread,
//!         ask_inclusive_of_buy_spread,  buy_spread,
//!         timestamp
//!       }]
//! ```
//!
//! # What the published document says
//!
//! Robinhood's OpenAPI document, embedded in `docs.robinhood.com/crypto/trading/`
//! (read 2026-09-26, `design/measured.md`), settles what two secondary
//! sources disputed:
//!
//! - **v1 has `price`**, defined as the midpoint of the two spread-inclusive
//!   prices, and **no `quantity`**. That belongs to `estimated_price`.
//! - **v2** (`/api/v2/…`, partner exchanges, fee tiers) answers `symbol`,
//!   `bid` and `ask` alone. A bug report that `best_bid_ask` "structurally
//!   has no `price`" was describing that one.
//! - `sell_spread` and `buy_spread` are **"the percent difference between the
//!   bid (ask) and the mid price"**. They are percentages, not price
//!   differences. The quote carries them as stated.
//!
//! # What it does not settle: how a number is spelled
//!
//! The document types every price as a JSON **number**. At least one client
//! (`rizome-dev/go-robinhood`) parses JSON **strings** on purpose. This tree
//! has met that kind of gap before: Hyperliquid's `bbo` was documented as
//! "functionally equivalent to `l2Book` with `nLevels: 1`", which was true of
//! the meaning and false of the shape, and every frame failed to normalise
//! until a live run showed the real form. So each number is read **in either
//! spelling**, with its digits as the venue wrote them (never through a float;
//! `check-no-float-money.sh`), and `price` and `quantity` are read where
//! present and never required.
//!
//! **This has still not been checked against the live endpoint**, because
//! that needs credentials. The record will hold the first real answer.

use galata_wire::{Envelope, Event, Num, Quote, Ticker, Venue};

use crate::normalise::NormaliseError;

/// A number the venue wrote as a JSON string **or** a JSON number, kept as
/// the digits it wrote.
///
/// Through `RawValue`, never `serde_json::Number`: without
/// `arbitrary_precision` that holds an `f64`, and a price that has been
/// through a double cannot say which digits it lost. Any other JSON type is
/// a shape error naming the field.
fn number_text<'de, D>(deserializer: D, field: &str) -> Result<Option<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::Deserialize;
    use serde::de::Error;

    let Some(raw) = Option::<Box<serde_json::value::RawValue>>::deserialize(deserializer)? else {
        return Ok(None);
    };
    let text = raw.get();
    match text.as_bytes().first() {
        Some(b'"') => serde_json::from_str::<String>(text)
            .map(Some)
            .map_err(D::Error::custom),
        Some(b'-' | b'0'..=b'9') => Ok(Some(text.to_owned())),
        Some(b'n') if text == "null" => Ok(None),
        _ => Err(D::Error::custom(format!(
            "{field}: expected a number or a numeric string, got {text}"
        ))),
    }
}

/// One `deserialize_with` target per field, so a refusal names its field:
/// serde hands a field's deserializer no name of its own.
macro_rules! number_fields {
    ($($field:ident),* $(,)?) => {
        mod number_field {
            $(
                pub(super) fn $field<'de, D>(d: D) -> Result<Option<String>, D::Error>
                where
                    D: serde::Deserializer<'de>,
                {
                    super::number_text(d, stringify!($field))
                }
            )*
        }
    };
}

number_fields!(
    bid_inclusive_of_sell_spread,
    ask_inclusive_of_buy_spread,
    sell_spread,
    buy_spread,
    quantity,
    price,
);

/// One symbol's top of book.
#[derive(Debug, Clone, PartialEq, serde::Deserialize)]
pub struct BestBidAsk {
    /// The pair, as the venue spells it: `BTC-USD`.
    pub symbol: String,
    /// The bid **a seller would actually receive**, spread included.
    ///
    /// This rather than a raw mid or a documented `price`: a broker's quote is
    /// what you would get, and the spread is how it is paid.
    #[serde(
        default,
        deserialize_with = "number_field::bid_inclusive_of_sell_spread"
    )]
    pub bid_inclusive_of_sell_spread: Option<String>,
    /// The ask **a buyer would actually pay**, spread included.
    #[serde(
        default,
        deserialize_with = "number_field::ask_inclusive_of_buy_spread"
    )]
    pub ask_inclusive_of_buy_spread: Option<String>,
    /// The spread taken on a sell: **a percent of the mid**, as the document
    /// defines it, not a price difference.
    #[serde(default, deserialize_with = "number_field::sell_spread")]
    pub sell_spread: Option<String>,
    /// The spread taken on a buy: a percent of the mid.
    #[serde(default, deserialize_with = "number_field::buy_spread")]
    pub buy_spread: Option<String>,
    /// Where a quantity is stated. **v1's document states none**; read where
    /// present, never required.
    #[serde(default, deserialize_with = "number_field::quantity")]
    pub quantity: Option<String>,
    /// The midpoint, in v1's document; absent from v2's. Read where present,
    /// never required, and not used: the tradeable prices are the two above.
    #[serde(default, deserialize_with = "number_field::price")]
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

    /// The v1 shape as the published document gives it, with strings, which is
    /// how one client parses it. **Not captured from the venue** (no
    /// credentials), so it is a hypothesis with a test, and it says so. The
    /// spreads are percentages of the mid, as the document defines them:
    /// (81235.50 - 81213.00) / 81213.00 is about 0.0277%.
    const RESPONSE: &str = r#"{
      "results": [{
        "symbol": "BTC-USD",
        "bid_inclusive_of_sell_spread": "81190.50",
        "sell_spread": "0.0277",
        "ask_inclusive_of_buy_spread": "81235.50",
        "buy_spread": "0.0277",
        "quantity": "0.5",
        "timestamp": "2026-09-21T10:15:30.123456Z"
      }]
    }"#;

    /// The same entry as the document types it: every price a JSON number,
    /// with `price` (the midpoint) and no `quantity`.
    const DOCUMENTED: &str = r#"{
      "results": [{
        "symbol": "BTC-USD",
        "price": 81213.00,
        "bid_inclusive_of_sell_spread": 81190.50,
        "sell_spread": 0.0277,
        "ask_inclusive_of_buy_spread": 81235.50,
        "buy_spread": 0.0277,
        "timestamp": "2026-09-21T10:15:30.123456Z"
      }]
    }"#;

    fn quote(bytes: &[u8]) -> Quote {
        let parsed = response(bytes).unwrap();
        match read(&venue(), &parsed, &tickers(), 100).remove(0).event {
            Event::Quote(q) => q,
            other => panic!("{other:?}"),
        }
    }

    fn n(text: &str) -> Option<Num> {
        Some(Num::from_str(text).unwrap())
    }

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
                assert_eq!(q.bid_spread, Some(Num::from_str("0.0277").unwrap()));
                assert_eq!(q.ask_spread, Some(Num::from_str("0.0277").unwrap()));
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn a_missing_price_is_absent_and_never_zero() {
        // v2's answer has no `price` and v1's does, so nothing here requires
        // it. A field the venue did not state is absent rather than zero,
        // because zero is a price.
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
    fn numbers_as_the_document_types_them_are_exact() {
        let q = quote(DOCUMENTED.as_bytes());
        assert_eq!(q.bid_px, n("81190.50"), "the digits, not a double's");
        assert_eq!(q.ask_px, n("81235.50"));
        assert_eq!(q.bid_spread, n("0.0277"));
        // A double would have made this 0.1 + 0.2 = 0.30000000000000004.
        let exact = quote(br#"{"results":[{"symbol":"BTC-USD","bid_inclusive_of_sell_spread":0.30000000000000000001}]}"#);
        assert_eq!(exact.bid_px, n("0.30000000000000000001"));
    }

    #[test]
    fn numbers_as_strings_are_exact() {
        let q = quote(RESPONSE.as_bytes());
        assert_eq!(q.bid_px, n("81190.50"));
        assert_eq!(q.ask_px, n("81235.50"));
    }

    #[test]
    fn a_value_that_is_neither_is_refused_by_name() {
        let error = response(br#"{"results":[{"symbol":"BTC-USD","sell_spread":true}]}"#)
            .unwrap_err()
            .to_string();
        assert!(error.contains("sell_spread"), "{error}");
        assert!(response(br#"{"results":[{"symbol":"BTC-USD","price":{"v":1}}]}"#).is_err());
    }

    #[test]
    fn the_documents_full_v1_entry_is_one_quote_with_no_size() {
        let parsed = response(DOCUMENTED.as_bytes()).unwrap();
        assert_eq!(parsed.results[0].price.as_deref(), Some("81213.00"));
        let quotes = read(&venue(), &parsed, &tickers(), 100);
        assert_eq!(quotes.len(), 1);
        assert!(quotes[0].at_micros.is_some(), "the venue's own time");
        match &quotes[0].event {
            Event::Quote(q) => {
                assert_eq!((q.bid_px.is_some(), q.ask_px.is_some()), (true, true));
                assert_eq!(
                    (q.bid_spread.is_some(), q.ask_spread.is_some()),
                    (true, true)
                );
                // v1 states no quantity, so no size is claimed on either side.
                assert_eq!((q.bid_sz, q.ask_sz), (None, None));
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn an_exponent_is_read_exactly_and_not_through_a_float() {
        // A JSON number may be written `8.119e4`. rust_decimal parses that
        // spelling itself, as decimal, so it costs no digits either.
        let q =
            quote(br#"{"results":[{"symbol":"BTC-USD","bid_inclusive_of_sell_spread":8.119e4}]}"#);
        assert_eq!(q.bid_px, n("81190"));
    }

    #[test]
    fn a_response_that_is_not_one_is_refused() {
        assert!(response(b"{\"nope\":1}").is_err());
        assert!(response(b"not json").is_err());
    }
}
