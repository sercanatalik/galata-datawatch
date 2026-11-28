//! **Prices now, as JSON** (`market-prices`, `move-watcher-onto-markets`
//! design D8).
//!
//! ```text
//!   GET /v1/prices?venue&tickers[&since]
//! ```
//!
//! For each ticker, the newest mid and the newest mark the market window
//! holds, each with its time, and its **price**: the mid, or the mark where
//! there is no mid. A quote's mid is halfway between its bid and its ask; a
//! mark states the venue's own mid beside its mark. With `since`, the highest
//! and lowest price since that moment, each with when it was seen, from every
//! quote and mark since then.
//!
//! **Read from the market window alone**, seconds old, by consumers
//! galata-markets already runs: one delivering the last message on each
//! subject, and one replaying from a moment. So its bus grant is unchanged. A
//! ticker the window holds nothing of is stated `absent`, never zero.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use async_nats::jetstream::{self, consumer::DeliverPolicy, consumer::pull::OrderedConfig};
use axum::Json;
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use futures_util::StreamExt;
use galata_broker::streams::MARKET_STREAM;
use galata_wire::{Envelope, Event, Num};
use serde::Deserialize;
use serde_json::{Value, json};

use crate::Shared;

/// The most tickers one question names: the instrument set's cap, as the live
/// stream's.
pub const MAX_TICKERS: usize = crate::live::MAX_TICKERS;

/// The most messages one range reads before it is refused, never cut short:
/// two hours of one busy ticker's quotes is well under it.
pub const MAX_MESSAGES: u64 = 2_000_000;

/// How long a question waits for the window.
const WAIT: Duration = Duration::from_secs(20);

/// What a client asks.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct PricesQuery {
    /// The venue.
    pub venue: String,
    /// Tickers, comma-separated.
    pub tickers: String,
    /// A moment, micros, to give each ticker's range since.
    pub since: Option<i64>,
}

/// Why prices were not given.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
#[non_exhaustive]
pub enum Refused {
    /// Something asked for is not a name, or too much.
    #[error("{0}")]
    Asked(String),
    /// A moment the window no longer holds.
    #[error(
        "{since} is older than the market window, whose oldest moment is {oldest}: ask the candles for older prices"
    )]
    BeforeWindow {
        /// What was asked.
        since: i64,
        /// The window's oldest moment, micros.
        oldest: i64,
    },
    /// More messages than one answer reads.
    #[error("the range holds more than {0} messages; ask since a later moment")]
    TooMuch(u64),
    /// The bus could not be asked, or did not answer in time.
    #[error("the bus is not reachable: {0}")]
    Bus(String),
}

impl IntoResponse for Refused {
    fn into_response(self) -> Response {
        let status = match self {
            Refused::Asked(_) | Refused::TooMuch(_) => StatusCode::BAD_REQUEST,
            Refused::BeforeWindow { .. } => StatusCode::GONE,
            Refused::Bus(_) => StatusCode::SERVICE_UNAVAILABLE,
        };
        (status, format!("{self}\n")).into_response()
    }
}

fn bus(error: impl std::fmt::Display) -> Refused {
    Refused::Bus(error.to_string())
}

/// A venue or a ticker as one subject token: anything but `.`, `*`, `>` and
/// white space, which would reach past it.
fn token(what: &str, value: &str) -> Result<(), Refused> {
    let ok = !value.is_empty()
        && value.len() <= 64
        && !value
            .chars()
            .any(|c| c == '.' || c == '*' || c == '>' || c.is_whitespace());
    if ok {
        Ok(())
    } else {
        Err(Refused::Asked(format!("{what} {value:?} is not a name")))
    }
}

/// A question, checked: the venue and its tickers.
pub fn tickers_of(query: &PricesQuery) -> Result<BTreeSet<String>, Refused> {
    token("venue", &query.venue)?;
    let tickers: BTreeSet<String> = query
        .tickers
        .split(',')
        .map(str::trim)
        .filter(|t| !t.is_empty())
        .map(str::to_string)
        .collect();
    if tickers.is_empty() {
        return Err(Refused::Asked("name at least one ticker".into()));
    }
    if tickers.len() > MAX_TICKERS {
        return Err(Refused::Asked(format!(
            "{} tickers asked; one question names at most {MAX_TICKERS}",
            tickers.len()
        )));
    }
    for ticker in &tickers {
        token("ticker", ticker)?;
    }
    Ok(tickers)
}

/// The window's subjects for a ticker's prices.
fn subjects(venue: &str, tickers: &BTreeSet<String>) -> Vec<String> {
    tickers
        .iter()
        .flat_map(|t| {
            [
                format!("markets.{venue}.{t}.quotes"),
                format!("markets.{venue}.{t}.marks"),
            ]
        })
        .collect()
}

/// A price seen, and when.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Seen {
    /// The price.
    pub value: Num,
    /// When: the venue's time, else when it was heard.
    pub at_micros: i64,
}

/// What one ticker's messages say.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Held {
    /// The newest mid, from a quote or a mark.
    pub mid: Option<Seen>,
    /// The newest mark.
    pub mark: Option<Seen>,
    /// The highest price seen.
    pub high: Option<Seen>,
    /// The lowest price seen.
    pub low: Option<Seen>,
}

impl Held {
    /// Its price: the mid, or the mark where there is no mid.
    pub fn price(&self) -> Option<Seen> {
        self.mid.or(self.mark)
    }

    /// Take one message's prices in.
    pub fn take(&mut self, envelope: &Envelope) {
        let at = envelope.at_micros.unwrap_or(envelope.recv_micros);
        let newer = |held: Option<Seen>| held.is_none_or(|h| h.at_micros <= at);
        let (mid, mark) = match &envelope.event {
            Event::Quote(q) => (
                q.bid_px
                    .zip(q.ask_px)
                    .map(|(bid, ask)| ((bid + ask) / Num::from(2)).normalize()),
                None,
            ),
            Event::Mark(m) => (m.mid, m.mark),
            _ => (None, None),
        };
        if let Some(value) = mid
            && newer(self.mid)
        {
            self.mid = Some(Seen {
                value,
                at_micros: at,
            });
        }
        if let Some(value) = mark
            && newer(self.mark)
        {
            self.mark = Some(Seen {
                value,
                at_micros: at,
            });
        }
        // The range: each message's own price, its mid or else its mark.
        if let Some(value) = mid.or(mark) {
            let seen = Seen {
                value,
                at_micros: at,
            };
            if self.high.is_none_or(|h| value > h.value) {
                self.high = Some(seen);
            }
            if self.low.is_none_or(|l| value < l.value) {
                self.low = Some(seen);
            }
        }
    }
}

/// **Read a set of subjects** from a deliver policy to the end of what the
/// window holds now, each message taken into its ticker.
async fn read(
    js: &jetstream::Context,
    venue: &str,
    tickers: &BTreeSet<String>,
    deliver_policy: DeliverPolicy,
    held: &mut BTreeMap<String, Held>,
) -> Result<(), Refused> {
    let consumer = js
        .get_stream(MARKET_STREAM)
        .await
        .map_err(bus)?
        .create_consumer(OrderedConfig {
            filter_subjects: subjects(venue, tickers),
            deliver_policy,
            ..Default::default()
        })
        .await
        .map_err(bus)?;
    let pending = consumer.cached_info().num_pending;
    if pending > MAX_MESSAGES {
        return Err(Refused::TooMuch(MAX_MESSAGES));
    }
    if pending == 0 {
        return Ok(());
    }
    let mut messages = consumer.messages().await.map_err(bus)?;
    while let Some(message) = messages.next().await {
        let message = message.map_err(bus)?;
        let left = message.info().map_err(bus)?.pending;
        let envelope = galata_broker::decode(&message.payload).map_err(bus)?;
        if let Some(ticker) = envelope.ticker()
            && let Some(one) = held.get_mut(ticker.as_str())
        {
            one.take(&envelope);
        }
        if left == 0 {
            break;
        }
    }
    Ok(())
}

/// A moment, as async-nats takes one.
fn moment(micros: i64) -> Result<time::OffsetDateTime, Refused> {
    time::OffsetDateTime::from_unix_timestamp_nanos(i128::from(micros) * 1_000)
        .map_err(|e| Refused::Asked(format!("since {micros}: {e}")))
}

/// **Each ticker's prices**: the newest, and the range since a moment.
pub async fn prices(
    js: &jetstream::Context,
    query: &PricesQuery,
) -> Result<(BTreeMap<String, Held>, Vec<String>), Refused> {
    let tickers = tickers_of(query)?;
    let mut held: BTreeMap<String, Held> = tickers
        .iter()
        .map(|t| (t.clone(), Held::default()))
        .collect();
    let asked = async {
        // The newest of each subject. Its own range is set aside: the range
        // is the replay's, where one was asked for.
        read(
            js,
            &query.venue,
            &tickers,
            DeliverPolicy::LastPerSubject,
            &mut held,
        )
        .await?;
        for one in held.values_mut() {
            one.high = None;
            one.low = None;
        }
        if let Some(since) = query.since {
            let mut stream = js.get_stream(MARKET_STREAM).await.map_err(bus)?;
            let state = stream.info().await.map_err(bus)?.state.clone();
            let oldest = i64::try_from(state.first_timestamp.unix_timestamp_nanos() / 1_000)
                .unwrap_or(i64::MAX);
            if state.messages > 0 && since < oldest {
                return Err(Refused::BeforeWindow { since, oldest });
            }
            let mut ranged: BTreeMap<String, Held> = tickers
                .iter()
                .map(|t| (t.clone(), Held::default()))
                .collect();
            read(
                js,
                &query.venue,
                &tickers,
                DeliverPolicy::ByStartTime {
                    start_time: moment(since)?,
                },
                &mut ranged,
            )
            .await?;
            for (ticker, range) in ranged {
                if let Some(one) = held.get_mut(&ticker) {
                    one.high = range.high;
                    one.low = range.low;
                }
            }
        }
        Ok::<_, Refused>(())
    };
    tokio::time::timeout(WAIT, asked)
        .await
        .map_err(|_| Refused::Bus("the market window did not answer in time".into()))??;
    let absent: Vec<String> = held
        .iter()
        .filter(|(_, h)| h.price().is_none())
        .map(|(t, _)| t.clone())
        .collect();
    held.retain(|_, h| h.price().is_some());
    Ok((held, absent))
}

fn seen(value: Option<Seen>) -> Value {
    match value {
        Some(s) => json!({ "value": s.value.to_string(), "at_micros": s.at_micros }),
        None => Value::Null,
    }
}

/// The answer, as sent.
pub fn answer(query: &PricesQuery, held: &BTreeMap<String, Held>, absent: &[String]) -> Value {
    let prices: serde_json::Map<String, Value> = held
        .iter()
        .map(|(ticker, h)| {
            let mut one = json!({
                "price": seen(h.price()),
                "mid": seen(h.mid),
                "mark": seen(h.mark),
            });
            if query.since.is_some() {
                one["high"] = seen(h.high);
                one["low"] = seen(h.low);
            }
            (ticker.clone(), one)
        })
        .collect();
    json!({
        "venue": query.venue,
        "since": query.since,
        "prices": prices,
        "absent": absent,
    })
}

/// `GET /v1/prices`.
pub async fn prices_route(
    State(markets): State<Shared>,
    Query(query): Query<PricesQuery>,
) -> Response {
    if let Err(refused) = tickers_of(&query) {
        return refused.into_response();
    }
    let Some(client) = markets.bus.clone() else {
        return Refused::Bus("galata-markets is not connected to the bus".into()).into_response();
    };
    match prices(&jetstream::new(client), &query).await {
        Ok((held, absent)) => Json(answer(&query, &held, &absent)).into_response(),
        Err(refused) => refused.into_response(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use galata_wire::{Mark, Quote, Ticker, Venue};
    use std::str::FromStr;

    fn n(s: &str) -> Num {
        Num::from_str(s).unwrap()
    }

    fn quote(bid: &str, ask: &str, at: i64) -> Envelope {
        Envelope::new(
            Venue::new("hyperliquid").unwrap(),
            Ticker::new("BTC").unwrap(),
            Some(at),
            at,
            Event::Quote(Quote {
                bid_px: Some(n(bid)),
                ask_px: Some(n(ask)),
                ..Quote::default()
            }),
        )
    }

    fn mark(mark: &str, mid: Option<&str>, at: i64) -> Envelope {
        Envelope::new(
            Venue::new("hyperliquid").unwrap(),
            Ticker::new("BTC").unwrap(),
            Some(at),
            at,
            Event::Mark(Mark {
                mark: Some(n(mark)),
                mid: mid.map(n),
                ..Mark::default()
            }),
        )
    }

    #[test]
    fn the_price_is_the_newest_mid_else_the_mark() {
        let mut held = Held::default();
        held.take(&mark("61000", None, 10));
        assert_eq!(
            held.price().unwrap().value,
            n("61000"),
            "the mark, with no mid"
        );
        held.take(&quote("61200", "61201", 20));
        assert_eq!(held.price().unwrap().value, n("61200.5"), "a quote's mid");
        assert_eq!(held.mark.unwrap().value, n("61000"), "the mark kept");
        // An older message never replaces a newer one.
        held.take(&quote("60000", "60002", 15));
        assert_eq!(held.mid.unwrap().value, n("61200.5"));
        assert_eq!(held.mid.unwrap().at_micros, 20);
    }

    #[test]
    fn the_range_is_every_messages_price_each_with_its_time() {
        let mut held = Held::default();
        for (bid, ask, at) in [("100", "102", 1), ("110", "112", 2), ("90", "92", 3)] {
            held.take(&quote(bid, ask, at));
        }
        held.take(&mark("120", Some("95"), 4));
        assert_eq!(
            (held.high.unwrap().value, held.high.unwrap().at_micros),
            (n("111"), 2)
        );
        assert_eq!(
            (held.low.unwrap().value, held.low.unwrap().at_micros),
            (n("91"), 3)
        );
    }

    #[test]
    fn a_ticker_that_would_reach_past_its_subject_is_refused() {
        let ask = |tickers: &str| {
            tickers_of(&PricesQuery {
                venue: "hyperliquid".into(),
                tickers: tickers.into(),
                since: None,
            })
        };
        assert!(ask("BTC,xyz:NVDA").is_ok());
        for bad in ["", "BTC.quotes", "*", ">", "B TC"] {
            assert!(ask(bad).is_err(), "{bad:?}");
        }
        let many: Vec<String> = (0..51).map(|i| format!("T{i}")).collect();
        assert!(
            ask(&many.join(","))
                .unwrap_err()
                .to_string()
                .contains("at most 50")
        );
    }
}
