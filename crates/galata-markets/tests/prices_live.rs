//! `market-prices`, against a real `nats-server` (the broker's own test
//! harness, shared by path), through galata-markets' own router
//! (`move-watcher-onto-markets`, task 3.2).

#[path = "../../galata-broker/tests/support/mod.rs"]
mod support;

use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use galata_broker::{BUS_ADMIN, BUS_ADMIN_INBOX, GALATA_MARKETS, GALATA_MARKETS_INBOX, streams};
use galata_datawatch::config::{ConfigError, Secret, SecretSource};
use galata_markets::access::{Tokens, token_var};
use galata_markets::{Markets, app, routes};
use galata_wire::{Envelope, Event, Mark, Num, Quote, Ticker, Venue};
use serde_json::Value;
use support::{Broker, connect, connect_with};
use tower::ServiceExt;

/// Stand-in tokens, made for the test and printed nowhere.
struct Stand;

impl SecretSource for Stand {
    fn secret(&self, name: &str) -> Result<Secret, ConfigError> {
        Ok(Secret::new(format!("stand-in-{name}")))
    }
}

fn n(s: &str) -> Num {
    Num::from_str(s).unwrap()
}

fn now_micros() -> i64 {
    galata_markets::now_micros()
}

struct Harness {
    _broker: Broker,
    _tape: tempfile::TempDir,
    admin: async_nats::Client,
    capture: async_nats::Client,
    markets: Arc<Markets>,
}

impl Harness {
    async fn start() -> Harness {
        let broker = Broker::start();
        let admin = connect_with(
            &broker,
            BUS_ADMIN,
            async_nats::ConnectOptions::new().custom_inbox_prefix(BUS_ADMIN_INBOX),
        )
        .await;
        streams::apply(&admin).await;
        let capture = connect(&broker, "datawatch-hyperliquid").await;
        let bus = connect_with(
            &broker,
            GALATA_MARKETS,
            async_nats::ConnectOptions::new().custom_inbox_prefix(GALATA_MARKETS_INBOX),
        )
        .await;
        let tape = tempfile::tempdir().unwrap();
        let mut markets = Markets::over(tape.path().to_path_buf());
        markets.bus = Some(bus);
        Harness {
            _broker: broker,
            _tape: tape,
            admin,
            capture,
            markets: Arc::new(markets),
        }
    }

    /// Capture publishing one envelope, and the window holding it.
    async fn publish(&self, ticker: &str, at: i64, event: Event) {
        let kind = match &event {
            Event::Quote(_) => "quotes",
            Event::Mark(_) => "marks",
            _ => unreachable!(),
        };
        let envelope = Envelope::new(
            Venue::new("hyperliquid").unwrap(),
            Ticker::new(ticker).unwrap(),
            Some(at),
            at,
            event,
        );
        self.capture
            .publish(
                format!("markets.hyperliquid.{ticker}.{kind}"),
                galata_broker::encode(&envelope).unwrap().into(),
            )
            .await
            .unwrap();
        self.capture.flush().await.unwrap();
    }

    async fn settled(&self, want: u64) {
        for _ in 0..50 {
            if streams::held(&self.admin, "MARKETS").await.map(|(m, _)| m) == Some(want) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        panic!("the window never held {want}");
    }

    async fn get(&self, path: &str) -> (StatusCode, String) {
        let app = app(
            Tokens::from_source(&Stand).unwrap(),
            routes(self.markets.clone()),
        );
        let response = app
            .oneshot(
                Request::get(path)
                    .header(
                        "Authorization",
                        format!("Bearer stand-in-{}", token_var("watcher")),
                    )
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = response.status();
        let bytes = axum::body::to_bytes(response.into_body(), 1 << 20)
            .await
            .unwrap();
        // Lossy: the Arrow answer is bytes, not text.
        (status, String::from_utf8_lossy(&bytes).into_owned())
    }
}

fn quote(bid: &str, ask: &str) -> Event {
    Event::Quote(Quote {
        bid_px: Some(n(bid)),
        ask_px: Some(n(ask)),
        ..Quote::default()
    })
}

fn mark(mark: &str, mid: Option<&str>) -> Event {
    Event::Mark(Mark {
        mark: Some(n(mark)),
        mid: mid.map(n),
        ..Mark::default()
    })
}

#[tokio::test]
async fn each_tickers_newest_price_and_an_absent_one() {
    let h = Harness::start().await;
    let t = now_micros();
    h.publish("BTC", t, quote("61000", "61002")).await;
    h.publish("BTC", t + 1, mark("61010", Some("61003"))).await;
    h.publish("BTC", t + 2, quote("61100", "61102")).await;
    // ETH: a mark with no mid, only.
    h.publish("ETH", t + 3, mark("3400.5", None)).await;
    h.settled(4).await;

    let (status, body) = h
        .get("/v1/prices?venue=hyperliquid&tickers=BTC,ETH,GONE")
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let answer: Value = serde_json::from_str(&body).unwrap();
    let btc = &answer["prices"]["BTC"];
    assert_eq!(btc["price"]["value"], "61101", "the newest mid, a quote's");
    assert_eq!(btc["price"]["at_micros"], t + 2);
    assert_eq!(btc["mark"]["value"], "61010");
    assert!(btc.get("high").is_none(), "no range unasked");
    let eth = &answer["prices"]["ETH"];
    assert_eq!(eth["price"]["value"], "3400.5", "the mark, with no mid");
    assert_eq!(eth["mid"], Value::Null);
    assert_eq!(answer["absent"], serde_json::json!(["GONE"]));
    assert!(answer["prices"].get("GONE").is_none());
}

#[tokio::test]
async fn the_range_since_a_moment_holds_only_what_came_after_it() {
    let h = Harness::start().await;
    let t = now_micros();
    // Before the moment: a high that must not count.
    h.publish("BTC", t, quote("70000", "70002")).await;
    h.settled(1).await;
    tokio::time::sleep(Duration::from_millis(1_100)).await;
    let since = now_micros();
    for (i, (bid, ask)) in [("61000", "61002"), ("61450", "61452"), ("60990", "60992")]
        .into_iter()
        .enumerate()
    {
        h.publish("BTC", since + 10 + i as i64, quote(bid, ask))
            .await;
    }
    h.settled(4).await;

    let (status, body) = h
        .get(&format!(
            "/v1/prices?venue=hyperliquid&tickers=BTC&since={since}"
        ))
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let btc: Value = serde_json::from_str::<Value>(&body).unwrap()["prices"]["BTC"].clone();
    assert_eq!(btc["high"]["value"], "61451");
    assert_eq!(btc["high"]["at_micros"], since + 11);
    assert_eq!(btc["low"]["value"], "60991");
    assert_eq!(btc["price"]["value"], "60991", "the newest");
}

#[tokio::test]
async fn a_moment_older_than_the_window_is_refused_naming_its_oldest() {
    let h = Harness::start().await;
    let t = now_micros();
    h.publish("BTC", t, quote("61000", "61002")).await;
    h.settled(1).await;
    let (status, body) = h
        .get("/v1/prices?venue=hyperliquid&tickers=BTC&since=1")
        .await;
    assert_eq!(status, StatusCode::GONE, "{body}");
    assert!(body.contains("oldest moment is"), "{body}");
}

#[tokio::test]
async fn json_candles_are_asked_by_format_and_arrow_stays_the_default() {
    let h = Harness::start().await;
    let path = "/v1/candles?venue=hyperliquid&ticker=BTC&period=1m&from=0&to=3600000000";
    let (status, body) = h.get(&format!("{path}&format=json")).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let json: Value = serde_json::from_str(&body).unwrap();
    assert_eq!(
        json["bars"],
        serde_json::json!([]),
        "an empty tape: no bars"
    );
    assert_eq!(json["ticker"], "BTC");
    let (status, arrow) = h.get(path).await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        serde_json::from_str::<Value>(&arrow).is_err(),
        "Arrow IPC, not JSON"
    );
    let (status, body) = h.get(&format!("{path}&format=csv")).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(body.contains("\"csv\""), "{body}");
}
