//! `market-live-stream`, against a real `nats-server` (the broker's own test
//! harness, shared by path), through galata-markets' own router.

#[path = "../../galata-broker/tests/support/mod.rs"]
mod support;

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use futures_util::StreamExt;
use galata_broker::{BUS_ADMIN, BUS_ADMIN_INBOX, GALATA_MARKETS, GALATA_MARKETS_INBOX, streams};
use galata_datawatch::config::{ConfigError, Secret, SecretSource};
use galata_markets::access::{CLIENTS, Tokens, token_var};
use galata_markets::{Markets, app, routes};
use galata_wire::{Envelope, Event, Quote, Ticker, Venue};
use support::{Broker, connect, connect_with};
use tower::ServiceExt;

/// Stand-in tokens, made for the test and printed nowhere.
struct Stand;

impl SecretSource for Stand {
    fn secret(&self, name: &str) -> Result<Secret, ConfigError> {
        Ok(Secret::new(format!("stand-in-{name}")))
    }
}

fn bearer() -> String {
    format!("Bearer stand-in-{}", token_var(CLIENTS[2]))
}

/// One event as a client reads it.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Read {
    id: Option<u64>,
    event: Option<String>,
    seq: u64,
    ticker: String,
}

struct Harness {
    _tape: tempfile::TempDir,
    broker: Broker,
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
            _tape: tape,
            broker,
            admin,
            capture,
            markets: Arc::new(markets),
        }
    }

    /// Capture publishing an envelope of its own sequence.
    async fn publish(&self, ticker: &str, kind: &str, seq: u64) {
        let event = match kind {
            "quotes" => Event::Quote(Quote::default()),
            "marks" => Event::Mark(galata_wire::Mark::default()),
            other => panic!("{other}"),
        };
        let envelope = Envelope::new(
            Venue::new("hyperliquid").unwrap(),
            Ticker::new(ticker).unwrap(),
            Some(1_000 + seq as i64),
            1_000 + seq as i64,
            event,
        )
        .stamped(seq);
        self.capture
            .publish(
                format!("markets.hyperliquid.{ticker}.{kind}"),
                galata_broker::encode(&envelope).unwrap().into(),
            )
            .await
            .unwrap();
        self.capture.flush().await.unwrap();
    }

    /// Open the stream; the status, and the body to read events from.
    async fn open(&self, query: &str, last: Option<u64>) -> (StatusCode, Body) {
        let app = app(
            Tokens::from_source(&Stand).unwrap(),
            routes(self.markets.clone()),
        );
        let mut request = Request::get(format!("/v1/live?venue=hyperliquid&{query}"))
            .header("Authorization", bearer());
        if let Some(last) = last {
            request = request.header("Last-Event-ID", last.to_string());
        }
        let response = app
            .oneshot(request.body(Body::empty()).unwrap())
            .await
            .unwrap();
        let status = response.status();
        (status, response.into_body())
    }
}

/// The next `n` events, within five seconds.
async fn read(body: &mut Body, n: usize, carried: &mut String) -> Vec<Read> {
    let mut out = Vec::new();
    let mut frames = std::mem::take(body).into_data_stream();
    while out.len() < n {
        while let Some(end) = carried.find("\n\n") {
            let block: String = carried.drain(..end + 2).collect();
            let fields: BTreeMap<&str, &str> = block
                .lines()
                .filter_map(|l| l.split_once(':'))
                .map(|(k, v)| (k, v.trim_start()))
                .collect();
            let Some(data) = fields.get("data") else {
                continue;
            };
            let envelope: serde_json::Value = serde_json::from_str(data).unwrap();
            out.push(Read {
                id: fields.get("id").map(|i| i.parse().unwrap()),
                event: fields.get("event").map(|e| e.to_string()),
                seq: envelope["seq"].as_u64().unwrap(),
                ticker: envelope["address"]["Venue"]["ticker"]
                    .as_str()
                    .unwrap()
                    .to_string(),
            });
            if out.len() == n {
                break;
            }
        }
        if out.len() == n {
            break;
        }
        let frame = tokio::time::timeout(Duration::from_secs(5), frames.next())
            .await
            .expect("an event within 5 s")
            .expect("the stream is open")
            .unwrap();
        carried.push_str(std::str::from_utf8(&frame).unwrap());
    }
    *body = Body::from_stream(frames);
    out
}

#[tokio::test]
async fn a_client_follows_its_instruments_and_datasets_and_nothing_else() {
    let h = Harness::start().await;
    let (status, mut body) = h.open("tickers=BTC,ETH&kinds=quotes&from=0", None).await;
    assert_eq!(status, StatusCode::OK);
    h.publish("BTC", "quotes", 1).await;
    h.publish("GOLD", "quotes", 2).await;
    h.publish("BTC", "marks", 3).await;
    h.publish("ETH", "quotes", 4).await;
    let mut carried = String::new();
    let got = read(&mut body, 2, &mut carried).await;
    let seen: Vec<(u64, &str)> = got.iter().map(|r| (r.seq, r.ticker.as_str())).collect();
    assert_eq!(
        seen,
        [(1, "BTC"), (4, "ETH")],
        "BTC and ETH quotes, nothing else"
    );
    assert!(got.iter().all(|r| r.id.is_some() && r.event.is_none()));
    drop(h.broker);
}

#[tokio::test]
async fn a_reconnecting_client_resumes_with_nothing_missed_or_repeated() {
    let h = Harness::start().await;
    for seq in 1..=3 {
        h.publish("BTC", "quotes", seq).await;
    }
    let (_, mut body) = h.open("tickers=BTC&kinds=quotes&from=0", None).await;
    let mut carried = String::new();
    let first = read(&mut body, 2, &mut carried).await;
    let last = first[1].id.unwrap();
    drop(body);
    // Missed while away.
    for seq in 4..=5 {
        h.publish("BTC", "quotes", seq).await;
    }
    let (status, mut body) = h.open("tickers=BTC&kinds=quotes", Some(last)).await;
    assert_eq!(status, StatusCode::OK);
    let mut carried = String::new();
    let resumed = read(&mut body, 3, &mut carried).await;
    assert_eq!(resumed.iter().map(|r| r.seq).collect::<Vec<_>>(), [3, 4, 5]);
    // And then live.
    h.publish("BTC", "quotes", 6).await;
    assert_eq!(read(&mut body, 1, &mut carried).await[0].seq, 6);
}

#[tokio::test]
async fn a_position_that_has_left_the_window_is_refused_naming_the_oldest() {
    let h = Harness::start().await;
    // The window kept to five messages, as an age would keep it.
    let js = async_nats::jetstream::new(h.admin.clone());
    let mut config = js
        .get_stream("MARKETS")
        .await
        .unwrap()
        .info()
        .await
        .unwrap()
        .config
        .clone();
    config.max_messages = 5;
    js.update_stream(&config).await.unwrap();
    for seq in 1..=12 {
        h.publish("BTC", "quotes", seq).await;
    }
    let (status, body) = h.open("tickers=BTC", Some(2)).await;
    assert_eq!(status, StatusCode::GONE);
    let text =
        String::from_utf8(axum::body::to_bytes(body, 1 << 16).await.unwrap().to_vec()).unwrap();
    assert!(text.contains("the oldest it holds is 8"), "{text}");
}

#[tokio::test]
async fn a_stream_after_a_tape_bound_holds_every_envelope_once_with_the_history() {
    use arrow::array::UInt64Array;
    use galata_datawatch::tape::{Row, Tape};
    use galata_markets::history::{self, Limits, TapeQuery};

    let mut h = Harness::start().await;
    // The tape holds 1 to 5, projected; the window 1 to 10.
    let tape_root = tempfile::tempdir().unwrap();
    let mut tape = Tape::open(tape_root.path());
    for seq in 1..=5u64 {
        let envelope = Envelope::new(
            Venue::new("hyperliquid").unwrap(),
            Ticker::new("BTC").unwrap(),
            Some(1_000 + seq as i64),
            1_000 + seq as i64,
            Event::Quote(Quote::default()),
        )
        .stamped(seq);
        tape.take(Row {
            stream_seq: seq,
            source_recv_micros: 1_000 + seq as i64,
            envelope,
        });
    }
    tape.commit().unwrap();
    for seq in 1..=10 {
        h.publish("BTC", "quotes", seq).await;
    }

    let answer = history::read(
        tape_root.path(),
        galata_wire::Kind::Quotes,
        &TapeQuery {
            venue: "hyperliquid".into(),
            ticker: Some("BTC".into()),
            from: 0,
            to: 1_000_000,
        },
        Limits::default(),
    )
    .unwrap();
    let bound = answer.schema.metadata()["galata.bound.hyperliquid"].clone();
    assert_eq!(bound, "5");
    let mut all: Vec<u64> = answer
        .batches
        .iter()
        .flat_map(|b| {
            b.column_by_name("stream_seq")
                .unwrap()
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap()
                .values()
                .to_vec()
        })
        .collect();

    Arc::get_mut(&mut h.markets).unwrap().tape = tape_root.path().to_path_buf();
    let (status, mut body) = h
        .open(&format!("tickers=BTC&kinds=quotes&after={bound}"), None)
        .await;
    assert_eq!(status, StatusCode::OK);
    let mut carried = String::new();
    all.extend(read(&mut body, 5, &mut carried).await.iter().map(|r| r.seq));
    assert_eq!(all, (1..=10).collect::<Vec<_>>(), "every envelope once");
}

#[tokio::test]
async fn a_tape_bound_older_than_the_window_is_refused() {
    let h = Harness::start().await;
    for seq in 100..=102 {
        h.publish("BTC", "quotes", seq).await;
    }
    let (status, _) = h.open("tickers=BTC&after=5", None).await;
    assert_eq!(status, StatusCode::GONE);
}

#[tokio::test]
async fn the_last_bar_streamed_for_each_period_is_the_bar_history_returns() {
    use galata_datawatch::tape::bars::{Period, Sources, bars};
    use galata_datawatch::tape::{Row, Tape};
    use galata_markets::live::bar_json;
    use galata_wire::{Candle, Num};

    let h = Harness::start().await;
    let t = 1_795_687_200_000_000i64; // 10:00 UTC, a five-minute boundary
    let minute = 60_000_000i64;
    let tape_root = tempfile::tempdir().unwrap();
    let mut tape = Tape::open(tape_root.path());
    let mut seq = 0u64;
    // Twelve minutes, each stated three times: forming, forming, final.
    for k in 0..12i64 {
        for version in 0..3i64 {
            seq += 1;
            let at = t + k * minute;
            let recv = at + version * 20_000_000 + 1;
            let price = Num::from(60_000 + k * 10 + version);
            let envelope = Envelope::new(
                Venue::new("hyperliquid").unwrap(),
                Ticker::new("BTC").unwrap(),
                Some(at),
                recv,
                Event::Candle(Candle {
                    interval: "1m".into(),
                    open: Num::from(60_000 + k * 10),
                    high: price + Num::from(3),
                    low: price - Num::from(3),
                    close: price,
                    volume: Num::from(version + 1),
                    trade_count: Some(version as u32 + 1),
                    is_final: version == 2,
                }),
            )
            .stamped(seq);
            tape.take(Row {
                stream_seq: seq,
                source_recv_micros: recv,
                envelope: envelope.clone(),
            });
            h.capture
                .publish(
                    "markets.hyperliquid.BTC.candles",
                    galata_broker::encode(&envelope).unwrap().into(),
                )
                .await
                .unwrap();
        }
    }
    h.capture.flush().await.unwrap();
    tape.commit().unwrap();

    // What history builds from the same rows.
    let period = Period::parse("5m").unwrap();
    let reader = galata_datawatch::tape::Reader::open(tape_root.path(), &["kind=candles"]).unwrap();
    let batches = reader
        .view(galata_datawatch::tape::Window {
            kind: galata_wire::Kind::Candles,
            from_micros: t,
            to_micros: t + 15 * minute,
            ticker: Some("BTC".into()),
        })
        .unwrap();
    let sources = Sources::from_batches(&batches, "hyperliquid", "BTC").unwrap();
    let expected: Vec<String> = bars(&sources, period, t, t + 15 * minute, &[])
        .bars
        .iter()
        .map(|b| bar_json("hyperliquid", "BTC", period, b))
        .collect();
    assert_eq!(expected.len(), 3);

    // What the stream sends, from the window's start, one bar per update.
    let (status, body) = h
        .open("tickers=BTC&kinds=candles&period=5m&from=0", None)
        .await;
    assert_eq!(status, StatusCode::OK);
    let mut frames = body.into_data_stream();
    let mut text = String::new();
    let mut last: BTreeMap<i64, String> = BTreeMap::new();
    let mut sent = 0;
    while sent < 36 {
        let frame = tokio::time::timeout(Duration::from_secs(5), frames.next())
            .await
            .expect("a bar within 5 s")
            .unwrap()
            .unwrap();
        text.push_str(std::str::from_utf8(&frame).unwrap());
        while let Some(end) = text.find("\n\n") {
            let block: String = text.drain(..end + 2).collect();
            if !block.contains("event: bar") {
                continue;
            }
            let data = block
                .lines()
                .find_map(|l| l.strip_prefix("data: "))
                .unwrap()
                .to_string();
            let start = serde_json::from_str::<serde_json::Value>(&data).unwrap()["start_micros"]
                .as_i64()
                .unwrap();
            last.insert(start, data);
            sent += 1;
        }
    }
    assert_eq!(last.into_values().collect::<Vec<_>>(), expected);
}

#[tokio::test]
async fn a_client_that_falls_behind_is_disconnected_and_resumes_with_nothing_missed() {
    let h = Harness::start().await;
    // A stream that holds four events for its client, and one that holds the
    // default; the same bus behind both.
    let mut slow = Markets::over(h._tape.path().to_path_buf());
    slow.bus = h.markets.bus.clone();
    slow.queue = 4;
    let slow = Arc::new(slow);
    let open = |markets: Arc<Markets>, last: Option<u64>| async move {
        let app = app(Tokens::from_source(&Stand).unwrap(), routes(markets));
        let mut request =
            Request::get("/v1/live?venue=hyperliquid&tickers=BTC&kinds=quotes&from=0")
                .header("Authorization", bearer());
        if let Some(last) = last {
            request = request.header("Last-Event-ID", last.to_string());
        }
        app.oneshot(request.body(Body::empty()).unwrap())
            .await
            .unwrap()
            .into_body()
    };
    let stalled = open(slow.clone(), None).await;
    let mut steady = open(h.markets.clone(), None).await;
    for seq in 1..=20 {
        h.publish("BTC", "quotes", seq).await;
    }
    tokio::time::sleep(Duration::from_millis(300)).await;

    // The stalled one: what its queue held, then a comment saying why, then
    // the end.
    let text = String::from_utf8(
        tokio::time::timeout(
            Duration::from_secs(5),
            axum::body::to_bytes(stalled, 1 << 20),
        )
        .await
        .expect("the stalled stream ends")
        .unwrap()
        .to_vec(),
    )
    .unwrap();
    let ids: Vec<u64> = text
        .lines()
        .filter_map(|l| l.strip_prefix("id: "))
        .map(|i| i.parse().unwrap())
        .collect();
    assert_eq!(ids.len(), 4, "{text}");
    assert!(text.contains(": behind:"), "{text}");

    // The steady one is unaffected.
    let mut carried = String::new();
    assert_eq!(read(&mut steady, 20, &mut carried).await.len(), 20);

    // And the stalled one, resuming, has what it missed.
    let mut resumed = open(h.markets.clone(), Some(*ids.last().unwrap())).await;
    let mut carried = String::new();
    let rest = read(&mut resumed, 16, &mut carried).await;
    assert_eq!(
        rest.iter().map(|r| r.seq).collect::<Vec<_>>(),
        (5..=20).collect::<Vec<_>>()
    );
}
