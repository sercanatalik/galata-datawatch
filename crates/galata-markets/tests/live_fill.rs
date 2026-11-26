//! A declared hole, filled in place from the record (`market-live-stream`,
//! design D6), against a real `nats-server` and a scratch archive written by
//! capture's own path.

#[path = "../../galata-broker/tests/support/mod.rs"]
mod support;

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use futures_util::StreamExt;
use galata_broker::{BUS_ADMIN, BUS_ADMIN_INBOX, GALATA_MARKETS, GALATA_MARKETS_INBOX, streams};
use galata_datawatch::adapters::hyperliquid::{Config, Hyperliquid, Instrument, Market};
use galata_datawatch::config::{ConfigError, Secret, SecretSource};
use galata_datawatch::ingest::ingest;
use galata_datawatch::record::Archive;
use galata_datawatch::sink::{Sink, SinkError};
use galata_datawatch::venue::{Adapter, Construct};
use galata_markets::access::{CLIENTS, Tokens, token_var};
use galata_markets::fill::{FillLimits, Normalisers};
use galata_markets::{Markets, app, routes};
use galata_wire::{Clipped, Envelope, Event, Gap, GapCause, Series, Ticker, Venue};
use support::{Broker, connect, connect_with};
use tower::ServiceExt;

struct Stand;

impl SecretSource for Stand {
    fn secret(&self, name: &str) -> Result<Secret, ConfigError> {
        Ok(Secret::new(format!("stand-in-{name}")))
    }
}

/// What capture's path handed its sink: the envelopes it would have published.
#[derive(Default)]
struct Published(Mutex<Vec<Envelope>>);

impl Sink for Published {
    fn emit(&self, envelope: &Envelope) -> Result<(), SinkError> {
        self.0.lock().unwrap().push(envelope.clone());
        Ok(())
    }
}

fn adapter() -> Hyperliquid {
    Hyperliquid::new(
        Config {
            market: Market::Mainnet,
            instruments: vec![Instrument::main("BTC"), Instrument::main("ETH")],
            candle_interval: "1m".into(),
        },
        None,
    )
    .unwrap()
}

fn bbo(coin: &str, time_millis: i64, bid: u32) -> Vec<u8> {
    format!(
        r#"{{"channel":"bbo","data":{{"coin":"{coin}","time":{time_millis},"bbo":[{{"px":"{bid}.0","sz":"1.5","n":4}},{{"px":"{}.0","sz":"2.5","n":7}}]}}}}"#,
        bid + 1
    )
    .into_bytes()
}

/// One event as a client reads it.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Read {
    event: String,
    id: Option<u64>,
    seq: u64,
}

struct Harness {
    _broker: Broker,
    _archive: tempfile::TempDir,
    _tape: tempfile::TempDir,
    capture: async_nats::Client,
    markets: Arc<Markets>,
    /// E1 to E10, as capture produced them.
    produced: Vec<Envelope>,
}

impl Harness {
    async fn start(archive_readable: bool) -> Harness {
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

        // The record: ten BTC quotes through capture's own path, a second
        // apart, received a minute ago — the window is searched by when the
        // broker stored a message, which is just after capture received it.
        let first = galata_markets::now_micros() - 60_000_000;
        let archive_root = tempfile::tempdir().unwrap();
        let hl = adapter();
        let published = Published::default();
        let mut archive = Archive::open(archive_root.path());
        for n in 0..10i64 {
            let recv = first + n * 1_000_000;
            let payload = hl.classify(&bbo("BTC", recv / 1_000, 60_000 + n as u32), recv);
            ingest(&mut archive, &hl, &published, payload).unwrap();
        }
        archive.flush().unwrap();
        drop(archive);
        let produced = published.0.into_inner().unwrap();
        assert_eq!(produced.len(), 10);

        let tape = tempfile::tempdir().unwrap();
        let empty = tempfile::tempdir().unwrap();
        let mut markets = Markets::over(tape.path().to_path_buf());
        markets.bus = Some(bus);
        markets.archive = if archive_readable {
            archive_root.path().to_path_buf()
        } else {
            empty.path().to_path_buf()
        };
        markets.normalisers = Normalisers::of(vec![(
            "hyperliquid".into(),
            Box::new(adapter()) as Box<dyn Adapter>,
        )]);
        markets.fill_limits = FillLimits {
            wait: Duration::from_millis(500),
            ..FillLimits::default()
        };
        Harness {
            _broker: broker,
            _archive: if archive_readable {
                archive_root
            } else {
                empty
            },
            _tape: tape,
            capture,
            markets: Arc::new(markets),
            produced,
        }
    }

    async fn publish(&self, subject: &str, envelope: &Envelope) {
        self.capture
            .publish(
                subject.to_string(),
                galata_broker::encode(envelope).unwrap().into(),
            )
            .await
            .unwrap();
        self.capture.flush().await.unwrap();
    }

    /// The window as the broker stopped for E4 to E8 leaves it: E1 to E3, E5
    /// which slipped through, the declaration where the hole is, then E9 and
    /// E10.
    async fn outage(&self) {
        let quote = |n: usize| &self.produced[n - 1];
        for n in [1, 2, 3, 5] {
            self.publish("markets.hyperliquid.BTC.quotes", quote(n))
                .await;
        }
        let (from, to) = (quote(4).recv_micros, quote(8).recv_micros);
        let declaration = Envelope::new(
            Venue::new("hyperliquid").unwrap(),
            Ticker::new("BTC").unwrap(),
            Some(from),
            to,
            Event::Gap(Gap {
                series: Series::Quotes,
                from_micros: from,
                to_micros: to,
                cause: GapCause::Unpublished,
                clipped: Clipped::Continuous,
                dex: None,
            }),
        );
        self.publish("markets.hyperliquid.BTC.gaps", &declaration)
            .await;
        for n in [9, 10] {
            self.publish("markets.hyperliquid.BTC.quotes", quote(n))
                .await;
        }
    }

    async fn open(&self, last: Option<u64>) -> (StatusCode, Body) {
        let app = app(
            Tokens::from_source(&Stand).unwrap(),
            routes(self.markets.clone()),
        );
        let mut request =
            Request::get("/v1/live?venue=hyperliquid&tickers=BTC&kinds=quotes&from=0").header(
                "Authorization",
                format!("Bearer stand-in-{}", token_var(CLIENTS[2])),
            );
        if let Some(last) = last {
            request = request.header("Last-Event-ID", last.to_string());
        }
        let response = app
            .oneshot(request.body(Body::empty()).unwrap())
            .await
            .unwrap();
        (response.status(), response.into_body())
    }

    fn seq(&self, n: usize) -> u64 {
        self.produced[n - 1].seq
    }
}

/// Every event until `n` have been read.
async fn read(body: Body, n: usize) -> Vec<Read> {
    let mut frames = body.into_data_stream();
    let mut text = String::new();
    let mut out = Vec::new();
    while out.len() < n {
        let frame = tokio::time::timeout(Duration::from_secs(5), frames.next())
            .await
            .expect("an event within 5 s")
            .unwrap()
            .unwrap();
        text.push_str(std::str::from_utf8(&frame).unwrap());
        while let Some(end) = text.find("\n\n") {
            let block: String = text.drain(..end + 2).collect();
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
                event: fields
                    .get("event")
                    .copied()
                    .unwrap_or("message")
                    .to_string(),
                id: fields.get("id").map(|i| i.parse().unwrap()),
                seq: envelope["seq"].as_u64().unwrap(),
            });
        }
    }
    out
}

#[tokio::test]
async fn a_declaration_is_replaced_in_place_by_exactly_what_the_bus_missed() {
    let h = Harness::start(true).await;
    h.outage().await;
    let (status, body) = h.open(None).await;
    assert_eq!(status, StatusCode::OK);
    let got = read(body, 10).await;
    let order: Vec<(&str, u64)> = got.iter().map(|r| (r.event.as_str(), r.seq)).collect();
    assert_eq!(
        order,
        [
            ("message", h.seq(1)),
            ("message", h.seq(2)),
            ("message", h.seq(3)),
            ("message", h.seq(5)),
            ("fill", h.seq(4)),
            ("fill", h.seq(6)),
            ("fill", h.seq(7)),
            ("fill", h.seq(8)),
            ("message", h.seq(9)),
            ("message", h.seq(10)),
        ],
        "every quote once, the missed ones in the declaration's place"
    );
    assert!(got.iter().all(|r| r.event != "declaration"));
    // The fill's last event carries the declaration's position; the others none.
    let fills: Vec<&Read> = got.iter().filter(|r| r.event == "fill").collect();
    assert!(fills[..3].iter().all(|r| r.id.is_none()));
    let declared_at = fills[3].id.expect("the last fill names the declaration");
    assert!(declared_at > got[3].id.unwrap() && declared_at < got[8].id.unwrap());
}

#[tokio::test]
async fn a_client_resuming_across_a_fill_receives_the_same_fill() {
    let h = Harness::start(true).await;
    h.outage().await;
    let first = read(h.open(None).await.1, 10).await;
    let before = first[3].id.unwrap();
    let declared_at = first[7].id.unwrap();

    // From before the declaration: the same fill, then the rest.
    let resumed = read(h.open(Some(before)).await.1, 6).await;
    assert_eq!(resumed, first[4..].to_vec(), "the identical fill");

    // From the declaration itself: no fill again.
    let after = read(h.open(Some(declared_at)).await.1, 2).await;
    assert_eq!(after, first[8..].to_vec());
}

#[tokio::test]
async fn a_record_that_cannot_be_read_passes_the_declaration_through() {
    let h = Harness::start(false).await;
    h.outage().await;
    let got = read(h.open(None).await.1, 7).await;
    let events: Vec<&str> = got.iter().map(|r| r.event.as_str()).collect();
    assert_eq!(
        events,
        [
            "message",
            "message",
            "message",
            "message",
            "declaration",
            "message",
            "message"
        ]
    );
}
