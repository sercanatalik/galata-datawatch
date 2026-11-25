//! **A connection that dies without closing is found, and its silence is a gap.**
//!
//! Measured on 2026-11-25 (`design/measured.md`, "Tailscale stopped for three
//! minutes, beside a native capture"): a change in the host's network killed a
//! container's connection to the venue without closing it. Capture said
//! "connected" for minutes, and a handover then claimed the dead minutes as
//! covered. This runs the real loop against a local server that does the same
//! thing: it answers, delivers, and then stops — reading nothing, writing
//! nothing, closing nothing.
//!
//! The venue keeps alive by protocol ping, which the server's websocket answers
//! by itself while it reads, so the answers stop the moment it stops reading.
//! No external network is touched.

#![cfg(feature = "capture")]

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures_util::{SinkExt, StreamExt};
use galata_datawatch::capture::clock::SystemClock;
use galata_datawatch::capture::run::{Capture, Wiring};
use galata_datawatch::record::{Payload, PayloadAddress};
use galata_datawatch::sink::testing::RecordingSink;
use galata_datawatch::venue::{
    Adapter, Budget, ConnectionPolicy, Declaration, Endpoint, Keepalive, Streaming, Subscription,
    Transport,
};
use galata_datawatch::{Normalise, NormaliseError};
use galata_wire::{Clipped, Envelope, Event, GapCause, Num, Origin, Quote, Series, Ticker, Venue};
use tokio::net::TcpListener;
use tokio_tungstenite::tungstenite::Message;

/// The venue's keepalive interval and reply time: short, so the test is.
const KEEPALIVE_SECS: u64 = 1;
const REPLY_SECS: u64 = 1;

/// A venue whose frames are `top|<symbol>|<bid>|<ask>`, at a local address.
struct Deadwood {
    venue: Venue,
    declaration: Declaration,
}

impl Deadwood {
    fn at(ws_url: &'static str) -> Deadwood {
        Deadwood {
            venue: Venue::new("deadwood").expect("a legal venue name"),
            declaration: Declaration {
                streams: vec![Series::Quotes],
                historical: Vec::new(),
                paging: BTreeMap::new(),
                budget: Budget {
                    weight_per_minute: 60.0,
                    min_historical_interval_ms: 1_000,
                },
                connection: ConnectionPolicy::KeepAliveOnly {
                    keepalive_secs: KEEPALIVE_SECS,
                    reply_secs: Some(REPLY_SECS),
                },
                ws_url,
                rest_url: "http://deadwood.invalid",
            },
        }
    }
}

fn btc() -> Ticker {
    Ticker::new("BTC").expect("legal")
}

impl Normalise for Deadwood {
    fn venue(&self) -> &Venue {
        &self.venue
    }

    fn normalise(&self, payload: &Payload) -> Result<Vec<Envelope>, NormaliseError> {
        let text = std::str::from_utf8(&payload.payload).unwrap_or_default();
        let parts: Vec<&str> = text.split('|').collect();
        let ["top", "XBT", bid, ask] = parts.as_slice() else {
            return Err(NormaliseError::Shape {
                kind: "top",
                detail: "not a top frame".into(),
            });
        };
        Ok(vec![Envelope::new(
            self.venue.clone(),
            btc(),
            None,
            payload.recv_micros,
            Event::Quote(Quote {
                bid_px: bid.parse::<Num>().ok(),
                ask_px: ask.parse::<Num>().ok(),
                bid_sz: None,
                ask_sz: None,
                bid_spread: None,
                ask_spread: None,
            }),
        )])
    }
}

impl Adapter for Deadwood {
    fn declaration(&self) -> &Declaration {
        &self.declaration
    }

    fn transport(&self) -> Transport {
        Transport::Stream {
            ws_url: Endpoint::public(self.declaration.ws_url),
            keepalive: Keepalive::Protocol,
        }
    }

    fn streaming(&self) -> Option<&dyn Streaming> {
        Some(self)
    }

    fn series_of_channel(&self, channel: &str) -> Option<Series> {
        (channel == "top").then_some(Series::Quotes)
    }

    fn classify(&self, bytes: &[u8], recv_micros: i64) -> Payload {
        Payload {
            seq: 0,
            recv_micros,
            address: PayloadAddress::Venue("deadwood".into()),
            channel: "top".into(),
            kind: Series::Quotes.as_str().to_string(),
            symbol: Some("XBT".into()),
            origin: Origin::Streamed,
            payload: bytes.to_vec(),
        }
    }

    fn venue_symbol(&self, ticker: &Ticker) -> Option<String> {
        (ticker == &btc()).then(|| "XBT".to_string())
    }

    fn interval_label(&self, _interval_micros: i64) -> Option<String> {
        None
    }

    fn venue_ticker(&self, _channel: &str, venue_symbol: &str) -> Option<Ticker> {
        (venue_symbol == "XBT").then(btc)
    }
}

impl Streaming for Deadwood {
    fn channel_of(&self, _subscription: &Subscription) -> String {
        "top".into()
    }

    fn subscribe_frames(&self, _subscriptions: &[Subscription]) -> Vec<String> {
        vec!["sub|XBT".into()]
    }
}

/// What the server saw, for the test to wait on.
enum Seen {
    /// A connection was accepted, at this instant.
    Connected(Instant),
    /// The first connection stopped answering, at this instant.
    WentSilent(Instant),
}

/// Serve connections: the first answers and delivers for `live_for`, then goes
/// silent without closing; every later one answers and delivers until the test
/// ends.
async fn serve(listener: TcpListener, live_for: Duration, seen: tokio::sync::mpsc::Sender<Seen>) {
    let mut accepted = 0usize;
    loop {
        let Ok((tcp, _)) = listener.accept().await else {
            return;
        };
        let Ok(mut ws) = tokio_tungstenite::accept_async(tcp).await else {
            continue;
        };
        accepted += 1;
        let _ = seen.send(Seen::Connected(Instant::now())).await;
        let seen = seen.clone();
        let first = accepted == 1;
        tokio::spawn(async move {
            let until = if first {
                Instant::now() + live_for
            } else {
                Instant::now() + Duration::from_secs(60)
            };
            // **Alive**: read, which answers the client's pings, and deliver.
            while Instant::now() < until {
                match tokio::time::timeout(Duration::from_millis(200), ws.next()).await {
                    Ok(Some(Ok(_))) | Err(_) => {}
                    Ok(None) | Ok(Some(Err(_))) => return,
                }
                if ws
                    .send(Message::Text("top|XBT|100.0|100.5".into()))
                    .await
                    .is_err()
                {
                    return;
                }
            }
            // **Dead without closing**: hold the socket, read and write nothing.
            let _ = seen.send(Seen::WentSilent(Instant::now())).await;
            tokio::time::sleep(Duration::from_secs(60)).await;
            drop(ws);
        });
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_connection_that_dies_without_closing_is_found_and_its_silence_is_a_gap() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("a local port");
    let port = listener.local_addr().expect("bound").port();
    let url: &'static str = Box::leak(format!("ws://127.0.0.1:{port}").into_boxed_str());
    let (tx, mut seen) = tokio::sync::mpsc::channel(16);
    tokio::spawn(serve(listener, Duration::from_secs(2), tx));

    let root = tempfile::tempdir().expect("a scratch root");
    let sink = Arc::new(RecordingSink::default());
    let mut capture = Capture::new(Wiring {
        adapter: Box::new(Deadwood::at(url)),
        sink: sink.clone(),
        clock: Arc::new(SystemClock),
        archive_root: root.path().join("archive"),
        status_dir: root.path().join("status"),
        flush_secs: 1,
        status_secs: 1,
        declared: vec![Subscription {
            ticker: btc(),
            series: Series::Quotes,
        }],
        clipped: Clipped::Continuous,
        config_hash: "test".into(),
    });

    let shutdown = tokio_util::sync::CancellationToken::new();
    let stop = shutdown.clone();
    let watch = async move {
        let mut silent_at = None;
        let mut reconnected = None;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
        while reconnected.is_none() {
            match tokio::time::timeout_at(deadline, seen.recv()).await {
                Ok(Some(Seen::WentSilent(at))) => silent_at = Some(at),
                Ok(Some(Seen::Connected(at))) if silent_at.is_some() => reconnected = Some(at),
                Ok(Some(Seen::Connected(_))) => {}
                Ok(None) | Err(_) => break,
            }
        }
        // Let the new connection deliver before stopping.
        tokio::time::sleep(Duration::from_millis(800)).await;
        stop.cancel();
        (silent_at, reconnected)
    };

    let (ran, (silent_at, reconnected)) = tokio::join!(capture.run(shutdown), watch);
    ran.expect("capture ran and stopped cleanly");

    let silent_at = silent_at.expect("the server went silent");
    let reconnected =
        reconnected.expect("capture never connected again: the dead connection was not found");
    let took = reconnected - silent_at;
    let bound = Duration::from_secs(KEEPALIVE_SECS + REPLY_SECS) + Duration::from_secs(2);
    assert!(
        took <= bound,
        "found and replaced after {took:?}; the keepalive plus the reply time, with margin, is {bound:?}"
    );

    let gaps: Vec<_> = sink
        .emitted()
        .into_iter()
        .filter_map(|e| match e.event {
            Event::Gap(g) if e.ticker().is_some_and(|t| t == &btc()) => Some(g),
            _ => None,
        })
        .collect();
    assert!(
        gaps.iter()
            .any(|g| g.cause == GapCause::SessionLost && g.series == Series::Quotes),
        "the dead connection's silence was not recorded as a gap: {gaps:?}"
    );
    let quotes = sink
        .emitted()
        .into_iter()
        .filter(|e| matches!(e.event, Event::Quote(_)))
        .count();
    assert!(
        quotes > 0,
        "nothing was delivered at all, so nothing was tested"
    );
}
