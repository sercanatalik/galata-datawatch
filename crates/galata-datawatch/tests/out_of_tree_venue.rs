//! **Can somebody add a venue without forking?**
//!
//! The roadmap claims the `Adapter` seam is an out-of-tree extension point, and
//! this project's method is that claims get checked.
//!
//! Every adapter in the tree lives *inside* `galata-datawatch` and can reach
//! `pub(crate)` items without anybody noticing. This file is compiled by cargo
//! as **its own crate**, so it sees exactly what a stranger sees. If it
//! compiles, the claim is true. If it needs something private, the compiler
//! says which thing — here, rather than in somebody's repository.
//!
//! The venue is **fictional on purpose**. One resembling Hyperliquid would
//! tempt reuse of its helpers, and reuse is what makes a test pass for the
//! wrong reason.

use std::collections::BTreeMap;

use galata_datawatch::ingest::ingest;
use galata_datawatch::normalise::{Normalise, NormaliseError};
use galata_datawatch::record::{Archive, Payload, PayloadAddress};
use galata_datawatch::sink::testing::RecordingSink;
use galata_datawatch::venue::{
    Adapter, Budget, ConnectionPolicy, Declaration, Keepalive, Paging, Streaming, Subscription,
    Transport,
};
use galata_wire::{Envelope, Event, Kind, Num, Origin, Quote, Series, Ticker, Venue};

/// A venue nobody has heard of, which pushes frames shaped like nothing else.
struct Bazaar {
    venue: Venue,
    declaration: Declaration,
    symbols: BTreeMap<String, Ticker>,
}

impl Bazaar {
    fn new() -> Bazaar {
        Bazaar {
            venue: Venue::new("bazaar").expect("a legal venue name"),
            declaration: Declaration {
                streams: vec![Series::Quotes],
                historical: vec![Series::Candles],
                paging: BTreeMap::from([(Series::Candles, Paging::forward_from_start(100))]),
                budget: Budget {
                    requests_per_minute: 60.0,
                    min_historical_interval_ms: 1_000,
                },
                connection: ConnectionPolicy::KeepAliveOnly { keepalive_secs: 30 },
                ws_url: "wss://bazaar.invalid/stream",
                rest_url: "https://bazaar.invalid",
            },
            symbols: BTreeMap::from([("XBT".into(), Ticker::new("BTC").expect("legal"))]),
        }
    }
}

/// The venue's own frame shape: `top|<symbol>|<bid>|<ask>`.
#[derive(Debug)]
struct Top {
    symbol: String,
    bid: String,
    ask: String,
}

fn parse(bytes: &[u8]) -> Option<Top> {
    let text = std::str::from_utf8(bytes).ok()?;
    let mut parts = text.trim().split('|');
    if parts.next()? != "top" {
        return None;
    }
    Some(Top {
        symbol: parts.next()?.to_string(),
        bid: parts.next()?.to_string(),
        ask: parts.next()?.to_string(),
    })
}

impl Normalise for Bazaar {
    fn venue(&self) -> &Venue {
        &self.venue
    }

    fn normalise(&self, payload: &Payload) -> Result<Vec<Envelope>, NormaliseError> {
        let top = parse(&payload.payload).ok_or(NormaliseError::Shape {
            kind: "top",
            detail: "not a top frame".into(),
        })?;
        let Some(ticker) = self.symbols.get(&top.symbol) else {
            return Ok(Vec::new());
        };
        Ok(vec![Envelope::new(
            self.venue.clone(),
            ticker.clone(),
            None,
            payload.recv_micros,
            Event::Quote(Quote {
                bid_px: top.bid.parse::<Num>().ok(),
                ask_px: top.ask.parse::<Num>().ok(),
                bid_sz: None,
                ask_sz: None,
                bid_spread: None,
                ask_spread: None,
            }),
        )])
    }
}

impl Adapter for Bazaar {
    fn declaration(&self) -> &Declaration {
        &self.declaration
    }

    fn transport(&self) -> Transport {
        Transport::Stream {
            ws_url: self.declaration.ws_url,
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
        let symbol = parse(bytes).map(|t| t.symbol);
        Payload {
            seq: 0,
            recv_micros,
            address: PayloadAddress::Venue("bazaar".into()),
            channel: "top".into(),
            kind: Series::Quotes.as_str().to_string(),
            symbol,
            origin: Origin::Streamed,
            payload: bytes.to_vec(),
        }
    }

    fn venue_symbol(&self, ticker: &Ticker) -> Option<String> {
        self.symbols
            .iter()
            .find(|(_, t)| *t == ticker)
            .map(|(s, _)| s.clone())
    }

    fn interval_label(&self, interval_micros: i64) -> Option<String> {
        (interval_micros == 60_000_000).then(|| "1min".to_string())
    }

    fn venue_ticker(&self, _channel: &str, venue_symbol: &str) -> Option<Ticker> {
        self.symbols.get(venue_symbol).cloned()
    }
}

impl Streaming for Bazaar {
    fn channel_of(&self, _subscription: &Subscription) -> String {
        "top".into()
    }

    fn subscribe_frames(&self, subscriptions: &[Subscription]) -> Vec<String> {
        subscriptions
            .iter()
            .filter_map(|s| self.venue_symbol(&s.ticker))
            .map(|symbol| format!("sub|{symbol}"))
            .collect()
    }
}

#[test]
fn a_venue_can_be_written_entirely_from_outside_the_crate() {
    // **Compiling is most of the assertion.** This file is its own crate, so
    // anything it needed that were `pub(crate)` would have failed the build.
    let bazaar = Bazaar::new();
    assert_eq!(bazaar.venue().as_str(), "bazaar");
    assert!(bazaar.transport().is_stream());
    assert_eq!(
        bazaar.subscribe_frames(&[Subscription {
            ticker: Ticker::new("BTC").unwrap(),
            series: Series::Quotes,
        }]),
        vec!["sub|XBT".to_string()]
    );
}

#[test]
fn an_out_of_tree_adapter_reaches_the_one_path() {
    // Archive, normalise, emit — the same function the venues in the tree use,
    // reachable by one that is not.
    let root = tempfile::tempdir().unwrap();
    let bazaar = Bazaar::new();
    let sink = RecordingSink::default();
    let mut archive = Archive::open(root.path()).scoped_to("bazaar");

    let payload = bazaar.classify(b"top|XBT|81213.0|81214.5", 1_789_941_180_000_000);
    let result = ingest(&mut archive, &bazaar, &sink, payload).unwrap();

    assert_eq!(result.emitted, 1);
    assert!(!result.unparsed);
    archive.flush().unwrap();

    let emitted = sink.emitted();
    assert_eq!(emitted[0].kind(), Kind::Quotes);
    assert_eq!(emitted[0].ticker().unwrap().as_str(), "BTC");
    assert_eq!(
        emitted[0].seq, result.seq,
        "the stream position is stamped by the one path, not by the adapter"
    );
}

#[test]
fn an_out_of_tree_adapter_can_be_boxed_as_the_trait_object_the_loop_holds() {
    // The loop holds `Box<dyn Adapter>`. If a foreign adapter could not become
    // one, "add a venue later" would be true only for venues in this repo.
    let boxed: Box<dyn Adapter> = Box::new(Bazaar::new());
    assert_eq!(boxed.declaration().streams, vec![Series::Quotes]);
    assert!(boxed.streaming().is_some());
}
