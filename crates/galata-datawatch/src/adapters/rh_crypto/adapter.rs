//! Robinhood Crypto as an [`Adapter`]: declared instruments, one polled
//! request for all of them, and the signer that authenticates it.
//!
//! Generalised from the fixture `capture::poll`'s tests carried since Tier 8,
//! which was the only adapter this venue had — so the venue could be proved
//! in a test and not declared in a file.

use std::collections::BTreeMap;
use std::sync::Arc;

use galata_wire::{Envelope, Origin, Series, Ticker, Venue};

use super::sign::Credential;
use super::{BEST_BID_ASK_PATH, REST_URL, VENUE, wire};
use crate::normalise::{Normalise, NormaliseError};
use crate::record::{Payload, PayloadAddress};
use crate::venue::{
    Adapter, Budget, ConnectionPolicy, ConstructError, Declaration, Endpoint, Paging, Signer,
    Transport,
};

/// The channel every archived answer is filed under.
pub const CHANNEL: &str = "best_bid_ask";

/// The quote currency every pair on this API is priced in.
///
/// **Composed, never declared**, as Hyperliquid's dex prefix is: the Crypto
/// Trading API quotes against USD only, so a ticker `BTC` is the venue's
/// `BTC-USD`. The day it quotes another currency, an instrument needs its own
/// `symbol` and this constant stops being true — in one place.
pub const QUOTE: &str = "USD";

/// What a declared `[venue.rh-crypto]` block means.
#[derive(Clone)]
pub struct Config {
    /// The tickers, as this tree names them: `BTC`, not `BTC-USD`.
    pub tickers: Vec<String>,
    /// Seconds between polls — also the width of a gap one failure produces.
    pub poll_secs: u32,
    /// The signing credential, or `None` for a tool that does not connect.
    pub credential: Option<Arc<Credential>>,
}

impl std::fmt::Debug for Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Config")
            .field("tickers", &self.tickers)
            .field("poll_secs", &self.poll_secs)
            .field("signed", &self.credential.is_some())
            .finish()
    }
}

/// The adapter.
pub struct RhCrypto {
    rest_url: Endpoint,
    venue: Venue,
    declaration: Declaration,
    /// Venue symbol → ticker.
    tickers: BTreeMap<String, Ticker>,
    interval_micros: i64,
    signer: Option<Signer>,
}

impl RhCrypto {
    /// Build it, refusing a name that would not survive validation.
    pub fn new(config: Config) -> Result<RhCrypto, ConstructError> {
        let mut tickers = BTreeMap::new();
        for ticker in &config.tickers {
            tickers.insert(symbol_of(ticker), Ticker::new(ticker)?);
        }
        Ok(RhCrypto {
            rest_url: Endpoint::public(REST_URL),
            venue: Venue::new(VENUE)?,
            declaration: Declaration {
                // Nothing is pushed and nothing is walked: the venue serves
                // the current state, and only when asked.
                streams: Vec::new(),
                historical: Vec::new(),
                paging: BTreeMap::from([(Series::Quotes, Paging::forward_from_start(1))]),
                budget: Budget {
                    requests_per_minute: 60.0 / f64::from(config.poll_secs.max(1)),
                    min_historical_interval_ms: u64::from(config.poll_secs) * 1_000,
                },
                connection: ConnectionPolicy::KeepAliveOnly { keepalive_secs: 0 },
                ws_url: "",
                rest_url: REST_URL,
            },
            tickers,
            interval_micros: i64::from(config.poll_secs) * 1_000_000,
            signer: config
                .credential
                .map(|credential| Signer(credential as Arc<dyn crate::venue::RequestSigner>)),
        })
    }
}

impl RhCrypto {
    /// The same adapter, asking somewhere else — a local server in a test.
    ///
    /// Test-only, because the address a venue is reached at is a code
    /// identity (see [`Endpoint::Public`]) and a configurable one is how a
    /// process ends up polling a host nobody chose.
    #[cfg(any(test, feature = "testing"))]
    pub fn reaching(mut self, url: &'static str) -> RhCrypto {
        self.rest_url = Endpoint::public(url);
        self
    }
}

/// `BTC` → `BTC-USD`.
pub fn symbol_of(ticker: &str) -> String {
    format!("{ticker}-{QUOTE}")
}

impl Normalise for RhCrypto {
    fn venue(&self) -> &Venue {
        &self.venue
    }

    fn normalise(&self, payload: &Payload) -> Result<Vec<Envelope>, NormaliseError> {
        let parsed = wire::response(&payload.payload)?;
        Ok(wire::read(
            &self.venue,
            &parsed,
            &self.tickers,
            payload.recv_micros,
        ))
    }
}

impl Adapter for RhCrypto {
    fn declaration(&self) -> &Declaration {
        &self.declaration
    }

    fn transport(&self) -> Transport {
        Transport::Poll {
            rest_url: self.rest_url.clone(),
            path: BEST_BID_ASK_PATH,
            interval_micros: self.interval_micros,
            // A BTreeMap's keys: sorted, so one declaration is one request.
            symbols: self.tickers.keys().cloned().collect(),
            signer: self.signer.clone(),
        }
    }

    fn series_of_channel(&self, channel: &str) -> Option<Series> {
        (channel == CHANNEL).then_some(Series::Quotes)
    }

    fn classify(&self, bytes: &[u8], recv_micros: i64) -> Payload {
        Payload {
            seq: 0,
            recv_micros,
            address: PayloadAddress::Venue(VENUE.into()),
            channel: CHANNEL.into(),
            kind: Series::Quotes.as_str().to_string(),
            // One answer covers every symbol asked for.
            symbol: None,
            origin: Origin::Fetched,
            payload: bytes.to_vec(),
        }
    }

    fn venue_symbol(&self, ticker: &Ticker) -> Option<String> {
        let symbol = symbol_of(ticker.as_str());
        self.tickers.contains_key(&symbol).then_some(symbol)
    }

    fn interval_label(&self, _micros: i64) -> Option<String> {
        None
    }

    fn venue_ticker(&self, _channel: &str, symbol: &str) -> Option<Ticker> {
        self.tickers.get(symbol).cloned()
    }
}

/// A local HTTP server standing in for the venue: it answers every request with
/// one status and body, and records what it was asked.
#[cfg(test)]
pub(crate) mod stand_in {
    use std::sync::{Arc, Mutex};

    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    /// A `best_bid_ask` answer for BTC and ETH, in the shape Tier 8 decodes.
    pub const ANSWER: &str = r#"{"results":[{"symbol":"BTC-USD","bid_inclusive_of_sell_spread":"81190.50","ask_inclusive_of_buy_spread":"81235.50"},{"symbol":"ETH-USD","bid_inclusive_of_sell_spread":"4010.10","ask_inclusive_of_buy_spread":"4012.20"}]}"#;

    /// One request as it arrived.
    #[derive(Debug, Clone)]
    pub struct Asked {
        pub path_and_query: String,
        pub headers: Vec<(String, String)>,
    }

    impl Asked {
        pub fn header(&self, name: &str) -> Option<&str> {
            self.headers
                .iter()
                .find(|(n, _)| n.eq_ignore_ascii_case(name))
                .map(|(_, v)| v.as_str())
        }
    }

    /// Serve `status` and `body` on 127.0.0.1 until the test ends. Returns the
    /// base URL (leaked, as an endpoint is `&'static str`) and what was asked.
    pub async fn serve(status: u16, body: &'static str) -> (&'static str, Arc<Mutex<Vec<Asked>>>) {
        serve_script(&[(status, body)]).await
    }

    /// Answer the n-th request with the n-th response; the last one repeats.
    pub async fn serve_script(
        script: &[(u16, &'static str)],
    ) -> (&'static str, Arc<Mutex<Vec<Asked>>>) {
        let script = script.to_vec();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url: &'static str =
            Box::leak(format!("http://{}", listener.local_addr().unwrap()).into_boxed_str());
        let asked = Arc::new(Mutex::new(Vec::new()));
        let record = asked.clone();
        tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    return;
                };
                let mut raw = Vec::new();
                let mut buf = [0u8; 4096];
                while !raw.windows(4).any(|w| w == b"\r\n\r\n") {
                    match socket.read(&mut buf).await {
                        Ok(0) | Err(_) => break,
                        Ok(n) => raw.extend_from_slice(&buf[..n]),
                    }
                }
                let text = String::from_utf8_lossy(&raw).to_string();
                let mut lines = text.split("\r\n");
                let path_and_query = lines
                    .next()
                    .and_then(|l| l.split(' ').nth(1))
                    .unwrap_or_default()
                    .to_string();
                let headers = lines
                    .take_while(|l| !l.is_empty())
                    .filter_map(|l| l.split_once(": "))
                    .map(|(n, v)| (n.to_string(), v.to_string()))
                    .collect();
                let n = {
                    let mut asked = record.lock().unwrap();
                    asked.push(Asked {
                        path_and_query,
                        headers,
                    });
                    asked.len() - 1
                };
                let (status, body) = script[n.min(script.len() - 1)];
                let reason = if status == 200 { "OK" } else { "Refused" };
                let response = format!(
                    "HTTP/1.1 {status} {reason}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = socket.write_all(response.as_bytes()).await;
                let _ = socket.shutdown().await;
            }
        });
        (url, asked)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::Engine;
    use ed25519_dalek::{Signature, SigningKey, Verifier};

    use super::stand_in::ANSWER;
    const SEED: [u8; 32] = [7u8; 32];
    const AT: i64 = 1_789_941_180_123_456;

    fn signed(url: &'static str) -> RhCrypto {
        let seed = base64::engine::general_purpose::STANDARD.encode(SEED);
        RhCrypto::new(Config {
            // Declared out of order: the request must not depend on it.
            tickers: vec!["ETH".into(), "BTC".into()],
            poll_secs: 5,
            credential: Some(Arc::new(Credential::new("API-KEY", &seed).unwrap())),
        })
        .unwrap()
        .reaching(url)
    }

    fn source(adapter: &RhCrypto) -> crate::source::poll::PollSource {
        let Transport::Poll {
            rest_url,
            path,
            symbols,
            signer,
            ..
        } = adapter.transport()
        else {
            panic!("a poll");
        };
        crate::source::poll::PollSource::new(rest_url, path, &symbols, signer.unwrap())
    }

    #[tokio::test]
    async fn the_request_signed_is_the_request_sent() {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let (url, asked) = stand_in::serve(200, ANSWER).await;
        let bytes = source(&signed(url)).ask(AT).await.unwrap();
        assert_eq!(bytes, ANSWER.as_bytes());

        let asked = asked.lock().unwrap()[0].clone();
        let expected = "/api/v1/crypto/marketdata/best_bid_ask/?symbol=BTC-USD&symbol=ETH-USD";
        assert_eq!(
            asked.path_and_query, expected,
            "sorted, repeated, one request"
        );

        // Whole seconds, and a signature that VERIFIES over exactly what was
        // sent — not one recomputed and compared, which a shared bug passes.
        assert_eq!(asked.header("x-api-key"), Some("API-KEY"));
        let timestamp = asked.header("x-timestamp").unwrap();
        assert_eq!(timestamp, (AT / 1_000_000).to_string());
        let signature = base64::engine::general_purpose::STANDARD
            .decode(asked.header("x-signature").unwrap())
            .unwrap();
        let message = format!("API-KEY{timestamp}{expected}GET");
        SigningKey::from_bytes(&SEED)
            .verifying_key()
            .verify(
                message.as_bytes(),
                &Signature::from_slice(&signature).unwrap(),
            )
            .expect("the signature covers the path WITH its query");
    }

    #[tokio::test]
    async fn a_throttled_answer_is_throttled_and_a_refusal_is_unreachable() {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let (throttling, _) = stand_in::serve(429, "slow down").await;
        assert_eq!(
            source(&signed(throttling)).ask(AT).await,
            Err(crate::capture::poll::Refusal::Throttled)
        );
        let (refusing, _) = stand_in::serve(401, "who are you").await;
        assert_eq!(
            source(&signed(refusing)).ask(AT).await,
            Err(crate::capture::poll::Refusal::Unreachable)
        );
    }

    #[test]
    fn a_replay_builds_it_with_no_signer() {
        let adapter = RhCrypto::new(Config {
            tickers: vec!["BTC".into()],
            poll_secs: 5,
            credential: None,
        })
        .unwrap();
        let Transport::Poll {
            signer,
            interval_micros,
            ..
        } = adapter.transport()
        else {
            panic!("a poll");
        };
        assert!(signer.is_none());
        assert_eq!(interval_micros, 5_000_000);
    }
}
