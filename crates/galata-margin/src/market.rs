//! **Each venue's perpetual, read when advice is asked for** (tasks 6.4 and
//! 6.5): its book at the hedge's size, its current funding and its mark.
//! Design D7, revised 2026-11-27: no Derive capture is deployed, so nothing
//! of a Derive perpetual is on the tape, and depth beyond the best price is
//! captured on neither venue.
//!
//! Read live on 2026-11-27 (`design/measured.md`, "Each venue's perpetual,
//! read live"):
//! - **Hyperliquid** answers both over REST at its `info` endpoint:
//!   `metaAndAssetCtxs` (funding hourly, mark, oracle, maximum leverage) and
//!   `l2Book` (20 levels a side).
//! - **Derive** states funding in `public/get_ticker` (`f`, hourly, with the
//!   mark `M`). Its book is a websocket channel only,
//!   `orderbook.<name>.1.<depth>`: one snapshot is taken and the connection
//!   closed.
//!
//! Every read is public, with no credential. No error carries a URL.

use std::str::FromStr;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use rust_decimal::Decimal;

use crate::placement::{Book, Funding};

/// Hyperliquid's mainnet `info` endpoint, compiled in.
pub const HYPERLIQUID_INFO: &str = "https://api.hyperliquid.xyz/info";
/// Derive's mainnet REST root and websocket, compiled in.
pub const DERIVE_REST: &str = "https://api.derive.xyz/v3";
/// Derive's websocket.
pub const DERIVE_WS: &str = "wss://api.derive.xyz/v3/ws";
/// Levels of Derive's book read: the deepest the channel offers below 100.
const DERIVE_DEPTH: u32 = 20;
/// Longest a read may take, the websocket's first snapshot included.
const READ_TIMEOUT: Duration = Duration::from_secs(10);

/// Why a market read gave nothing. No variant holds a URL.
#[derive(Debug, thiserror::Error)]
pub enum MarketError {
    /// The request did not reach the venue.
    #[error("{what} did not reach {venue}: {source}")]
    Unreachable {
        /// The venue.
        venue: &'static str,
        /// What was asked.
        what: &'static str,
        /// What failed, with its URL removed where it was built.
        source: reqwest::Error,
    },
    /// The venue answered with something unusable.
    #[error("{venue}'s {what} could not be read: {reason}")]
    Unreadable {
        /// The venue.
        venue: &'static str,
        /// What was asked.
        what: &'static str,
        /// Why.
        reason: String,
    },
}

impl MarketError {
    fn unreachable(venue: &'static str, what: &'static str, source: reqwest::Error) -> MarketError {
        MarketError::Unreachable {
            venue,
            what,
            source: source.without_url(),
        }
    }

    fn unreadable(
        venue: &'static str,
        what: &'static str,
        reason: impl Into<String>,
    ) -> MarketError {
        MarketError::Unreadable {
            venue,
            what,
            reason: reason.into(),
        }
    }
}

/// What Hyperliquid states of one market.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HyperliquidMarket {
    /// Its current funding and mark.
    pub funding: Funding,
    /// Its maximum leverage.
    pub max_leverage: u32,
    /// The borrow oracle price of each collateral token asked for: the median
    /// of its perpetual's mark and oracle, the spot price the venue's median
    /// also takes not being read (`hyperliquid`).
    pub prices: std::collections::BTreeMap<String, Decimal>,
}

/// The perpetual whose mark and oracle price a spot token: the venue's BTC
/// is `UBTC` in spot and `BTC` as a perpetual.
fn perp_of(token: &str) -> &str {
    match token {
        "UBTC" => "BTC",
        other => other,
    }
}

/// The reads, against the venues' compiled-in addresses or a test's.
#[derive(Debug, Clone)]
pub struct MarketReader {
    http: reqwest::Client,
    hyperliquid_info: String,
    derive_rest: String,
    derive_ws: String,
}

fn dec(v: &serde_json::Value) -> Option<Decimal> {
    match v {
        serde_json::Value::String(s) => Decimal::from_str(s).ok(),
        _ => None,
    }
}

impl MarketReader {
    /// The venues' mainnet addresses.
    pub fn mainnet() -> MarketReader {
        MarketReader::at(HYPERLIQUID_INFO, DERIVE_REST, DERIVE_WS)
    }

    /// Given addresses: a test's local servers.
    pub fn at(hyperliquid_info: &str, derive_rest: &str, derive_ws: &str) -> MarketReader {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let http = reqwest::Client::builder()
            .connect_timeout(READ_TIMEOUT)
            .timeout(READ_TIMEOUT)
            .build()
            .unwrap_or_else(|_| reqwest::Client::new());
        MarketReader {
            http,
            hyperliquid_info: hyperliquid_info.to_string(),
            derive_rest: derive_rest.trim_end_matches('/').to_string(),
            derive_ws: derive_ws.to_string(),
        }
    }

    async fn hyperliquid(
        &self,
        body: serde_json::Value,
        what: &'static str,
    ) -> Result<serde_json::Value, MarketError> {
        let response = self
            .http
            .post(&self.hyperliquid_info)
            .json(&body)
            .send()
            .await
            .map_err(|e| MarketError::unreachable("Hyperliquid", what, e))?;
        if !response.status().is_success() {
            return Err(MarketError::unreadable(
                "Hyperliquid",
                what,
                format!("HTTP {}", response.status().as_u16()),
            ));
        }
        response
            .json()
            .await
            .map_err(|e| MarketError::unreachable("Hyperliquid", what, e))
    }

    /// Hyperliquid's market for a coin on the main dex, read at `now_micros`;
    /// `Ok(None)` when Hyperliquid lists no perpetual for it.
    pub async fn hyperliquid_market(
        &self,
        coin: &str,
        collateral: &[String],
        now_micros: i64,
    ) -> Result<Option<HyperliquidMarket>, MarketError> {
        const WHAT: &str = "metaAndAssetCtxs";
        let answer = self
            .hyperliquid(serde_json::json!({"type": WHAT}), WHAT)
            .await?;
        let (Some(meta), Some(ctxs)) = (answer.get(0), answer.get(1).and_then(|c| c.as_array()))
        else {
            return Err(MarketError::unreadable(
                "Hyperliquid",
                WHAT,
                "not a meta and its contexts",
            ));
        };
        let universe = meta
            .get("universe")
            .and_then(|u| u.as_array())
            .ok_or_else(|| MarketError::unreadable("Hyperliquid", WHAT, "no universe"))?;
        let Some(i) = universe
            .iter()
            .position(|m| m.get("name").and_then(|n| n.as_str()) == Some(coin))
        else {
            return Ok(None);
        };
        let ctx = ctxs.get(i).ok_or_else(|| {
            MarketError::unreadable("Hyperliquid", WHAT, format!("no context for {coin}"))
        })?;
        let field = |name: &str| {
            dec(&ctx[name]).ok_or_else(|| {
                MarketError::unreadable("Hyperliquid", WHAT, format!("`{name}` is not a decimal"))
            })
        };
        let mut prices = std::collections::BTreeMap::new();
        for token in collateral {
            let perp = perp_of(token);
            let found = universe
                .iter()
                .position(|m| m.get("name").and_then(|n| n.as_str()) == Some(perp))
                .and_then(|j| ctxs.get(j))
                .and_then(|c| Some((dec(&c["markPx"])?, dec(&c["oraclePx"])?)));
            if let Some((mark, oracle)) = found {
                prices.insert(token.clone(), (mark + oracle) / Decimal::from(2));
            }
        }
        Ok(Some(HyperliquidMarket {
            prices,
            funding: Funding {
                hourly_rate: field("funding")?,
                mark: field("markPx")?,
                at_micros: now_micros,
            },
            max_leverage: universe[i]
                .get("maxLeverage")
                .and_then(|m| m.as_u64())
                .and_then(|m| u32::try_from(m).ok())
                .ok_or_else(|| {
                    MarketError::unreadable(
                        "Hyperliquid",
                        WHAT,
                        format!("no maximum leverage for {coin}"),
                    )
                })?,
        }))
    }

    /// Hyperliquid's book for a coin.
    pub async fn hyperliquid_book(&self, coin: &str) -> Result<Book, MarketError> {
        const WHAT: &str = "l2Book";
        let answer = self
            .hyperliquid(serde_json::json!({"type": WHAT, "coin": coin}), WHAT)
            .await?;
        let side = |i: usize| -> Result<Vec<(Decimal, Decimal)>, MarketError> {
            answer["levels"][i]
                .as_array()
                .ok_or_else(|| MarketError::unreadable("Hyperliquid", WHAT, "no levels"))?
                .iter()
                .map(|l| match (dec(&l["px"]), dec(&l["sz"])) {
                    (Some(p), Some(s)) => Ok((p, s)),
                    _ => Err(MarketError::unreadable(
                        "Hyperliquid",
                        WHAT,
                        "a level is not two decimals",
                    )),
                })
                .collect()
        };
        Ok(Book {
            bids: side(0)?,
            asks: side(1)?,
            at_micros: answer["time"]
                .as_i64()
                .ok_or_else(|| MarketError::unreadable("Hyperliquid", WHAT, "no time"))?
                * 1_000,
        })
    }

    /// Derive's funding and mark for a perpetual, from its ticker.
    pub async fn derive_funding(&self, perp: &str) -> Result<Funding, MarketError> {
        const WHAT: &str = "public/get_ticker";
        let response = self
            .http
            .post(format!("{}/{WHAT}", self.derive_rest))
            .json(&serde_json::json!({"instrument_name": perp}))
            .send()
            .await
            .map_err(|e| MarketError::unreachable("Derive", WHAT, e))?;
        let answer: serde_json::Value = response
            .json()
            .await
            .map_err(|e| MarketError::unreachable("Derive", WHAT, e))?;
        let t = answer
            .get("result")
            .ok_or_else(|| MarketError::unreadable("Derive", WHAT, "no result"))?;
        Ok(Funding {
            hourly_rate: dec(&t["f"]).ok_or_else(|| {
                MarketError::unreadable("Derive", WHAT, "no funding rate: is it a perpetual?")
            })?,
            mark: dec(&t["M"]).ok_or_else(|| MarketError::unreadable("Derive", WHAT, "no mark"))?,
            at_micros: t["t"]
                .as_i64()
                .ok_or_else(|| MarketError::unreadable("Derive", WHAT, "no time"))?
                * 1_000,
        })
    }

    /// Derive's book for a perpetual: one snapshot of its order-book channel.
    pub async fn derive_book(&self, perp: &str) -> Result<Book, MarketError> {
        const WHAT: &str = "order book";
        let channel = format!("orderbook.{perp}.1.{DERIVE_DEPTH}");
        let read = async {
            let (mut ws, _) = tokio_tungstenite::connect_async(self.derive_ws.as_str())
                .await
                .map_err(|e| {
                    MarketError::unreadable(
                        "Derive",
                        WHAT,
                        format!("the websocket did not open: {}", websocket_reason(&e)),
                    )
                })?;
            let subscribe = serde_json::json!({"jsonrpc": "2.0", "id": 1, "method": "subscribe", "params": {"channels": [channel]}});
            ws.send(tokio_tungstenite::tungstenite::Message::text(
                subscribe.to_string(),
            ))
            .await
            .map_err(|e| MarketError::unreadable("Derive", WHAT, websocket_reason(&e)))?;
            while let Some(message) = ws.next().await {
                let message = message
                    .map_err(|e| MarketError::unreadable("Derive", WHAT, websocket_reason(&e)))?;
                let Ok(text) = message.to_text() else {
                    continue;
                };
                let Ok(frame) = serde_json::from_str::<serde_json::Value>(text) else {
                    continue;
                };
                if frame.get("method").and_then(|m| m.as_str()) != Some("subscription") {
                    continue;
                }
                let data = &frame["params"]["data"];
                let _ = ws.close(None).await;
                let side = |k: &str| -> Result<Vec<(Decimal, Decimal)>, MarketError> {
                    data[k]
                        .as_array()
                        .ok_or_else(|| MarketError::unreadable("Derive", WHAT, "no levels"))?
                        .iter()
                        .map(|l| match (dec(&l[0]), dec(&l[1])) {
                            (Some(p), Some(s)) => Ok((p, s)),
                            _ => Err(MarketError::unreadable(
                                "Derive",
                                WHAT,
                                "a level is not two decimals",
                            )),
                        })
                        .collect()
                };
                return Ok(Book {
                    bids: side("bids")?,
                    asks: side("asks")?,
                    at_micros: data["timestamp"]
                        .as_i64()
                        .ok_or_else(|| MarketError::unreadable("Derive", WHAT, "no time"))?
                        * 1_000,
                });
            }
            Err(MarketError::unreadable(
                "Derive",
                WHAT,
                "the websocket closed before a snapshot",
            ))
        };
        tokio::time::timeout(READ_TIMEOUT, read)
            .await
            .map_err(|_| {
                MarketError::unreadable(
                    "Derive",
                    WHAT,
                    format!("no snapshot within {} s", READ_TIMEOUT.as_secs()),
                )
            })?
    }
}

/// A websocket error's kind, without the URL some of them carry.
fn websocket_reason(e: &tokio_tungstenite::tungstenite::Error) -> String {
    use tokio_tungstenite::tungstenite::Error;
    match e {
        Error::Http(response) => format!("HTTP {}", response.status().as_u16()),
        Error::Io(io) => io.kind().to_string(),
        Error::ConnectionClosed | Error::AlreadyClosed => "the connection closed".into(),
        Error::Tls(_) => "TLS failed".into(),
        other => format!("{:?}", std::mem::discriminant(other)),
    }
}

#[cfg(test)]
mod tests {
    use std::io::{BufRead, BufReader, Read, Write};
    use std::net::TcpListener;

    use super::*;

    fn d(s: &str) -> Decimal {
        Decimal::from_str(s).unwrap()
    }

    fn fixture(name: &str) -> String {
        std::fs::read_to_string(format!(
            "{}/tests/fixtures/{name}",
            env!("CARGO_MANIFEST_DIR")
        ))
        .unwrap()
    }

    /// Answer each POST by its body's `type` (Hyperliquid) or path (Derive).
    fn serve(route: fn(&str, &serde_json::Value) -> String) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        std::thread::spawn(move || {
            for stream in listener.incoming().flatten() {
                let mut reader = BufReader::new(stream.try_clone().unwrap());
                let mut line = String::new();
                reader.read_line(&mut line).unwrap();
                let path = line
                    .split_whitespace()
                    .nth(1)
                    .unwrap_or_default()
                    .to_string();
                let mut length = 0;
                loop {
                    let mut h = String::new();
                    reader.read_line(&mut h).unwrap();
                    if h == "\r\n" || h.is_empty() {
                        break;
                    }
                    if let Some(v) = h.to_ascii_lowercase().strip_prefix("content-length:") {
                        length = v.trim().parse().unwrap();
                    }
                }
                let mut body = vec![0; length];
                reader.read_exact(&mut body).unwrap();
                let answer = route(&path, &serde_json::from_slice(&body).unwrap_or_default());
                let mut stream = stream;
                let _ = stream.write_all(format!("HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{answer}", answer.len()).as_bytes());
            }
        });
        url
    }

    fn venues(_: &str, body: &serde_json::Value) -> String {
        match body["type"].as_str() {
            Some("metaAndAssetCtxs") => fixture("hyperliquid_metaAndAssetCtxs_BTC.json"),
            Some("l2Book") => fixture("hyperliquid_l2Book_BTC.json"),
            _ => fixture("derive_ticker_BTC-PERP.json"),
        }
    }

    fn block_on<F: std::future::Future>(f: F) -> F::Output {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(f)
    }

    #[test]
    fn hyperliquids_market_and_book_are_read_as_it_stated_them() {
        let url = serve(venues);
        let reader = MarketReader::at(&url, &url, "ws://127.0.0.1:1");
        let market =
            block_on(reader.hyperliquid_market("BTC", &["UBTC".into(), "NOPE".into()], 42))
                .unwrap()
                .expect("listed");
        // UBTC priced by the BTC perpetual: the median of its mark and oracle.
        assert_eq!(
            market.prices.get("UBTC"),
            Some(&((d("59875.0") + d("59896.5")) / d("2")))
        );
        assert!(!market.prices.contains_key("NOPE"));
        assert_eq!(market.funding.hourly_rate, d("0.0000125"));
        assert_eq!(market.funding.mark, d("59875.0"));
        assert_eq!(market.funding.at_micros, 42);
        assert_eq!(market.max_leverage, 40);
        assert_eq!(
            block_on(reader.hyperliquid_market("NOPE", &[], 42)).unwrap(),
            None,
            "an unlisted coin"
        );
        let book = block_on(reader.hyperliquid_book("BTC")).unwrap();
        assert_eq!((book.bids.len(), book.asks.len()), (20, 20));
        assert_eq!(book.bids[0].0, d("59872.0"));
        assert_eq!(book.at_micros, 1_795_800_253_517_000);
    }

    #[test]
    fn derives_funding_is_its_tickers_rate_mark_and_time() {
        let url = serve(venues);
        let reader = MarketReader::at(&url, &url, "ws://127.0.0.1:1");
        let f = block_on(reader.derive_funding("BTC-PERP")).unwrap();
        assert_eq!(f.hourly_rate, d("0.000012240"));
        assert_eq!(f.at_micros, 1_795_800_245_092_000);
    }

    #[test]
    fn derives_book_is_one_snapshot_of_its_channel() {
        let snapshot = fixture("derive_orderbook_BTC-PERP.json");
        let book = block_on(async move {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let url = format!("ws://{}", listener.local_addr().unwrap());
            let server = tokio::spawn(async move {
                let (stream, _) = listener.accept().await.unwrap();
                let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();
                let asked = ws.next().await.unwrap().unwrap();
                let asked: serde_json::Value =
                    serde_json::from_str(asked.to_text().unwrap()).unwrap();
                assert_eq!(asked["params"]["channels"][0], "orderbook.BTC-PERP.1.20");
                ws.send(tokio_tungstenite::tungstenite::Message::text(
                    r#"{"id":1,"result":{"status":{}}}"#,
                ))
                .await
                .unwrap();
                ws.send(tokio_tungstenite::tungstenite::Message::text(snapshot))
                    .await
                    .unwrap();
                let _ = ws.next().await;
            });
            let book = MarketReader::at("http://127.0.0.1:1", "http://127.0.0.1:1", &url)
                .derive_book("BTC-PERP")
                .await;
            server.await.unwrap();
            book
        })
        .unwrap();
        assert_eq!((book.bids.len(), book.asks.len()), (10, 10));
        assert_eq!(book.bids[0], (d("59791.2"), d("1.7058")));
        assert_eq!(book.at_micros, 1_795_800_275_699_000);
    }

    #[test]
    fn an_unanswered_read_says_which_and_no_address() {
        let reader = MarketReader::at(
            "http://127.0.0.1:1/info",
            "http://127.0.0.1:1",
            "ws://127.0.0.1:1",
        );
        let said = block_on(reader.hyperliquid_book("BTC"))
            .unwrap_err()
            .to_string();
        assert!(
            said.contains("l2Book") && said.contains("Hyperliquid"),
            "{said}"
        );
        assert!(!said.contains("127.0.0.1"), "{said}");
        let said = block_on(reader.derive_book("BTC-PERP"))
            .unwrap_err()
            .to_string();
        assert!(
            said.contains("Derive") && !said.contains("127.0.0.1"),
            "{said}"
        );
    }
}
