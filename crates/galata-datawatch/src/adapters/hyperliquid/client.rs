//! The venue's info endpoint: history, and the asset universe.
//!
//! Every call returns **raw bytes and the moment they arrived**, because the
//! record stores what arrived and normalisation happens after it is durable.
//! Nothing here parses a payload it is about to hand back.
//!
//! And nothing here pages. The venue's paging shape is **declared**, and a
//! client that paged for itself would hold a second copy of that knowledge —
//! which would disagree with the declaration the moment a venue changed one of
//! them.

use galata_wire::Origin;

use super::{VENUE, meta_page, wire};
use crate::record::{Payload, PayloadAddress};

/// The one path this client asks for.
const INFO_PATH: &str = "/info";

/// Why a request to the venue failed.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum FetchError {
    /// The request could not be made.
    ///
    /// **Build it with `FetchError::http`**, never as a struct literal.
    #[error("{venue} {path}: {source}")]
    Http {
        /// Which venue.
        venue: &'static str,
        /// **The path asked for, not the URL.** This venue's endpoint is a
        /// compiled-in public address today, but an error type that holds a
        /// URL is one configuration change away from holding a key — and the
        /// path is the part that actually says which call failed.
        path: &'static str,
        /// Why. **Already stripped of its URL.**
        #[source]
        source: reqwest::Error,
    },
    /// The venue answered with a failure.
    #[error("{venue} {path}: the venue answered {status}")]
    Status {
        /// Which venue.
        venue: &'static str,
        /// The path asked for.
        path: &'static str,
        /// What it said.
        status: u16,
        /// Its `Retry-After`, in whole seconds, where it sent one. Hyperliquid
        /// documents none and none has been seen (2026-09-29); read in case.
        retry_after_secs: Option<u64>,
    },
    /// The universe payload is not what the venue documents.
    #[error("{venue}: the universe payload is not what the venue documents: {detail}")]
    Universe {
        /// Which venue.
        venue: &'static str,
        /// What was wrong.
        detail: String,
    },
}

impl FetchError {
    /// **The only way to build a [`FetchError::Http`].**
    ///
    /// reqwest's `Display` carries the whole URL, path and query; `without_url`
    /// removes it without losing the source chain that says why.
    fn http(venue: &'static str, source: reqwest::Error) -> FetchError {
        FetchError::Http {
            venue,
            path: INFO_PATH,
            source: source.without_url(),
        }
    }
}

/// The info endpoint.
#[derive(Debug, Clone)]
pub struct Client {
    http: reqwest::Client,
    info_url: String,
}

impl Client {
    /// A client for a venue's REST root.
    pub fn new(rest_url: &str) -> Client {
        Client {
            http: crate::source::http_client(),
            info_url: format!("{rest_url}/info"),
        }
    }

    /// One page of candle history.
    ///
    /// Takes a **range** and returns **what it got**; the caller does the
    /// walking. `now_micros` is the caller's clock — nothing below the capture
    /// loop reads one.
    pub async fn candles(
        &self,
        symbol: &str,
        interval: &str,
        from_micros: i64,
        to_micros: i64,
        now_micros: i64,
    ) -> Result<Payload, FetchError> {
        let body = serde_json::json!({
            "type": "candleSnapshot",
            "req": {
                "coin": symbol,
                "interval": interval,
                "startTime": from_micros / 1_000,
                "endTime": to_micros / 1_000,
            }
        });
        let bytes = self.post(&body).await?;
        Ok(candle_page(symbol, bytes, now_micros))
    }

    /// One page of funding history: the oldest rows at or after `from_micros`,
    /// as the venue pages it. The caller pages forward from the last row's
    /// time; this returns what it got.
    pub async fn funding(
        &self,
        symbol: &str,
        from_micros: i64,
        to_micros: i64,
        now_micros: i64,
    ) -> Result<Payload, FetchError> {
        let body = serde_json::json!({
            "type": "fundingHistory",
            "coin": symbol,
            "startTime": from_micros / 1_000,
            "endTime": to_micros / 1_000,
        });
        let bytes = self.post(&body).await?;
        Ok(Payload {
            seq: 0,
            recv_micros: now_micros,
            address: PayloadAddress::Venue(VENUE.into()),
            channel: "fundingHistory".into(),
            // The kind the live `activeAssetCtx` partitions under: one series,
            // two ways of hearing it.
            kind: "funding".into(),
            symbol: Some(symbol.to_string()),
            origin: Origin::Fetched,
            payload: bytes,
        })
    }

    /// A dex's `meta`, **as the venue's bytes**: every coin it lists, with the
    /// rules reference data is normalised from.
    ///
    /// **One request answers for a whole dex.** `dex` is empty for the main
    /// perp dex. The answer is a payload rather than a list, so that what the
    /// universe check and the reference data were decided on is what the
    /// record holds (`normalise-instrument-reference`). A reply that is not a
    /// universe is refused here, before anything is archived as one.
    pub async fn meta(&self, dex: &str, now_micros: i64) -> Result<Payload, FetchError> {
        let body = serde_json::json!({ "type": wire::META, "dex": dex });
        let bytes = self.post(&body).await?;
        serde_json::from_slice::<wire::Meta>(&bytes).map_err(|e| FetchError::Universe {
            venue: VENUE,
            detail: e.to_string(),
        })?;
        Ok(meta_page(bytes, now_micros))
    }

    /// One account's perp state on one dex, raw. `dex` is empty for the main one.
    ///
    /// **The address is exposed here and nowhere else**: into the request
    /// body, which goes to the venue and into no error and no log.
    #[cfg(feature = "ledger")]
    pub async fn clearinghouse_state(
        &self,
        address: &crate::config::Secret,
        dex: &str,
    ) -> Result<Vec<u8>, FetchError> {
        let mut body = serde_json::json!({
            "type": "clearinghouseState",
            "user": address.expose(),
        });
        // The main dex is asked without the key, as measured; an explicit
        // empty string is not a spelling the venue documents.
        if !dex.is_empty() {
            body["dex"] = serde_json::Value::String(dex.to_string());
        }
        self.post(&body).await
    }

    /// A master's sub-accounts, raw. `null` where it has none.
    #[cfg(feature = "ledger")]
    pub async fn sub_accounts(
        &self,
        address: &crate::config::Secret,
    ) -> Result<Vec<u8>, FetchError> {
        self.post(&serde_json::json!({ "type": "subAccounts", "user": address.expose() }))
            .await
    }

    /// What the venue says an address is, raw. Weighs 60: asked once, at boot.
    #[cfg(feature = "ledger")]
    pub async fn user_role(&self, address: &crate::config::Secret) -> Result<Vec<u8>, FetchError> {
        self.post(&serde_json::json!({ "type": "userRole", "user": address.expose() }))
            .await
    }

    /// How the venue holds an account's collateral, raw. **Undocumented**
    /// (`design/measured.md`, 2026-09-25), so its answer is read as a mode
    /// only when it is one of the four measured strings.
    #[cfg(feature = "ledger")]
    pub async fn user_abstraction(
        &self,
        address: &crate::config::Secret,
    ) -> Result<Vec<u8>, FetchError> {
        self.post(&serde_json::json!({ "type": "userAbstraction", "user": address.expose() }))
            .await
    }

    /// An account's spot state, raw: its balances and, under portfolio margin,
    /// the venue's own portfolio-margin figures, which the documentation does
    /// not list (`design/measured.md`, 2026-11-26). Account-wide, so asked
    /// once per account and never per dex. Weighs 2.
    #[cfg(feature = "ledger")]
    pub async fn spot_clearinghouse_state(
        &self,
        address: &crate::config::Secret,
    ) -> Result<Vec<u8>, FetchError> {
        self.post(&serde_json::json!({
            "type": "spotClearinghouseState",
            "user": address.expose(),
        }))
        .await
    }

    /// What an account has borrowed and supplied, per token index, and its
    /// health, raw. Account-wide. Weighs 20.
    #[cfg(feature = "ledger")]
    pub async fn borrow_lend_user_state(
        &self,
        address: &crate::config::Secret,
    ) -> Result<Vec<u8>, FetchError> {
        self.post(&serde_json::json!({
            "type": "borrowLendUserState",
            "user": address.expose(),
        }))
        .await
    }

    /// The spot tokens' reference data, raw: what names a token index in the
    /// two answers above. Asks about no account. Weighs 20.
    #[cfg(feature = "ledger")]
    pub async fn spot_meta(&self) -> Result<Vec<u8>, FetchError> {
        self.post(&serde_json::json!({ "type": "spotMeta" })).await
    }

    /// One page of an account's events, raw: the oldest at or after
    /// `start_ms`, as the venue pages each kind (2,000 fills, 500 funding
    /// payments, 2,000 ledger updates; measured 2026-09-25). `kind` is the
    /// venue's request type: `userFillsByTime`, `userFunding` or
    /// `userNonFundingLedgerUpdates`.
    #[cfg(feature = "ledger")]
    pub async fn events_page(
        &self,
        kind: &'static str,
        address: &crate::config::Secret,
        start_ms: i64,
    ) -> Result<Vec<u8>, FetchError> {
        self.post(&serde_json::json!({
            "type": kind,
            "user": address.expose(),
            "startTime": start_ms,
        }))
        .await
    }

    /// Whether the venue knows a dex: `Some(true)` it listed one, `Some(false)`
    /// **it said there is none**, `None` it did not answer the question.
    ///
    /// Measured 2026-09-25: `meta` (and `clearinghouseState`) for a dex that
    /// does not exist answer **HTTP 500 with the body `null`**. A bare 500 is
    /// also what an outage looks like, so only that exact pair is read as *no
    /// such dex*; any other failure is not an answer.
    #[cfg(feature = "ledger")]
    pub async fn dex_known(&self, dex: &str) -> Result<Option<bool>, FetchError> {
        let response = self
            .http
            .post(&self.info_url)
            .json(&serde_json::json!({ "type": "meta", "dex": dex }))
            .send()
            .await
            .map_err(|source| FetchError::http(VENUE, source))?;
        let status = response.status().as_u16();
        let body = response
            .bytes()
            .await
            .map_err(|source| FetchError::http(VENUE, source))?;
        Ok(match status {
            200 => Some(true),
            500 if body.as_ref().trim_ascii() == b"null" => Some(false),
            _ => None,
        })
    }

    async fn post(&self, body: &serde_json::Value) -> Result<Vec<u8>, FetchError> {
        let response = self
            .http
            // **The one place the URL is used.** It goes to the client and
            // nowhere else — not into an error, not into a log.
            .post(&self.info_url)
            .json(body)
            .send()
            .await
            .map_err(|source| FetchError::http(VENUE, source))?;
        let status = response.status();
        if !status.is_success() {
            let retry_after_secs = response
                .headers()
                .get(reqwest::header::RETRY_AFTER)
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.trim().parse::<u64>().ok());
            return Err(FetchError::Status {
                venue: VENUE,
                path: INFO_PATH,
                status: status.as_u16(),
                retry_after_secs,
            });
        }
        response
            .bytes()
            .await
            .map(|b| b.to_vec())
            .map_err(|source| FetchError::http(VENUE, source))
    }
}

impl FetchError {
    /// The walk's reading of this failure: **a 429 is a wait**, with the
    /// venue's `Retry-After` where it sent one, and anything else a failure.
    #[cfg(feature = "capture")]
    pub fn into_failure(self) -> crate::capture::FetchFailure {
        match self {
            FetchError::Status {
                status: 429,
                retry_after_secs,
                ..
            } => crate::capture::FetchFailure::throttled(self.to_string(), retry_after_secs),
            other => crate::capture::FetchFailure::from(other.to_string()),
        }
    }

    /// The poll lane's reading of this failure: **a 429 is ours to fix by
    /// asking less often**, and anything else is the venue not answering.
    #[cfg(feature = "ledger")]
    pub fn refusal(&self) -> crate::capture::poll::Refusal {
        match self {
            FetchError::Status { status: 429, .. } => crate::capture::poll::Refusal::Throttled,
            _ => crate::capture::poll::Refusal::Unreachable,
        }
    }
}

/// Where a funding page ended: the last row's time and how many rows it held,
/// so the walk can issue the next page or know it had the last one.
///
/// Read **borrowed** from the bytes — the walk needs two numbers, and parsing
/// the whole page a second time to get them would double the cost of every
/// page.
pub fn funding_page_end(bytes: &[u8]) -> Option<crate::venue::PageEnd> {
    #[derive(serde::Deserialize)]
    struct Timed {
        time: i64,
    }
    let rows: Vec<Timed> = serde_json::from_slice(bytes).ok()?;
    let last = rows.last()?.time;
    Some(crate::venue::PageEnd {
        last_micros: galata_wire::millis_to_micros(last),
        rows: rows.len() as u32,
    })
}

/// A `candleSnapshot` response as the payload the one path archives — **the
/// one constructor**, used by the walk's own fetch and by an import of pages
/// saved from it, so the two cannot build different records of the same bytes.
pub fn candle_page(symbol: &str, bytes: Vec<u8>, recv_micros: i64) -> Payload {
    Payload {
        seq: 0,
        recv_micros,
        address: PayloadAddress::Venue(VENUE.into()),
        channel: "candleSnapshot".into(),
        kind: "candles".into(),
        symbol: Some(symbol.to_string()),
        // Covers a range nothing will fetch again, so it is durable before the
        // walk advances past it.
        origin: Origin::Fetched,
        payload: bytes,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_funding_page_reports_where_it_ended() {
        let page = br#"[{"coin":"BTC","fundingRate":"0.0000125","time":1000},
                        {"coin":"BTC","fundingRate":"-0.0000125","time":2000}]"#;
        let end = funding_page_end(page).unwrap();
        assert_eq!(end.rows, 2);
        assert_eq!(end.last_micros, 2_000_000);
    }

    #[test]
    fn an_empty_page_has_no_end() {
        // Not an error: a venue with nothing in the range says so, and the walk
        // stops rather than paging forever from a time it invented.
        assert!(funding_page_end(b"[]").is_none());
    }

    #[test]
    fn a_page_that_is_not_rows_has_no_end() {
        assert!(funding_page_end(b"{\"error\":\"nope\"}").is_none());
    }

    #[cfg(feature = "ledger")]
    mod balances {
        use std::sync::{Arc, Mutex};

        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        use super::super::*;
        use crate::config::Secret;

        const ADDRESS: &str = "0x00000000000000000000000000000000000000aa";

        /// Answer every request on 127.0.0.1 with one status and body, and keep
        /// what was asked: the request line and the body.
        async fn serve(status: u16, body: &'static str) -> (String, Arc<Mutex<Vec<String>>>) {
            let _ = rustls::crypto::ring::default_provider().install_default();
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let url = format!("http://{}", listener.local_addr().unwrap());
            let asked = Arc::new(Mutex::new(Vec::new()));
            let record = asked.clone();
            tokio::spawn(async move {
                loop {
                    let Ok((mut socket, _)) = listener.accept().await else {
                        return;
                    };
                    let mut raw = Vec::new();
                    let mut buf = [0u8; 4096];
                    loop {
                        match socket.read(&mut buf).await {
                            Ok(0) | Err(_) => break,
                            Ok(n) => raw.extend_from_slice(&buf[..n]),
                        }
                        let text = String::from_utf8_lossy(&raw);
                        if let Some(end) = text.find("\r\n\r\n") {
                            let length: usize = text
                                .to_lowercase()
                                .lines()
                                .find_map(|l| {
                                    l.strip_prefix("content-length: ").map(str::to_string)
                                })
                                .and_then(|v| v.trim().parse().ok())
                                .unwrap_or(0);
                            if raw.len() >= end + 4 + length {
                                break;
                            }
                        }
                    }
                    record
                        .lock()
                        .unwrap()
                        .push(String::from_utf8_lossy(&raw).to_string());
                    let reason = if status == 200 { "OK" } else { "Refused" };
                    let response = format!(
                        "HTTP/1.1 {status} {reason}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                        body.len()
                    );
                    let _ = socket.write_all(response.as_bytes()).await;
                }
            });
            (url, asked)
        }

        fn body_of(asked: &str) -> serde_json::Value {
            let (_, body) = asked.split_once("\r\n\r\n").unwrap();
            serde_json::from_str(body).unwrap()
        }

        #[tokio::test]
        async fn the_three_reads_ask_what_they_say_and_return_the_bytes_unread() {
            let answer = r#"{"balances":[],"tokenToState":[],"tokens":[]}"#;
            let (url, asked) = serve(200, answer).await;
            let client = Client::new(&url);
            let address = Secret::new(ADDRESS);
            let spot = client.spot_clearinghouse_state(&address).await.unwrap();
            let lend = client.borrow_lend_user_state(&address).await.unwrap();
            let meta = client.spot_meta().await.unwrap();
            for bytes in [spot, lend, meta] {
                assert_eq!(bytes, answer.as_bytes(), "the venue's bytes, untouched");
            }
            let asked = asked.lock().unwrap();
            assert_eq!(asked.len(), 3);
            for request in asked.iter() {
                assert!(request.starts_with("POST /info "), "{request}");
            }
            assert_eq!(
                body_of(&asked[0]),
                serde_json::json!({"type": "spotClearinghouseState", "user": ADDRESS})
            );
            assert_eq!(
                body_of(&asked[1]),
                serde_json::json!({"type": "borrowLendUserState", "user": ADDRESS})
            );
            // Account-wide reads name no dex; the reference read names no account.
            assert_eq!(body_of(&asked[2]), serde_json::json!({"type": "spotMeta"}));
        }

        #[tokio::test]
        async fn a_refused_read_names_neither_the_address_nor_the_url() {
            let (url, _) = serve(500, "busy").await;
            let client = Client::new(&url);
            let address = Secret::new(ADDRESS);
            for refusal in [
                client.spot_clearinghouse_state(&address).await.unwrap_err(),
                client.borrow_lend_user_state(&address).await.unwrap_err(),
            ] {
                let said = refusal.to_string();
                assert!(said.contains("hyperliquid"), "{said}");
                assert!(said.contains("500"), "{said}");
                assert!(!said.contains(ADDRESS), "{said}");
                assert!(!said.contains("127.0.0.1"), "{said}");
                assert!(!said.contains("://"), "{said}");
            }
        }

        #[tokio::test]
        async fn an_unreachable_venue_is_said_without_its_address() {
            let _ = rustls::crypto::ring::default_provider().install_default();
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let url = format!("http://{}", listener.local_addr().unwrap());
            drop(listener);
            let said = Client::new(&url)
                .spot_clearinghouse_state(&Secret::new(ADDRESS))
                .await
                .unwrap_err()
                .to_string();
            assert!(said.contains("hyperliquid"), "{said}");
            assert!(!said.contains("127.0.0.1"), "{said}");
            assert!(!said.contains(ADDRESS), "{said}");
        }
    }
}
