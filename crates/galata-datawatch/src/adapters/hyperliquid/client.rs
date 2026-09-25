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

use super::VENUE;
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
            http: reqwest::Client::new(),
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
        Ok(Payload {
            seq: 0,
            recv_micros: now_micros,
            address: PayloadAddress::Venue(VENUE.into()),
            channel: "candleSnapshot".into(),
            kind: "candles".into(),
            symbol: Some(symbol.to_string()),
            // Covers a range nothing will fetch again, so it is durable before
            // the walk advances past it.
            origin: Origin::Fetched,
            payload: bytes,
        })
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

    /// Every coin a dex lists.
    ///
    /// **One request answers for a whole dex.** `dex` is empty for the main
    /// perp dex.
    pub async fn universe(&self, dex: &str) -> Result<Vec<String>, FetchError> {
        let body = serde_json::json!({ "type": "meta", "dex": dex });
        let bytes = self.post(&body).await?;
        let value: serde_json::Value =
            serde_json::from_slice(&bytes).map_err(|e| FetchError::Universe {
                venue: VENUE,
                detail: e.to_string(),
            })?;
        let universe = value
            .get("universe")
            .and_then(|u| u.as_array())
            .ok_or_else(|| FetchError::Universe {
                venue: VENUE,
                detail: "no `universe` array".into(),
            })?;
        Ok(universe
            .iter()
            .filter_map(|a| a.get("name").and_then(|n| n.as_str()))
            .map(str::to_string)
            .collect())
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
            return Err(FetchError::Status {
                venue: VENUE,
                path: INFO_PATH,
                status: status.as_u16(),
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
}
