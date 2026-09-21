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
    /// **Build it with [`FetchError::http`]**, never as a struct literal.
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
