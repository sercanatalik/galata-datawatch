//! The chain's JSON-RPC endpoint.
//!
//! **Behind the `capture` feature**: it makes requests, and a thing that makes
//! requests is transport whatever else it is near.
//!
//! Like every client here, it returns **raw bytes and the moment they
//! arrived**, and parses nothing it hands back — the record stores what
//! arrived, and normalisation happens after it is durable.
//!
//! The exceptions are the two calls whose *answers are the loop's control
//! flow* rather than its data: the head, and a block's header. Those are read,
//! because a number you cannot read is a number you cannot page by.

use galata_wire::Origin;

use super::{CHAIN_ID, VENUE};
use crate::adapters::rh_chain::trail::Seen;
use crate::record::{Payload, PayloadAddress};

/// Why a call to the chain failed.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ChainError {
    /// The request could not be made.
    #[error("{venue} {method}: {source}")]
    Http {
        /// Which venue.
        venue: &'static str,
        /// Which call.
        method: &'static str,
        /// Why.
        #[source]
        source: reqwest::Error,
    },
    /// The node answered with an error object.
    #[error("{venue} {method}: the node said {detail}")]
    Rpc {
        /// Which venue.
        venue: &'static str,
        /// Which call.
        method: &'static str,
        /// What it said.
        detail: String,
    },
    /// The answer is not the shape the method documents.
    #[error("{venue} {method}: {detail}")]
    Shape {
        /// Which venue.
        venue: &'static str,
        /// Which call.
        method: &'static str,
        /// What was wrong.
        detail: String,
    },
    /// The provider is serving a different chain.
    #[error(
        "the provider at {url} serves chain {found}, and this adapter is {CHAIN_ID}. Its blocks \
         would be real and not ours"
    )]
    WrongChain {
        /// Where.
        url: String,
        /// What it said it was.
        found: u64,
    },
}

/// A JSON-RPC client for one chain.
#[derive(Debug, Clone)]
pub struct ChainClient {
    http: reqwest::Client,
    url: String,
}

impl ChainClient {
    /// A client for an endpoint.
    pub fn new(url: impl Into<String>) -> ChainClient {
        ChainClient {
            http: reqwest::Client::new(),
            url: url.into(),
        }
    }

    /// Where it points.
    pub fn url(&self) -> &str {
        &self.url
    }

    /// **Refuse a provider pointed at a different chain.**
    ///
    /// Checked at boot, like the universe check for a venue's instruments, and
    /// for the same reason: a provider silently serving another chain answers
    /// every request correctly and produces blocks that are real and not ours.
    pub async fn check_chain_id(&self) -> Result<(), ChainError> {
        let found = self.number("eth_chainId", vec![]).await?;
        if found != CHAIN_ID {
            return Err(ChainError::WrongChain {
                url: self.url.clone(),
                found,
            });
        }
        Ok(())
    }

    /// The newest block the node has, or the newest that cannot be taken back.
    pub async fn frontier(&self, frontier: crate::venue::Frontier) -> Result<u64, ChainError> {
        match frontier {
            crate::venue::Frontier::Head => self.number("eth_blockNumber", vec![]).await,
            crate::venue::Frontier::Finalized => {
                let block = self.header(frontier.tag()).await?;
                Ok(block.number)
            }
        }
    }

    /// One block's header — its number, its hash and its parent's.
    ///
    /// **Read rather than returned raw**, because this answer is the loop's
    /// control flow: the parent hash is how a reorganisation is detected, and a
    /// hash you cannot read is a hash you cannot compare.
    pub async fn header(&self, tag: &str) -> Result<Header, ChainError> {
        let value = self
            .call("eth_getBlockByNumber", vec![tag.into(), false.into()])
            .await?;
        Header::read(&value)
    }

    /// The same, by number.
    pub async fn header_at(&self, number: u64) -> Result<Header, ChainError> {
        let value = self
            .call(
                "eth_getBlockByNumber",
                vec![format!("0x{number:x}").into(), false.into()],
            )
            .await?;
        Header::read(&value)
    }

    /// Every log in a range of blocks, **as the node sent them**.
    ///
    /// One response is one payload. A trade is provable only by matching two
    /// transfers inside one `transactionHash`, and splitting the response would
    /// put that evidence in different rows.
    pub async fn logs(&self, from: u64, to: u64, now_micros: i64) -> Result<Payload, ChainError> {
        let body = serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "eth_getLogs",
            "params": [{ "fromBlock": format!("0x{from:x}"), "toBlock": format!("0x{to:x}") }]
        });
        let bytes = self.post(&body, "eth_getLogs").await?;
        // The node's envelope is unwrapped so the payload is the RESULT — the
        // logs themselves — which is what `normalise` reads and what a person
        // looking at the record expects to find.
        let value: serde_json::Value =
            serde_json::from_slice(&bytes).map_err(|e| ChainError::Shape {
                venue: VENUE,
                method: "eth_getLogs",
                detail: e.to_string(),
            })?;
        let result = Self::result(&value, "eth_getLogs")?;
        Ok(Payload {
            seq: 0,
            recv_micros: now_micros,
            address: PayloadAddress::Venue(VENUE.into()),
            channel: "eth_getLogs".into(),
            kind: galata_wire::Series::Transfers.as_str().to_string(),
            // A response is about many instruments, so it names none.
            symbol: None,
            // Asked for, covering a range nothing will fetch again — durable
            // before the cursor advances past it.
            origin: Origin::Fetched,
            payload: serde_json::to_vec(&result).unwrap_or_default(),
        })
    }

    async fn number(
        &self,
        method: &'static str,
        params: Vec<serde_json::Value>,
    ) -> Result<u64, ChainError> {
        let value = self.call(method, params).await?;
        value
            .as_str()
            .and_then(|hex| u64::from_str_radix(hex.trim_start_matches("0x"), 16).ok())
            .ok_or(ChainError::Shape {
                venue: VENUE,
                method,
                detail: format!("{value} is not a hex number"),
            })
    }

    async fn call(
        &self,
        method: &'static str,
        params: Vec<serde_json::Value>,
    ) -> Result<serde_json::Value, ChainError> {
        let body = serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": method, "params": params
        });
        let bytes = self.post(&body, method).await?;
        let value: serde_json::Value =
            serde_json::from_slice(&bytes).map_err(|e| ChainError::Shape {
                venue: VENUE,
                method,
                detail: e.to_string(),
            })?;
        Ok(Self::result(&value, method)?.clone())
    }

    fn result<'a>(
        value: &'a serde_json::Value,
        method: &'static str,
    ) -> Result<&'a serde_json::Value, ChainError> {
        if let Some(error) = value.get("error") {
            return Err(ChainError::Rpc {
                venue: VENUE,
                method,
                detail: error.to_string(),
            });
        }
        value.get("result").ok_or(ChainError::Shape {
            venue: VENUE,
            method,
            detail: "no `result` and no `error`".into(),
        })
    }

    async fn post(
        &self,
        body: &serde_json::Value,
        method: &'static str,
    ) -> Result<Vec<u8>, ChainError> {
        let response = self
            .http
            .post(&self.url)
            .json(body)
            .send()
            .await
            .map_err(|source| ChainError::Http {
                venue: VENUE,
                method,
                source,
            })?;
        response
            .bytes()
            .await
            .map(|b| b.to_vec())
            .map_err(|source| ChainError::Http {
                venue: VENUE,
                method,
                source,
            })
    }
}

/// A block's header, as far as the loop cares.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Header {
    /// Its height.
    pub number: u64,
    /// Its own hash.
    pub hash: String,
    /// Its predecessor's — the whole reorganisation mechanism.
    pub parent_hash: String,
    /// When the chain says it happened. **One-second granular**, which is the
    /// reason paging is by block and not by this.
    pub timestamp_secs: i64,
}

impl Header {
    fn read(value: &serde_json::Value) -> Result<Header, ChainError> {
        let shape = |detail: String| ChainError::Shape {
            venue: VENUE,
            method: "eth_getBlockByNumber",
            detail,
        };
        let hex = |name: &str| -> Result<u64, ChainError> {
            value
                .get(name)
                .and_then(|v| v.as_str())
                .and_then(|h| u64::from_str_radix(h.trim_start_matches("0x"), 16).ok())
                .ok_or_else(|| shape(format!("{name} is not a hex number")))
        };
        let text = |name: &str| -> Result<String, ChainError> {
            value
                .get(name)
                .and_then(|v| v.as_str())
                .map(str::to_string)
                .ok_or_else(|| shape(format!("no {name}")))
        };
        Ok(Header {
            number: hex("number")?,
            hash: text("hash")?,
            parent_hash: text("parentHash")?,
            timestamp_secs: hex("timestamp")? as i64,
        })
    }

    /// The block's own time, in the units everything else here uses.
    pub fn at_micros(&self) -> i64 {
        self.timestamp_secs.saturating_mul(1_000_000)
    }

    /// What the trail needs.
    pub fn seen(&self) -> Seen {
        Seen {
            number: self.number,
            hash: self.hash.clone(),
            parent_hash: self.parent_hash.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A real header from Robinhood Chain, 2026-09-21.
    const HEADER: &str = r#"{
      "number": "0x4176edd",
      "hash": "0xe93c183f4dc195dc1342555071b69906e0e89bf52f99a39043373a046c29847c",
      "parentHash": "0x5e10d0259c15ca2ffc5b4a1c5c5019d63e5b4a9124126f28d548040c368ffaf1",
      "timestamp": "0x6ab0f4d1"
    }"#;

    #[test]
    fn a_real_header_reads() {
        let header = Header::read(&serde_json::from_str(HEADER).unwrap()).unwrap();
        assert_eq!(header.number, 0x4176edd);
        assert!(header.hash.starts_with("0xe93c"));
        assert!(header.parent_hash.starts_with("0x5e10"));
        // The block's own clock, in micros, and NOT ours.
        assert_eq!(header.at_micros(), header.timestamp_secs * 1_000_000);
        assert_eq!(header.seen().number, header.number);
    }

    #[test]
    fn a_header_missing_a_field_is_refused_by_name() {
        let error = Header::read(&serde_json::json!({"number": "0x1"})).unwrap_err();
        assert!(error.to_string().contains("hash"), "{error}");
    }

    #[test]
    fn an_rpc_error_object_is_not_read_as_a_result() {
        let value = serde_json::json!({"error": {"code": -32000, "message": "too many blocks"}});
        let error = ChainClient::result(&value, "eth_getLogs").unwrap_err();
        assert!(error.to_string().contains("too many blocks"), "{error}");
    }

    #[test]
    fn an_answer_with_neither_result_nor_error_is_refused() {
        // A node that answered nothing at all is not a node that answered an
        // empty range.
        let error = ChainClient::result(&serde_json::json!({}), "eth_getLogs").unwrap_err();
        assert!(error.to_string().contains("no `result`"), "{error}");
    }
}
