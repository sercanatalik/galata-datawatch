//! The one request this venue is asked: an instrument's reference data.
//!
//! **Transport**, so it is behind `capture`. It returns the venue's bytes and
//! interprets nothing; the adapter's `normalise` reads them after the record holds
//! them.
//!
//! No error here carries a URL. The address is a compiled-in public one and
//! could safely print, and the rule is absolute anyway
//! (`check-endpoint-reach.sh`): an exception is where the next keyed URL goes.

use crate::record::Payload;

use super::{VENUE, reference_page};

/// The method asked, for an error that says which.
const GET_INSTRUMENT: &str = "public/get_instrument";

/// Why reference data could not be fetched.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum FetchError {
    /// The request did not complete.
    #[error("{venue} {method}: {source}")]
    Http {
        /// Which venue.
        venue: &'static str,
        /// Which method.
        method: &'static str,
        /// What the transport said, with its URL removed.
        #[source]
        source: reqwest::Error,
    },
    /// The venue answered with a status that is not success.
    #[error("{venue} {method}: the venue answered {status}")]
    Status {
        /// Which venue.
        venue: &'static str,
        /// Which method.
        method: &'static str,
        /// The HTTP status.
        status: u16,
    },
}

/// The REST client.
#[derive(Debug, Clone)]
pub struct Client {
    http: reqwest::Client,
    url: String,
}

impl Client {
    /// A client for one network's REST root.
    pub fn new(rest_url: &str) -> Client {
        Client {
            http: crate::source::http_client(),
            url: format!("{rest_url}/{GET_INSTRUMENT}"),
        }
    }

    /// One instrument's reference data, as the venue sent it.
    ///
    /// **A name the venue does not know is not an error here.** The venue
    /// answers it with HTTP 200 and a JSON-RPC error in the body (measured
    /// 2026-11-24), and telling that apart from a real instrument is a matter
    /// of reading the bytes, which this does not do.
    pub async fn instrument(&self, name: &str, now_micros: i64) -> Result<Payload, FetchError> {
        let http = |source: reqwest::Error| FetchError::Http {
            venue: VENUE,
            method: GET_INSTRUMENT,
            source: source.without_url(),
        };
        let response = self
            .http
            .post(&self.url)
            .json(&serde_json::json!({ "instrument_name": name }))
            .send()
            .await
            .map_err(http)?;
        let status = response.status();
        if !status.is_success() {
            return Err(FetchError::Status {
                venue: VENUE,
                method: GET_INSTRUMENT,
                status: status.as_u16(),
            });
        }
        let bytes = response.bytes().await.map_err(http)?.to_vec();
        Ok(reference_page(name, bytes, now_micros))
    }
}

#[cfg(test)]
mod tests {
    use super::super::local_server::{provider, serve};
    use super::*;
    use crate::venue::REFERENCE_CHANNEL;

    #[tokio::test]
    async fn an_instrument_is_asked_by_name_and_its_bytes_come_back_unread() {
        let body = r#"{"id":"x","result":{"instrument_name":"BTC-20261225-60000-P"}}"#;
        let (url, asked) = serve(200, body).await;
        let page = Client::new(&url)
            .instrument("BTC-20261225-60000-P", 42)
            .await
            .unwrap();

        // The venue's bytes, classified and not interpreted.
        assert_eq!(page.payload, body.as_bytes());
        assert_eq!(page.channel, REFERENCE_CHANNEL);
        assert_eq!(page.symbol.as_deref(), Some("BTC-20261225-60000-P"));
        assert_eq!(page.kind, "instruments");
        assert_eq!(page.recv_micros, 42);
        assert_eq!(page.origin, galata_wire::Origin::Fetched);

        let asked = asked.lock().unwrap();
        assert_eq!(asked.len(), 1);
        assert!(
            asked[0].starts_with("POST /public/get_instrument "),
            "{}",
            asked[0]
        );
        assert!(
            asked[0].contains(r#"{"instrument_name":"BTC-20261225-60000-P"}"#),
            "{}",
            asked[0]
        );
    }

    #[tokio::test]
    async fn a_name_the_venue_does_not_know_is_an_answer_and_not_a_failure() {
        // HTTP 200 with a JSON-RPC error in the body, as measured. Telling it
        // from a real instrument is the reader's job, not the transport's.
        let body = r#"{"error":{"code":12001,"message":"Instrument not found"}}"#;
        let (url, _) = serve(200, body).await;
        let page = Client::new(&url).instrument("NOPE", 0).await.unwrap();
        assert_eq!(page.payload, body.as_bytes());
    }

    #[tokio::test]
    async fn a_refusal_names_the_venue_and_the_method_and_no_address() {
        let (url, _) = serve(503, "busy").await;
        let refusal = Client::new(&url)
            .instrument("BTC-20261225-60000-P", 0)
            .await
            .unwrap_err()
            .to_string();
        assert!(refusal.contains("derive-xyz"), "{refusal}");
        assert!(refusal.contains("public/get_instrument"), "{refusal}");
        assert!(refusal.contains("503"), "{refusal}");
        assert!(!refusal.contains("127.0.0.1"), "{refusal}");
        assert!(!refusal.contains("://"), "{refusal}");
    }

    #[tokio::test]
    async fn a_venue_that_cannot_be_reached_says_so_without_its_address() {
        // Nothing listens here: bound, read for its port, and dropped.
        provider();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        drop(listener);
        let refusal = Client::new(&url)
            .instrument("BTC-20261225-60000-P", 0)
            .await
            .unwrap_err()
            .to_string();
        assert!(refusal.contains("derive-xyz"), "{refusal}");
        assert!(!refusal.contains("127.0.0.1"), "{refusal}");
    }
}
