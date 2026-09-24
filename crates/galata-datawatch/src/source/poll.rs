//! One signed ask of a polled venue.
//!
//! **Venue-free.** Everything that differs by venue arrives in the
//! [`Transport::Poll`](crate::venue::Transport) the adapter declared — where to
//! ask, the path, the symbols, and what signs the request — so this is the one
//! HTTP `GET` every polled venue shares, the way `stream.rs` is the one socket
//! every pushing venue shares.
//!
//! ```text
//!   path_and_query   path + "?symbol=A&symbol=B"   — asked AND signed
//!   200              the bytes, archived whether or not anything changed
//!   429              Throttled — ours to fix, by asking less often
//!   anything else    Unreachable — not ours, and waiting longer will not help
//! ```

use crate::capture::poll::Refusal;
use crate::venue::{Endpoint, Signer};

/// The path and query a poll asks for, and signs: `path?symbol=A&symbol=B`.
///
/// **One string for both**, because signing a shorter string than the one
/// sent is a refusal on every request with no other symptom — the official
/// sample signs the whole path it requests, query included.
pub fn path_and_query(path: &str, symbols: &[String]) -> String {
    if symbols.is_empty() {
        return path.to_string();
    }
    let query: Vec<String> = symbols.iter().map(|s| format!("symbol={s}")).collect();
    format!("{path}?{}", query.join("&"))
}

/// A client for one polled venue.
#[derive(Clone)]
pub struct PollSource {
    http: reqwest::Client,
    rest_url: Endpoint,
    path_and_query: String,
    signer: Signer,
}

impl PollSource {
    /// Built from what the adapter declared.
    pub fn new(rest_url: Endpoint, path: &str, symbols: &[String], signer: Signer) -> PollSource {
        PollSource {
            http: reqwest::Client::new(),
            rest_url,
            path_and_query: path_and_query(path, symbols),
            signer,
        }
    }

    /// What is asked, and signed.
    pub fn path_and_query(&self) -> &str {
        &self.path_and_query
    }

    /// Ask once, at a moment the loop read from its own clock.
    ///
    /// **The moment is a parameter**, like everything below the loop: the
    /// signature's timestamp is the loop's clock, and a venue that expires a
    /// signature after thirty seconds makes a wrong clock a refusal rather
    /// than a mislabelled row.
    pub async fn ask(&self, at_micros: i64) -> Result<Vec<u8>, Refusal> {
        let url = format!(
            "{}{}",
            self.rest_url.expose().trim_end_matches('/'),
            self.path_and_query
        );
        let mut request = self.http.get(url);
        for (name, value) in self
            .signer
            .0
            .headers(at_micros, &self.path_and_query, "GET", "")
        {
            request = request.header(name, value);
        }
        let response = match request.send().await {
            Ok(response) => response,
            Err(error) => {
                // Redacted where it is built: reqwest's Display carries the
                // whole URL, and a keyed venue may keep its key in it.
                let error = error.without_url();
                tracing::warn!(%error, endpoint = %self.rest_url, "a poll was not answered");
                return Err(Refusal::Unreachable);
            }
        };
        let status = response.status();
        if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
            tracing::warn!(endpoint = %self.rest_url, "the venue says we ask too often");
            return Err(Refusal::Throttled);
        }
        if !status.is_success() {
            // A 401 lands here, and is logged by number: it is a wrong key, a
            // wrong clock or a revoked credential, and the operator is the one
            // who can tell which.
            tracing::warn!(%status, endpoint = %self.rest_url, "the venue refused a poll");
            return Err(Refusal::Unreachable);
        }
        match response.bytes().await {
            Ok(bytes) => Ok(bytes.to_vec()),
            Err(error) => {
                let error = error.without_url();
                tracing::warn!(%error, endpoint = %self.rest_url, "a poll's answer did not arrive whole");
                Err(Refusal::Unreachable)
            }
        }
    }
}
