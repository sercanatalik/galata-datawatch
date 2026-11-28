//! The ledger's requests to Derive: **REST POSTs signed by a session key**
//! (design D2 of `add-derive-account-ledger`).
//!
//! **Transport**, like `client`: it returns the venue's bytes and reads none of
//! them. A signed request carries the wallet in `X-DeriveWallet` and in its
//! body, the time in milliseconds in `X-DeriveTimestamp`, and that time signed
//! by the session key in `X-DeriveSignature`. The time is the caller's:
//! nothing below the loop reads a clock.
//!
//! One request is not signed: `public/get_wallets_from_session_key`, which
//! names the key's public address and nothing else, and says which wallets the
//! key is registered to (design D4).
//!
//! **No error carries the URL, the key, a signature or the wallet.** An error
//! names the method and what went wrong. Of a venue's error it keeps the code
//! alone: its message and data can name the wallet. Measured 2026-11-27: a
//! request naming a wallet the key is not registered to is refused by the
//! venue's gateway with a bare HTML `403`, which is a status and no code.

use super::VENUE;
use super::sign::SessionKey;
use crate::config::Secret;

/// The wallet's session keys, with their scopes and expiry.
pub const SESSION_KEYS: &str = "private/session_keys";
/// The wallet's subaccount identifiers.
pub const GET_SUBACCOUNTS: &str = super::ledger::LISTING_CHANNEL;
/// Every subaccount's margin, positions and collateral, in one answer.
pub const GET_ALL_PORTFOLIOS: &str = super::ledger::SNAPSHOT_CHANNEL;
/// The wallets a session key is registered to. Unsigned.
pub const WALLETS_OF_KEY: &str = "public/get_wallets_from_session_key";
/// A wallet's orders, each at its latest status, over a window, by page.
pub const GET_ORDER_HISTORY: &str = super::ledger::ORDER_HISTORY_CHANNEL;
/// A wallet's trades over a window, by page.
pub const GET_TRADE_HISTORY: &str = super::ledger::TRADE_HISTORY_CHANNEL;
/// A wallet's funding payments over a window, by page.
pub const GET_FUNDING_HISTORY: &str = super::ledger::FUNDING_HISTORY_CHANNEL;
/// A wallet's subaccounts' value and margins at each period, by page.
pub const GET_VALUE_HISTORY: &str = super::ledger::VALUE_HISTORY_CHANNEL;

/// The venue's code for a request refused for its rate: `data` states the
/// wait ("Retry after {ms} ms", read from Derive's documentation 2026-11-26).
pub const RATE_REFUSED: i64 = -32000;

/// The JSON-RPC error code an answer holds, where it holds one.
pub fn answer_error(bytes: &[u8]) -> Option<i64> {
    error_code(bytes)
}

/// The wait a rate refusal states, in milliseconds: the number in its
/// `data`'s "Retry after {ms} ms", and nothing else of the answer.
pub fn retry_after_ms(bytes: &[u8]) -> Option<u64> {
    let answer: serde_json::Value = serde_json::from_slice(bytes).ok()?;
    let error = answer.get("error")?;
    if error.get("code")?.as_i64()? != RATE_REFUSED {
        return None;
    }
    let data = error.get("data")?.as_str()?;
    data.split("Retry after ")
        .nth(1)?
        .split(' ')
        .next()?
        .parse()
        .ok()
}

/// What the ledger makes of a refusal: a throttle with the wait it stated, or
/// the venue not answering.
pub fn refused(
    refusal: crate::capture::Refusal,
    retry_after_ms: Option<u64>,
) -> crate::ledger::run::Refused {
    crate::ledger::run::Refused {
        refusal,
        wait_micros: retry_after_ms
            .map(|ms| i64::try_from(ms.saturating_mul(1_000)).unwrap_or(i64::MAX)),
    }
}

/// Why a request to Derive did not answer.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum PrivateError {
    /// The request did not complete.
    ///
    /// **Build it with `PrivateError::http`**, never as a struct literal.
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
    #[error("{venue} {method}: the venue answered {status}{}{}", said(.code), waited(.retry_after_ms))]
    Status {
        /// Which venue.
        venue: &'static str,
        /// Which method.
        method: &'static str,
        /// The HTTP status.
        status: u16,
        /// The JSON-RPC error code in the answer, where it held one.
        code: Option<i64>,
        /// The wait a rate refusal stated, in milliseconds.
        retry_after_ms: Option<u64>,
    },
}

fn said(code: &Option<i64>) -> String {
    code.map(|c| format!(", error {c}")).unwrap_or_default()
}

fn waited(ms: &Option<u64>) -> String {
    ms.map(|ms| format!(", to wait {ms} ms"))
        .unwrap_or_default()
}

impl PrivateError {
    /// What the ledger makes of this failure, with the wait it stated.
    pub fn refused(&self) -> crate::ledger::run::Refused {
        let wait = match self {
            PrivateError::Status { retry_after_ms, .. } => *retry_after_ms,
            _ => None,
        };
        refused(self.refusal(), wait)
    }

    /// The ledger's reading of this failure: **a rate refusal is ours to fix
    /// by asking less often**, and anything else is the venue not answering.
    pub fn refusal(&self) -> crate::capture::Refusal {
        match self {
            PrivateError::Status { status: 429, .. }
            | PrivateError::Status {
                code: Some(RATE_REFUSED),
                ..
            } => crate::capture::Refusal::Throttled,
            _ => crate::capture::Refusal::Unreachable,
        }
    }

    /// **The only way to build a [`PrivateError::Http`].** reqwest's
    /// `Display` carries the whole URL; `without_url` removes it.
    fn http(method: &'static str, source: reqwest::Error) -> PrivateError {
        PrivateError::Http {
            venue: VENUE,
            method,
            source: source.without_url(),
        }
    }
}

/// The JSON-RPC error code an answer holds, and nothing else of it.
fn error_code(bytes: &[u8]) -> Option<i64> {
    let answer: serde_json::Value = serde_json::from_slice(bytes).ok()?;
    answer.get("error")?.get("code")?.as_i64()
}

/// The REST client for the ledger's reads.
#[derive(Debug, Clone)]
pub struct PrivateClient {
    http: reqwest::Client,
    rest_url: String,
}

impl PrivateClient {
    /// A client for one network's REST root.
    pub fn new(rest_url: &str) -> PrivateClient {
        PrivateClient {
            http: crate::source::http_client(),
            rest_url: rest_url.to_string(),
        }
    }

    /// The wallet's session keys, as the venue sent them.
    pub async fn session_keys(
        &self,
        key: &SessionKey,
        wallet: &Secret,
        now_micros: i64,
    ) -> Result<Vec<u8>, PrivateError> {
        self.signed(SESSION_KEYS, key, wallet, now_micros).await
    }

    /// The wallet's subaccounts, as the venue sent them.
    pub async fn subaccounts(
        &self,
        key: &SessionKey,
        wallet: &Secret,
        now_micros: i64,
    ) -> Result<Vec<u8>, PrivateError> {
        self.signed(GET_SUBACCOUNTS, key, wallet, now_micros).await
    }

    /// Every subaccount's portfolio, as the venue sent it.
    pub async fn all_portfolios(
        &self,
        key: &SessionKey,
        wallet: &Secret,
        now_micros: i64,
    ) -> Result<Vec<u8>, PrivateError> {
        self.signed(GET_ALL_PORTFOLIOS, key, wallet, now_micros)
            .await
    }

    /// One page of a wallet's history, as the venue sent it: `method` is one of
    /// the four history methods, and `window` its own parameters (each names
    /// its bounds its own way, measured 2026-11-27: `from_timestamp` for
    /// orders and trades, `start_timestamp` for funding and value). The
    /// wallet is added here, as every signed request adds it.
    pub async fn history(
        &self,
        method: &'static str,
        key: &SessionKey,
        wallet: &Secret,
        now_micros: i64,
        window: serde_json::Map<String, serde_json::Value>,
    ) -> Result<Vec<u8>, PrivateError> {
        self.signed_with(method, key, wallet, now_micros, window)
            .await
    }

    /// The wallets the key is registered to, as the venue sent them. Unsigned:
    /// the request names the key's public address alone.
    pub async fn wallets_of(&self, key: &SessionKey) -> Result<Vec<u8>, PrivateError> {
        let request = self
            .http
            .post(format!("{}/{WALLETS_OF_KEY}", self.rest_url))
            .json(&serde_json::json!({ "public_session_key": key.address() }));
        send(WALLETS_OF_KEY, request).await
    }

    async fn signed(
        &self,
        method: &'static str,
        key: &SessionKey,
        wallet: &Secret,
        now_micros: i64,
    ) -> Result<Vec<u8>, PrivateError> {
        self.signed_with(method, key, wallet, now_micros, serde_json::Map::new())
            .await
    }

    async fn signed_with(
        &self,
        method: &'static str,
        key: &SessionKey,
        wallet: &Secret,
        now_micros: i64,
        mut body: serde_json::Map<String, serde_json::Value>,
    ) -> Result<Vec<u8>, PrivateError> {
        let time = (now_micros / 1_000).to_string();
        // **The one place the wallet leaves its `Secret`**: into the request,
        // to the venue. The URL goes to the client and nowhere else.
        let request = self
            .http
            .post(format!("{}/{method}", self.rest_url))
            .header("X-DeriveWallet", wallet.expose())
            .header("X-DeriveTimestamp", &time)
            .header("X-DeriveSignature", key.sign(&time))
            .json(&{
                body.insert(
                    "wallet".into(),
                    serde_json::Value::String(wallet.expose().to_string()),
                );
                body
            });
        send(method, request).await
    }
}

async fn send(
    method: &'static str,
    request: reqwest::RequestBuilder,
) -> Result<Vec<u8>, PrivateError> {
    let response = request
        .send()
        .await
        .map_err(|source| PrivateError::http(method, source))?;
    let status = response.status();
    let bytes = response
        .bytes()
        .await
        .map_err(|source| PrivateError::http(method, source))?;
    if !status.is_success() {
        return Err(PrivateError::Status {
            venue: VENUE,
            method,
            status: status.as_u16(),
            code: error_code(&bytes),
            retry_after_ms: retry_after_ms(&bytes),
        });
    }
    Ok(bytes.to_vec())
}

#[cfg(test)]
mod tests {
    use super::super::local_server::{body, header, provider, serve};
    use super::super::sign::recover;
    use super::*;

    /// The published throwaway key of `sign`'s tests, and its address.
    const KEY: &str = "0x4c0883a69102937d6231471b5dbb6204fe5129617082792ae468d01a3f362318";
    const KEY_ADDRESS: &str = "0x2c7536e3605d9c16a7a3d7b1898e529396a65c23";
    /// A stand-in wallet.
    const WALLET: &str = "0x0003000300030003000300030003000300030003";
    /// A request's time, in microseconds; the header carries milliseconds.
    const NOW_MICROS: i64 = 1_795_705_200_000_123;

    fn key() -> SessionKey {
        SessionKey::from_secret(&Secret::new(KEY)).unwrap()
    }

    #[tokio::test]
    async fn a_signed_read_carries_the_wallet_the_time_and_a_signature_that_recovers_to_the_key() {
        let (url, asked) = serve(200, r#"{"id":"x","result":{}}"#).await;
        let client = PrivateClient::new(&url);
        let wallet = Secret::new(WALLET);
        client
            .session_keys(&key(), &wallet, NOW_MICROS)
            .await
            .unwrap();
        client
            .subaccounts(&key(), &wallet, NOW_MICROS)
            .await
            .unwrap();
        client
            .all_portfolios(&key(), &wallet, NOW_MICROS)
            .await
            .unwrap();

        let asked = asked.lock().unwrap();
        assert_eq!(asked.len(), 3);
        for (request, method) in
            asked
                .iter()
                .zip([SESSION_KEYS, GET_SUBACCOUNTS, GET_ALL_PORTFOLIOS])
        {
            assert!(
                request.starts_with(&format!("POST /{method} ")),
                "{request}"
            );
            assert_eq!(header(request, "X-DeriveWallet"), Some(WALLET));
            let time = header(request, "X-DeriveTimestamp").unwrap();
            assert_eq!(time, "1795705200000", "milliseconds, as a decimal string");
            let signature = header(request, "X-DeriveSignature").unwrap();
            assert_eq!(recover(time, signature).as_deref(), Some(KEY_ADDRESS));
            let sent: serde_json::Value = serde_json::from_str(body(request)).unwrap();
            assert_eq!(sent, serde_json::json!({ "wallet": WALLET }));
            // The key itself goes nowhere: only its signature does.
            assert!(!request.contains(&KEY[2..]), "{request}");
        }
    }

    #[tokio::test]
    async fn the_key_lookup_is_unsigned_and_names_only_the_keys_address() {
        let (url, asked) = serve(200, r#"{"id":"x","result":{"wallets":[]}}"#).await;
        PrivateClient::new(&url).wallets_of(&key()).await.unwrap();

        let asked = asked.lock().unwrap();
        let request = &asked[0];
        assert!(
            request.starts_with(&format!("POST /{WALLETS_OF_KEY} ")),
            "{request}"
        );
        for name in ["X-DeriveWallet", "X-DeriveTimestamp", "X-DeriveSignature"] {
            assert_eq!(header(request, name), None, "{request}");
        }
        let sent: serde_json::Value = serde_json::from_str(body(request)).unwrap();
        assert_eq!(
            sent,
            serde_json::json!({ "public_session_key": KEY_ADDRESS })
        );
    }

    #[tokio::test]
    async fn the_gateways_refusal_names_the_method_and_status_and_nothing_secret() {
        // What the venue's gateway sent for a wallet the key is not
        // registered to (measured 2026-11-27): HTML, not JSON.
        let (url, _) = serve(
            403,
            "<html><head><title>403 Forbidden</title></head></html>",
        )
        .await;
        let refusal = PrivateClient::new(&url)
            .all_portfolios(&key(), &Secret::new(WALLET), NOW_MICROS)
            .await
            .unwrap_err()
            .to_string();
        assert!(refusal.contains("derive-xyz"), "{refusal}");
        assert!(refusal.contains(GET_ALL_PORTFOLIOS), "{refusal}");
        assert!(refusal.contains("403"), "{refusal}");
        assert!(
            !refusal.contains("error"),
            "no code was in the answer: {refusal}"
        );
        for secret in ["127.0.0.1", "://", &WALLET[2..], &KEY[2..], "0x"] {
            assert!(!refusal.contains(secret), "{secret} in {refusal}");
        }
    }

    #[tokio::test]
    async fn a_venue_error_gives_its_code_and_not_its_message() {
        // A message or data that names the wallet stays out of the error.
        let (url, _) = serve(
            429,
            r#"{"error":{"code":-32000,"message":"Retry after 3120 ms for 0x0003000300030003000300030003000300030003","data":"0x0003000300030003000300030003000300030003"}}"#,
        )
        .await;
        let refusal = PrivateClient::new(&url)
            .session_keys(&key(), &Secret::new(WALLET), NOW_MICROS)
            .await
            .unwrap_err();
        assert!(matches!(
            refusal,
            PrivateError::Status {
                status: 429,
                code: Some(-32000),
                ..
            }
        ));
        let said = refusal.to_string();
        assert!(said.contains("error -32000"), "{said}");
        assert!(
            !said.contains(&WALLET[2..]) && !said.contains("Retry"),
            "{said}"
        );
    }

    #[tokio::test]
    async fn a_venue_that_cannot_be_reached_says_so_without_its_address_or_the_wallet() {
        provider();
        // Nothing listens here: bound, read for its port, and dropped.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        drop(listener);
        let refusal = PrivateClient::new(&url)
            .subaccounts(&key(), &Secret::new(WALLET), NOW_MICROS)
            .await
            .unwrap_err()
            .to_string();
        assert!(refusal.contains(GET_SUBACCOUNTS), "{refusal}");
        for secret in ["127.0.0.1", "://", &WALLET[2..], &KEY[2..]] {
            assert!(!refusal.contains(secret), "{secret} in {refusal}");
        }
    }
}
