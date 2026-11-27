//! **Derive's figure is Derive's** (design D3): its own simulation of the
//! owner's subaccount with a placement's positions added, through
//! `private/get_margin`.
//!
//! The request is signed as the ledger's reads are: the wallet in
//! `X-DeriveWallet`, the time in `X-DeriveTimestamp`, and the session key's
//! EIP-191 signature of that time in `X-DeriveSignature`. The key arrives
//! already read, through `SecretSource::secret`, the one door
//! (`scripts/check-secret-reach.sh`); the address is the venue's compiled-in
//! one, by network (`Market::rest_url`). No error carries the URL
//! (`scripts/check-endpoint-reach.sh`).
//!
//! The answer's figures are **net**: mark-to-market value less the
//! requirement, positive while healthy (`design/measured.md`, "Derive's
//! margin simulation"). They are carried as the venue wrote them, digit for
//! digit.

use std::str::FromStr;
use std::time::Duration;

use galata_datawatch::adapters::derive_xyz::Market;
use galata_datawatch::adapters::derive_xyz::sign::SessionKey;
use galata_datawatch::config::Secret;
use rust_decimal::Decimal;
use serde::Serialize;

/// The method this client asks to simulate.
pub const GET_MARGIN: &str = "private/get_margin";
/// The method that lists the wallet's keys and their scopes.
pub const SESSION_KEYS: &str = "private/session_keys";
/// The method that lists the wallet's subaccounts.
pub const GET_SUBACCOUNTS: &str = "private/get_subaccounts";

/// Longest a connection may take to open.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
/// Longest one request may take, as galata-datawatch's venue client allows.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(60);

/// One position change to simulate.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SimulatedPosition {
    /// The instrument, by Derive's name (`BTC-PERP`, `BTC-20261225-60000-P`).
    pub instrument_name: String,
    /// The signed amount to add.
    pub amount: Decimal,
    /// The entry price, for a perpetual. The venue takes none for an option,
    /// which it marks at its current value.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub entry_price: Option<Decimal>,
}

/// What `private/get_margin` is asked.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct MarginRequest {
    /// The subaccount the placement would land in.
    pub subaccount_id: u64,
    /// The placement's positions.
    pub simulated_position_changes: Vec<SimulatedPosition>,
}

/// What the venue answered, exactly as it wrote it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MarginAnswer {
    /// The subaccount the answer is for.
    pub subaccount_id: u64,
    /// Net initial margin before the change.
    pub pre_initial: Decimal,
    /// Net maintenance margin before the change.
    pub pre_maintenance: Decimal,
    /// Net initial margin after it.
    pub post_initial: Decimal,
    /// Net maintenance margin after it.
    pub post_maintenance: Decimal,
    /// Whether the venue would take the change.
    pub is_valid_trade: bool,
}

/// Why the simulation gave no figure. No variant holds a URL.
#[derive(Debug, thiserror::Error)]
pub enum SimError {
    /// The request did not reach the venue, or its answer did not arrive.
    #[error("{method} did not reach Derive: {source}")]
    Unreachable {
        /// The method asked.
        method: &'static str,
        /// What failed, with its URL removed where it was built.
        source: reqwest::Error,
    },
    /// The venue answered with an error status.
    #[error("Derive refused {method}: HTTP {status}{}{}", .code.map(|c| format!(", code {c}")).unwrap_or_default(), .retry_after_ms.map(|ms| format!(", retry after {ms} ms")).unwrap_or_default())]
    Status {
        /// The method asked.
        method: &'static str,
        /// The HTTP status.
        status: u16,
        /// The JSON-RPC error code, when the answer held one.
        code: Option<i64>,
        /// The wait the venue stated, for a rate-limit refusal.
        retry_after_ms: Option<u64>,
    },
    /// The venue answered, and the answer could not be read.
    #[error("Derive's answer to {method} could not be read: {reason}")]
    Unreadable {
        /// The method asked.
        method: &'static str,
        /// What was wrong with it.
        reason: String,
    },
}

impl SimError {
    /// The only way an `Unreachable` is built: with the URL taken out, here,
    /// once (`check-endpoint-reach.sh`, rule 2).
    fn unreachable(method: &'static str, source: reqwest::Error) -> SimError {
        SimError::Unreachable {
            method,
            source: source.without_url(),
        }
    }

    /// The wait the venue asked for, when this is a rate-limit refusal.
    pub fn retry_after(&self) -> Option<Duration> {
        match self {
            SimError::Status {
                retry_after_ms: Some(ms),
                ..
            } => Some(Duration::from_millis(*ms)),
            _ => None,
        }
    }
}

/// The client that asks Derive's simulation.
#[derive(Debug, Clone)]
pub struct Simulator {
    http: reqwest::Client,
    rest_url: String,
}

impl Simulator {
    /// A client for the venue's compiled-in address on a network.
    pub fn for_market(market: Market) -> Simulator {
        Simulator::at(market.rest_url())
    }

    /// A client for a REST root: the venue's, or a test's local server.
    pub fn at(rest_url: &str) -> Simulator {
        // The TLS provider galata-datawatch's binaries install, installed here
        // too so a client cannot be built without one. A second install is
        // refused, harmlessly, which is why its result is ignored.
        let _ = rustls::crypto::ring::default_provider().install_default();
        let http = reqwest::Client::builder()
            .connect_timeout(CONNECT_TIMEOUT)
            .timeout(REQUEST_TIMEOUT)
            .build()
            .unwrap_or_else(|_| reqwest::Client::new());
        Simulator {
            http,
            rest_url: rest_url.trim_end_matches('/').to_string(),
        }
    }

    /// Ask the venue to simulate the subaccount with the request's positions
    /// added, signed for `now_micros`.
    pub async fn get_margin(
        &self,
        key: &SessionKey,
        wallet: &Secret,
        request: &MarginRequest,
        now_micros: i64,
    ) -> Result<MarginAnswer, SimError> {
        let time = (now_micros / 1_000).to_string();
        // The one place the wallet leaves its `Secret`: into the request.
        let response = self
            .http
            .post(format!("{}/{GET_MARGIN}", self.rest_url))
            .header("X-DeriveWallet", wallet.expose())
            .header("X-DeriveTimestamp", &time)
            .header("X-DeriveSignature", key.sign(&time))
            .json(request)
            .send()
            .await
            .map_err(|e| SimError::unreachable(GET_MARGIN, e))?;
        let status = response.status();
        let bytes = response
            .bytes()
            .await
            .map_err(|e| SimError::unreachable(GET_MARGIN, e))?;
        let answer: Option<serde_json::Value> = serde_json::from_slice(&bytes).ok();
        let error = answer.as_ref().and_then(|a| a.get("error"));
        if !status.is_success() || error.is_some() {
            return Err(SimError::Status {
                method: GET_MARGIN,
                status: status.as_u16(),
                code: error.and_then(|e| e.get("code")).and_then(|c| c.as_i64()),
                retry_after_ms: retry_after_ms(error),
            });
        }
        let answer = answer.ok_or_else(|| SimError::Unreadable {
            method: GET_MARGIN,
            reason: "it is not JSON".into(),
        })?;
        read_answer(&answer)
    }
}

/// Why the optimiser will not start with its key (design D11).
#[derive(Debug, thiserror::Error)]
pub enum KeyCheck {
    /// The venue could not be asked.
    #[error("the session key could not be checked: {0}")]
    Unasked(SimError),
    /// The ledger's rule refuses the key. Its message names a scope, never
    /// the key or the wallet.
    #[error("the session key is refused by the rule the ledger reads with: {0}")]
    Refused(galata_datawatch::ledger::accounts::KeyRefusal),
}

impl Simulator {
    /// The wallet's keys and their scopes, as the venue sent them.
    async fn session_keys(
        &self,
        key: &SessionKey,
        wallet: &Secret,
        now_micros: i64,
    ) -> Result<Vec<u8>, SimError> {
        self.signed_read(SESSION_KEYS, key, wallet, now_micros)
            .await
    }

    /// A signed read whose body names the wallet, as the ledger's are.
    async fn signed_read(
        &self,
        method: &'static str,
        key: &SessionKey,
        wallet: &Secret,
        now_micros: i64,
    ) -> Result<Vec<u8>, SimError> {
        let time = (now_micros / 1_000).to_string();
        let response = self
            .http
            .post(format!("{}/{method}", self.rest_url))
            .header("X-DeriveWallet", wallet.expose())
            .header("X-DeriveTimestamp", &time)
            .header("X-DeriveSignature", key.sign(&time))
            .json(&serde_json::json!({ "wallet": wallet.expose() }))
            .send()
            .await
            .map_err(|e| SimError::unreachable(method, e))?;
        let status = response.status();
        let bytes = response
            .bytes()
            .await
            .map_err(|e| SimError::unreachable(method, e))?;
        if !status.is_success() {
            let answer: Option<serde_json::Value> = serde_json::from_slice(&bytes).ok();
            let error = answer.as_ref().and_then(|a| a.get("error"));
            return Err(SimError::Status {
                method,
                status: status.as_u16(),
                code: error.and_then(|e| e.get("code")).and_then(|c| c.as_i64()),
                retry_after_ms: retry_after_ms(error),
            });
        }
        Ok(bytes.to_vec())
    }

    /// The id of the subaccount the ledger binds to `alias` under `master`:
    /// the wallet's subaccounts are listed, each id fingerprinted with the
    /// ledger's key, and the one bound to the alias taken. The ids are
    /// compared here and printed nowhere.
    // The key, the wallet and the time sign the read; the bindings, the
    // master and the fingerprint key resolve the alias, as the ledger does.
    #[allow(clippy::too_many_arguments)]
    pub async fn subaccount_of(
        &self,
        key: &SessionKey,
        wallet: &Secret,
        now_micros: i64,
        bindings: &galata_datawatch::ledger::Bindings,
        master: &galata_wire::Account,
        fingerprint_key: &galata_datawatch::ledger::FingerprintKey,
        alias: &str,
    ) -> Result<u64, String> {
        let answer = self
            .signed_read(GET_SUBACCOUNTS, key, wallet, now_micros)
            .await
            .map_err(|e| e.to_string())?;
        let listed = galata_datawatch::adapters::derive_xyz::ledger::subaccounts_listed(&answer)
            .map_err(|e| format!("Derive's list of subaccounts could not be read: {e}"))?;
        for one in &listed {
            let id = one.address.expose();
            let bound = bindings.alias_of(
                master,
                &galata_datawatch::ledger::fingerprint(fingerprint_key, id),
            );
            if bound.as_ref().map(|a| a.as_str()) == Some(alias) {
                return id
                    .parse()
                    .map_err(|_| format!("the subaccount bound to `{alias}` has no numeric id"));
            }
        }
        Err(format!(
            "no subaccount of the wallet is bound to `{alias}` in the ledger's record ({} listed)",
            listed.len()
        ))
    }

    /// At start: ask the venue what the key may do and apply the ledger's
    /// rule to it, the one check both use (design D11, revised 2026-11-27).
    /// Trading scopes, `account_info` and `block_trade` are accepted; any
    /// other, an unknown one included, is refused by name, as is a key the
    /// venue does not list or whose expiry has passed.
    pub async fn check_key(
        &self,
        key: &SessionKey,
        wallet: &Secret,
        now_micros: i64,
    ) -> Result<galata_datawatch::ledger::accounts::KeyStanding, KeyCheck> {
        let answer = self
            .session_keys(key, wallet, now_micros)
            .await
            .map_err(KeyCheck::Unasked)?;
        galata_datawatch::adapters::derive_xyz::ledger::key_standing(
            &answer,
            &key.address(),
            now_micros,
        )
        .map_err(KeyCheck::Refused)
    }
}

impl Simulator {
    /// [`Simulator::get_margin`], asked again after a rate-limit refusal's
    /// stated wait, and no sooner, at most `retries` times. Each attempt is
    /// signed afresh, at the time `clock` gives: a signature is of its time.
    /// Every other failure is returned as it came.
    pub async fn get_margin_waiting(
        &self,
        key: &SessionKey,
        wallet: &Secret,
        request: &MarginRequest,
        clock: &(dyn Fn() -> i64 + Sync),
        retries: u32,
    ) -> Result<MarginAnswer, SimError> {
        let mut left = retries;
        loop {
            match self.get_margin(key, wallet, request, clock()).await {
                Err(refused) if left > 0 && refused.retry_after().is_some() => {
                    left -= 1;
                    tokio::time::sleep(refused.retry_after().unwrap_or_default()).await;
                }
                other => return other,
            }
        }
    }
}

/// The wait a rate-limit refusal states: `-32000`, with `data` reading
/// "Retry after {ms} ms" (`design/measured.md`, Derive's rate limits).
fn retry_after_ms(error: Option<&serde_json::Value>) -> Option<u64> {
    let data = error?.get("data")?.as_str()?;
    data.strip_prefix("Retry after ")?
        .strip_suffix(" ms")?
        .trim()
        .parse()
        .ok()
}

/// The answer's figures, each read from the venue's decimal string exactly.
pub fn read_answer(answer: &serde_json::Value) -> Result<MarginAnswer, SimError> {
    let unreadable = |reason: String| SimError::Unreadable {
        method: GET_MARGIN,
        reason,
    };
    let result = answer
        .get("result")
        .ok_or_else(|| unreadable("it holds no result".into()))?;
    let decimal = |field: &str| -> Result<Decimal, SimError> {
        let text = result
            .get(field)
            .and_then(|v| v.as_str())
            .ok_or_else(|| unreadable(format!("`{field}` is not a decimal string")))?;
        Decimal::from_str(text)
            .map_err(|_| unreadable(format!("`{field}` is not a decimal: {text:?}")))
    };
    Ok(MarginAnswer {
        subaccount_id: result
            .get("subaccount_id")
            .and_then(|v| v.as_u64())
            .ok_or_else(|| unreadable("`subaccount_id` is missing".into()))?,
        pre_initial: decimal("pre_initial_margin")?,
        pre_maintenance: decimal("pre_maintenance_margin")?,
        post_initial: decimal("post_initial_margin")?,
        post_maintenance: decimal("post_maintenance_margin")?,
        is_valid_trade: result
            .get("is_valid_trade")
            .and_then(|v| v.as_bool())
            .ok_or_else(|| unreadable("`is_valid_trade` is missing".into()))?,
    })
}

#[cfg(test)]
mod tests {
    use std::io::{BufRead, BufReader, Read, Write};
    use std::net::TcpListener;
    use std::sync::{Arc, Mutex};
    use std::time::Instant;

    use galata_datawatch::adapters::derive_xyz::sign::recover;

    use super::*;

    /// A test key: 32 bytes of hex, never anyone's.
    const KEY: &str = "0x4c0883a69102937d6231471b5dbb6204fe5129617082792ae468d01a3f362318";
    const WALLET: &str = "0x00000000000000000000000000000000000000aa";

    /// One request the local server took.
    #[derive(Debug, Clone)]
    struct Taken {
        path: String,
        headers: Vec<(String, String)>,
        body: serde_json::Value,
        at: Instant,
    }

    impl Taken {
        fn header(&self, name: &str) -> &str {
            &self
                .headers
                .iter()
                .find(|(k, _)| k.eq_ignore_ascii_case(name))
                .unwrap_or_else(|| panic!("no {name} header"))
                .1
        }
    }

    /// Serve the answers in order, one per request, recording each request.
    fn serve(answers: Vec<(u16, String)>) -> (String, Arc<Mutex<Vec<Taken>>>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let taken = Arc::new(Mutex::new(Vec::new()));
        let log = taken.clone();
        std::thread::spawn(move || {
            let mut answers = answers.into_iter();
            for stream in listener.incoming().flatten() {
                let Some((status, body)) = answers.next() else {
                    return;
                };
                let mut reader = BufReader::new(stream.try_clone().unwrap());
                let mut line = String::new();
                reader.read_line(&mut line).unwrap();
                let path = line
                    .split_whitespace()
                    .nth(1)
                    .unwrap_or_default()
                    .to_string();
                let mut headers = Vec::new();
                let mut length = 0usize;
                loop {
                    let mut h = String::new();
                    reader.read_line(&mut h).unwrap();
                    if h == "\r\n" || h.is_empty() {
                        break;
                    }
                    let (k, v) = h.trim_end().split_once(':').unwrap();
                    if k.eq_ignore_ascii_case("content-length") {
                        length = v.trim().parse().unwrap();
                    }
                    headers.push((k.to_string(), v.trim().to_string()));
                }
                let mut bytes = vec![0; length];
                reader.read_exact(&mut bytes).unwrap();
                log.lock().unwrap().push(Taken {
                    path,
                    headers,
                    body: serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null),
                    at: Instant::now(),
                });
                let mut stream = stream;
                let reply = format!(
                    "HTTP/1.1 {status} X\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = stream.write_all(reply.as_bytes());
            }
        });
        (url, taken)
    }

    fn fixture(name: &str) -> String {
        std::fs::read_to_string(format!(
            "{}/tests/fixtures/{name}",
            env!("CARGO_MANIFEST_DIR")
        ))
        .unwrap()
    }

    fn key() -> SessionKey {
        SessionKey::from_secret(&Secret::new(KEY)).unwrap()
    }

    /// The request of the fixture: 0.01 BTC-PERP short at 59,574.5 on 1003.
    fn fixture_request() -> MarginRequest {
        MarginRequest {
            subaccount_id: 1003,
            simulated_position_changes: vec![SimulatedPosition {
                instrument_name: "BTC-PERP".into(),
                amount: Decimal::from_str("-0.01").unwrap(),
                entry_price: Some(Decimal::from_str("59574.5").unwrap()),
            }],
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
    fn a_simulation_is_signed_by_the_key_and_sent_as_the_venue_documents() {
        let (url, taken) = serve(vec![(200, fixture("get_margin.json"))]);
        let now = 1_795_800_000_123_456;
        block_on(Simulator::at(&url).get_margin(
            &key(),
            &Secret::new(WALLET),
            &fixture_request(),
            now,
        ))
        .unwrap();
        let request = taken.lock().unwrap()[0].clone();
        assert_eq!(request.path, "/private/get_margin");
        assert_eq!(request.header("X-DeriveWallet"), WALLET);
        assert_eq!(request.header("X-DeriveTimestamp"), "1795800000123");
        let signer = recover("1795800000123", request.header("X-DeriveSignature")).unwrap();
        assert_eq!(signer, key().address());
        // The body is the fixture's request, as the venue took it.
        let sent: serde_json::Value =
            serde_json::from_str(&fixture("get_margin_request.json")).unwrap();
        assert_eq!(request.body, sent);
    }

    #[test]
    fn the_venues_figures_are_carried_digit_for_digit() {
        let (url, _) = serve(vec![(200, fixture("get_margin.json"))]);
        let answer = block_on(Simulator::at(&url).get_margin(
            &key(),
            &Secret::new(WALLET),
            &fixture_request(),
            0,
        ))
        .unwrap();
        assert_eq!(answer.subaccount_id, 1003);
        assert_eq!(answer.pre_initial.to_string(), "100000.123456789012");
        assert_eq!(answer.pre_maintenance.to_string(), "120000.123456789012");
        assert_eq!(answer.post_initial.to_string(), "99907.412814464023");
        assert_eq!(answer.post_maintenance.to_string(), "119928.573441920621");
        assert!(answer.is_valid_trade);
    }

    #[test]
    fn an_unreachable_venue_gives_no_figure_and_no_url() {
        // A port that was free a moment ago and has nothing listening.
        let url = {
            let l = TcpListener::bind("127.0.0.1:0").unwrap();
            format!("http://{}", l.local_addr().unwrap())
        };
        let err = block_on(Simulator::at(&url).get_margin(
            &key(),
            &Secret::new(WALLET),
            &fixture_request(),
            0,
        ))
        .unwrap_err();
        assert!(matches!(err, SimError::Unreachable { .. }), "{err}");
        let said = err.to_string();
        assert!(
            !said.contains("127.0.0.1") && !said.contains("http"),
            "{said}"
        );
        assert!(said.contains("private/get_margin"), "{said}");
    }

    #[test]
    fn an_unreadable_answer_gives_no_figure_and_says_why() {
        for body in [
            "not json",
            r#"{"result": {"subaccount_id": 1003}}"#,
            r#"{"result": {"subaccount_id": 1003, "pre_initial_margin": 1.5}}"#,
        ] {
            let (url, _) = serve(vec![(200, body.to_string())]);
            let err = block_on(Simulator::at(&url).get_margin(
                &key(),
                &Secret::new(WALLET),
                &fixture_request(),
                0,
            ))
            .unwrap_err();
            assert!(matches!(err, SimError::Unreadable { .. }), "{body}: {err}");
        }
    }

    #[test]
    fn a_rate_limit_refusal_is_asked_again_after_its_stated_wait_and_no_sooner() {
        let refusal = r#"{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"Rate limit exceeded","data":"Retry after 300 ms"}}"#;
        let (url, taken) = serve(vec![
            (429, refusal.into()),
            (200, fixture("get_margin.json")),
        ]);
        let clock = || 1_795_800_000_000_000_i64;
        let answer = block_on(Simulator::at(&url).get_margin_waiting(
            &key(),
            &Secret::new(WALLET),
            &fixture_request(),
            &clock,
            1,
        ))
        .unwrap();
        assert!(answer.is_valid_trade);
        let taken = taken.lock().unwrap();
        assert_eq!(taken.len(), 2);
        let waited = taken[1].at - taken[0].at;
        assert!(
            waited >= Duration::from_millis(300),
            "asked again after {waited:?}"
        );

        // With no retry left, the refusal is the answer, and says its wait.
        let (url, _) = serve(vec![(429, refusal.into())]);
        let err = block_on(Simulator::at(&url).get_margin_waiting(
            &key(),
            &Secret::new(WALLET),
            &fixture_request(),
            &clock,
            0,
        ))
        .unwrap_err();
        assert_eq!(err.retry_after(), Some(Duration::from_millis(300)));
        assert!(
            err.to_string().contains("-32000") && err.to_string().contains("300 ms"),
            "{err}"
        );
    }

    /// The venue's list of the wallet's keys, with ours holding these scopes.
    fn keys_answer(protocol: &[&str], offchain: &[&str]) -> String {
        serde_json::json!({"id": 1, "result": {"public_session_keys": [{
            "public_session_key": key().address(),
            "protocol_scopes": protocol,
            "offchain_scopes": offchain,
            "label": "test",
            "ip_whitelist": [],
            "expiry_sec": 1_900_000_000_i64,
            "registered_sec": 1_700_000_000_i64,
            "subaccount_ids": []
        }]}})
        .to_string()
    }

    #[test]
    fn a_key_with_no_scope_is_accepted() {
        let (url, taken) = serve(vec![(200, keys_answer(&[], &[]))]);
        let standing = block_on(Simulator::at(&url).check_key(
            &key(),
            &Secret::new(WALLET),
            1_795_800_000_000_000,
        ))
        .unwrap();
        assert_eq!(standing.expiry_micros, Some(1_900_000_000_000_000));
        let request = taken.lock().unwrap()[0].clone();
        assert_eq!(request.path, "/private/session_keys");
        assert_eq!(request.body["wallet"], WALLET);
    }

    #[test]
    fn the_key_the_ledger_reads_with_is_accepted() {
        let (url, _) = serve(vec![(200, keys_answer(&["trade:all"], &["account_info"]))]);
        assert!(
            block_on(Simulator::at(&url).check_key(
                &key(),
                &Secret::new(WALLET),
                1_795_800_000_000_000
            ))
            .is_ok()
        );
    }

    #[test]
    fn a_key_that_can_move_funds_is_refused_naming_the_scope() {
        for scope in ["transfer:all", "withdraw", "vault:all", "admin"] {
            let (url, _) = serve(vec![(
                200,
                keys_answer(&["trade:all", scope], &["account_info"]),
            )]);
            let err = block_on(Simulator::at(&url).check_key(
                &key(),
                &Secret::new(WALLET),
                1_795_800_000_000_000,
            ))
            .unwrap_err();
            assert!(matches!(err, KeyCheck::Refused(_)), "{scope}: {err}");
            let said = err.to_string();
            assert!(said.contains(scope), "{scope}: {said}");
            assert!(
                !said.contains(&key().address()) && !said.contains(WALLET),
                "{said}"
            );
        }
    }

    #[test]
    fn a_refusal_without_a_wait_is_not_asked_again() {
        let refusal =
            r#"{"jsonrpc":"2.0","id":1,"error":{"code":14000,"message":"Invalid subaccount"}}"#;
        let (url, taken) = serve(vec![
            (200, refusal.into()),
            (200, fixture("get_margin.json")),
        ]);
        let clock = || 0_i64;
        let err = block_on(Simulator::at(&url).get_margin_waiting(
            &key(),
            &Secret::new(WALLET),
            &fixture_request(),
            &clock,
            3,
        ))
        .unwrap_err();
        assert!(
            matches!(
                err,
                SimError::Status {
                    code: Some(14000),
                    ..
                }
            ),
            "{err}"
        );
        assert_eq!(taken.lock().unwrap().len(), 1);
    }
}
