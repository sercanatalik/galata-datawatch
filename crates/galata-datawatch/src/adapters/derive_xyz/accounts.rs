//! Derive, asked about the owner's account for the ledger. Transport, like
//! `private`; the reading is `ledger`'s.
//!
//! **The session key is checked before anything is asked with it** (design D4
//! of `add-derive-account-ledger`): the venue's public lookup must name the
//! declared wallet as the key's, so nothing is signed for a wallet the key
//! does not belong to; then the wallet's own list of keys must hold it, with
//! no scope outside the accepted set and an expiry still ahead.

use std::collections::BTreeMap;

use galata_wire::{Account, Kind, Series, Venue};

use super::ledger::{self, LedgerNormaliser};
use super::private::{
    GET_FUNDING_HISTORY, GET_ORDER_HISTORY, GET_TRADE_HISTORY, GET_VALUE_HISTORY, PrivateClient,
    PrivateError, RATE_REFUSED, answer_error, refused, retry_after_ms,
};
use super::sign::SessionKey;
use crate::capture::Refusal;
use crate::config::Secret;
use crate::ledger::Listed;
use crate::ledger::accounts::{FingerprintKey, KeyRefusal, KeyStanding, ResolvedAccount};
use crate::ledger::run::{AccountVenue, Ask, Refused};
use crate::normalise::{Normalise, NormaliseError};

/// The ledger's view of Derive: **a wallet read in one request** (design D1),
/// its subaccounts bound at discovery, every request signed by the account's
/// session key once that key has been checked.
#[derive(Debug, Clone)]
pub struct DeriveAccounts {
    client: PrivateClient,
    normaliser: LedgerNormaliser,
    key: FingerprintKey,
    /// Where its account stream is, where one is read.
    stream: Option<crate::venue::Endpoint>,
}

impl DeriveAccounts {
    /// Over the venue's REST root, fingerprinting subaccount identifiers with
    /// the ledger's key, as discovery fingerprints them.
    pub fn new(
        rest_url: &str,
        key: FingerprintKey,
    ) -> Result<DeriveAccounts, galata_wire::TokenError> {
        Ok(DeriveAccounts {
            client: PrivateClient::new(rest_url),
            normaliser: LedgerNormaliser::new()?,
            key,
            stream: None,
        })
    }

    /// Reading its subaccounts' streams too, at the network's websocket,
    /// logged in with the ledger's key (`add-realtime-account-ledger`, design
    /// D2).
    pub fn with_stream(mut self, endpoint: crate::venue::Endpoint) -> DeriveAccounts {
        self.stream = Some(endpoint);
        self
    }
}

/// An answer that is an answer: JSON, and not the venue's error. A rate
/// refusal is a throttle, with the wait it stated; any other error, or bytes
/// that are not JSON, the venue not answering.
fn answered(result: Result<Vec<u8>, PrivateError>) -> Result<Vec<u8>, Refused> {
    let bytes = result.map_err(|e| e.refused())?;
    match answer_error(&bytes) {
        Some(RATE_REFUSED) => Err(refused(Refusal::Throttled, retry_after_ms(&bytes))),
        Some(_) => Err(Refusal::Unreachable.into()),
        None if serde_json::from_slice::<serde_json::Value>(&bytes).is_err() => {
            Err(Refusal::Unreachable.into())
        }
        None => Ok(bytes),
    }
}

/// The account's key, as a signer. Checked at boot, so absent or unreadable
/// here only if the configuration changed under a running ledger.
fn signer(account: &ResolvedAccount) -> Result<SessionKey, Refused> {
    account
        .session_key()
        .and_then(|k| SessionKey::from_secret(k).ok())
        .ok_or(Refusal::Unreachable.into())
}

/// Never asked of this venue: it has no per-dex snapshot, roles, modes or
/// account-wide reads.
const NOT_ASKED: &str = "not-asked";

/// The websocket's methods the ledger names: a login, a subscription and a
/// keepalive. **Nothing that acts** (`tests/ledger_acts_on_nothing.rs`).
pub const STREAM_LOGIN: &str = "public/login";
/// Subscribing a subaccount's channels.
pub const STREAM_SUBSCRIBE: &str = "subscribe";
/// The keepalive: the venue's time, asked.
pub const STREAM_KEEPALIVE: &str = "public/get_time";
const STREAM_LOGIN_ID: u64 = 1;
const STREAM_SUBSCRIBE_ID: u64 = 2;
const STREAM_KEEPALIVE_ID: u64 = 3;

/// Rows a history page holds. The owner's whole month was one page of 50
/// (measured 2026-11-27).
const HISTORY_PAGE_SIZE: u32 = 100;

/// The most pages one window reads: a window past it is read to here, and
/// the next window starts from the newest of it.
const HISTORY_MAX_PAGES: u32 = 20;

/// The period a value history is asked at, in seconds: hourly.
const VALUE_PERIOD_SECS: i64 = 3_600;

impl AccountVenue for DeriveAccounts {
    fn venue(&self) -> &Venue {
        self.normaliser.venue()
    }

    fn normaliser(&self) -> &dyn Normalise {
        &self.normaliser
    }

    fn channel(&self, ask: Ask) -> &'static str {
        match ask {
            Ask::Snapshot => ledger::SNAPSHOT_CHANNEL,
            Ask::Listing => ledger::LISTING_CHANNEL,
            Ask::OrderHistory => ledger::ORDER_HISTORY_CHANNEL,
            Ask::TradeHistory => ledger::TRADE_HISTORY_CHANNEL,
            Ask::FundingHistory => ledger::FUNDING_HISTORY_CHANNEL,
            Ask::ValueHistory => ledger::VALUE_HISTORY_CHANNEL,
            _ => NOT_ASKED,
        }
    }

    fn reads_by_wallet(&self) -> bool {
        true
    }

    fn has_roles(&self) -> bool {
        false
    }

    fn has_modes(&self) -> bool {
        false
    }

    fn snapshot_series(&self) -> Series {
        Series::MarginState
    }

    fn projected_kinds(&self) -> &'static [Kind] {
        &crate::ledger::project::WALLET_PROJECTED
    }

    /// The open orders ride the wallet's snapshot: its newest receipt is
    /// every subaccount's newest set.
    fn orders_answer(&self) -> Option<&'static str> {
        Some(ledger::SNAPSHOT_CHANNEL)
    }

    fn stream_endpoint(&self) -> Option<crate::venue::Endpoint> {
        self.stream.clone()
    }

    /// `public/login`: the wallet, a millisecond time, and the session key's
    /// EIP-191 signature of that time, as the REST headers carry them.
    fn stream_hello(&self, master: &ResolvedAccount, now_micros: i64) -> Option<String> {
        let key = signer(master).ok()?;
        let time = (now_micros / 1_000).to_string();
        Some(
            serde_json::json!({
                "id": STREAM_LOGIN_ID,
                "method": STREAM_LOGIN,
                "params": {
                    "wallet": master.address().expose(),
                    "timestamp": time,
                    "signature": key.sign(&time),
                },
            })
            .to_string(),
        )
    }

    /// The login's answer: the subaccount ids it was granted, or the venue's
    /// error (measured 2026-11-28: a list of ids, in 237 ms).
    fn stream_hello_answer(&self, frame: &[u8]) -> Option<bool> {
        let v: serde_json::Value = serde_json::from_slice(frame).ok()?;
        (v.get("id")? == STREAM_LOGIN_ID).then(|| v.get("result").is_some_and(|r| r.is_array()))
    }

    /// A subaccount's three channels, named by its id: to the venue, and
    /// nowhere else. A wallet subscribes nothing of its own.
    fn stream_subscriptions(&self, account: &ResolvedAccount) -> Vec<String> {
        let id = account.address().expose();
        if id.is_empty() || !id.bytes().all(|b| b.is_ascii_digit()) {
            return Vec::new();
        }
        let channels: Vec<String> = ["balances", "orders", "trades"]
            .iter()
            .map(|c| format!("{id}.{c}"))
            .collect();
        vec![
            serde_json::json!({
                "id": STREAM_SUBSCRIBE_ID,
                "method": STREAM_SUBSCRIBE,
                "params": { "channels": channels },
            })
            .to_string(),
        ]
    }

    fn stream_keepalive(&self) -> crate::venue::Keepalive {
        crate::venue::Keepalive::Frame(
            serde_json::json!({ "id": STREAM_KEEPALIVE_ID, "method": STREAM_KEEPALIVE, "params": {} })
                .to_string(),
        )
    }

    fn stream_frame(&self, frame: &[u8]) -> crate::ledger::run::StreamFrame {
        ledger::streamed(frame)
    }

    fn compose_stream(
        &self,
        account: &ResolvedAccount,
        _mode: Option<(&[u8], i64)>,
        _reference: Option<&[u8]>,
        frame: &[u8],
    ) -> Vec<u8> {
        ledger::stream_bytes(account.address().expose(), &account.alias, frame)
    }

    /// Orders change after they first appear (Derive states each at its
    /// latest status), so their window always reaches back; trades, funding
    /// and value points do not, and are read from the newest held.
    fn wallet_history_asks(&self) -> &'static [(Ask, Series, bool)] {
        &[
            (Ask::OrderHistory, Series::OrderUpdates, false),
            (Ask::TradeHistory, Series::Fills, true),
            (Ask::FundingHistory, Series::FundingPayments, true),
            (Ask::ValueHistory, Series::PnlHistory, true),
        ]
    }

    /// Every page of the window: the first says how many there are. Each
    /// method names its bounds its own way (measured 2026-11-27).
    async fn wallet_history(
        &self,
        master: &ResolvedAccount,
        ask: Ask,
        from_micros: i64,
        now_micros: i64,
    ) -> Result<Vec<u8>, Refused> {
        let key = signer(master)?;
        let (method, from_key, to_key, period) = match ask {
            Ask::OrderHistory => (GET_ORDER_HISTORY, "from_timestamp", "to_timestamp", None),
            Ask::TradeHistory => (GET_TRADE_HISTORY, "from_timestamp", "to_timestamp", None),
            Ask::FundingHistory => (
                GET_FUNDING_HISTORY,
                "start_timestamp",
                "end_timestamp",
                None,
            ),
            Ask::ValueHistory => (
                GET_VALUE_HISTORY,
                "start_timestamp",
                "end_timestamp",
                Some(VALUE_PERIOD_SECS),
            ),
            _ => return Err(Refusal::Unreachable.into()),
        };
        let mut pages: Vec<Vec<u8>> = Vec::new();
        let mut total = 1;
        let mut page = 1;
        while page <= total {
            let mut window = serde_json::Map::new();
            window.insert(from_key.into(), (from_micros / 1_000).into());
            window.insert(to_key.into(), (now_micros / 1_000).into());
            window.insert("page".into(), page.into());
            window.insert("page_size".into(), HISTORY_PAGE_SIZE.into());
            if let Some(period) = period {
                window.insert("period".into(), period.into());
            }
            let answer = answered(
                self.client
                    .history(method, &key, master.address(), now_micros, window)
                    .await,
            )?;
            if page == 1 {
                total = ledger::num_pages(&answer).min(HISTORY_MAX_PAGES);
            }
            pages.push(answer);
            page += 1;
        }
        Ok(ledger::history_window(period, &pages))
    }

    async fn check_key(
        &self,
        account: &ResolvedAccount,
        now_micros: i64,
    ) -> Result<Option<KeyStanding>, KeyRefusal> {
        check_key(&self.client, account, now_micros).await.map(Some)
    }

    async fn list(&self, master: &ResolvedAccount, now_micros: i64) -> Result<Vec<u8>, Refused> {
        let key = signer(master)?;
        answered(
            self.client
                .subaccounts(&key, master.address(), now_micros)
                .await,
        )
    }

    async fn wallet_snapshot(
        &self,
        master: &ResolvedAccount,
        now_micros: i64,
    ) -> Result<Vec<u8>, Refused> {
        let key = signer(master)?;
        answered(
            self.client
                .all_portfolios(&key, master.address(), now_micros)
                .await,
        )
    }

    fn compose_wallet(&self, aliases: &BTreeMap<String, Account>, answer: &[u8]) -> Vec<u8> {
        ledger::wallet_bytes(&self.key, aliases, answer)
    }

    fn listed(&self, answer: &[u8]) -> Result<Vec<Listed>, NormaliseError> {
        ledger::subaccounts_listed(answer)
    }

    // What follows is never asked of a venue read by wallet: no declared
    // account has a dex, the venue has no roles or modes, and the ledger keeps
    // neither its account-wide reads nor its history.

    async fn snapshot(&self, _: &Secret, _: &str) -> Result<Vec<u8>, Refusal> {
        Err(Refusal::Unreachable)
    }

    async fn listing(&self, _: &Secret) -> Result<Vec<u8>, Refusal> {
        Err(Refusal::Unreachable)
    }

    async fn role(&self, _: &Secret) -> Result<Vec<u8>, Refusal> {
        Err(Refusal::Unreachable)
    }

    async fn mode(&self, _: &Secret) -> Result<Vec<u8>, Refusal> {
        Err(Refusal::Unreachable)
    }

    async fn dex_known(&self, _: &str) -> Result<bool, Refusal> {
        Err(Refusal::Unreachable)
    }

    fn compose(&self, _: &str, _: Option<(&[u8], i64)>, state: &[u8]) -> Vec<u8> {
        state.to_vec()
    }

    fn states(
        &self,
        payload: &crate::record::Payload,
        rows: &[galata_wire::Envelope],
    ) -> Vec<crate::ledger::publish::Stated> {
        ledger::states(payload, rows)
    }

    fn account_wide_asks(&self) -> &'static [(Ask, Series)] {
        &[]
    }

    async fn account_wide(&self, _: Ask, _: &Secret, _: &[String]) -> Result<Vec<u8>, Refusal> {
        Err(Refusal::Unreachable)
    }

    async fn reference(&self) -> Result<Vec<u8>, Refusal> {
        Err(Refusal::Unreachable)
    }

    fn compose_wide(&self, _: Ask, _: Option<&[u8]>, answer: &[u8]) -> Vec<u8> {
        answer.to_vec()
    }

    fn is_sub_account(&self, _: &[u8]) -> Result<bool, NormaliseError> {
        Ok(false)
    }

    fn events_channel(&self, _: Kind) -> Option<&'static str> {
        None
    }

    async fn events_page(&self, _: Kind, _: &Secret, _: i64) -> Result<Vec<u8>, Refusal> {
        Err(Refusal::Unreachable)
    }

    fn page_size(&self, _: Kind) -> usize {
        usize::MAX
    }

    fn page_span(&self, _: &[u8]) -> Option<(i64, i64, usize)> {
        None
    }
}

/// Check the session key `account` is read with, at `now_micros`.
///
/// Asks two things and nothing of the account's state: the unsigned lookup
/// of the key's wallets, and, only when that names the declared wallet, the
/// wallet's session keys.
pub async fn check_key(
    client: &PrivateClient,
    account: &ResolvedAccount,
    now_micros: i64,
) -> Result<KeyStanding, KeyRefusal> {
    let secret = account.session_key().ok_or(KeyRefusal::Absent)?;
    let key = SessionKey::from_secret(secret).map_err(|e| KeyRefusal::Unreadable(e.to_string()))?;
    let wallets = client
        .wallets_of(&key)
        .await
        .ok()
        .and_then(|answer| ledger::wallets_named(&answer))
        .ok_or(KeyRefusal::Unanswered)?;
    if wallets.is_empty() {
        return Err(KeyRefusal::NotRegistered);
    }
    // **The one comparison of the wallet with anything**: the venue's own list
    // of the wallets this key is registered to. Neither is printed.
    let wallet = account.address().expose().trim();
    if !wallets.iter().any(|w| w.eq_ignore_ascii_case(wallet)) {
        return Err(KeyRefusal::OtherWallet);
    }
    let answer = client
        .session_keys(&key, account.address(), now_micros)
        .await
        .map_err(|_| KeyRefusal::Unanswered)?;
    ledger::key_standing(&answer, &key.address(), now_micros)
}

#[cfg(test)]
mod tests {
    use super::super::local_server::{header, serve_routes};
    use super::super::private::{
        GET_ALL_PORTFOLIOS, GET_SUBACCOUNTS, SESSION_KEYS, WALLETS_OF_KEY,
    };
    use super::*;
    use crate::config::{ConfigError, Ledger, LedgerAccount, Secret, SecretSource};
    use crate::ledger::accounts::utc;

    /// The published throwaway key of `sign`'s tests, and its address.
    const KEY: &str = "0x4c0883a69102937d6231471b5dbb6204fe5129617082792ae468d01a3f362318";
    const KEY_ADDRESS: &str = "0x2c7536e3605d9c16a7a3d7b1898e529396a65c23";
    const WALLET: &str = "0x0003000300030003000300030003000300030003";
    const ELSEWHERE: &str = "0x0009000900090009000900090009000900090009";
    const NOW: i64 = 1_795_769_769_000_000;
    const AHEAD: i64 = 1_803_545_765;

    struct Vault(Option<&'static str>);
    impl SecretSource for Vault {
        fn secret(&self, name: &str) -> Result<Secret, ConfigError> {
            match name {
                "WALLET" => Ok(Secret::new(WALLET)),
                "FP" => Ok(Secret::new("fingerprint-key")),
                "KEY" => self.0.map(Secret::new).ok_or(ConfigError::SecretAbsent {
                    name: name.to_string(),
                }),
                _ => Err(ConfigError::SecretAbsent {
                    name: name.to_string(),
                }),
            }
        }
    }

    fn account(key: Option<&'static str>) -> ResolvedAccount {
        let dir = tempfile::tempdir().unwrap();
        let ledger = Ledger {
            root: dir.path().to_path_buf(),
            tape: None,
            snapshot_secs: 300,
            discover_secs: 600,
            events_secs: None,
            fold_position_tolerance: None,
            fold_relative_tolerance: None,
            ledger_share: 0.25,
            fingerprint_key_var: "FP".into(),
            account: [(
                "owner".to_string(),
                LedgerAccount {
                    venue: "derive-xyz".into(),
                    address_var: "WALLET".into(),
                    dexes: vec![],
                    session_key_var: key.map(|_| "KEY".to_string()),
                    session_key_warn_days: None,
                    market: None,
                    address: None,
                },
            )]
            .into(),
        };
        let (_, mut accounts) =
            crate::ledger::accounts::resolve(&ledger, "derive-xyz", &Vault(key)).unwrap();
        accounts.remove(0)
    }

    fn wallets(of: &[&str]) -> String {
        let quoted: Vec<String> = of.iter().map(|w| format!(r#""{w}""#)).collect();
        format!(
            r#"{{"id":"x","result":{{"wallets":[{}]}}}}"#,
            quoted.join(",")
        )
    }

    fn keys(protocol: &str, offchain: &str, expiry_sec: i64) -> String {
        format!(
            r#"{{"id":"x","result":{{"public_session_keys":[{{"public_session_key":"{KEY_ADDRESS}","protocol_scopes":{protocol},"offchain_scopes":{offchain},"expiry_sec":{expiry_sec}}}]}}}}"#
        )
    }

    async fn check(
        lookup: (u16, String),
        listed: (u16, String),
        key: Option<&'static str>,
    ) -> (Result<KeyStanding, KeyRefusal>, Vec<String>) {
        let (url, asked) = serve_routes(vec![
            (WALLETS_OF_KEY, lookup.0, lookup.1),
            (SESSION_KEYS, listed.0, listed.1),
        ])
        .await;
        let result = check_key(&PrivateClient::new(&url), &account(key), NOW).await;
        let asked = asked.lock().unwrap().clone();
        (result, asked)
    }

    /// The methods asked, in order.
    fn methods(asked: &[String]) -> Vec<String> {
        asked
            .iter()
            .map(|r| {
                r.split(' ')
                    .nth(1)
                    .unwrap_or_default()
                    .trim_start_matches('/')
                    .to_string()
            })
            .collect()
    }

    #[tokio::test]
    async fn a_key_with_no_scope_and_one_that_trades_are_each_read_with() {
        for (protocol, offchain) in [("[]", "[]"), (r#"["trade:all"]"#, r#"["account_info"]"#)] {
            let (result, asked) = check(
                (
                    200,
                    wallets(&[ELSEWHERE, &WALLET.to_uppercase().replace("0X", "0x")]),
                ),
                (200, keys(protocol, offchain, AHEAD)),
                Some(KEY),
            )
            .await;
            assert_eq!(
                result,
                Ok(KeyStanding {
                    expiry_micros: Some(AHEAD * 1_000_000)
                }),
                "{protocol} {offchain}"
            );
            assert_eq!(methods(&asked), vec![WALLETS_OF_KEY, SESSION_KEYS]);
        }
    }

    #[tokio::test]
    async fn a_key_that_can_withdraw_is_refused_by_name_before_any_account_request() {
        let (result, asked) = check(
            (200, wallets(&[WALLET])),
            (200, keys(r#"["trade:all","withdraw"]"#, "[]", AHEAD)),
            Some(KEY),
        )
        .await;
        let refused = result.unwrap_err();
        assert_eq!(
            refused,
            KeyRefusal::Scopes {
                scopes: vec!["withdraw".into()]
            }
        );
        assert!(refused.to_string().contains("`withdraw`"), "{refused}");
        for request in &asked {
            assert!(
                !request.contains(GET_SUBACCOUNTS) && !request.contains(GET_ALL_PORTFOLIOS),
                "{request}"
            );
        }
    }

    #[tokio::test]
    async fn a_scope_the_reader_does_not_know_is_refused_by_name() {
        let (result, _) = check(
            (200, wallets(&[WALLET])),
            (
                200,
                keys(r#"["trade:all"]"#, r#"["a_scope_added_later"]"#, AHEAD),
            ),
            Some(KEY),
        )
        .await;
        assert_eq!(
            result,
            Err(KeyRefusal::Scopes {
                scopes: vec!["a_scope_added_later".into()]
            })
        );
    }

    #[tokio::test]
    async fn a_key_registered_to_another_wallet_signs_nothing() {
        let (result, asked) = check(
            (200, wallets(&[ELSEWHERE])),
            (200, keys("[]", "[]", AHEAD)),
            Some(KEY),
        )
        .await;
        assert_eq!(result, Err(KeyRefusal::OtherWallet));
        assert_eq!(methods(&asked), vec![WALLETS_OF_KEY]);
        assert_eq!(header(&asked[0], "X-DeriveSignature"), None);
        let said = KeyRefusal::OtherWallet.to_string();
        assert!(!said.contains("0x"), "names neither wallet: {said}");
    }

    #[tokio::test]
    async fn an_unlisted_key_and_an_unanswered_question_are_refused_each_with_its_reason() {
        // The venue holds no registration of the key at all.
        let (result, asked) = check(
            (
                200,
                r#"{"error":{"code":14026,"message":"Session key not found"}}"#.to_string(),
            ),
            (200, keys("[]", "[]", AHEAD)),
            Some(KEY),
        )
        .await;
        assert_eq!(result, Err(KeyRefusal::NotRegistered));
        assert_eq!(methods(&asked), vec![WALLETS_OF_KEY]);

        // Registered to the wallet, and not in the wallet's own list.
        let elsewhere_key = keys("[]", "[]", AHEAD).replace(KEY_ADDRESS, ELSEWHERE);
        let (result, _) = check((200, wallets(&[WALLET])), (200, elsewhere_key), Some(KEY)).await;
        assert_eq!(result, Err(KeyRefusal::NotRegistered));

        // The lookup unanswered: nothing signed is sent.
        let (result, asked) = check(
            (503, "busy".into()),
            (200, keys("[]", "[]", AHEAD)),
            Some(KEY),
        )
        .await;
        assert_eq!(result, Err(KeyRefusal::Unanswered));
        assert_eq!(methods(&asked), vec![WALLETS_OF_KEY]);

        // The wallet's keys unanswered: the gateway's page.
        let (result, _) = check(
            (200, wallets(&[WALLET])),
            (403, "<html>403 Forbidden</html>".into()),
            Some(KEY),
        )
        .await;
        assert_eq!(result, Err(KeyRefusal::Unanswered));
        assert_ne!(
            KeyRefusal::Unanswered.to_string(),
            KeyRefusal::NotRegistered.to_string()
        );
    }

    #[tokio::test]
    async fn an_expired_key_is_refused_stating_its_expiry() {
        let (result, _) = check(
            (200, wallets(&[WALLET])),
            (200, keys("[]", "[]", 1_795_000_000)),
            Some(KEY),
        )
        .await;
        let refused = result.unwrap_err();
        assert_eq!(
            refused,
            KeyRefusal::Expired {
                expiry: utc(1_795_000_000_000_000)
            }
        );
        assert!(refused.to_string().contains(&utc(1_795_000_000_000_000)));
    }

    #[tokio::test]
    async fn an_account_without_a_key_asks_nothing() {
        let (result, asked) = check(
            (200, wallets(&[WALLET])),
            (200, keys("[]", "[]", AHEAD)),
            None,
        )
        .await;
        assert_eq!(result, Err(KeyRefusal::Absent));
        assert!(asked.is_empty());
    }

    /// The ledger's loop over this venue, on the local server: one wallet,
    /// its subaccounts bound at discovery (task 4.3).
    mod in_the_loop {
        use std::path::Path;
        use std::sync::{Arc, Mutex};

        use galata_wire::{Envelope, Event};

        use super::super::super::local_server::{Routes, serve_live};
        use super::*;
        use crate::capture::TestClock;
        use crate::ledger::accounts::{Bindings, resolve};
        use crate::ledger::run::{Cadences, LedgerRun};
        use crate::record::Archive;
        use crate::sink::NullSink;

        /// Identifiers no timestamp or figure here could hold by chance.
        const IDS: [u64; 3] = [424_242_001, 424_242_002, 424_242_003];

        /// The 3.4 fixture with these identifiers: the subaccount holding the
        /// option is the second.
        fn portfolios() -> String {
            let mut answer: serde_json::Value = serde_json::from_slice(include_bytes!(
                "../../../tests/fixtures/derive_xyz/ledger/get_all_portfolios.json"
            ))
            .unwrap();
            let held_second = [IDS[0], IDS[2], IDS[1]];
            for (portfolio, id) in answer["result"]
                .as_array_mut()
                .unwrap()
                .iter_mut()
                .zip(held_second)
            {
                portfolio["subaccount_id"] = id.into();
                for order in portfolio["open_orders"].as_array_mut().unwrap() {
                    order["subaccount_id"] = id.into();
                }
            }
            answer.to_string()
        }

        fn listing(ids: &[u64]) -> String {
            format!(r#"{{"id":"x","result":{{"subaccount_ids":{ids:?},"wallet":"{WALLET}"}}}}"#)
        }

        fn routes(listed: &[u64]) -> Vec<(&'static str, u16, String)> {
            vec![
                (WALLETS_OF_KEY, 200, wallets(&[WALLET])),
                (
                    SESSION_KEYS,
                    200,
                    keys(r#"["trade:all"]"#, r#"["account_info"]"#, AHEAD),
                ),
                (GET_SUBACCOUNTS, 200, listing(listed)),
                (GET_ALL_PORTFOLIOS, 200, portfolios()),
                (
                    GET_ORDER_HISTORY,
                    200,
                    history(include_bytes!(
                        "../../../tests/fixtures/derive_xyz/ledger/get_order_history.json"
                    )),
                ),
                (
                    GET_TRADE_HISTORY,
                    200,
                    history(include_bytes!(
                        "../../../tests/fixtures/derive_xyz/ledger/get_trade_history.json"
                    )),
                ),
                (
                    GET_FUNDING_HISTORY,
                    200,
                    history(include_bytes!(
                        "../../../tests/fixtures/derive_xyz/ledger/get_funding_history.json"
                    )),
                ),
                (
                    GET_VALUE_HISTORY,
                    200,
                    history(include_bytes!(
                        "../../../tests/fixtures/derive_xyz/ledger/get_subaccount_value_history.json"
                    )),
                ),
            ]
        }

        /// A history fixture, its subaccounts `1001` and `1002` renamed to the
        /// first two listed.
        fn history(fixture: &[u8]) -> String {
            fn rename(v: &mut serde_json::Value) {
                match v {
                    serde_json::Value::Object(map) => {
                        for (k, x) in map.iter_mut() {
                            if k == "subaccount_id"
                                && let Some(id) = x.as_u64()
                                && (1001..=1003).contains(&id)
                            {
                                *x = IDS[(id - 1001) as usize].into();
                            }
                            rename(x);
                        }
                    }
                    serde_json::Value::Array(items) => items.iter_mut().for_each(rename),
                    _ => {}
                }
            }
            let mut answer: serde_json::Value = serde_json::from_slice(fixture).unwrap();
            rename(&mut answer);
            answer.to_string()
        }

        /// The body of the last request of `method`.
        fn last_body(asked: &Mutex<Vec<String>>, method: &str) -> serde_json::Value {
            let asked = asked.lock().unwrap();
            let request = asked.iter().rev().find(|r| r.contains(method)).unwrap();
            serde_json::from_str(crate::adapters::derive_xyz::local_server::body(request)).unwrap()
        }

        /// The rows one subaccount's projection holds of a kind.
        fn projected(tape: &Path, alias: &str, kind: Kind) -> usize {
            galata_segments::read_segment(&crate::ledger::project::path_of(
                tape,
                "derive-xyz",
                alias,
                kind,
            ))
            .unwrap()
            .iter()
            .map(|b| b.num_rows())
            .sum()
        }

        fn folding(
            run: LedgerRun<DeriveAccounts, TestClock>,
            dir: &Path,
        ) -> LedgerRun<DeriveAccounts, TestClock> {
            run.with_fold(
                crate::capture::StatusFile::named(&dir.join("status"), "ledger-fold-derive-xyz"),
                crate::ledger::fold::Tolerances {
                    position: "0".parse().unwrap(),
                    relative: "0.00002".parse().unwrap(),
                },
            )
            .with_projection(Some(dir.join("ledger-tape")))
        }

        #[tokio::test]
        async fn a_wallets_history_is_four_windows_each_bound_to_its_subaccount() {
            let dir = tempfile::tempdir().unwrap();
            let (run, asked, _) = ledger_over(dir.path(), &IDS[..2]).await;
            let mut run = folding(run, dir.path());
            run.boot().await.unwrap();
            let pass = run.events_all().await.unwrap();
            assert_eq!((pass.reads, pass.missed), (4, 0));
            run.fold_all().unwrap();

            // Every window starts thirty days back: nothing is held yet.
            let reach = (NOW - crate::ledger::run::FIRST_REACH_MICROS) / 1_000;
            assert_eq!(
                last_body(&asked, GET_ORDER_HISTORY)["from_timestamp"],
                reach
            );
            assert_eq!(
                last_body(&asked, GET_TRADE_HISTORY)["from_timestamp"],
                reach
            );
            assert_eq!(
                last_body(&asked, GET_FUNDING_HISTORY)["start_timestamp"],
                reach
            );
            assert_eq!(
                last_body(&asked, GET_VALUE_HISTORY)["start_timestamp"],
                reach
            );
            assert_eq!(last_body(&asked, GET_VALUE_HISTORY)["period"], 3_600);

            // Each row under its subaccount's alias, and nothing else.
            let tape = dir.path().join("ledger-tape");
            assert_eq!(projected(&tape, "owner_s1", Kind::OrderUpdates), 3);
            assert_eq!(projected(&tape, "owner_s1", Kind::Fills), 2);
            assert_eq!(projected(&tape, "owner_s1", Kind::FundingPayments), 0);
            let values: serde_json::Value = serde_json::from_slice(include_bytes!(
                "../../../tests/fixtures/derive_xyz/ledger/get_subaccount_value_history.json"
            ))
            .unwrap();
            let points = |id: u64| {
                values["result"]["subaccount_value_history"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .filter(|p| p["subaccount_id"] == id)
                    .count()
            };
            assert_eq!(projected(&tape, "owner_s1", Kind::PnlHistory), points(1001));
            assert_eq!(projected(&tape, "owner_s2", Kind::PnlHistory), points(1002));
            fn walk(dir: &Path, out: &mut Vec<String>) {
                for entry in std::fs::read_dir(dir).unwrap().flatten() {
                    out.push(entry.path().display().to_string());
                    if entry.path().is_dir() {
                        walk(&entry.path(), out);
                    }
                }
            }
            let mut paths = Vec::new();
            walk(&tape, &mut paths);
            assert!(
                paths
                    .iter()
                    .all(|p| !p.contains(WALLET) && !p.contains("424242")),
                "{paths:?}"
            );
        }

        #[tokio::test]
        async fn a_missed_window_is_a_gap_and_the_next_starts_where_the_record_does() {
            let dir = tempfile::tempdir().unwrap();
            let (mut run, asked, routes) = ledger_over(dir.path(), &IDS[..2]).await;
            run.boot().await.unwrap();
            run.events_all().await.unwrap();
            let trades: serde_json::Value = serde_json::from_slice(include_bytes!(
                "../../../tests/fixtures/derive_xyz/ledger/get_trade_history.json"
            ))
            .unwrap();
            let newest = trades["result"]["trades"]
                .as_array()
                .unwrap()
                .iter()
                .map(|t| t["timestamp"].as_i64().unwrap())
                .max()
                .unwrap();

            // The trades window fails: a gap for each subaccount, and no
            // window recorded in its place.
            let good = history(include_bytes!(
                "../../../tests/fixtures/derive_xyz/ledger/get_trade_history.json"
            ));
            set(
                &routes,
                GET_TRADE_HISTORY,
                r#"{"id":"x","error":{"code":10000,"message":"x"}}"#.into(),
            );
            run.clock().advance_secs(300);
            let pass = run.events_all().await.unwrap();
            assert_eq!((pass.reads, pass.missed, pass.gaps), (3, 1, 2));
            let fill_gaps: Vec<_> = gaps(dir.path())
                .into_iter()
                .filter(|(_, g)| g.series == galata_wire::Series::Fills)
                .collect();
            assert_eq!(fill_gaps.len(), 2);

            // The next window starts at the newest trade the record holds, so
            // the missed span is read again.
            set(&routes, GET_TRADE_HISTORY, good);
            run.clock().advance_secs(300);
            let pass = run.events_all().await.unwrap();
            assert_eq!(pass.missed, 0);
            assert_eq!(
                last_body(&asked, GET_TRADE_HISTORY)["from_timestamp"],
                newest
            );
            // Orders change after they appear: their window always reaches back.
            assert_eq!(
                last_body(&asked, GET_ORDER_HISTORY)["from_timestamp"],
                (NOW + 600_000_000 - crate::ledger::run::FIRST_REACH_MICROS) / 1_000
            );
        }

        fn set(routes: &Routes, path: &str, body: String) {
            for route in routes.lock().unwrap().iter_mut() {
                if route.0 == path {
                    route.2 = body.clone();
                }
            }
        }

        async fn ledger_over(
            root: &Path,
            listed: &[u64],
        ) -> (
            LedgerRun<DeriveAccounts, TestClock>,
            Arc<Mutex<Vec<String>>>,
            Routes,
        ) {
            ledger_streaming(root, listed, None).await
        }

        /// As [`ledger_over`], reading its subaccounts' stream at `ws`.
        async fn ledger_streaming(
            root: &Path,
            listed: &[u64],
            ws: Option<String>,
        ) -> (
            LedgerRun<DeriveAccounts, TestClock>,
            Arc<Mutex<Vec<String>>>,
            Routes,
        ) {
            let (url, asked, routes) = serve_live(routes(listed)).await;
            let config = Ledger {
                root: root.to_path_buf(),
                tape: None,
                snapshot_secs: 300,
                discover_secs: 600,
                events_secs: None,
                fold_position_tolerance: None,
                fold_relative_tolerance: None,
                ledger_share: 0.25,
                fingerprint_key_var: "FP".into(),
                account: [(
                    "owner".to_string(),
                    LedgerAccount {
                        venue: "derive-xyz".into(),
                        address_var: "WALLET".into(),
                        dexes: vec![],
                        session_key_var: Some("KEY".into()),
                        session_key_warn_days: None,
                        market: None,
                        address: None,
                    },
                )]
                .into(),
            };
            let (key, masters) = resolve(&config, "derive-xyz", &Vault(Some(KEY))).unwrap();
            let run = LedgerRun::new(
                match ws {
                    Some(ws) => DeriveAccounts::new(&url, key.clone())
                        .unwrap()
                        .with_stream(crate::venue::Endpoint::held("STREAM", Secret::new(ws))),
                    None => DeriveAccounts::new(&url, key.clone()).unwrap(),
                },
                TestClock::at(NOW),
                Archive::open(root),
                Box::new(NullSink),
                key,
                Cadences {
                    snapshot_micros: 300_000_000,
                    discover_micros: 600_000_000,
                    events_micros: 300_000_000,
                    page_pause_micros: 0,
                },
                masters,
                Bindings::read(root, "derive-xyz").unwrap(),
            );
            (run, asked, routes)
        }

        fn snapshots(root: &Path) -> Vec<crate::record::Payload> {
            crate::replay::read_all(root)
                .unwrap()
                .into_iter()
                .map(|r| r.payload().clone())
                .filter(|p| p.channel == ledger::SNAPSHOT_CHANNEL)
                .collect()
        }

        fn rows(root: &Path) -> Vec<Envelope> {
            let normaliser = LedgerNormaliser::new().unwrap();
            snapshots(root)
                .iter()
                .flat_map(|p| normaliser.normalise(p).unwrap())
                .collect()
        }

        /// (margin, position, collateral) rows addressed to `alias`.
        fn count(rows: &[Envelope], alias: &str) -> (usize, usize, usize) {
            let of = |f: fn(&Event) -> bool| {
                rows.iter()
                    .filter(|e| e.account().map(|a| a.as_str()) == Some(alias) && f(&e.event))
                    .count()
            };
            (
                of(|e| matches!(e, Event::MarginState(_))),
                of(|e| matches!(e, Event::PositionState(_))),
                of(|e| matches!(e, Event::Collateral(_))),
            )
        }

        fn portfolio_requests(asked: &Mutex<Vec<String>>) -> usize {
            asked
                .lock()
                .unwrap()
                .iter()
                .filter(|r| r.contains(GET_ALL_PORTFOLIOS))
                .count()
        }

        #[tokio::test]
        async fn two_subaccounts_are_one_request_and_two_sets_of_rows_and_a_third_joins_later() {
            let dir = tempfile::tempdir().unwrap();
            let (mut run, asked, routes) = ledger_over(dir.path(), &IDS[..2]).await;
            run.boot().await.unwrap();
            run.snapshot_all().await.unwrap();

            assert_eq!(portfolio_requests(&asked), 1, "one request for the wallet");
            let first = rows(dir.path());
            assert_eq!(count(&first, "owner_s1"), (1, 0, 0), "the flat one");
            assert_eq!(
                count(&first, "owner_s2"),
                (1, 1, 1),
                "the one holding the option"
            );
            assert_eq!(
                count(&first, "owner_s3"),
                (0, 0, 0),
                "in the answer, and not bound yet"
            );
            assert_eq!(
                count(&first, "owner"),
                (0, 0, 0),
                "the wallet itself has no rows"
            );

            // A third subaccount, listed at the next discovery, is recorded
            // from the next snapshot under the next ordinal.
            set(&routes, GET_SUBACCOUNTS, listing(&IDS));
            let found = run.discover().await.unwrap();
            assert_eq!(found[0].new, vec!["owner_s3".to_string()]);
            run.snapshot_all().await.unwrap();

            assert_eq!(portfolio_requests(&asked), 2, "still one a snapshot");
            let second = rows(dir.path());
            assert_eq!(count(&second, "owner_s1"), (2, 0, 0));
            assert_eq!(count(&second, "owner_s2"), (2, 2, 2));
            assert_eq!(count(&second, "owner_s3"), (1, 0, 0));
            assert!(run.status().accounts["owner_s3"].polled);
        }

        #[tokio::test]
        async fn ordinals_survive_a_restart_from_the_record_alone() {
            let dir = tempfile::tempdir().unwrap();
            {
                let (mut run, _, _) = ledger_over(dir.path(), &IDS[..2]).await;
                run.boot().await.unwrap();
            }
            // Listed in another order, with a newcomer first: the bound keep
            // their ordinals, read back from the record, and the newcomer takes
            // the next.
            let (mut run, _, _) = ledger_over(dir.path(), &[IDS[2], IDS[1], IDS[0]]).await;
            run.boot().await.unwrap();
            run.snapshot_all().await.unwrap();

            let latest = snapshots(dir.path()).pop().unwrap();
            let composed: serde_json::Value = serde_json::from_slice(&latest.payload).unwrap();
            assert_eq!(
                composed["aliases"],
                serde_json::json!({
                    IDS[0].to_string(): "owner_s1",
                    IDS[1].to_string(): "owner_s2",
                    IDS[2].to_string(): "owner_s3",
                })
            );
        }

        #[tokio::test]
        async fn no_path_status_or_binding_names_a_subaccounts_identifier() {
            let dir = tempfile::tempdir().unwrap();
            let (mut run, _, _) = ledger_over(dir.path(), &IDS).await;
            run.boot().await.unwrap();
            run.snapshot_all().await.unwrap();

            let ids: Vec<String> = IDS.iter().map(u64::to_string).collect();
            let mut stack = vec![dir.path().to_path_buf()];
            while let Some(here) = stack.pop() {
                for entry in std::fs::read_dir(&here).unwrap().flatten() {
                    let path = entry.path();
                    let shown = path.to_string_lossy().to_string();
                    for id in &ids {
                        assert!(!shown.contains(id.as_str()), "{shown}");
                    }
                    if path.is_dir() {
                        stack.push(path);
                    }
                }
            }
            let status = serde_json::to_string(run.status()).unwrap();
            for id in &ids {
                assert!(!status.contains(id.as_str()), "{status}");
            }
            // The identifiers stay inside the venue's archived answers: every
            // other payload, the bindings among them, holds none.
            let others: Vec<_> = crate::replay::read_all(dir.path())
                .unwrap()
                .into_iter()
                .map(|r| r.payload().clone())
                .filter(|p| {
                    p.channel != ledger::SNAPSHOT_CHANNEL && p.channel != ledger::LISTING_CHANNEL
                })
                .collect();
            assert!(!others.is_empty(), "the bindings were recorded");
            for payload in others {
                let text = String::from_utf8_lossy(&payload.payload).to_string();
                for id in &ids {
                    assert!(!text.contains(id.as_str()), "{}: {text}", payload.channel);
                }
            }
        }

        fn gaps(root: &Path) -> Vec<(String, galata_wire::Gap)> {
            crate::replay::read_all(root)
                .unwrap()
                .into_iter()
                .map(|r| r.payload().clone())
                .filter(|p| p.kind == "gaps")
                .filter_map(|p| crate::ingest::generated_envelope(&p.payload).ok())
                .filter_map(|e| match (e.account().map(|a| a.to_string()), e.event) {
                    (Some(account), Event::Gap(g)) => Some((account, g)),
                    _ => None,
                })
                .collect()
        }

        const RATE: &str = r#"{"error":{"code":-32000,"message":"Rate limit exceeded","data":"Retry after 3120 ms"}}"#;

        #[tokio::test]
        async fn a_failed_read_is_a_gap_for_each_subaccount_and_repeats_no_earlier_row() {
            let dir = tempfile::tempdir().unwrap();
            let (mut run, asked, routes) = ledger_over(dir.path(), &IDS[..2]).await;
            run.boot().await.unwrap();
            run.snapshot_all().await.unwrap();
            let answered_at = NOW;

            set(
                &routes,
                GET_ALL_PORTFOLIOS,
                "<html>502 Bad Gateway</html>".into(),
            );
            for route in routes.lock().unwrap().iter_mut() {
                if route.0 == GET_ALL_PORTFOLIOS {
                    route.1 = 502;
                }
            }
            run.clock().advance_secs(300);
            let pass = run.snapshot_all().await.unwrap();
            assert_eq!((pass.missed, pass.gaps), (1, 2));
            assert_eq!(portfolio_requests(&asked), 2);

            let gaps = gaps(dir.path());
            let mut accounts: Vec<&str> = gaps.iter().map(|(a, _)| a.as_str()).collect();
            accounts.sort();
            assert_eq!(accounts, vec!["owner_s1", "owner_s2"]);
            for (_, gap) in &gaps {
                assert_eq!(gap.series, galata_wire::Series::MarginState);
                assert_eq!(
                    (gap.from_micros, gap.to_micros),
                    (answered_at, NOW + 300_000_000)
                );
                assert_eq!(gap.cause, galata_wire::GapCause::PollFailed);
            }
            // Nothing earlier is recorded in the miss's place.
            let rows = rows(dir.path());
            assert_eq!(count(&rows, "owner_s1"), (1, 0, 0));
            assert_eq!(count(&rows, "owner_s2"), (1, 1, 1));
        }

        #[tokio::test]
        async fn a_rate_refusal_holds_the_venue_for_the_wait_it_states() {
            let dir = tempfile::tempdir().unwrap();
            let (mut run, asked, routes) = ledger_over(dir.path(), &IDS[..2]).await;
            run.boot().await.unwrap();
            run.snapshot_all().await.unwrap();

            // Refused with HTTP 429, stating 3120 ms.
            for route in routes.lock().unwrap().iter_mut() {
                if route.0 == GET_ALL_PORTFOLIOS {
                    *route = (GET_ALL_PORTFOLIOS, 429, RATE.to_string());
                }
            }
            run.clock().advance_secs(300);
            let refused_at = NOW + 300_000_000;
            let pass = run.snapshot_all().await.unwrap();
            assert!(pass.throttled);
            assert_eq!(portfolio_requests(&asked), 2);
            assert_eq!(run.status().held_until_micros, Some(refused_at + 3_120_000));

            // Inside the wait, with the venue answering again: not asked, and
            // a miss all the same, throttled.
            set(&routes, GET_ALL_PORTFOLIOS, portfolios());
            for route in routes.lock().unwrap().iter_mut() {
                if route.0 == GET_ALL_PORTFOLIOS {
                    route.1 = 200;
                }
            }
            run.clock().advance(3_119_000);
            let pass = run.snapshot_all().await.unwrap();
            assert_eq!(portfolio_requests(&asked), 2, "no request inside the wait");
            assert_eq!(pass.missed, 1);
            // Discovery is held too.
            let listings = |asked: &Mutex<Vec<String>>| {
                asked
                    .lock()
                    .unwrap()
                    .iter()
                    .filter(|r| r.contains(GET_SUBACCOUNTS))
                    .count()
            };
            let before = listings(&asked);
            run.discover().await.unwrap();
            assert_eq!(listings(&asked), before, "no listing inside the wait");
            let throttled = gaps(dir.path())
                .iter()
                .filter(|(_, g)| g.cause == galata_wire::GapCause::Throttled)
                .count();
            assert_eq!(throttled, 4, "two misses, two subaccounts each");

            // Past it, asked again, and recorded again.
            run.clock().advance(1_000);
            run.snapshot_all().await.unwrap();
            assert_eq!(portfolio_requests(&asked), 3);
            assert_eq!(run.status().held_until_micros, None);
            let rows = rows(dir.path());
            assert_eq!(count(&rows, "owner_s1"), (2, 0, 0));
            assert_eq!(count(&rows, "owner_s2"), (2, 2, 2));
        }

        #[tokio::test]
        async fn a_rate_refusal_inside_an_answer_holds_the_venue_too() {
            let dir = tempfile::tempdir().unwrap();
            let (mut run, asked, routes) = ledger_over(dir.path(), &IDS[..2]).await;
            run.boot().await.unwrap();
            // HTTP 200 with the refusal in the body.
            set(&routes, GET_ALL_PORTFOLIOS, RATE.to_string());
            run.snapshot_all().await.unwrap();
            assert_eq!(run.status().held_until_micros, Some(NOW + 3_120_000));
            assert!(
                snapshots(dir.path()).is_empty(),
                "a refusal is not an answer"
            );
            run.clock().advance(3_000_000);
            run.snapshot_all().await.unwrap();
            assert_eq!(portfolio_requests(&asked), 1);
        }

        /// What the stream's server heard, frame by frame.
        type Heard = Arc<Mutex<Vec<serde_json::Value>>>;

        /// A bus that keeps what it acknowledged, in order.
        #[derive(Clone, Default)]
        struct Recorded {
            sent: Arc<Mutex<Vec<(String, Envelope)>>>,
        }

        impl crate::ledger::publish::AccountBus for Recorded {
            fn publish_acked<'a>(
                &'a self,
                id: &'a str,
                envelope: &'a Envelope,
            ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), String>> + Send + 'a>>
            {
                Box::pin(async move {
                    self.sent
                        .lock()
                        .unwrap()
                        .push((id.to_string(), envelope.clone()));
                    Ok(())
                })
            }
        }

        /// Derive's stream, standing in: it answers the login with the ids
        /// granted and the subscription with each channel `ok`, then pushes
        /// a trade of the first listed subaccount and a balance change.
        async fn derive_stream(granted: Vec<u64>) -> (String, Heard) {
            use futures_util::{SinkExt, StreamExt};
            use tokio_tungstenite::tungstenite::Message;
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let url = format!("ws://{}", listener.local_addr().unwrap());
            let heard: Heard = Arc::new(Mutex::new(Vec::new()));
            let kept = heard.clone();
            tokio::spawn(async move {
                let Ok((tcp, _)) = listener.accept().await else {
                    return;
                };
                let Ok(mut ws) = tokio_tungstenite::accept_async(tcp).await else {
                    return;
                };
                while let Some(Ok(message)) = ws.next().await {
                    let Message::Text(text) = message else {
                        continue;
                    };
                    let frame: serde_json::Value = serde_json::from_str(&text).unwrap();
                    kept.lock().unwrap().push(frame.clone());
                    let answer = match frame["method"].as_str() {
                        Some("public/login") => {
                            serde_json::json!({"id": frame["id"], "result": granted})
                        }
                        Some("subscribe") => {
                            let status: serde_json::Map<String, serde_json::Value> =
                                frame["params"]["channels"]
                                    .as_array()
                                    .unwrap()
                                    .iter()
                                    .map(|c| (c.as_str().unwrap().to_string(), "ok".into()))
                                    .collect();
                            serde_json::json!({"id": frame["id"], "result": {"status": status}})
                        }
                        _ => serde_json::json!({"id": frame["id"], "result": 0}),
                    };
                    let _ = ws.send(Message::Text(answer.to_string().into())).await;
                    if frame["method"] == "subscribe" {
                        let mut trade: serde_json::Value =
                            serde_json::from_slice::<serde_json::Value>(include_bytes!(
                                "../../../tests/fixtures/derive_xyz/ledger/get_trade_history.json"
                            ))
                            .unwrap()["result"]["trades"][0]
                                .clone();
                        trade["subaccount_id"] = granted[0].into();
                        let trades = serde_json::json!({"method": "subscription", "params": {
                            "channel": format!("{}.trades", granted[0]), "data": [trade]}});
                        let balances = serde_json::json!({"method": "subscription", "params": {
                            "channel": format!("{}.balances", granted[0]),
                            "data": [{"name": "ETH-PERP", "new_balance": "1", "previous_balance": "0",
                                      "update_type": "trade"}]}});
                        for push in [trades, balances] {
                            let _ = ws.send(Message::Text(push.to_string().into())).await;
                        }
                    }
                }
            });
            (url, heard)
        }

        #[tokio::test]
        async fn the_login_is_the_keys_and_only_bound_subaccounts_are_subscribed() {
            let dir = tempfile::tempdir().unwrap();
            let (ws, heard) = derive_stream(IDS.to_vec()).await;
            let (run, asked, _) = ledger_streaming(dir.path(), &IDS[..2], Some(ws)).await;
            let bus = Recorded::default();
            let mut run = run.with_publication(Box::new(bus.clone()));
            run.publish().await.unwrap();
            run.boot().await.unwrap();
            let before = portfolio_requests(&asked);
            for _ in 0..20 {
                run.step().await.unwrap();
                run.clock().advance(500_000);
                if bus
                    .sent
                    .lock()
                    .unwrap()
                    .iter()
                    .any(|(id, _)| id.contains(":fill:"))
                {
                    break;
                }
            }
            let heard = heard.lock().unwrap().clone();

            // The login: the wallet, a millisecond time, signed by the key.
            let login = &heard[0];
            assert_eq!(login["method"], STREAM_LOGIN);
            assert_eq!(login["params"]["wallet"], WALLET);
            let time = login["params"]["timestamp"].as_str().unwrap();
            let signature = login["params"]["signature"].as_str().unwrap();
            assert_eq!(
                super::super::super::sign::recover(time, signature).as_deref(),
                Some(KEY_ADDRESS),
                "signed by the ledger's key"
            );

            // The two bound subaccounts' channels, and nothing of the third.
            let mut channels: Vec<String> = heard
                .iter()
                .filter(|f| f["method"] == STREAM_SUBSCRIBE)
                .flat_map(|f| {
                    f["params"]["channels"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .map(|c| c.as_str().unwrap().to_string())
                        .collect::<Vec<_>>()
                })
                .collect();
            channels.sort();
            let mut expected: Vec<String> = IDS[..2]
                .iter()
                .flat_map(|id| ["balances", "orders", "trades"].map(|c| format!("{id}.{c}")))
                .collect();
            expected.sort();
            assert_eq!(channels, expected);
            assert!(
                heard
                    .iter()
                    .all(|f| !f.to_string().contains(&IDS[2].to_string())),
                "an id the ledger did not bind is named"
            );

            // The trade pushed is a fill of the subaccount, by alias, and the
            // balance change asked for the wallet's state at once.
            let fills: Vec<(String, Envelope)> = bus
                .sent
                .lock()
                .unwrap()
                .iter()
                .filter(|(id, _)| id.contains(":fill:"))
                .cloned()
                .collect();
            // The streamed trade, and the backstop's window on connecting
            // holding it and the other the venue has: each once.
            let streamed: serde_json::Value = serde_json::from_slice::<serde_json::Value>(
                include_bytes!("../../../tests/fixtures/derive_xyz/ledger/get_trade_history.json"),
            )
            .unwrap()["result"]["trades"][0]["trade_id"]
                .clone();
            assert_eq!(
                fills.len(),
                2,
                "{:?}",
                fills.iter().map(|f| &f.0).collect::<Vec<_>>()
            );
            let ids: std::collections::BTreeSet<&String> = fills.iter().map(|f| &f.0).collect();
            assert_eq!(ids.len(), 2, "a fill published twice");
            assert_eq!(
                fills
                    .iter()
                    .filter(|f| f.0.contains(streamed.as_str().unwrap()))
                    .count(),
                1,
                "the streamed trade, once"
            );
            for (_, fill) in &fills {
                assert_eq!(fill.account().map(|a| a.as_str()), Some("owner_s1"));
                assert!(matches!(&fill.event, Event::Fill(_)));
            }
            assert!(portfolio_requests(&asked) > before, "the snapshot, at once");
            // No identifier anywhere it would leave the ledger.
            let status = serde_json::to_string(run.status()).unwrap();
            for id in IDS {
                assert!(!status.contains(&id.to_string()), "{id} in the status");
                assert!(
                    !fills[0].0.contains(&id.to_string()),
                    "{id} in a message id"
                );
            }
            assert_eq!(
                run.status().accounts["owner_s1"].stream_connected,
                Some(true)
            );
        }

        #[tokio::test]
        async fn a_reader_takes_a_subaccounts_newest_rows_and_their_time() {
            use crate::ledger::project::{newest, path_of};
            use arrow::array::{Array, Decimal128Array, StringArray};

            let dir = tempfile::tempdir().unwrap();
            let tape = dir.path().join("ledger-tape");
            let (run, _, _) = ledger_over(dir.path(), &IDS[..2]).await;
            let mut run = run
                .with_fold(
                    crate::capture::StatusFile::named(
                        &dir.path().join("status"),
                        "ledger-fold-derive-xyz",
                    ),
                    crate::ledger::fold::Tolerances {
                        position: "0".parse().unwrap(),
                        relative: "0.00002".parse().unwrap(),
                    },
                )
                .with_projection(Some(tape.clone()));
            run.boot().await.unwrap();
            run.snapshot_all().await.unwrap();
            run.clock().advance_secs(300);
            let second = NOW + 300_000_000;
            run.snapshot_all().await.unwrap();
            run.fold_all().unwrap();

            let column = |batch: &arrow::record_batch::RecordBatch, name: &str| {
                batch.column_by_name(name).unwrap().clone()
            };
            let answer: serde_json::Value = serde_json::from_slice(include_bytes!(
                "../../../tests/fixtures/derive_xyz/ledger/get_all_portfolios.json"
            ))
            .unwrap();
            let held = &answer["result"][2];

            // The subaccount holding the option: one row of each, the newest
            // read's, at its receipt.
            let (at, margin) = newest(&tape, "derive-xyz", "owner_s2", Kind::MarginState)
                .unwrap()
                .unwrap();
            assert_eq!((at, margin.num_rows()), (second, 1));
            let types = column(&margin, "margin_type");
            let types = types.as_any().downcast_ref::<StringArray>().unwrap();
            assert_eq!(types.value(0), "PM2");
            let initial = column(&margin, "initial_margin");
            let initial = initial.as_any().downcast_ref::<Decimal128Array>().unwrap();
            assert_eq!(
                initial
                    .value_as_string(0)
                    .parse::<galata_wire::Num>()
                    .unwrap(),
                held["initial_margin"]
                    .as_str()
                    .unwrap()
                    .parse::<galata_wire::Num>()
                    .unwrap(),
                "carried as stated"
            );
            let (at, positions) = newest(&tape, "derive-xyz", "owner_s2", Kind::PositionState)
                .unwrap()
                .unwrap();
            assert_eq!((at, positions.num_rows()), (second, 1));
            let tickers = column(&positions, "ticker");
            let tickers = tickers.as_any().downcast_ref::<StringArray>().unwrap();
            assert_eq!(
                tickers.value(0),
                held["positions"][0]["instrument_name"].as_str().unwrap()
            );
            // Its open order, the newest read's set; the other subaccount
            // rests none, a file saying so.
            let resting = |alias: &str| -> usize {
                galata_segments::read_segment(&path_of(&tape, "derive-xyz", alias, Kind::Orders))
                    .unwrap()
                    .iter()
                    .map(|b| b.num_rows())
                    .sum()
            };
            assert_eq!(resting("owner_s2"), 1);
            assert_eq!(resting("owner_s1"), 0);
            let (at, orders) = newest(&tape, "derive-xyz", "owner_s2", Kind::Orders)
                .unwrap()
                .unwrap();
            assert_eq!(at, second);
            let ids = column(&orders, "order_id");
            let ids = ids.as_any().downcast_ref::<StringArray>().unwrap();
            assert_eq!(
                ids.value(0),
                held["open_orders"][0]["order_id"].as_str().unwrap()
            );
            let (at, collateral) = newest(&tape, "derive-xyz", "owner_s2", Kind::Collateral)
                .unwrap()
                .unwrap();
            assert_eq!((at, collateral.num_rows()), (second, 1));

            // The flat one: its margin row, and a positions file saying none.
            let (at, margin) = newest(&tape, "derive-xyz", "owner_s1", Kind::MarginState)
                .unwrap()
                .unwrap();
            assert_eq!((at, margin.num_rows()), (second, 1));
            assert!(
                newest(&tape, "derive-xyz", "owner_s1", Kind::PositionState)
                    .unwrap()
                    .is_none()
            );
            // Two reads of the same flat figures are two rows in the file.
            let all: usize = galata_segments::read_segment(&path_of(
                &tape,
                "derive-xyz",
                "owner_s1",
                Kind::MarginState,
            ))
            .unwrap()
            .iter()
            .map(|b| b.num_rows())
            .sum();
            assert_eq!(all, 2);
            // The wallet itself is projected as nothing: its rows are its
            // subaccounts'.
            assert!(!path_of(&tape, "derive-xyz", "owner", Kind::MarginState).exists());
            assert!(!path_of(&tape, "derive-xyz", "owner", Kind::Fills).exists());
        }
    }
}
