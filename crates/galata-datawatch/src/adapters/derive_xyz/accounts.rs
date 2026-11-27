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
    PrivateClient, PrivateError, RATE_REFUSED, answer_error, refused, retry_after_ms,
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
        })
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

/// Never asked of this venue: it has no per-dex snapshot, roles, modes,
/// account-wide reads or history.
const NOT_ASKED: &str = "not-asked";

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

    fn account_wide_asks(&self) -> &'static [(Ask, Series)] {
        &[]
    }

    async fn account_wide(&self, _: Ask, _: &Secret) -> Result<Vec<u8>, Refusal> {
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
            ]
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
                DeriveAccounts::new(&url, key.clone()).unwrap(),
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
