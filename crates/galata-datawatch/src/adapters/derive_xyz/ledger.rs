//! Derive's account answers, for the ledger. **Pure**, like `normalise`:
//! bytes in, a reading out.
//!
//! What a session key may hold to be read with is design D4 of
//! `add-derive-account-ledger`, confirmed 2026-11-27: one key serves the
//! ledger and the owner's trading, so trading is accepted and the line is
//! drawn at moving funds and granting authority. **The set lists what is
//! accepted, not what is refused**, so a scope the venue adds later is refused
//! until somebody has looked at it.
//!
//! # A wallet's snapshot
//!
//! One `private/get_all_portfolios` answer holds every subaccount of the
//! wallet (design D1). It is archived under the wallet's alias **as composed
//! here**: the answer verbatim, beside the alias each subaccount is bound to,
//!
//! ```text
//!   {"aliases": {"<subaccount id>": "<wallet>_s<ordinal>", …}, "answer": <the venue's bytes>}
//! ```
//!
//! so the rows read from it are addressed to each subaccount from the record
//! alone, and the identifier stays inside the archived bytes. A subaccount
//! the answer holds and no discovery has bound yet has no alias, and no rows
//! until the next snapshot after it is bound.
//!
//! **Every figure is read leniently**: a figure that is absent, null or not a
//! decimal is absent from its row, so a field the venue renames costs that
//! figure and not the snapshot. The venue's v3 API had renamed fields within
//! seven weeks of its release (design, Risks).

use std::collections::BTreeMap;

use galata_wire::{Account, Collateral, Envelope, Event, MarginState, Num, PositionState, Venue};
use serde_json::Value;

use crate::ledger::Listed;
use crate::ledger::accounts::{FingerprintKey, KeyRefusal, KeyStanding, fingerprint, utc};
use crate::normalise::{Normalise, NormaliseError};
use crate::record::{Payload, PayloadAddress};

use super::VENUE;

/// The channel a wallet's snapshot is recorded under: the method that
/// answered it, `private/get_all_portfolios`.
pub const SNAPSHOT_CHANNEL: &str = "private/get_all_portfolios";
/// The channel a wallet's subaccount listing is recorded under: the method
/// that answered it, `private/get_subaccounts`.
pub const LISTING_CHANNEL: &str = "private/get_subaccounts";

/// The subaccounts a `private/get_subaccounts` answer names, each by its
/// identifier, held so that it cannot print itself. The venue names none.
pub fn subaccounts_listed(answer: &[u8]) -> Result<Vec<Listed>, NormaliseError> {
    let answer: Value =
        serde_json::from_slice(answer).map_err(|e| NormaliseError::Json(e.to_string()))?;
    let ids = answer
        .get("result")
        .and_then(|r| r.get("subaccount_ids"))
        .and_then(Value::as_array)
        .ok_or_else(|| NormaliseError::Shape {
            kind: "subaccounts",
            detail: "no result.subaccount_ids".into(),
        })?;
    ids.iter()
        .map(|id| {
            identifier(id)
                .map(|id| Listed {
                    address: crate::config::Secret::new(id),
                    name: None,
                })
                .ok_or_else(|| NormaliseError::Shape {
                    kind: "subaccounts",
                    detail: "a subaccount identifier that is not a number".into(),
                })
        })
        .collect()
}

/// A subaccount identifier as the ledger fingerprints it: decimal digits.
fn identifier(id: &Value) -> Option<String> {
    match id {
        Value::Number(n) => n.as_u64().map(|n| n.to_string()),
        Value::String(s) if !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit()) => {
            Some(s.clone())
        }
        _ => None,
    }
}

/// The portfolios of an answer, where it holds them.
fn portfolios(answer: &Value) -> &[Value] {
    answer
        .get("result")
        .and_then(Value::as_array)
        .map_or(&[], Vec::as_slice)
}

/// A wallet's snapshot as it is archived: the answer verbatim, with the alias
/// each subaccount in it is bound to. `aliases` is keyed by fingerprint, the
/// identifier fingerprinted as discovery fingerprinted it.
pub fn wallet_bytes(
    key: &FingerprintKey,
    aliases: &BTreeMap<String, Account>,
    answer: &[u8],
) -> Vec<u8> {
    let mut named = serde_json::Map::new();
    if let Ok(parsed) = serde_json::from_slice::<Value>(answer) {
        for portfolio in portfolios(&parsed) {
            if let Some(id) = portfolio.get("subaccount_id").and_then(identifier)
                && let Some(alias) = aliases.get(&fingerprint(key, &id))
            {
                named.insert(id, Value::String(alias.to_string()));
            }
        }
    }
    let mut out = Vec::with_capacity(answer.len() + 64 * named.len() + 32);
    out.extend_from_slice(b"{\"aliases\":");
    out.extend_from_slice(Value::Object(named).to_string().as_bytes());
    out.extend_from_slice(b",\"answer\":");
    out.extend_from_slice(answer);
    out.push(b'}');
    out
}

/// A figure as the venue states it: a decimal string or number, or absent.
fn num(v: Option<&Value>) -> Option<Num> {
    match v? {
        Value::String(s) => s.parse().ok(),
        Value::Number(n) => n.to_string().parse().ok(),
        _ => None,
    }
}

fn text(v: Option<&Value>) -> Option<String> {
    match v? {
        Value::String(s) => Some(s.clone()),
        Value::Number(n) => Some(n.to_string()),
        _ => None,
    }
}

/// The rows of a wallet's snapshot: per bound subaccount, its margin, each
/// position and each collateral, every figure as stated.
pub fn wallet_rows(
    venue: &Venue,
    recv_micros: i64,
    bytes: &[u8],
) -> Result<Vec<Envelope>, NormaliseError> {
    let composed: Value =
        serde_json::from_slice(bytes).map_err(|e| NormaliseError::Json(e.to_string()))?;
    let aliases = composed
        .get("aliases")
        .and_then(Value::as_object)
        .ok_or_else(|| NormaliseError::Shape {
            kind: "portfolios",
            detail: "a snapshot without its aliases".into(),
        })?;
    let answer = composed
        .get("answer")
        .ok_or_else(|| NormaliseError::Shape {
            kind: "portfolios",
            detail: "a snapshot without its answer".into(),
        })?;
    if answer.get("result").and_then(Value::as_array).is_none() {
        return Err(NormaliseError::Shape {
            kind: "portfolios",
            detail: "an answer without a result list".into(),
        });
    }

    let mut out = Vec::new();
    for portfolio in portfolios(answer) {
        let Some(alias) = portfolio
            .get("subaccount_id")
            .and_then(identifier)
            .and_then(|id| aliases.get(&id))
            .and_then(Value::as_str)
        else {
            // Not bound yet: recorded from the next snapshot after discovery.
            continue;
        };
        let account = Account::new(alias)?;
        let envelope =
            |event| Envelope::for_account(venue.clone(), account.clone(), None, recv_micros, event);
        let f = |field: &str| num(portfolio.get(field));
        out.push(envelope(Event::MarginState(MarginState {
            margin_type: text(portfolio.get("margin_type")),
            manager: text(portfolio.get("manager_id")),
            risk_universe: text(portfolio.get("risk_universe_id")),
            value: f("subaccount_value"),
            initial_margin: f("initial_margin"),
            maintenance_margin: f("maintenance_margin"),
            positions_value: f("positions_value"),
            collateral_value: f("collaterals_value"),
            open_orders_margin: f("open_orders_margin"),
            under_liquidation: portfolio
                .get("is_under_liquidation")
                .and_then(Value::as_bool),
        })));
        for position in list(portfolio, "positions") {
            let Some(instrument) = text(position.get("instrument_name")) else {
                continue;
            };
            let f = |field: &str| num(position.get(field));
            out.push(envelope(Event::PositionState(PositionState {
                instrument,
                instrument_type: text(position.get("instrument_type")),
                amount: f("amount"),
                average_price: f("average_price"),
                mark_price: f("mark_price"),
                mark_value: f("mark_value"),
                unrealised_pnl: f("unrealized_pnl"),
                delta: f("delta"),
                gamma: f("gamma"),
                theta: f("theta"),
                vega: f("vega"),
                initial_margin: f("initial_margin"),
                maintenance_margin: f("maintenance_margin"),
                liquidation_price: f("liquidation_price"),
                cumulative_funding: f("cumulative_funding"),
            })));
        }
        for collateral in list(portfolio, "collaterals") {
            let Some(asset) = text(collateral.get("asset_name")) else {
                continue;
            };
            let f = |field: &str| num(collateral.get(field));
            out.push(envelope(Event::Collateral(Collateral {
                asset,
                asset_type: text(collateral.get("asset_type")),
                amount: f("amount"),
                mark_price: f("mark_price"),
                mark_value: f("mark_value"),
                initial_margin: f("initial_margin"),
                maintenance_margin: f("maintenance_margin"),
                cumulative_interest: f("cumulative_interest"),
                pending_interest: f("pending_interest"),
            })));
        }
    }
    Ok(out)
}

fn list<'a>(portfolio: &'a Value, field: &str) -> &'a [Value] {
    portfolio
        .get(field)
        .and_then(Value::as_array)
        .map_or(&[], Vec::as_slice)
}

/// Derive's ledger answers to rows: a wallet's snapshot to its subaccounts'
/// margin, positions and collateral. A listing carries no rows; the bindings
/// discovery makes of it are recorded by the loop.
#[derive(Debug, Clone)]
pub struct LedgerNormaliser {
    venue: Venue,
}

impl LedgerNormaliser {
    /// For the venue's ledger.
    pub fn new() -> Result<LedgerNormaliser, galata_wire::TokenError> {
        Ok(LedgerNormaliser {
            venue: Venue::new(VENUE)?,
        })
    }
}

impl Normalise for LedgerNormaliser {
    fn normalise(&self, payload: &Payload) -> Result<Vec<Envelope>, NormaliseError> {
        match payload.channel.as_str() {
            SNAPSHOT_CHANNEL => {
                let PayloadAddress::Account(_) = &payload.address else {
                    return Err(NormaliseError::Shape {
                        kind: "portfolios",
                        detail: "a snapshot not addressed to an account".into(),
                    });
                };
                wallet_rows(&self.venue, payload.recv_micros, &payload.payload)
            }
            LISTING_CHANNEL => Ok(Vec::new()),
            other => Err(NormaliseError::UnknownChannel(other.to_string())),
        }
    }

    fn venue(&self) -> &Venue {
        &self.venue
    }
}

/// The off-chain scopes the ledger reads with. `account_info` gates only
/// label changes; `block_trade` is trading.
const ACCEPTED_OFFCHAIN: [&str; 2] = ["account_info", "block_trade"];

/// The venue's error for a key it holds no registration of (measured
/// 2026-11-27).
const KEY_NOT_FOUND: i64 = 14026;

/// Whether the ledger reads with a protocol scope: trading, of any market.
fn accepted_protocol(scope: &str) -> bool {
    scope == "trade" || scope.starts_with("trade:")
}

/// The wallets `public/get_wallets_from_session_key` says a key is
/// registered to: none for a key the venue does not hold. `None` for an
/// answer that says neither.
pub fn wallets_named(answer: &[u8]) -> Option<Vec<String>> {
    let answer: serde_json::Value = serde_json::from_slice(answer).ok()?;
    if let Some(error) = answer.get("error") {
        return (error.get("code")?.as_i64()? == KEY_NOT_FOUND).then(Vec::new);
    }
    answer
        .get("result")?
        .get("wallets")?
        .as_array()?
        .iter()
        .map(|w| w.as_str().map(str::to_string))
        .collect()
}

/// What `private/session_keys` says of the key at `key_address`, at
/// `now_micros`: its standing, or why it is not read with.
///
/// A key whose scope lists are not both stated could not be checked, and is
/// refused as unanswered rather than taken to hold nothing.
pub fn key_standing(
    answer: &[u8],
    key_address: &str,
    now_micros: i64,
) -> Result<KeyStanding, KeyRefusal> {
    let answer: serde_json::Value =
        serde_json::from_slice(answer).map_err(|_| KeyRefusal::Unanswered)?;
    let keys = answer
        .get("result")
        .and_then(|r| r.get("public_session_keys"))
        .and_then(|k| k.as_array())
        .ok_or(KeyRefusal::Unanswered)?;
    let ours = keys
        .iter()
        .find(|k| {
            k.get("public_session_key")
                .and_then(|a| a.as_str())
                .is_some_and(|a| a.eq_ignore_ascii_case(key_address))
        })
        .ok_or(KeyRefusal::NotRegistered)?;

    let scopes = |field: &str| -> Result<Vec<String>, KeyRefusal> {
        ours.get(field)
            .and_then(|s| s.as_array())
            .ok_or(KeyRefusal::Unanswered)?
            .iter()
            .map(|s| s.as_str().map(str::to_string).ok_or(KeyRefusal::Unanswered))
            .collect()
    };
    let mut refused: Vec<String> = scopes("protocol_scopes")?
        .into_iter()
        .filter(|s| !accepted_protocol(s))
        .collect();
    refused.extend(
        scopes("offchain_scopes")?
            .into_iter()
            .filter(|s| !ACCEPTED_OFFCHAIN.contains(&s.as_str())),
    );
    if !refused.is_empty() {
        return Err(KeyRefusal::Scopes { scopes: refused });
    }

    let expiry_micros = ours
        .get("expiry_sec")
        .and_then(|e| e.as_i64())
        .map(|secs| secs.saturating_mul(1_000_000));
    if let Some(expiry) = expiry_micros
        && expiry <= now_micros
    {
        return Err(KeyRefusal::Expired {
            expiry: utc(expiry),
        });
    }
    Ok(KeyStanding { expiry_micros })
}

#[cfg(test)]
mod tests {
    use super::*;

    const KEY: &str = "0x0001000100010001000100010001000100010001";
    const NOW: i64 = 1_795_769_769_000_000;

    fn keys(protocol: &str, offchain: &str, expiry_sec: i64) -> Vec<u8> {
        format!(
            r#"{{"id":"x","result":{{"public_session_keys":[
                {{"public_session_key":"0x0002000200020002000200020002000200020002","protocol_scopes":["admin"],"offchain_scopes":[],"expiry_sec":1803545765}},
                {{"public_session_key":"{}","protocol_scopes":{protocol},"offchain_scopes":{offchain},"expiry_sec":{expiry_sec},"label":"read-only"}}
            ]}}}}"#,
            KEY.to_uppercase().replace("0X", "0x")
        )
        .into_bytes()
    }

    #[test]
    fn the_answer_of_2026_11_27_holds_vault_all_and_is_refused_for_it() {
        let answer = include_bytes!("../../../tests/fixtures/derive_xyz/ledger/session_keys.json");
        let refused = key_standing(answer, KEY, NOW).unwrap_err();
        assert_eq!(
            refused,
            KeyRefusal::Scopes {
                scopes: vec!["vault:all".into()]
            }
        );
    }

    #[test]
    fn no_scope_and_trading_scopes_are_read_with() {
        for (protocol, offchain) in [
            ("[]", "[]"),
            (r#"["trade:all"]"#, r#"["account_info"]"#),
            (
                r#"["trade:orderbook:option","trade:rfq:perp"]"#,
                r#"["block_trade"]"#,
            ),
        ] {
            let standing = key_standing(&keys(protocol, offchain, 1_803_545_765), KEY, NOW)
                .unwrap_or_else(|e| panic!("{protocol} {offchain}: {e}"));
            assert_eq!(standing.expiry_micros, Some(1_803_545_765_000_000));
        }
    }

    #[test]
    fn every_scope_outside_the_accepted_set_is_named() {
        let refused = key_standing(
            &keys(
                r#"["trade:all","withdraw","set_session_key"]"#,
                r#"["account_info","a_scope_added_later"]"#,
                1_803_545_765,
            ),
            KEY,
            NOW,
        )
        .unwrap_err();
        assert_eq!(
            refused,
            KeyRefusal::Scopes {
                scopes: vec![
                    "withdraw".into(),
                    "set_session_key".into(),
                    "a_scope_added_later".into()
                ]
            }
        );
        // A prefix of `trade` is not `trade`.
        let refused = key_standing(&keys(r#"["trader"]"#, "[]", 1_803_545_765), KEY, NOW);
        assert!(matches!(refused, Err(KeyRefusal::Scopes { .. })));
    }

    #[test]
    fn an_expired_key_is_refused_stating_its_expiry() {
        let refused = key_standing(&keys("[]", "[]", 1_795_000_000), KEY, NOW).unwrap_err();
        assert_eq!(
            refused,
            KeyRefusal::Expired {
                expiry: utc(1_795_000_000_000_000)
            }
        );
        assert!(refused.to_string().contains("UTC"), "{refused}");
    }

    #[test]
    fn a_key_the_wallet_does_not_list_and_an_answer_without_scopes_are_told_apart() {
        let other = "0x0009000900090009000900090009000900090009";
        assert_eq!(
            key_standing(&keys("[]", "[]", 1_803_545_765), other, NOW),
            Err(KeyRefusal::NotRegistered)
        );
        let no_lists = br#"{"result":{"public_session_keys":[{"public_session_key":"0x0001000100010001000100010001000100010001"}]}}"#;
        assert_eq!(
            key_standing(no_lists, KEY, NOW),
            Err(KeyRefusal::Unanswered)
        );
        assert_eq!(
            key_standing(b"<html>403 Forbidden</html>", KEY, NOW),
            Err(KeyRefusal::Unanswered)
        );
    }

    #[test]
    fn the_lookup_says_a_wallet_none_or_nothing() {
        assert_eq!(
            wallets_named(br#"{"id":"x","result":{"wallets":["0xAbC"]}}"#),
            Some(vec!["0xAbC".to_string()])
        );
        // The venue's answer for a key it does not hold (measured 2026-11-27).
        assert_eq!(
            wallets_named(br#"{"error":{"code":14026,"message":"Session key not found"}}"#),
            Some(vec![])
        );
        assert_eq!(wallets_named(br#"{"error":{"code":-32000}}"#), None);
        assert_eq!(wallets_named(b"<html></html>"), None);
    }

    /// Task 4.4: a portfolio answer to rows, against the 3.4 fixtures.
    mod rows {
        use super::super::*;
        use crate::config::Secret;

        const PORTFOLIOS: &[u8] =
            include_bytes!("../../../tests/fixtures/derive_xyz/ledger/get_all_portfolios.json");

        fn key() -> FingerprintKey {
            FingerprintKey::new(Secret::new("fingerprint-key"))
        }

        /// The fixture's three subaccounts, bound to `owner_s1` to `_s3`.
        fn bound(answer: &[u8]) -> Vec<u8> {
            let aliases = (1..=3)
                .map(|n| {
                    (
                        fingerprint(&key(), &(1000 + n).to_string()),
                        Account::new(format!("owner_s{n}")).unwrap(),
                    )
                })
                .collect();
            wallet_bytes(&key(), &aliases, answer)
        }

        fn read(answer: &[u8]) -> Vec<Envelope> {
            wallet_rows(&Venue::new(VENUE).unwrap(), 42, &bound(answer)).unwrap()
        }

        fn of(rows: &[Envelope], alias: &str) -> Vec<Event> {
            rows.iter()
                .filter(|e| e.account().map(|a| a.as_str()) == Some(alias))
                .map(|e| e.event.clone())
                .collect()
        }

        fn stated(v: &Value) -> Option<String> {
            v.as_str().map(str::to_string)
        }

        fn digits(n: &Option<Num>) -> Option<String> {
            n.map(|n| n.to_string())
        }

        #[test]
        fn every_figure_is_carried_digit_for_digit() {
            let answer: Value = serde_json::from_slice(PORTFOLIOS).unwrap();
            let held = &answer["result"][2];
            let rows = read(PORTFOLIOS);
            let events = of(&rows, "owner_s3");
            assert_eq!(events.len(), 3, "margin, one position, one collateral");

            let Event::MarginState(m) = &events[0] else {
                panic!("{:?}", events[0])
            };
            for (got, field) in [
                (&m.value, "subaccount_value"),
                (&m.initial_margin, "initial_margin"),
                (&m.maintenance_margin, "maintenance_margin"),
                (&m.positions_value, "positions_value"),
                (&m.collateral_value, "collaterals_value"),
                (&m.open_orders_margin, "open_orders_margin"),
            ] {
                assert_eq!(digits(got), stated(&held[field]), "{field}");
            }
            assert_eq!(m.margin_type.as_deref(), Some("PM2"));
            assert_eq!(m.manager, Some(held["manager_id"].to_string()));
            assert_eq!(m.risk_universe, Some(held["risk_universe_id"].to_string()));
            assert_eq!(m.under_liquidation, Some(false));

            let Event::PositionState(p) = &events[1] else {
                panic!("{:?}", events[1])
            };
            let stated_position = &held["positions"][0];
            assert_eq!(
                p.instrument,
                stated_position["instrument_name"].as_str().unwrap()
            );
            assert_eq!(p.instrument_type.as_deref(), Some("option"));
            for (got, field) in [
                (&p.amount, "amount"),
                (&p.average_price, "average_price"),
                (&p.mark_price, "mark_price"),
                (&p.mark_value, "mark_value"),
                (&p.unrealised_pnl, "unrealized_pnl"),
                (&p.delta, "delta"),
                (&p.gamma, "gamma"),
                (&p.theta, "theta"),
                (&p.vega, "vega"),
                (&p.initial_margin, "initial_margin"),
                (&p.maintenance_margin, "maintenance_margin"),
                (&p.liquidation_price, "liquidation_price"),
                (&p.cumulative_funding, "cumulative_funding"),
            ] {
                assert_eq!(
                    digits(got),
                    stated(&stated_position[field]),
                    "position {field}"
                );
            }

            let Event::Collateral(c) = &events[2] else {
                panic!("{:?}", events[2])
            };
            let stated_collateral = &held["collaterals"][0];
            assert_eq!(c.asset, stated_collateral["asset_name"].as_str().unwrap());
            assert_eq!(c.asset_type, stated(&stated_collateral["asset_type"]));
            for (got, field) in [
                (&c.amount, "amount"),
                (&c.mark_price, "mark_price"),
                (&c.mark_value, "mark_value"),
                (&c.initial_margin, "initial_margin"),
                (&c.maintenance_margin, "maintenance_margin"),
                (&c.cumulative_interest, "cumulative_interest"),
                (&c.pending_interest, "pending_interest"),
            ] {
                assert_eq!(
                    digits(got),
                    stated(&stated_collateral[field]),
                    "collateral {field}"
                );
            }

            // Every row carries the answer's receipt, and no venue time: the
            // answer states none.
            assert!(
                rows.iter()
                    .all(|e| e.recv_micros == 42 && e.at_micros.is_none())
            );
        }

        #[test]
        fn a_flat_subaccount_gives_its_margin_row_and_no_position_row() {
            let rows = read(PORTFOLIOS);
            for flat in ["owner_s1", "owner_s2"] {
                let events = of(&rows, flat);
                assert_eq!(events.len(), 1, "{flat}: {events:?}");
                assert!(matches!(events[0], Event::MarginState(_)), "{flat}");
            }
        }

        #[test]
        fn an_absent_or_unreadable_figure_is_absent_and_costs_nothing_else() {
            let mut answer: Value = serde_json::from_slice(PORTFOLIOS).unwrap();
            let held = &mut answer["result"][2];
            held.as_object_mut().unwrap().remove("initial_margin");
            held["maintenance_margin"] = Value::Null;
            held["positions"][0]["liquidation_price"] = Value::String(String::new());
            held["positions"][0]["delta"] = Value::String("not a number".into());
            let rows = read(answer.to_string().as_bytes());
            let events = of(&rows, "owner_s3");
            assert_eq!(events.len(), 3, "the snapshot survives");
            let (Event::MarginState(m), Event::PositionState(p)) = (&events[0], &events[1]) else {
                panic!("{events:?}")
            };
            assert_eq!((m.initial_margin, m.maintenance_margin), (None, None));
            assert!(m.value.is_some(), "the others stay");
            assert_eq!((p.liquidation_price, p.delta), (None, None));
            assert!(p.gamma.is_some());
        }

        #[test]
        fn an_unbound_subaccount_has_no_rows_and_a_payload_without_aliases_is_refused() {
            let key = key();
            let one = [(fingerprint(&key, "1003"), Account::new("owner_s3").unwrap())].into();
            let composed = wallet_bytes(&key, &one, PORTFOLIOS);
            let rows = wallet_rows(&Venue::new(VENUE).unwrap(), 0, &composed).unwrap();
            assert!(
                rows.iter()
                    .all(|e| e.account().unwrap().as_str() == "owner_s3")
            );
            assert_eq!(rows.len(), 3);

            assert!(wallet_rows(&Venue::new(VENUE).unwrap(), 0, PORTFOLIOS).is_err());
        }
    }
}
