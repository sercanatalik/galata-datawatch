//! Hyperliquid's account answers: **pure**, bytes in and rows out.
//!
//! ```text
//!   snapshot   { dex, mode: { answer, recv_micros }, clearinghouseState }
//!                 ──▶ one `margin` row + one `positions` row per open position
//!   listing    subAccounts   ──▶ the sub-accounts, addresses held as secrets
//!   role       userRole      ──▶ user | subAccount | …
//!   mode       userAbstraction ──▶ AccountMode
//! ```
//!
//! **A snapshot is recorded as one payload holding two answers**, each byte for
//! byte. The margin row needs the account's mode, which a different call
//! states, read on the discovery cadence rather than every snapshot because it
//! weighs ten times as much (`design/measured.md`, 2026-09-25). A normaliser is
//! a pure function of the bytes (`crate::normalise`), so the mode the row was
//! stamped with must be *in* the bytes, with the moment it was heard — or
//! replay could not rebuild the row the live path wrote.
//!
//! Shapes, measured on mainnet on 2026-09-25: each dex answers
//! `clearinghouseState` with its own margin; a coin on a HIP-3 dex comes back
//! prefixed (`xyz:GOLD`); a position carries **no mark**; `subAccounts` is
//! `null`, not `[]`, when there are none.

use galata_wire::{
    Account, AccountMode, Envelope, Event, Margin, Num, Position, Ticker, Token, Venue,
    millis_to_micros,
};
use serde::Deserialize;
use serde_json::value::RawValue;

use crate::config::Secret;
use crate::normalise::{Normalise, NormaliseError};
use crate::record::{Payload, PayloadAddress};

/// The channel a composed snapshot is recorded under.
pub const SNAPSHOT_CHANNEL: &str = "clearinghouseState";
/// The channel a sub-account listing is recorded under.
pub const LISTING_CHANNEL: &str = "subAccounts";
/// The channel a role answer is recorded under.
pub const ROLE_CHANNEL: &str = "userRole";
/// The channel a mode answer is recorded under.
pub const MODE_CHANNEL: &str = "userAbstraction";

/// Compose a snapshot payload: the dex asked about, the most recent mode
/// answer and when it arrived, and the state answer — **both answers
/// verbatim**, embedded rather than re-encoded.
///
/// `None` for the mode where none has been heard yet; the row then says the
/// mode is unknown, which is what it is.
pub fn snapshot_bytes(dex: &str, mode: Option<(&[u8], i64)>, state: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(state.len() + 96);
    out.extend_from_slice(b"{\"dex\":");
    out.extend_from_slice(serde_json::to_string(dex).unwrap_or_default().as_bytes());
    out.extend_from_slice(b",\"mode\":");
    match mode {
        Some((answer, recv_micros)) => {
            out.extend_from_slice(b"{\"answer\":");
            out.extend_from_slice(answer);
            out.extend_from_slice(format!(",\"recv_micros\":{recv_micros}}}").as_bytes());
        }
        None => out.extend_from_slice(b"null"),
    }
    out.extend_from_slice(b",\"clearinghouseState\":");
    out.extend_from_slice(state);
    out.push(b'}');
    out
}

#[derive(Deserialize)]
struct Composed<'a> {
    dex: String,
    #[serde(borrow)]
    mode: Option<HeardMode<'a>>,
    #[serde(borrow, rename = "clearinghouseState")]
    state: &'a RawValue,
}

#[derive(Deserialize)]
struct HeardMode<'a> {
    #[serde(borrow)]
    answer: &'a RawValue,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct State {
    time: Option<i64>,
    margin_summary: Summary,
    cross_maintenance_margin_used: Option<Num>,
    withdrawable: Option<Num>,
    #[serde(default)]
    asset_positions: Vec<AssetPosition>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Summary {
    account_value: Option<Num>,
    total_ntl_pos: Option<Num>,
    total_raw_usd: Option<Num>,
    total_margin_used: Option<Num>,
}

#[derive(Deserialize)]
struct AssetPosition {
    position: Held,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Held {
    coin: String,
    szi: Num,
    entry_px: Option<Num>,
    position_value: Option<Num>,
    unrealized_pnl: Option<Num>,
    return_on_equity: Option<Num>,
    liquidation_px: Option<Num>,
    leverage: Option<Leverage>,
    max_leverage: Option<u32>,
    margin_used: Option<Num>,
    cum_funding: Option<CumFunding>,
}

#[derive(Deserialize)]
struct Leverage {
    #[serde(rename = "type")]
    kind: Option<String>,
    /// **A bare integer on the wire** (`"value": 25`), where every other
    /// figure here is a string — so a token, which takes either.
    value: Option<Token>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct CumFunding {
    all_time: Option<Num>,
    since_open: Option<Num>,
    since_change: Option<Num>,
}

/// The mode a `userAbstraction` answer states. Anything but a known string —
/// a failed read, `null`, a new word — is [`AccountMode::Unknown`].
pub fn mode_of(answer: &[u8]) -> AccountMode {
    serde_json::from_slice::<String>(answer)
        .map(|s| AccountMode::from_venue(&s))
        .unwrap_or(AccountMode::Unknown)
}

/// What a `userRole` answer says an address is.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Role {
    /// An ordinary account, which may have sub-accounts.
    User,
    /// A sub-account of some master.
    SubAccount,
    /// Anything else the venue says, verbatim.
    Other(String),
}

/// Read a `userRole` answer.
pub fn role_of(answer: &[u8]) -> Result<Role, NormaliseError> {
    #[derive(Deserialize)]
    struct Answer {
        role: String,
    }
    let answer: Answer =
        serde_json::from_slice(answer).map_err(|e| NormaliseError::Json(e.to_string()))?;
    Ok(match answer.role.as_str() {
        "user" => Role::User,
        "subAccount" => Role::SubAccount,
        other => Role::Other(other.to_string()),
    })
}

pub use crate::ledger::Listed;

/// Read a `subAccounts` answer. **`null` is no sub-accounts**, not a failure:
/// the venue answers `null` for a master with none (4 of 9 on 2026-09-25).
pub fn listing_of(answer: &[u8]) -> Result<Vec<Listed>, NormaliseError> {
    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct Entry {
        sub_account_user: String,
        name: Option<String>,
    }
    let entries: Option<Vec<Entry>> =
        serde_json::from_slice(answer).map_err(|e| NormaliseError::Json(e.to_string()))?;
    Ok(entries
        .unwrap_or_default()
        .into_iter()
        .map(|e| Listed {
            address: Secret::new(e.sub_account_user),
            name: e.name,
        })
        .collect())
}

/// The ticker a position's coin means: the dex prefix composed away, as market
/// data does it.
fn ticker_of(coin: &str) -> Result<Ticker, NormaliseError> {
    let bare = coin.split_once(':').map_or(coin, |(_, bare)| bare);
    Ok(Ticker::new(bare)?)
}

/// A snapshot payload to its rows.
///
/// The margin row always; a position row per open position. The margin row
/// carries the mode as it was last heard and, where the mode puts the
/// collateral in spot or is unknown, **why its account value is not equity**.
pub fn snapshot_rows(
    venue: &Venue,
    account: &Account,
    recv_micros: i64,
    bytes: &[u8],
) -> Result<Vec<Envelope>, NormaliseError> {
    let composed: Composed<'_> =
        serde_json::from_slice(bytes).map_err(|e| NormaliseError::Json(e.to_string()))?;
    let state: State =
        serde_json::from_str(composed.state.get()).map_err(|e| NormaliseError::Shape {
            kind: "clearinghouseState",
            detail: e.to_string(),
        })?;
    let mode = composed
        .mode
        .as_ref()
        .map_or(AccountMode::Unknown, |m| mode_of(m.answer.get().as_bytes()));
    let dex = (!composed.dex.is_empty()).then(|| composed.dex.clone());
    let at = state.time.map(millis_to_micros);

    let envelope =
        |event| Envelope::for_account(venue.clone(), account.clone(), at, recv_micros, event);

    let mut out = Vec::with_capacity(1 + state.asset_positions.len());
    out.push(envelope(Event::Margin(Margin {
        dex: dex.clone(),
        mode,
        equity_not_held: mode.equity_not_held().map(str::to_string),
        account_value: state.margin_summary.account_value,
        total_notional: state.margin_summary.total_ntl_pos,
        total_raw_usd: state.margin_summary.total_raw_usd,
        margin_used: state.margin_summary.total_margin_used,
        maintenance_margin_used: state.cross_maintenance_margin_used,
        withdrawable: state.withdrawable,
    })));
    for held in state.asset_positions.into_iter().map(|a| a.position) {
        let (leverage_type, leverage) = match held.leverage {
            Some(l) => (
                l.kind,
                l.value.map(|v| v.require("leverage.value")).transpose()?,
            ),
            None => (None, None),
        };
        let (all_time, since_open, since_change) =
            held.cum_funding.map_or((None, None, None), |f| {
                (f.all_time, f.since_open, f.since_change)
            });
        out.push(envelope(Event::Position(Position {
            dex: dex.clone(),
            ticker: ticker_of(&held.coin)?,
            size: held.szi,
            entry_price: held.entry_px,
            // The answer carries none (`design/datawatch/venues.md`, legacy,
            // and measured again 2026-09-25). Absent, never derived from
            // position value over size.
            mark: None,
            position_value: held.position_value,
            unrealised_pnl: held.unrealized_pnl,
            return_on_equity: held.return_on_equity,
            liquidation_price: held.liquidation_px,
            leverage,
            leverage_type,
            max_leverage: held.max_leverage,
            margin_used: held.margin_used,
            funding_all_time: all_time,
            funding_since_open: since_open,
            funding_since_change: since_change,
        })));
    }
    Ok(out)
}

/// The ledger's normaliser: snapshot payloads to rows; the listing, role and
/// mode answers to **none** — they are recorded, and what they mean is the
/// ledger's to act on (a binding, a refusal, a mode stamped on the next
/// snapshot), not a row of their own.
#[derive(Debug, Clone)]
pub struct LedgerNormaliser {
    venue: Venue,
    /// The fingerprint key, which decides which side of a transfer an account
    /// was on and names its counterparty. Configuration, like a symbol table:
    /// the normaliser is still a function of the bytes.
    key: Option<crate::ledger::FingerprintKey>,
}

impl LedgerNormaliser {
    /// For Hyperliquid, reading snapshots only.
    pub fn new() -> Result<LedgerNormaliser, galata_wire::TokenError> {
        Ok(LedgerNormaliser {
            venue: Venue::new(super::VENUE)?,
            key: None,
        })
    }

    /// Able to read ledger updates too, with the key their parties are
    /// fingerprinted under.
    pub fn with_key(mut self, key: crate::ledger::FingerprintKey) -> LedgerNormaliser {
        self.key = Some(key);
        self
    }
}

impl Normalise for LedgerNormaliser {
    fn normalise(&self, payload: &Payload) -> Result<Vec<Envelope>, NormaliseError> {
        match payload.channel.as_str() {
            SNAPSHOT_CHANNEL => {
                let PayloadAddress::Account(address) = &payload.address else {
                    return Err(NormaliseError::Shape {
                        kind: "clearinghouseState",
                        detail: "a snapshot not addressed to an account".into(),
                    });
                };
                let account = Account::new(address.account.as_str())?;
                snapshot_rows(&self.venue, &account, payload.recv_micros, &payload.payload)
            }
            LISTING_CHANNEL | ROLE_CHANNEL | MODE_CHANNEL => Ok(Vec::new()),
            super::events::FILLS_CHANNEL
            | super::events::FUNDING_CHANNEL
            | super::events::UPDATES_CHANNEL => {
                let PayloadAddress::Account(address) = &payload.address else {
                    return Err(NormaliseError::Shape {
                        kind: "events",
                        detail: "an events page not addressed to an account".into(),
                    });
                };
                let account = Account::new(address.account.as_str())?;
                let (venue, recv, bytes) = (&self.venue, payload.recv_micros, &payload.payload);
                match payload.channel.as_str() {
                    super::events::FILLS_CHANNEL => {
                        super::events::fill_rows(venue, &account, recv, bytes)
                    }
                    super::events::FUNDING_CHANNEL => {
                        super::events::funding_rows(venue, &account, recv, bytes)
                    }
                    _ => {
                        let key = self.key.as_ref().ok_or_else(|| NormaliseError::Shape {
                            kind: "userNonFundingLedgerUpdates",
                            detail: "no fingerprint key: this normaliser cannot tell the \
                                     parties of a transfer apart"
                                .into(),
                        })?;
                        super::events::update_rows(
                            venue,
                            &account,
                            key,
                            &address.fingerprint,
                            recv,
                            bytes,
                        )
                    }
                }
            }
            other => Err(NormaliseError::UnknownChannel(other.to_string())),
        }
    }

    fn venue(&self) -> &Venue {
        &self.venue
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    // The measured shape (2026-09-25), with invented figures.
    const STATE: &[u8] = br#"{"marginSummary":{"accountValue":"15322.02","totalNtlPos":"172112.0","totalRawUsd":"-156789.98","totalMarginUsed":"6884.48"},"crossMarginSummary":{"accountValue":"15322.02","totalNtlPos":"172112.0","totalRawUsd":"-156789.98","totalMarginUsed":"6884.48"},"crossMaintenanceMarginUsed":"3442.24","withdrawable":"8437.54","assetPositions":[{"type":"oneWay","position":{"coin":"xyz:GOLD","szi":"-40.0","leverage":{"type":"cross","value":25},"entryPx":"4301.5","positionValue":"172112.0","unrealizedPnl":"-58.0","returnOnEquity":"-0.0067","liquidationPx":null,"marginUsed":"6884.48","maxLeverage":25,"cumFunding":{"allTime":"1023.081078","sinceOpen":"1.062131","sinceChange":"0.0"}}}],"time":1790332213000}"#;
    const FLAT: &[u8] = br#"{"marginSummary":{"accountValue":"0.0","totalNtlPos":"0.0","totalRawUsd":"0.0","totalMarginUsed":"0.0"},"crossMarginSummary":{"accountValue":"0.0","totalNtlPos":"0.0","totalRawUsd":"0.0","totalMarginUsed":"0.0"},"crossMaintenanceMarginUsed":"0.0","withdrawable":"0.0","assetPositions":[],"time":1790332213000}"#;

    fn rows(dex: &str, mode: Option<&[u8]>, state: &[u8]) -> Vec<Envelope> {
        snapshot_rows(
            &Venue::new("hyperliquid").unwrap(),
            &Account::new("main").unwrap(),
            7,
            &snapshot_bytes(dex, mode.map(|m| (m, 5)), state),
        )
        .unwrap()
    }

    fn margin(rows: &[Envelope]) -> &Margin {
        match &rows[0].event {
            Event::Margin(m) => m,
            other => panic!("not a margin row: {other:?}"),
        }
    }

    #[test]
    fn a_composed_snapshot_keeps_each_answer_verbatim() {
        let bytes = snapshot_bytes("xyz", Some((br#""disabled""#, 5)), STATE);
        let text = std::str::from_utf8(&bytes).unwrap();
        assert!(
            text.contains(std::str::from_utf8(STATE).unwrap()),
            "the state answer, byte for byte"
        );
        assert!(text.contains(r#""answer":"disabled","recv_micros":5"#));
        serde_json::from_slice::<serde_json::Value>(&bytes).expect("one JSON document");
    }

    #[test]
    fn a_snapshot_gives_its_margin_and_each_position() {
        let rows = rows("xyz", Some(br#""disabled""#), STATE);
        assert_eq!(rows.len(), 2);
        let m = margin(&rows);
        assert_eq!(m.dex.as_deref(), Some("xyz"));
        assert_eq!(m.mode, AccountMode::Disabled);
        assert_eq!(
            m.equity_not_held, None,
            "a disabled account's perp value is its own"
        );
        assert_eq!(m.account_value, Some(Num::from_str("15322.02").unwrap()));
        let Event::Position(p) = &rows[1].event else {
            panic!()
        };
        assert_eq!(p.ticker.as_str(), "GOLD", "the dex prefix is composed away");
        assert_eq!(p.dex.as_deref(), Some("xyz"), "and kept beside the ticker");
        assert_eq!(p.size, Num::from_str("-40.0").unwrap());
        assert_eq!(p.liquidation_price, None, "null stays absent");
        assert_eq!(rows[0].at_micros, Some(1_790_332_213_000_000));
        assert_eq!(rows[0].account().unwrap().as_str(), "main");
    }

    #[test]
    fn an_absent_mark_stays_absent() {
        let rows = rows("", Some(br#""disabled""#), STATE);
        let Event::Position(p) = &rows[1].event else {
            panic!()
        };
        assert_eq!(p.mark, None, "not position value over size");
        assert!(
            p.position_value.is_some(),
            "though both parts of that division were there"
        );
    }

    #[test]
    fn a_flat_account_writes_margin_and_no_positions() {
        let rows = rows("", Some(br#""disabled""#), FLAT);
        assert_eq!(rows.len(), 1);
        assert_eq!(margin(&rows).dex, None, "\"\" is the main dex");
    }

    #[test]
    fn a_flat_unified_account_is_not_reported_as_empty() {
        let rows = rows("", Some(br#""unifiedAccount""#), FLAT);
        let m = margin(&rows);
        assert_eq!(m.mode, AccountMode::Unified);
        assert_eq!(
            m.account_value,
            Some(Num::from_str("0.0").unwrap()),
            "carried as stated"
        );
        assert!(
            m.equity_not_held
                .as_deref()
                .unwrap()
                .contains("collateral is spot"),
            "{:?}",
            m.equity_not_held
        );
    }

    #[test]
    fn portfolio_margin_is_not_reported_as_equity_either() {
        let m = rows("", Some(br#""portfolioMargin""#), STATE);
        assert!(
            margin(&m)
                .equity_not_held
                .as_deref()
                .unwrap()
                .contains("portfolioMargin")
        );
    }

    #[test]
    fn an_unrecognised_mode_is_recorded_as_unknown() {
        for mode in [Some(&br#""somethingNew""#[..]), Some(&b"null"[..]), None] {
            let rows = rows("", mode, STATE);
            let m = margin(&rows);
            assert_eq!(m.mode, AccountMode::Unknown, "{mode:?}");
            assert!(m.equity_not_held.is_some(), "{mode:?}");
        }
    }

    #[test]
    fn a_null_listing_is_no_sub_accounts() {
        assert!(listing_of(b"null").unwrap().is_empty());
        let listed = listing_of(
            br#"[{"name":"arb","master":"0x01","subAccountUser":"0x3f9aa0c1d2e3f4a5b6c7d8e9f0a1b2c3d4e5f6a7","clearinghouseState":{},"spotState":{}}]"#,
        )
        .unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].name.as_deref(), Some("arb"));
        assert!(
            !format!("{listed:?}").contains("3f9a"),
            "the listing's addresses stay held"
        );
    }

    #[test]
    fn a_role_is_read_as_the_venue_states_it() {
        assert_eq!(role_of(br#"{"role":"user"}"#).unwrap(), Role::User);
        assert_eq!(
            role_of(br#"{"role":"subAccount","data":{"master":"0x01"}}"#).unwrap(),
            Role::SubAccount
        );
        assert_eq!(
            role_of(br#"{"role":"vault"}"#).unwrap(),
            Role::Other("vault".into())
        );
    }

    #[test]
    fn the_listing_and_role_answers_are_recorded_and_make_no_rows() {
        let normaliser = LedgerNormaliser::new().unwrap();
        for channel in [LISTING_CHANNEL, ROLE_CHANNEL, MODE_CHANNEL] {
            let payload = Payload {
                seq: 1,
                recv_micros: 1,
                address: PayloadAddress::Venue("hyperliquid".into()),
                channel: channel.into(),
                kind: "accounts".into(),
                symbol: None,
                origin: galata_wire::Origin::Fetched,
                payload: b"null".to_vec(),
            };
            assert!(
                normaliser.normalise(&payload).unwrap().is_empty(),
                "{channel}"
            );
        }
    }
}
