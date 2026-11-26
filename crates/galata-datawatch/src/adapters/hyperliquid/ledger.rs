//! Hyperliquid's account answers: **pure**, bytes in and rows out.
//!
//! ```text
//!   snapshot   { dex, mode: { answer, recv_micros }, clearinghouseState }
//!                 ──▶ one `margin` row + one `positions` row per open position
//!   listing    subAccounts   ──▶ the sub-accounts, addresses held as secrets
//!   role       userRole      ──▶ user | subAccount | …
//!   mode       userAbstraction ──▶ AccountMode
//!   balances   { names, spotClearinghouseState }
//!                 ──▶ a `balances` row per coin, and where stated the
//!                     `portfolio_margin` row and a `token_margin` row per token
//!   borrow     { names, borrowLendUserState }
//!                 ──▶ a `borrow_lend` row per token + one `lend_health` row
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
/// The channel a composed spot-state answer is recorded under.
pub const BALANCES_CHANNEL: &str = "spotClearinghouseState";
/// The channel a composed borrow and lend answer is recorded under.
pub const BORROW_LEND_CHANNEL: &str = "borrowLendUserState";

/// The token indices an account-wide answer refers to: a spot answer's
/// balances and its three per-token lists, a borrow and lend answer's
/// `tokenToState`. Read from the answer as JSON, so a field the venue adds or
/// drops costs a name, never the answer.
fn referenced_indices(answer: &[u8]) -> std::collections::BTreeSet<u32> {
    let mut out = std::collections::BTreeSet::new();
    let Ok(value) = serde_json::from_slice::<serde_json::Value>(answer) else {
        return out;
    };
    let index = |v: &serde_json::Value| v.as_u64().and_then(|i| u32::try_from(i).ok());
    if let Some(balances) = value.get("balances").and_then(|b| b.as_array()) {
        out.extend(
            balances
                .iter()
                .filter_map(|b| b.get("token").and_then(index)),
        );
    }
    for list in [
        "tokenToState",
        "tokenToPortfolioBorrowRatio",
        "tokenToPortfolioSupplyRatio",
        "tokenToAvailableAfterMaintenance",
    ] {
        if let Some(pairs) = value.get(list).and_then(|l| l.as_array()) {
            out.extend(pairs.iter().filter_map(|p| p.get(0).and_then(index)));
        }
    }
    out
}

/// The names `spotMeta` gives the token indices an answer refers to. `None`
/// where no `spotMeta` has been read, or it would not parse; an index it does
/// not name is simply missing from the map.
pub fn names_for(
    reference: Option<&[u8]>,
    answer: &[u8],
) -> Option<std::collections::BTreeMap<u32, String>> {
    #[derive(Deserialize)]
    struct Meta {
        tokens: Vec<MetaToken>,
    }
    #[derive(Deserialize)]
    struct MetaToken {
        index: u32,
        name: String,
    }
    let meta: Meta = serde_json::from_slice(reference?).ok()?;
    let wanted = referenced_indices(answer);
    Some(
        meta.tokens
            .into_iter()
            .filter(|t| wanted.contains(&t.index))
            .map(|t| (t.index, t.name))
            .collect(),
    )
}

/// Compose an account-wide payload: the names its token indices had in the
/// `spotMeta` last read, and the answer **verbatim** under `key`.
///
/// The names travel with the answer for the reason the mode travels with a
/// snapshot: a normaliser is a function of the bytes, so what named a token
/// when the row was written must be in the record, or a rebuild could not name
/// it again.
pub fn wide_bytes(
    key: &str,
    names: Option<&std::collections::BTreeMap<u32, String>>,
    answer: &[u8],
) -> Vec<u8> {
    let mut out = Vec::with_capacity(answer.len() + 64);
    out.extend_from_slice(b"{\"names\":");
    match names {
        Some(names) => {
            out.extend_from_slice(serde_json::to_string(names).unwrap_or_default().as_bytes())
        }
        None => out.extend_from_slice(b"null"),
    }
    out.extend_from_slice(b",");
    out.extend_from_slice(serde_json::to_string(key).unwrap_or_default().as_bytes());
    out.push(b':');
    out.extend_from_slice(answer);
    out.push(b'}');
    out
}

#[derive(Deserialize)]
struct WideComposed<'a> {
    names: Option<std::collections::BTreeMap<u32, String>>,
    #[serde(borrow, rename = "spotClearinghouseState")]
    spot: Option<&'a RawValue>,
    #[serde(borrow, rename = "borrowLendUserState")]
    borrow_lend: Option<&'a RawValue>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Spot {
    #[serde(default)]
    balances: Vec<SpotBalance>,
    portfolio_margin_enabled: Option<bool>,
    portfolio_margin_ratio: Option<Num>,
    token_to_portfolio_borrow_ratio: Option<Vec<(u32, Num)>>,
    token_to_portfolio_supply_ratio: Option<Vec<(u32, Num)>>,
    token_to_available_after_maintenance: Option<Vec<(u32, Num)>>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct SpotBalance {
    coin: String,
    token: Option<u32>,
    total: Num,
    hold: Option<Num>,
    entry_ntl: Option<Num>,
    spot_hold: Option<Num>,
    ltv: Option<Num>,
    supplied: Option<Num>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct BorrowLendState {
    #[serde(default)]
    token_to_state: Vec<(u32, TokenState)>,
    health: Option<String>,
    health_factor: Option<Num>,
}

#[derive(Deserialize)]
struct TokenState {
    borrow: Option<BasisValue>,
    supply: Option<BasisValue>,
}

#[derive(Deserialize)]
struct BasisValue {
    basis: Option<Num>,
    value: Option<Num>,
}

/// A composed spot-state payload to its rows: a `balances` row per coin; where
/// the venue states them, the account's portfolio-margin row and a
/// `token_margin` row per token named in any of the three lists. **Every
/// figure as stated**, a negative hold included; nothing is computed.
pub fn balance_rows(
    venue: &Venue,
    account: &Account,
    recv_micros: i64,
    bytes: &[u8],
) -> Result<Vec<Envelope>, NormaliseError> {
    let composed: WideComposed<'_> =
        serde_json::from_slice(bytes).map_err(|e| NormaliseError::Json(e.to_string()))?;
    let raw = composed.spot.ok_or_else(|| NormaliseError::Shape {
        kind: BALANCES_CHANNEL,
        detail: "no spot state in the payload".into(),
    })?;
    let spot: Spot = serde_json::from_str(raw.get()).map_err(|e| NormaliseError::Shape {
        kind: BALANCES_CHANNEL,
        detail: e.to_string(),
    })?;
    let names = composed.names.unwrap_or_default();
    // The answer states no time of its own: the row's time is its receipt.
    let envelope =
        |event| Envelope::for_account(venue.clone(), account.clone(), None, recv_micros, event);
    let mut out = Vec::with_capacity(spot.balances.len() + 2);
    for b in spot.balances {
        out.push(envelope(Event::Balance(galata_wire::Balance {
            asset: b.coin,
            token_index: b.token,
            total: b.total,
            hold: b.hold,
            entry_notional: b.entry_ntl,
            spot_hold: b.spot_hold,
            ltv: b.ltv,
            supplied: b.supplied,
        })));
    }
    if spot.portfolio_margin_enabled.is_some() || spot.portfolio_margin_ratio.is_some() {
        out.push(envelope(Event::PortfolioMargin(
            galata_wire::PortfolioMargin {
                enabled: spot.portfolio_margin_enabled,
                ratio: spot.portfolio_margin_ratio,
            },
        )));
    }
    // One row per token named in any of the three lists, each figure from its
    // own list and absent where that list does not name the token.
    fn token<'m>(
        tokens: &'m mut std::collections::BTreeMap<u32, galata_wire::TokenMargin>,
        names: &std::collections::BTreeMap<u32, String>,
        index: u32,
    ) -> &'m mut galata_wire::TokenMargin {
        tokens
            .entry(index)
            .or_insert_with(|| galata_wire::TokenMargin {
                token_index: index,
                token: names.get(&index).cloned(),
                borrow_ratio: None,
                supply_ratio: None,
                available_after_maintenance: None,
            })
    }
    let mut tokens = std::collections::BTreeMap::new();
    for (index, ratio) in spot.token_to_portfolio_borrow_ratio.unwrap_or_default() {
        token(&mut tokens, &names, index).borrow_ratio = Some(ratio);
    }
    for (index, ratio) in spot.token_to_portfolio_supply_ratio.unwrap_or_default() {
        token(&mut tokens, &names, index).supply_ratio = Some(ratio);
    }
    for (index, amount) in spot
        .token_to_available_after_maintenance
        .unwrap_or_default()
    {
        token(&mut tokens, &names, index).available_after_maintenance = Some(amount);
    }
    out.extend(
        tokens
            .into_values()
            .map(|t| envelope(Event::TokenMargin(t))),
    );
    Ok(out)
}

/// A composed borrow and lend payload to its rows: a `borrow_lend` row per
/// token, and **always** the account's `lend_health` row, since an account
/// that borrows and supplies nothing still has a health.
pub fn borrow_lend_rows(
    venue: &Venue,
    account: &Account,
    recv_micros: i64,
    bytes: &[u8],
) -> Result<Vec<Envelope>, NormaliseError> {
    let composed: WideComposed<'_> =
        serde_json::from_slice(bytes).map_err(|e| NormaliseError::Json(e.to_string()))?;
    let raw = composed.borrow_lend.ok_or_else(|| NormaliseError::Shape {
        kind: BORROW_LEND_CHANNEL,
        detail: "no borrow and lend state in the payload".into(),
    })?;
    let state: BorrowLendState =
        serde_json::from_str(raw.get()).map_err(|e| NormaliseError::Shape {
            kind: BORROW_LEND_CHANNEL,
            detail: e.to_string(),
        })?;
    let names = composed.names.unwrap_or_default();
    let envelope =
        |event| Envelope::for_account(venue.clone(), account.clone(), None, recv_micros, event);
    let mut out = Vec::with_capacity(state.token_to_state.len() + 1);
    for (index, s) in state.token_to_state {
        let (borrow_basis, borrow_value) = s.borrow.map_or((None, None), |b| (b.basis, b.value));
        let (supply_basis, supply_value) = s.supply.map_or((None, None), |b| (b.basis, b.value));
        out.push(envelope(Event::BorrowLend(galata_wire::BorrowLend {
            token_index: index,
            token: names.get(&index).cloned(),
            borrow_basis,
            borrow_value,
            supply_basis,
            supply_value,
        })));
    }
    out.push(envelope(Event::LendHealth(galata_wire::LendHealth {
        health: state.health,
        health_factor: state.health_factor,
    })));
    Ok(out)
}

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
            BALANCES_CHANNEL | BORROW_LEND_CHANNEL => {
                let PayloadAddress::Account(address) = &payload.address else {
                    return Err(NormaliseError::Shape {
                        kind: "account-wide state",
                        detail: "an answer not addressed to an account".into(),
                    });
                };
                let account = Account::new(address.account.as_str())?;
                let (venue, recv, bytes) = (&self.venue, payload.recv_micros, &payload.payload);
                if payload.channel == BALANCES_CHANNEL {
                    balance_rows(venue, &account, recv, bytes)
                } else {
                    borrow_lend_rows(venue, &account, recv, bytes)
                }
            }
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
        // The account value is still not the equity, and the row says where
        // the collateral is: the balances recorded beside it.
        let m = rows("", Some(br#""portfolioMargin""#), STATE);
        let said = margin(&m).equity_not_held.clone().unwrap();
        assert!(said.contains("portfolioMargin"), "{said}");
        assert!(said.contains("recorded as balances"), "{said}");
        assert!(!said.contains("out of scope"), "{said}");
        // A standard account's margin is its own, and says nothing.
        let d = rows("", Some(br#""default""#), STATE);
        assert_eq!(margin(&d).equity_not_held, None);
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

    use std::collections::BTreeMap;

    // The answers read on 2026-11-26 from two public accounts
    // (`design/measured.md`), saved by add-hyperliquid-portfolio-margin-state.
    const PM_SPOT: &[u8] =
        include_bytes!("../../../tests/fixtures/hyperliquid/ledger-spot-portfolio-margin.json");
    const UNIFIED_SPOT: &[u8] =
        include_bytes!("../../../tests/fixtures/hyperliquid/ledger-spot-unified.json");
    const PM_LEND: &[u8] =
        include_bytes!("../../../tests/fixtures/hyperliquid/ledger-borrow-lend.json");
    const EMPTY_LEND: &[u8] =
        include_bytes!("../../../tests/fixtures/hyperliquid/ledger-borrow-lend-empty.json");
    const META: &[u8] = include_bytes!("../../../tests/fixtures/hyperliquid/ledger-spot-meta.json");

    fn wide(channel: &str, reference: Option<&[u8]>, answer: &[u8]) -> Vec<Envelope> {
        let bytes = wide_bytes(channel, names_for(reference, answer).as_ref(), answer);
        let venue = Venue::new("hyperliquid").unwrap();
        let account = Account::new("main").unwrap();
        if channel == BALANCES_CHANNEL {
            balance_rows(&venue, &account, 7, &bytes).unwrap()
        } else {
            borrow_lend_rows(&venue, &account, 7, &bytes).unwrap()
        }
    }

    fn d(s: &str) -> Option<Num> {
        Some(Num::from_str(s).unwrap())
    }

    #[test]
    fn a_portfolio_margin_spot_answer_is_carried_digit_for_digit() {
        let rows = wide(BALANCES_CHANNEL, Some(META), PM_SPOT);
        let balances: Vec<&galata_wire::Balance> = rows
            .iter()
            .filter_map(|e| match &e.event {
                Event::Balance(b) => Some(b),
                _ => None,
            })
            .collect();
        assert_eq!(balances.len(), 8);
        let usdc = balances.iter().find(|b| b.asset == "USDC").unwrap();
        assert_eq!(usdc.token_index, Some(0));
        assert_eq!(Some(usdc.total), d("185390.10278742"));
        assert_eq!(
            usdc.hold,
            d("-366701.7986019201"),
            "a negative hold, as stated"
        );
        assert_eq!(usdc.entry_notional, d("0.0"));
        assert_eq!(usdc.spot_hold, d("44280.81087107"));
        assert_eq!(usdc.ltv, d("0.0"));
        assert_eq!(usdc.supplied, d("141087.89598842"));
        // A coin that is not collateral carries none of the three.
        let wow = balances.iter().find(|b| b.asset == "WOW").unwrap();
        assert_eq!((wow.spot_hold, wow.ltv, wow.supplied), (None, None, None));
        assert_eq!(wow.entry_notional, d("3.44772284"));

        let pm: Vec<&galata_wire::PortfolioMargin> = rows
            .iter()
            .filter_map(|e| match &e.event {
                Event::PortfolioMargin(p) => Some(p),
                _ => None,
            })
            .collect();
        assert_eq!(pm.len(), 1);
        assert_eq!(pm[0].enabled, Some(true));
        assert_eq!(pm[0].ratio, d("0.0229134385"), "the venue's own ratio");

        let tokens: BTreeMap<u32, &galata_wire::TokenMargin> = rows
            .iter()
            .filter_map(|e| match &e.event {
                Event::TokenMargin(t) => Some((t.token_index, t)),
                _ => None,
            })
            .collect();
        // Every index any of the three lists names: 0, 150, 197, 268, 360.
        assert_eq!(
            tokens.keys().copied().collect::<Vec<_>>(),
            vec![0, 150, 197, 268, 360]
        );
        let hype = tokens[&150];
        assert_eq!(hype.token.as_deref(), Some("HYPE"));
        assert_eq!(hype.supply_ratio, d("0.0103768652"));
        assert_eq!(hype.borrow_ratio, None, "its borrow list does not name it");
        assert_eq!(hype.available_after_maintenance, d("10376.86515991"));
        assert_eq!(tokens[&0].borrow_ratio, d("0.0"));
        assert_eq!(tokens[&0].available_after_maintenance, d("690202.43559317"));
        // The answer states no time: every row's is its receipt.
        assert!(
            rows.iter()
                .all(|e| e.at_micros.is_none() && e.recv_micros == 7)
        );
    }

    #[test]
    fn a_unified_answer_gives_token_rows_and_no_portfolio_margin_row() {
        let rows = wide(BALANCES_CHANNEL, Some(META), UNIFIED_SPOT);
        assert!(
            !rows
                .iter()
                .any(|e| matches!(e.event, Event::PortfolioMargin(_)))
        );
        let tokens: Vec<&galata_wire::TokenMargin> = rows
            .iter()
            .filter_map(|e| match &e.event {
                Event::TokenMargin(t) => Some(t),
                _ => None,
            })
            .collect();
        assert!(!tokens.is_empty());
        for t in tokens {
            assert!(t.available_after_maintenance.is_some());
            assert_eq!((t.borrow_ratio, t.supply_ratio), (None, None));
        }
        assert_eq!(
            rows.iter()
                .filter(|e| matches!(e.event, Event::Balance(_)))
                .count(),
            2
        );
    }

    #[test]
    fn an_empty_balance_list_is_no_row() {
        let rows = wide(BALANCES_CHANNEL, Some(META), br#"{"balances":[]}"#);
        assert!(rows.is_empty(), "{rows:?}");
    }

    #[test]
    fn a_borrow_lend_answer_is_carried_as_stated() {
        let rows = wide(BORROW_LEND_CHANNEL, Some(META), PM_LEND);
        let lends: Vec<&galata_wire::BorrowLend> = rows
            .iter()
            .filter_map(|e| match &e.event {
                Event::BorrowLend(b) => Some(b),
                _ => None,
            })
            .collect();
        assert_eq!(lends.len(), 3);
        let usdc = lends.iter().find(|b| b.token_index == 0).unwrap();
        assert_eq!(usdc.token.as_deref(), Some("USDC"));
        assert_eq!(usdc.borrow_basis, d("0.0"));
        assert_eq!(usdc.supply_basis, d("141087.89571312"));
        assert_eq!(usdc.supply_value, d("141087.89608165"));
        let health: Vec<&galata_wire::LendHealth> = rows
            .iter()
            .filter_map(|e| match &e.event {
                Event::LendHealth(h) => Some(h),
                _ => None,
            })
            .collect();
        assert_eq!(health.len(), 1);
        assert_eq!(health[0].health.as_deref(), Some("healthy"));
        assert_eq!(health[0].health_factor, None, "null is absent, not zero");
    }

    #[test]
    fn an_account_that_lends_nothing_still_has_a_health() {
        let rows = wide(BORROW_LEND_CHANNEL, Some(META), EMPTY_LEND);
        assert_eq!(rows.len(), 1);
        assert!(matches!(rows[0].event, Event::LendHealth(_)));
    }

    #[test]
    fn a_name_is_absent_where_no_reference_names_it() {
        // No reference read yet: every name absent, every figure kept.
        let rows = wide(BORROW_LEND_CHANNEL, None, PM_LEND);
        for e in &rows {
            if let Event::BorrowLend(b) = &e.event {
                assert_eq!(b.token, None);
                assert!(b.supply_value.is_some());
            }
        }
        // A reference that does not name an index leaves that one nameless.
        let partial = br#"{"tokens":[{"index":0,"name":"USDC"}],"universe":[]}"#;
        let rows = wide(BORROW_LEND_CHANNEL, Some(partial), PM_LEND);
        let named: BTreeMap<u32, Option<String>> = rows
            .iter()
            .filter_map(|e| match &e.event {
                Event::BorrowLend(b) => Some((b.token_index, b.token.clone())),
                _ => None,
            })
            .collect();
        assert_eq!(named[&0].as_deref(), Some("USDC"));
        assert_eq!(named[&150], None);
    }

    #[test]
    fn the_names_travel_in_the_record_with_only_the_indices_used() {
        let bytes = wide_bytes(
            BORROW_LEND_CHANNEL,
            names_for(Some(META), PM_LEND).as_ref(),
            PM_LEND,
        );
        let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        let names = value["names"].as_object().unwrap();
        let mut keys: Vec<&str> = names.keys().map(String::as_str).collect();
        keys.sort();
        assert_eq!(keys, vec!["0", "150", "268"], "the three tokens it lends");
        // The answer itself is carried verbatim.
        assert_eq!(
            value["borrowLendUserState"],
            serde_json::from_slice::<serde_json::Value>(PM_LEND).unwrap()
        );
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
