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
//! Each subaccount's **open orders ride the same answer** (`open_orders`), so
//! they cost no request and a missed snapshot is a gap in them too: design D4
//! of `add-account-vocabulary-orders`.
//!
//! # A wallet's history
//!
//! Its orders, trades, funding and value history are each read wallet-wide,
//! **a whole window at a time**: every page of the window, composed in one
//! payload beside the aliases, as a snapshot is. A window with a page that did
//! not answer is not recorded at all, so a cursor read from the record never
//! passes a page nobody read.
//!
//! ```text
//!   {"aliases": {…}, "answer": {"period": <s, value history only>, "pages": [<answer>, …]}}
//! ```
//!
//! **Every figure is read leniently**: a figure that is absent, null or not a
//! decimal is absent from its row, so a field the venue renames costs that
//! figure and not the snapshot. The venue's v3 API had renamed fields within
//! seven weeks of its release (design, Risks).

use std::collections::BTreeMap;

use galata_wire::{
    Account, Collateral, Envelope, Event, Fill, FundingPayment, MarginState, Num, Order, PnlPoint,
    PositionState, Side, Ticker, Venue, millis_to_micros,
};
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
/// The channel a window of a wallet's orders is recorded under.
pub const ORDER_HISTORY_CHANNEL: &str = "private/get_order_history";
/// The channel a window of a wallet's trades is recorded under.
pub const TRADE_HISTORY_CHANNEL: &str = "private/get_trade_history";
/// The channel a window of a wallet's funding is recorded under.
pub const FUNDING_HISTORY_CHANNEL: &str = "private/get_funding_history";
/// The channel a window of a wallet's value history is recorded under.
pub const VALUE_HISTORY_CHANNEL: &str = "private/get_subaccount_value_history";

/// The streamed channels (`add-realtime-account-ledger`, design D2), each
/// subaccount's, after a login signed with the ledger's key. Their shapes are
/// Derive's documented ones (`docs.derive.xyz/api-reference/channels`, read
/// 2026-11-28): no frame of them arrived in ten minutes of a quiet account.
/// A subaccount's orders, each whole, at a change.
pub const WS_ORDERS: &str = "ws/orders";
/// A subaccount's trades, at a fill.
pub const WS_TRADES: &str = "ws/trades";
/// A subaccount's balance changes, `{name, new_balance, previous_balance,
/// update_type}`: **not its state**, which the wallet's snapshot states
/// whole, so a frame of them asks for one at once.
pub const WS_BALANCES: &str = "ws/balances";

/// A streamed frame as recorded: the frame verbatim, beside the alias its
/// channel's subaccount is bound to, as a wallet's answers are.
pub fn stream_bytes(subaccount_id: &str, alias: &Account, frame: &[u8]) -> Vec<u8> {
    let mut named = serde_json::Map::new();
    named.insert(subaccount_id.to_string(), Value::String(alias.to_string()));
    let mut out = Vec::with_capacity(frame.len() + 64);
    out.extend_from_slice(b"{\"aliases\":");
    out.extend_from_slice(Value::Object(named).to_string().as_bytes());
    out.extend_from_slice(b",\"answer\":");
    out.extend_from_slice(frame);
    out.push(b'}');
    out
}

/// The rows of a recorded streamed frame, bound to the subaccount's alias: an
/// order as it stood at its latest change, at that change's time; a trade as
/// a fill; a balance change, none.
pub fn stream_rows(
    venue: &Venue,
    channel: &str,
    recv_micros: i64,
    bytes: &[u8],
) -> Result<Vec<Envelope>, NormaliseError> {
    let composed: Value =
        serde_json::from_slice(bytes).map_err(|e| NormaliseError::Json(e.to_string()))?;
    let alias = composed["aliases"]
        .as_object()
        .and_then(|a| a.values().next())
        .and_then(Value::as_str)
        .ok_or_else(|| NormaliseError::Shape {
            kind: "stream",
            detail: "a frame without its alias".into(),
        })?;
    let account = Account::new(alias)?;
    let rows = composed["answer"]["params"]["data"]
        .as_array()
        .map_or(&[][..], Vec::as_slice);
    let at =
        |row: &Value, field: &str| row.get(field).and_then(Value::as_i64).map(millis_to_micros);
    let envelope = |at: Option<i64>, event: Event| {
        Envelope::for_account(venue.clone(), account.clone(), at, recv_micros, event)
    };
    Ok(match channel {
        WS_ORDERS => rows
            .iter()
            .filter_map(|r| {
                order_of(r).map(|o| envelope(at(r, "last_update_timestamp"), Event::OrderUpdate(o)))
            })
            .collect(),
        WS_TRADES => rows
            .iter()
            .filter_map(|r| trade_of(r).map(|f| envelope(at(r, "timestamp"), Event::Fill(f))))
            .collect(),
        WS_BALANCES => Vec::new(),
        other => return Err(NormaliseError::UnknownChannel(other.to_string())),
    })
}

/// What a streamed frame is: a subaccount's channel, by the id its channel
/// names, or an answer to one of the connection's own requests.
#[cfg(feature = "capture")]
pub fn streamed(frame: &[u8]) -> crate::ledger::run::StreamFrame {
    use crate::ledger::run::{StreamFrame, Then};
    use galata_wire::Kind;
    let Ok(v) = serde_json::from_slice::<Value>(frame) else {
        return StreamFrame::Other;
    };
    if v.get("method").and_then(Value::as_str) != Some("subscription") {
        // An answer to the login, a subscription or a keepalive.
        return if v.get("id").is_some() {
            StreamFrame::Control
        } else {
            StreamFrame::Other
        };
    }
    let Some((id, name)) = v["params"]["channel"]
        .as_str()
        .and_then(|c| c.split_once('.'))
    else {
        return StreamFrame::Other;
    };
    if id.is_empty() || !id.bytes().all(|b| b.is_ascii_digit()) {
        return StreamFrame::Other;
    }
    let (channel, kind) = match name {
        "orders" => (WS_ORDERS, Kind::OrderUpdates),
        "trades" => (WS_TRADES, Kind::Fills),
        "balances" => (WS_BALANCES, Kind::MarginState),
        _ => return StreamFrame::Other,
    };
    StreamFrame::Account {
        channel,
        kind,
        address: crate::config::Secret::new(id),
        // Every change on a subaccount moves its state: the snapshot, at once.
        then: Then::Snapshot,
    }
}

/// The list each history method's answer holds its rows under (measured
/// 2026-11-27).
fn history_list(channel: &str) -> Option<&'static str> {
    Some(match channel {
        ORDER_HISTORY_CHANNEL => "orders",
        TRADE_HISTORY_CHANNEL => "trades",
        FUNDING_HISTORY_CHANNEL => "events",
        VALUE_HISTORY_CHANNEL => "subaccount_value_history",
        _ => return None,
    })
}

/// A window of a wallet's history, every page verbatim, and the period a
/// value history was asked at, which its answer does not state.
pub fn history_window(period_secs: Option<i64>, pages: &[Vec<u8>]) -> Vec<u8> {
    let mut out = Vec::with_capacity(pages.iter().map(|p| p.len() + 1).sum::<usize>() + 32);
    out.extend_from_slice(b"{\"period\":");
    match period_secs {
        Some(p) => out.extend_from_slice(p.to_string().as_bytes()),
        None => out.extend_from_slice(b"null"),
    }
    out.extend_from_slice(b",\"pages\":[");
    for (i, page) in pages.iter().enumerate() {
        if i > 0 {
            out.push(b',');
        }
        out.extend_from_slice(page);
    }
    out.extend_from_slice(b"]}");
    out
}

/// How many pages a history answer says its window holds; one where it says
/// nothing.
pub fn num_pages(answer: &[u8]) -> u32 {
    serde_json::from_slice::<Value>(answer)
        .ok()
        .and_then(|v| v["result"]["pagination"]["num_pages"].as_u64())
        .map_or(1, |n| u32::try_from(n).unwrap_or(u32::MAX).max(1))
}

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
        // Every subaccount the answer names, wherever: a portfolio, or a row
        // of a history window.
        let mut ids = std::collections::BTreeSet::new();
        subaccount_ids(&parsed, &mut ids);
        for id in ids {
            if let Some(alias) = aliases.get(&fingerprint(key, &id)) {
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

/// Every `subaccount_id` an answer holds, at any depth.
fn subaccount_ids(v: &Value, out: &mut std::collections::BTreeSet<String>) {
    match v {
        Value::Object(map) => {
            for (k, x) in map {
                if k == "subaccount_id"
                    && let Some(id) = identifier(x)
                {
                    out.insert(id);
                }
                subaccount_ids(x, out);
            }
        }
        Value::Array(items) => items.iter().for_each(|x| subaccount_ids(x, out)),
        _ => {}
    }
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

/// One of Derive's orders, as its open orders and its order history state
/// them alike (measured 2026-11-27), **each field as stated** but one: the
/// size remaining is `amount` less `filled_amount`, both stated (design D2).
/// The order's signer and signature are not carried: the signer is a session
/// key's address. `None` for a row naming no order id, market or side, which
/// no order lacks.
pub(crate) fn order_of(o: &Value) -> Option<Order> {
    let side = match o.get("direction").and_then(Value::as_str)? {
        "buy" => Side::Bid,
        "sell" => Side::Ask,
        _ => return None,
    };
    let amount = num(o.get("amount"));
    let filled = num(o.get("filled_amount"));
    Some(Order {
        dex: None,
        market: text(o.get("instrument_name"))?,
        side,
        order_id: text(o.get("order_id"))?,
        client_id: text(o.get("label")).filter(|l| !l.is_empty()),
        order_type: text(o.get("order_type")).unwrap_or_default(),
        trigger_type: text(o.get("trigger_type")),
        is_trigger: None,
        position_tpsl: None,
        size: amount,
        remaining: amount.zip(filled).map(|(a, f)| a - f),
        limit_price: num(o.get("limit_price")),
        trigger_price: num(o.get("trigger_price")),
        trigger_condition: text(o.get("trigger_price_type")),
        reduce_only: o.get("reduce_only").and_then(Value::as_bool),
        time_in_force: text(o.get("time_in_force")),
        status: text(o.get("order_status")),
        placed_micros: o
            .get("creation_timestamp")
            .and_then(Value::as_i64)
            .map(millis_to_micros),
    })
}

/// The rows of a wallet's snapshot: per bound subaccount, its margin, each
/// position, each collateral and each open order, every figure as stated.
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
        // The subaccount's complete set of open orders at this read.
        for order in list(portfolio, "open_orders") {
            if let Some(order) = order_of(order) {
                out.push(envelope(Event::Order(order)));
            }
        }
    }
    Ok(out)
}

/// One of Derive's trades as a fill, each field as stated. Its transaction
/// hash is not carried: on a block explorer it resolves to the wallet.
fn trade_of(t: &Value) -> Option<Fill> {
    let side = match t.get("direction").and_then(Value::as_str)? {
        "buy" => Side::Bid,
        "sell" => Side::Ask,
        _ => return None,
    };
    Some(Fill {
        dex: None,
        market: text(t.get("instrument_name"))?,
        side,
        price: num(t.get("trade_price"))?,
        size: num(t.get("trade_amount"))?,
        start_position: None,
        direction: None,
        closed_pnl: num(t.get("realized_pnl")),
        fee: num(t.get("trade_fee")),
        fee_token: None,
        builder_fee: None,
        crossed: t
            .get("liquidity_role")
            .and_then(Value::as_str)
            .map(|role| role == "taker"),
        order_id: text(t.get("order_id"))?,
        trade_id: text(t.get("trade_id"))?,
        twap_id: None,
    })
}

/// One of Derive's funding events, **in its documented shape**: none exists
/// for the owner's accounts to measure (2026-11-27). The venue states the
/// amount, signed, and neither the position nor the rate.
fn funding_of(e: &Value) -> Option<FundingPayment> {
    Some(FundingPayment {
        dex: None,
        ticker: Ticker::new(text(e.get("instrument_name"))?).ok()?,
        usdc: num(e.get("funding"))?,
        size: None,
        rate: None,
        samples: None,
    })
}

/// The rows of a window of a wallet's history, each bound to its
/// subaccount's alias, at the venue's own time for it: an order's last
/// update, a trade's or a payment's time, a value point's. A row of a
/// subaccount no discovery has bound is left for the next window after it is.
pub fn history_rows(
    venue: &Venue,
    channel: &str,
    recv_micros: i64,
    bytes: &[u8],
) -> Result<Vec<Envelope>, NormaliseError> {
    let shape = |detail: &str| NormaliseError::Shape {
        kind: "history",
        detail: detail.into(),
    };
    let list = history_list(channel).ok_or_else(|| shape("not a history channel"))?;
    let composed: Value =
        serde_json::from_slice(bytes).map_err(|e| NormaliseError::Json(e.to_string()))?;
    let aliases = composed
        .get("aliases")
        .and_then(Value::as_object)
        .ok_or_else(|| shape("a window without its aliases"))?;
    let window = composed
        .get("answer")
        .ok_or_else(|| shape("a window without its answer"))?;
    let period = window.get("period").and_then(Value::as_i64);
    let pages = window
        .get("pages")
        .and_then(Value::as_array)
        .ok_or_else(|| shape("a window without its pages"))?;
    let mut out = Vec::new();
    for page in pages {
        let rows = page["result"]
            .get(list)
            .and_then(Value::as_array)
            .ok_or_else(|| shape("a page without its list"))?;
        for row in rows {
            let Some(alias) = row
                .get("subaccount_id")
                .and_then(identifier)
                .and_then(|id| aliases.get(&id))
                .and_then(Value::as_str)
            else {
                continue;
            };
            let at = |field: &str| row.get(field).and_then(Value::as_i64).map(millis_to_micros);
            let (event, at_micros) = match channel {
                ORDER_HISTORY_CHANNEL => match order_of(row) {
                    Some(o) => (Event::OrderUpdate(o), at("last_update_timestamp")),
                    None => continue,
                },
                TRADE_HISTORY_CHANNEL => match trade_of(row) {
                    Some(f) => (Event::Fill(f), at("timestamp")),
                    None => continue,
                },
                FUNDING_HISTORY_CHANNEL => match funding_of(row) {
                    Some(p) => (Event::FundingPayment(p), at("timestamp")),
                    None => continue,
                },
                _ => (
                    Event::PnlPoint(PnlPoint {
                        // Derive names no window: the period asked is it.
                        window: period.map_or_else(String::new, |p| p.to_string()),
                        account_value: num(row.get("subaccount_value")),
                        pnl: None,
                        initial_margin: num(row.get("initial_margin")),
                        maintenance_margin: num(row.get("maintenance_margin")),
                    }),
                    at("timestamp"),
                ),
            };
            out.push(Envelope::for_account(
                venue.clone(),
                Account::new(alias)?,
                at_micros,
                recv_micros,
                event,
            ));
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

/// What a recorded payload states **whole**, for the publication
/// (`add-realtime-account-ledger`, design D3): a wallet's snapshot states every
/// bound subaccount's margin, positions, collateral and open orders, the
/// subaccounts its aliases name. A kind it holds no row of for one is one that
/// subaccount holds none of.
#[cfg(feature = "capture")]
pub fn states(payload: &Payload, _rows: &[Envelope]) -> Vec<crate::ledger::publish::Stated> {
    use crate::ledger::publish::Stated;
    use galata_wire::Kind;
    if payload.channel != SNAPSHOT_CHANNEL {
        return Vec::new();
    }
    let Ok(composed) = serde_json::from_slice::<Value>(&payload.payload) else {
        return Vec::new();
    };
    let Some(aliases) = composed.get("aliases").and_then(Value::as_object) else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for alias in aliases.values().filter_map(Value::as_str) {
        let Ok(account) = Account::new(alias) else {
            continue;
        };
        for kind in [
            Kind::MarginState,
            Kind::PositionState,
            Kind::Collateral,
            Kind::Orders,
        ] {
            out.push(Stated {
                account: account.clone(),
                kind,
                scope: None,
            });
        }
    }
    out
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
            WS_ORDERS | WS_TRADES | WS_BALANCES => stream_rows(
                &self.venue,
                &payload.channel,
                payload.recv_micros,
                &payload.payload,
            ),
            ORDER_HISTORY_CHANNEL
            | TRADE_HISTORY_CHANNEL
            | FUNDING_HISTORY_CHANNEL
            | VALUE_HISTORY_CHANNEL => {
                let PayloadAddress::Account(_) = &payload.address else {
                    return Err(NormaliseError::Shape {
                        kind: "history",
                        detail: "a window not addressed to an account".into(),
                    });
                };
                history_rows(
                    &self.venue,
                    &payload.channel,
                    payload.recv_micros,
                    &payload.payload,
                )
            }
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

        const ORDERS: &[u8] =
            include_bytes!("../../../tests/fixtures/derive_xyz/ledger/get_order_history.json");
        const TRADES: &[u8] =
            include_bytes!("../../../tests/fixtures/derive_xyz/ledger/get_trade_history.json");
        const FUNDING: &[u8] =
            include_bytes!("../../../tests/fixtures/derive_xyz/ledger/get_funding_history.json");
        const VALUES: &[u8] = include_bytes!(
            "../../../tests/fixtures/derive_xyz/ledger/get_subaccount_value_history.json"
        );

        /// A one-page window of `channel`, composed as the ledger archives it,
        /// the fixture's subaccounts bound to `owner_s1` to `_s3`.
        fn window(channel: &str, period: Option<i64>, page: &[u8]) -> Vec<Envelope> {
            let aliases = (1..=3)
                .map(|n| {
                    (
                        fingerprint(&key(), &(1000 + n).to_string()),
                        Account::new(format!("owner_s{n}")).unwrap(),
                    )
                })
                .collect();
            let composed =
                wallet_bytes(&key(), &aliases, &history_window(period, &[page.to_vec()]));
            history_rows(&Venue::new(VENUE).unwrap(), channel, 42, &composed).unwrap()
        }

        /// No row names an address or a subaccount's identifier.
        fn names_neither(rows: &[Envelope]) {
            for row in rows {
                let json = serde_json::to_string(row).unwrap();
                assert!(!json.contains("0x"), "{json}");
                assert!(!json.contains("subaccount"), "{json}");
                assert!(
                    !json.contains("tx_hash") && !json.contains("signer"),
                    "{json}"
                );
            }
        }

        #[test]
        fn an_order_window_gives_each_order_at_its_latest_status() {
            let answer: Value = serde_json::from_slice(ORDERS).unwrap();
            let listed = answer["result"]["orders"].as_array().unwrap();
            let rows = window(ORDER_HISTORY_CHANNEL, None, ORDERS);
            assert_eq!(rows.len(), listed.len());
            for (row, o) in rows.iter().zip(listed) {
                let Event::OrderUpdate(u) = &row.event else {
                    panic!("{:?}", row.event)
                };
                assert_eq!(u.order_id, o["order_id"].as_str().unwrap());
                assert_eq!(u.status.as_deref(), o["order_status"].as_str());
                assert_eq!(
                    row.at_micros,
                    Some(millis_to_micros(
                        o["last_update_timestamp"].as_i64().unwrap()
                    )),
                    "the time of its latest status"
                );
                let alias = format!("owner_s{}", o["subaccount_id"].as_u64().unwrap() - 1000);
                assert_eq!(row.account().map(|a| a.to_string()), Some(alias));
            }
            names_neither(&rows);
        }

        #[test]
        fn a_trade_window_gives_each_fill_as_stated() {
            let answer: Value = serde_json::from_slice(TRADES).unwrap();
            let listed = answer["result"]["trades"].as_array().unwrap();
            let rows = window(TRADE_HISTORY_CHANNEL, None, TRADES);
            assert_eq!(rows.len(), listed.len());
            for (row, t) in rows.iter().zip(listed) {
                let Event::Fill(f) = &row.event else {
                    panic!("{:?}", row.event)
                };
                assert_eq!(f.market, t["instrument_name"].as_str().unwrap());
                assert_eq!(digits(&Some(f.price)), stated(&t["trade_price"]));
                assert_eq!(digits(&Some(f.size)), stated(&t["trade_amount"]));
                assert_eq!(digits(&f.fee), stated(&t["trade_fee"]));
                assert_eq!(digits(&f.closed_pnl), stated(&t["realized_pnl"]));
                assert_eq!(f.crossed, Some(t["liquidity_role"] == "taker"));
                assert_eq!(f.trade_id, t["trade_id"].as_str().unwrap());
                assert_eq!(f.order_id, t["order_id"].as_str().unwrap());
                assert_eq!(
                    row.at_micros,
                    Some(millis_to_micros(t["timestamp"].as_i64().unwrap()))
                );
            }
            names_neither(&rows);
        }

        #[test]
        fn a_funding_window_with_no_event_is_no_row() {
            assert!(window(FUNDING_HISTORY_CHANNEL, None, FUNDING).is_empty());
        }

        #[test]
        fn a_funding_event_in_its_documented_shape_is_a_payment() {
            // None exists for the owner's accounts to measure: Derive's
            // documented shape (2026-11-27).
            let page = br#"{"id":"x","result":{"events":[{"funding":"-0.25","instrument_name":"ETH-PERP","pnl":"1.5","subaccount_id":1002,"timestamp":1790000000000}],"pagination":{"count":1,"num_pages":1}}}"#;
            let rows = window(FUNDING_HISTORY_CHANNEL, None, page);
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].account().map(|a| a.as_str()), Some("owner_s2"));
            let Event::FundingPayment(p) = &rows[0].event else {
                panic!("{:?}", rows[0].event)
            };
            assert_eq!(p.ticker.as_str(), "ETH-PERP");
            assert_eq!(p.usdc.to_string(), "-0.25", "signed as the venue signs it");
            assert_eq!((p.size, p.rate), (None, None), "neither is stated");
        }

        #[test]
        fn a_value_window_gives_each_subaccounts_points_under_the_period() {
            let answer: Value = serde_json::from_slice(VALUES).unwrap();
            let listed = answer["result"]["subaccount_value_history"]
                .as_array()
                .unwrap();
            let rows = window(VALUE_HISTORY_CHANNEL, Some(3_600), VALUES);
            assert_eq!(rows.len(), listed.len());
            for (row, v) in rows.iter().zip(listed) {
                let Event::PnlPoint(p) = &row.event else {
                    panic!("{:?}", row.event)
                };
                assert_eq!(p.window, "3600");
                assert_eq!(digits(&p.account_value), stated(&v["subaccount_value"]));
                assert_eq!(digits(&p.initial_margin), stated(&v["initial_margin"]));
                assert_eq!(
                    digits(&p.maintenance_margin),
                    stated(&v["maintenance_margin"])
                );
                assert_eq!(p.pnl, None, "Derive states no PnL; none is computed");
            }
            let subaccounts: std::collections::BTreeSet<String> = rows
                .iter()
                .filter_map(|r| r.account().map(|a| a.to_string()))
                .collect();
            assert_eq!(subaccounts.len(), 2, "both subaccounts the window holds");
            names_neither(&rows);
        }

        #[test]
        fn a_window_of_an_unbound_subaccount_has_no_rows_and_a_bare_page_is_refused() {
            let composed = wallet_bytes(
                &key(),
                &BTreeMap::new(),
                &history_window(None, &[TRADES.to_vec()]),
            );
            let venue = Venue::new(VENUE).unwrap();
            assert!(
                history_rows(&venue, TRADE_HISTORY_CHANNEL, 1, &composed)
                    .unwrap()
                    .is_empty()
            );
            let bare = wallet_bytes(
                &key(),
                &BTreeMap::new(),
                &history_window(None, &[b"{}".to_vec()]),
            );
            assert!(history_rows(&venue, TRADE_HISTORY_CHANNEL, 1, &bare).is_err());
        }

        #[test]
        fn a_page_says_how_many_its_window_holds() {
            assert_eq!(num_pages(TRADES), 1);
            assert_eq!(
                num_pages(br#"{"result":{"trades":[],"pagination":{"count":250,"num_pages":3}}}"#),
                3
            );
            assert_eq!(num_pages(b"{}"), 1, "one where it says nothing");
        }

        /// A frame of a subaccount's channel, in the documented envelope, its
        /// `data` the fixture's rows of that shape.
        fn frame(channel: &str, data: Value) -> Vec<u8> {
            serde_json::json!({"method": "subscription", "params": {"channel": channel, "data": data}})
                .to_string()
                .into_bytes()
        }

        fn streamed_rows(ws: &str, frame: &[u8]) -> Vec<Envelope> {
            let composed = stream_bytes("1001", &Account::new("owner_s1").unwrap(), frame);
            stream_rows(&Venue::new(VENUE).unwrap(), ws, 7, &composed).unwrap()
        }

        #[test]
        fn a_streamed_order_is_an_update_at_its_latest_change_bound_to_its_alias() {
            let orders: Value = serde_json::from_slice(ORDERS).unwrap();
            let listed = orders["result"]["orders"].clone();
            let rows = streamed_rows(WS_ORDERS, &frame("1001.orders", listed.clone()));
            assert_eq!(rows.len(), listed.as_array().unwrap().len());
            for (row, o) in rows.iter().zip(listed.as_array().unwrap()) {
                assert_eq!(row.account().map(|a| a.as_str()), Some("owner_s1"));
                assert_eq!(
                    row.at_micros,
                    Some(millis_to_micros(
                        o["last_update_timestamp"].as_i64().unwrap()
                    ))
                );
                let Event::OrderUpdate(u) = &row.event else {
                    panic!("{:?}", row.event)
                };
                assert_eq!(u.status.as_deref(), o["order_status"].as_str());
            }
            // The same order streamed and read by history is one update.
            let history = window(ORDER_HISTORY_CHANNEL, None, ORDERS);
            assert_eq!(
                crate::ledger::events::identity(&rows[0]),
                crate::ledger::events::identity(&history[0])
            );
            names_neither(&rows);
        }

        #[test]
        fn a_streamed_trade_is_the_fill_the_history_gives() {
            let trades: Value = serde_json::from_slice(TRADES).unwrap();
            let listed = trades["result"]["trades"].clone();
            let rows = streamed_rows(WS_TRADES, &frame("1001.trades", listed));
            let history = window(TRADE_HISTORY_CHANNEL, None, TRADES);
            assert_eq!(rows.len(), history.len());
            for (streamed, read) in rows.iter().zip(&history) {
                assert_eq!(streamed.event, read.event);
                assert_eq!(
                    crate::ledger::events::identity(streamed),
                    crate::ledger::events::identity(read)
                );
            }
            names_neither(&rows);
        }

        #[test]
        fn a_balance_change_makes_no_row_and_asks_for_the_state() {
            let data = serde_json::json!([{"name": "ETH-PERP", "new_balance": "1",
                "previous_balance": "0", "update_type": "trade"}]);
            let bytes = frame("1001.balances", data);
            assert!(streamed_rows(WS_BALANCES, &bytes).is_empty());
            match streamed(&bytes) {
                crate::ledger::run::StreamFrame::Account {
                    channel,
                    then,
                    address,
                    ..
                } => {
                    assert_eq!(channel, WS_BALANCES);
                    assert_eq!(then, crate::ledger::run::Then::Snapshot);
                    assert_eq!(address.expose(), "1001");
                }
                other => panic!("{other:?}"),
            }
            // The connection's own answers are no account's.
            assert!(matches!(
                streamed(br#"{"id":1,"result":[1001,1002]}"#),
                crate::ledger::run::StreamFrame::Control
            ));
            assert!(matches!(
                streamed(br#"{"method":"subscription","params":{"channel":"ticker.ETH-PERP","data":{}}}"#),
                crate::ledger::run::StreamFrame::Other
            ));
        }

        #[test]
        fn a_subaccounts_resting_order_comes_from_the_same_answer() {
            let answer: Value = serde_json::from_slice(PORTFOLIOS).unwrap();
            let resting = &answer["result"][2]["open_orders"][0];
            let rows = read(PORTFOLIOS);
            let orders: Vec<&Envelope> = rows
                .iter()
                .filter(|e| matches!(e.event, Event::Order(_)))
                .collect();
            assert_eq!(orders.len(), 1, "the one open order the answer holds");
            let row = orders[0];
            assert_eq!(row.account().map(|a| a.as_str()), Some("owner_s3"));
            assert_eq!(row.recv_micros, 42, "the snapshot's own read");
            let Event::Order(o) = &row.event else {
                unreachable!()
            };
            assert_eq!(o.market, resting["instrument_name"].as_str().unwrap());
            assert_eq!(o.side, galata_wire::Side::Ask, "a sell");
            assert_eq!(o.order_id, resting["order_id"].as_str().unwrap());
            assert_eq!(o.order_type, "limit");
            assert_eq!(o.status.as_deref(), Some("open"));
            assert_eq!(o.time_in_force.as_deref(), Some("gtc"));
            assert_eq!(digits(&o.size), stated(&resting["amount"]));
            assert_eq!(digits(&o.limit_price), stated(&resting["limit_price"]));
            let amount: Num = resting["amount"].as_str().unwrap().parse().unwrap();
            let filled: Num = resting["filled_amount"].as_str().unwrap().parse().unwrap();
            assert_eq!(o.remaining, Some(amount - filled));
            assert_eq!(
                o.placed_micros,
                Some(millis_to_micros(
                    resting["creation_timestamp"].as_i64().unwrap()
                ))
            );
            assert_eq!(o.client_id, None, "an empty label is none");
            // No address in the row: the order's signer is a session key's.
            let json = serde_json::to_string(row).unwrap();
            assert!(!json.contains("0x"), "{json}");
            assert!(!json.contains("signer") && !json.contains("signature"));
        }

        #[test]
        fn an_order_without_an_id_market_or_side_is_no_order() {
            let whole = serde_json::json!({
                "order_id": "a1", "instrument_name": "ETH-PERP", "direction": "buy",
                "order_type": "limit", "amount": "1", "filled_amount": "0.25"
            });
            assert_eq!(
                order_of(&whole).unwrap().remaining,
                Some("0.75".parse().unwrap())
            );
            for missing in ["order_id", "instrument_name", "direction"] {
                let mut row = whole.clone();
                row.as_object_mut().unwrap().remove(missing);
                assert!(order_of(&row).is_none(), "{missing}");
            }
        }

        #[test]
        fn every_figure_is_carried_digit_for_digit() {
            let answer: Value = serde_json::from_slice(PORTFOLIOS).unwrap();
            let held = &answer["result"][2];
            let rows = read(PORTFOLIOS);
            let events = of(&rows, "owner_s3");
            assert_eq!(
                events.len(),
                4,
                "margin, one position, one collateral, one open order"
            );

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
            assert_eq!(events.len(), 4, "the snapshot survives, its open order too");
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
            assert_eq!(rows.len(), 4, "margin, position, collateral, open order");

            assert!(wallet_rows(&Venue::new(VENUE).unwrap(), 0, PORTFOLIOS).is_err());
        }
    }
}
