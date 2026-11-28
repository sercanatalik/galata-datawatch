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
//!   orders     { dexes: [{ dex, frontendOpenOrders }, …] }
//!                 ──▶ an `orders` row per resting order, every dex in one set
//!   history    historicalOrders ──▶ an `order_updates` row per status change
//!   portfolio  portfolio     ──▶ a `pnl_history` row per window and time
//!   stream     { mode, names, frame }, a `ws/<channel>` frame verbatim
//!                 ──▶ the rows its REST answer gives: margin and positions,
//!                     open orders, balances, fills, funding, ledger updates
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
    Account, AccountMode, Envelope, Event, Margin, Num, Order, Position, Side, Ticker, Token,
    Venue, millis_to_micros,
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
/// The channel an account's open orders, every dex composed, are recorded
/// under.
pub const OPEN_ORDERS_CHANNEL: &str = "frontendOpenOrders";
/// The channel an account's order history is recorded under, verbatim.
pub const ORDER_HISTORY_CHANNEL: &str = "historicalOrders";
/// The channel an account's value and PnL history is recorded under,
/// verbatim.
pub const PORTFOLIO_CHANNEL: &str = "portfolio";

/// The streamed channels (`add-realtime-account-ledger`, design D1): each frame
/// recorded verbatim under `ws/` and the venue's name for it, beside what its
/// rows need (the mode last heard, the reference's token names).
pub const WS_CLEARINGHOUSE: &str = "ws/clearinghouseState";
/// One dex's open orders, streamed.
pub const WS_OPEN_ORDERS: &str = "ws/openOrders";
/// The account's spot state, streamed.
pub const WS_SPOT: &str = "ws/spotState";
/// A change of an order's status, streamed **without the order's type or
/// trigger** (measured shape, 2026-11-28): recorded, and answered by reading
/// the order history at once, whose rows carry the whole order.
pub const WS_ORDER_UPDATES: &str = "ws/orderUpdates";
/// The account's fills, streamed, a snapshot first.
pub const WS_FILLS: &str = "ws/userFills";
/// The account's funding, streamed, a snapshot first.
pub const WS_FUNDINGS: &str = "ws/userFundings";
/// The account's ledger updates, streamed, a snapshot first.
pub const WS_LEDGER_UPDATES: &str = "ws/userNonFundingLedgerUpdates";

/// The frames that subscribe one account's streams: its state on each
/// declared dex, its spot state, and its order updates, fills, funding and
/// ledger updates. **The address goes into them**, to the venue and nowhere
/// else, as it goes into a REST body.
pub fn subscriptions(user: &str, dexes: &[String]) -> Vec<String> {
    let one = |kind: &str, dex: Option<&str>| {
        let mut subscription = serde_json::json!({ "type": kind, "user": user });
        if let Some(dex) = dex.filter(|d| !d.is_empty()) {
            subscription["dex"] = serde_json::Value::String(dex.to_string());
        }
        serde_json::json!({ "method": "subscribe", "subscription": subscription }).to_string()
    };
    let mut out = Vec::new();
    for dex in dexes {
        out.push(one("clearinghouseState", Some(dex)));
        out.push(one("openOrders", Some(dex)));
    }
    for kind in [
        "spotState",
        "orderUpdates",
        "userFills",
        "userFundings",
        "userNonFundingLedgerUpdates",
    ] {
        out.push(one(kind, None));
    }
    out
}

/// The keepalive the stream wants: a frame of its own within every minute.
pub const KEEPALIVE: &str = r#"{"method":"ping"}"#;

/// What one streamed frame is, for the loop.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Streamed {
    /// About the account the venue names by `user`: recorded under `channel`
    /// as a payload of `kind`.
    Account {
        /// The `ws/` channel.
        channel: &'static str,
        /// The dataset it lands in.
        kind: galata_wire::Kind,
        /// The address, as the venue names it. Held only to fingerprint.
        user: String,
    },
    /// An order's status changed, on an account the frame does not name.
    OrderUpdates,
    /// The venue acknowledging a subscription or answering a ping.
    Control,
    /// Anything else: recorded nowhere, counted.
    Other,
}

/// Classify a frame without building its rows.
pub fn streamed(frame: &[u8]) -> Streamed {
    use galata_wire::Kind;
    #[derive(Deserialize)]
    struct Head<'a> {
        channel: &'a str,
        #[serde(borrow)]
        data: Option<&'a RawValue>,
    }
    #[derive(Deserialize)]
    struct User {
        user: Option<String>,
    }
    let Ok(head) = serde_json::from_slice::<Head<'_>>(frame) else {
        return Streamed::Other;
    };
    let (channel, kind) = match head.channel {
        "subscriptionResponse" | "pong" => return Streamed::Control,
        "orderUpdates" => return Streamed::OrderUpdates,
        "clearinghouseState" => (WS_CLEARINGHOUSE, Kind::Margin),
        "openOrders" => (WS_OPEN_ORDERS, Kind::Orders),
        "spotState" => (WS_SPOT, Kind::Balances),
        "userFills" => (WS_FILLS, Kind::Fills),
        "userFundings" => (WS_FUNDINGS, Kind::FundingPayments),
        "userNonFundingLedgerUpdates" => (WS_LEDGER_UPDATES, Kind::LedgerUpdates),
        _ => return Streamed::Other,
    };
    let user = head
        .data
        .and_then(|d| serde_json::from_str::<User>(d.get()).ok())
        .and_then(|u| u.user);
    match user {
        Some(user) => Streamed::Account {
            channel,
            kind,
            user,
        },
        None => Streamed::Other,
    }
}

/// Compose a streamed frame for the record: the frame **verbatim**, beside the
/// mode last heard and the names the reference last gave, which its rows need
/// and a normaliser, a function of the bytes, cannot ask for.
pub fn stream_bytes(
    mode: Option<(&[u8], i64)>,
    names: Option<&std::collections::BTreeMap<u32, String>>,
    frame: &[u8],
) -> Vec<u8> {
    let mut out = Vec::with_capacity(frame.len() + 128);
    out.extend_from_slice(b"{\"mode\":");
    match mode {
        Some((answer, recv_micros)) => {
            out.extend_from_slice(b"{\"answer\":");
            out.extend_from_slice(answer);
            out.extend_from_slice(format!(",\"recv_micros\":{recv_micros}}}").as_bytes());
        }
        None => out.extend_from_slice(b"null"),
    }
    out.extend_from_slice(b",\"names\":");
    match names {
        Some(names) => {
            out.extend_from_slice(serde_json::to_string(names).unwrap_or_default().as_bytes())
        }
        None => out.extend_from_slice(b"null"),
    }
    out.extend_from_slice(b",\"frame\":");
    out.extend_from_slice(frame);
    out.push(b'}');
    out
}

#[derive(Deserialize)]
struct StreamComposed<'a> {
    #[serde(borrow)]
    mode: Option<StreamMode<'a>>,
    names: Option<std::collections::BTreeMap<u32, String>>,
    #[serde(borrow)]
    frame: StreamFrameBody<'a>,
}

#[derive(Deserialize)]
struct StreamMode<'a> {
    #[serde(borrow)]
    answer: &'a RawValue,
    recv_micros: i64,
}

#[derive(Deserialize)]
struct StreamFrameBody<'a> {
    #[serde(borrow)]
    data: &'a RawValue,
}

/// A recorded streamed frame to its rows: **the rows its REST answer gives**,
/// through the same normaliser, so a fill streamed and a fill walked are one
/// row by identity.
pub fn stream_rows(
    venue: &Venue,
    account: &Account,
    key: Option<&crate::ledger::FingerprintKey>,
    own_fingerprint: &str,
    channel: &str,
    recv_micros: i64,
    bytes: &[u8],
) -> Result<Vec<Envelope>, NormaliseError> {
    let composed: StreamComposed<'_> =
        serde_json::from_slice(bytes).map_err(|e| NormaliseError::Json(e.to_string()))?;
    let data: serde_json::Value = serde_json::from_str(composed.frame.data.get())
        .map_err(|e| NormaliseError::Json(e.to_string()))?;
    let raw = |field: &str| -> Result<Vec<u8>, NormaliseError> {
        data.get(field)
            .map(|v| serde_json::to_vec(v).unwrap_or_default())
            .ok_or_else(|| NormaliseError::Shape {
                kind: "stream",
                detail: format!("a {channel} frame without {field}"),
            })
    };
    let dex = data
        .get("dex")
        .and_then(|d| d.as_str())
        .unwrap_or_default()
        .to_string();
    match channel {
        WS_CLEARINGHOUSE => {
            let mode = composed
                .mode
                .as_ref()
                .map(|m| (m.answer.get().as_bytes(), m.recv_micros));
            let state = raw("clearinghouseState")?;
            snapshot_rows(
                venue,
                account,
                recv_micros,
                &snapshot_bytes(&dex, mode, &state),
            )
        }
        WS_OPEN_ORDERS => open_order_rows(
            venue,
            account,
            recv_micros,
            &open_orders_bytes(&[(dex.as_str(), raw("orders")?)]),
        ),
        WS_SPOT => balance_rows(
            venue,
            account,
            recv_micros,
            &wide_bytes(
                BALANCES_CHANNEL,
                composed.names.as_ref(),
                &raw("spotState")?,
            ),
        ),
        WS_FILLS => super::events::fill_rows(
            venue,
            account,
            key,
            own_fingerprint,
            recv_micros,
            &raw("fills")?,
        ),
        // Streamed flat, `{time, coin, usdc, szi, fundingRate, nSamples}`;
        // walked under `delta`. Lifted into the walked shape, so both are one
        // row by identity.
        WS_FUNDINGS => {
            let lifted: Vec<serde_json::Value> = data
                .get("fundings")
                .and_then(|f| f.as_array())
                .map(|rows| {
                    rows.iter()
                        .map(|r| {
                            let mut delta = r.clone();
                            if let Some(d) = delta.as_object_mut() {
                                d.remove("time");
                                d.insert("type".into(), "funding".into());
                            }
                            serde_json::json!({ "time": r.get("time"), "delta": delta })
                        })
                        .collect()
                })
                .unwrap_or_default();
            super::events::funding_rows(
                venue,
                account,
                recv_micros,
                &serde_json::to_vec(&lifted).unwrap_or_default(),
            )
        }
        WS_LEDGER_UPDATES => {
            let key = key.ok_or_else(|| NormaliseError::Shape {
                kind: "userNonFundingLedgerUpdates",
                detail: "no fingerprint key: this normaliser cannot tell the parties of a \
                         transfer apart"
                    .into(),
            })?;
            super::events::update_rows(
                venue,
                account,
                key,
                own_fingerprint,
                recv_micros,
                &raw("nonFundingLedgerUpdates")?,
            )
        }
        // States no whole order: the order history read it triggers does.
        WS_ORDER_UPDATES => Ok(Vec::new()),
        other => Err(NormaliseError::UnknownChannel(other.to_string())),
    }
}

/// Compose an account's open orders: **every declared dex's answer verbatim**,
/// one payload, so that it is recorded only as a complete set. A dex that did
/// not answer leaves no payload at all, never a set missing that dex.
pub fn open_orders_bytes(answers: &[(&str, Vec<u8>)]) -> Vec<u8> {
    let mut out = Vec::with_capacity(answers.iter().map(|(_, a)| a.len() + 48).sum::<usize>() + 12);
    out.extend_from_slice(b"{\"dexes\":[");
    for (i, (dex, answer)) in answers.iter().enumerate() {
        if i > 0 {
            out.push(b',');
        }
        out.extend_from_slice(b"{\"dex\":");
        out.extend_from_slice(serde_json::to_string(dex).unwrap_or_default().as_bytes());
        out.extend_from_slice(b",\"frontendOpenOrders\":");
        out.extend_from_slice(answer);
        out.push(b'}');
    }
    out.extend_from_slice(b"]}");
    out
}

#[derive(Deserialize)]
struct OpenOrdersComposed<'a> {
    #[serde(borrow)]
    dexes: Vec<DexOrders<'a>>,
}

#[derive(Deserialize)]
struct DexOrders<'a> {
    #[serde(borrow, rename = "frontendOpenOrders")]
    answer: &'a RawValue,
}

/// One order as Hyperliquid's `frontendOpenOrders` states it, and as
/// `historicalOrders` states each order under its status (measured
/// 2026-11-27).
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct VenueOrder {
    coin: String,
    side: String,
    limit_px: Option<Num>,
    sz: Option<Num>,
    oid: u64,
    timestamp: Option<i64>,
    trigger_condition: Option<String>,
    is_trigger: Option<bool>,
    trigger_px: Option<Num>,
    is_position_tpsl: Option<bool>,
    reduce_only: Option<bool>,
    order_type: String,
    orig_sz: Option<Num>,
    tif: Option<String>,
    cloid: Option<String>,
}

/// A venue order to the vocabulary's, **each field as stated**, with two
/// readings of the venue's own spelling of *none*: a trigger price of zero and
/// a trigger condition of `N/A` are absent (measured 2026-11-27: every plain
/// order states both). A dex prefix is composed away and kept apart; a spot
/// pair's name (`@107`) is kept as the venue gives it. Attached take-profit
/// and stop children are not orders of their own until the venue rests them.
pub(crate) fn order_of(o: VenueOrder, status: Option<String>) -> Result<Order, NormaliseError> {
    let side = match o.side.as_str() {
        "B" => Side::Bid,
        "A" => Side::Ask,
        other => {
            return Err(NormaliseError::Shape {
                kind: "order",
                detail: format!("side {other:?} is neither A nor B"),
            });
        }
    };
    let (dex, market) = match o.coin.split_once(':') {
        Some((dex, bare)) => (Some(dex.to_string()), bare.to_string()),
        None => (None, o.coin),
    };
    Ok(Order {
        dex,
        market,
        side,
        order_id: o.oid.to_string(),
        client_id: o.cloid,
        order_type: o.order_type,
        // The type's own words carry it: `Take Profit Market`, `Stop Limit`.
        trigger_type: None,
        is_trigger: o.is_trigger,
        position_tpsl: o.is_position_tpsl,
        size: o.orig_sz,
        remaining: o.sz,
        limit_price: o.limit_px,
        trigger_price: o.trigger_px.filter(|px| !px.is_zero()),
        trigger_condition: o.trigger_condition.filter(|c| c != "N/A"),
        reduce_only: o.reduce_only,
        time_in_force: o.tif,
        status,
        placed_micros: o.timestamp.map(millis_to_micros),
    })
}

/// One row of `historicalOrders`: an order as it stood at a change of its
/// status (measured 2026-11-27).
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct HistoryRow {
    order: VenueOrder,
    status: String,
    status_timestamp: i64,
}

/// An order history answer to one `order_updates` row per change of status,
/// each at the time the venue gives the change, **every one the answer holds**:
/// the venue repeats its recent history on every read, and identity keeps each
/// once (`crate::ledger::events::identity`). Every dex is in the one answer.
pub fn order_update_rows(
    venue: &Venue,
    account: &Account,
    recv_micros: i64,
    bytes: &[u8],
) -> Result<Vec<Envelope>, NormaliseError> {
    let rows: Vec<HistoryRow> =
        serde_json::from_slice(bytes).map_err(|e| NormaliseError::Shape {
            kind: "historicalOrders",
            detail: e.to_string(),
        })?;
    rows.into_iter()
        .map(|r| {
            Ok(Envelope::for_account(
                venue.clone(),
                account.clone(),
                Some(millis_to_micros(r.status_timestamp)),
                recv_micros,
                Event::OrderUpdate(order_of(r.order, Some(r.status))?),
            ))
        })
        .collect()
}

/// One window of `portfolio`: its value and PnL points, `[time ms, value]`.
/// Its volume is a figure for the window, not a point, and is left in the
/// record.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Window {
    #[serde(default)]
    account_value_history: Vec<(i64, Num)>,
    #[serde(default)]
    pnl_history: Vec<(i64, Num)>,
}

/// A portfolio answer to one `pnl_history` row per window and time, **each
/// window the venue states and only those**: `day`, `week`, `month`,
/// `allTime` and their `perp` forms, as measured on 2026-11-27. A time with a
/// value and a PnL is one row holding both; nothing is computed, so a time
/// the venue gives one of them for has the other absent.
pub fn pnl_rows(
    venue: &Venue,
    account: &Account,
    recv_micros: i64,
    bytes: &[u8],
) -> Result<Vec<Envelope>, NormaliseError> {
    let windows: Vec<(String, Window)> =
        serde_json::from_slice(bytes).map_err(|e| NormaliseError::Shape {
            kind: "portfolio",
            detail: e.to_string(),
        })?;
    let mut out = Vec::new();
    for (name, window) in windows {
        let mut points: std::collections::BTreeMap<i64, (Option<Num>, Option<Num>)> =
            std::collections::BTreeMap::new();
        for (at, value) in window.account_value_history {
            points.entry(at).or_default().0 = Some(value);
        }
        for (at, pnl) in window.pnl_history {
            points.entry(at).or_default().1 = Some(pnl);
        }
        for (at, (account_value, pnl)) in points {
            out.push(Envelope::for_account(
                venue.clone(),
                account.clone(),
                Some(millis_to_micros(at)),
                recv_micros,
                Event::PnlPoint(galata_wire::PnlPoint {
                    window: name.clone(),
                    account_value,
                    pnl,
                    initial_margin: None,
                    maintenance_margin: None,
                }),
            ));
        }
    }
    Ok(out)
}

/// A composed open-orders payload to one `orders` row per resting order, on
/// every dex: the complete set as of `recv_micros`. The answers state no
/// time of their own; an empty set is no rows.
pub fn open_order_rows(
    venue: &Venue,
    account: &Account,
    recv_micros: i64,
    bytes: &[u8],
) -> Result<Vec<Envelope>, NormaliseError> {
    let composed: OpenOrdersComposed<'_> =
        serde_json::from_slice(bytes).map_err(|e| NormaliseError::Json(e.to_string()))?;
    let mut out = Vec::new();
    for dex in composed.dexes {
        let orders: Vec<VenueOrder> =
            serde_json::from_str(dex.answer.get()).map_err(|e| NormaliseError::Shape {
                kind: "frontendOpenOrders",
                detail: e.to_string(),
            })?;
        for o in orders {
            out.push(Envelope::for_account(
                venue.clone(),
                account.clone(),
                None,
                recv_micros,
                Event::Order(order_of(o, None)?),
            ));
        }
    }
    Ok(out)
}

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

/// What a recorded payload states **whole**, for the publication
/// (`add-realtime-account-ledger`, design D3): its account's kinds, each within
/// the dex it was read on where the venue answers per dex. A kind it states
/// and holds no row of is one the account holds none of, there.
#[cfg(feature = "capture")]
pub fn states(payload: &Payload, rows: &[Envelope]) -> Vec<crate::ledger::publish::Stated> {
    use crate::ledger::publish::Stated;
    use galata_wire::Kind;
    let PayloadAddress::Account(address) = &payload.address else {
        return Vec::new();
    };
    let Ok(account) = Account::new(address.account.as_str()) else {
        return Vec::new();
    };
    let whole = |kind: Kind, scope: Option<String>| Stated {
        account: account.clone(),
        kind,
        scope,
    };
    match payload.channel.as_str() {
        // One dex's margin and positions: which dex is the margin row's.
        SNAPSHOT_CHANNEL => rows
            .iter()
            .find_map(|e| match &e.event {
                Event::Margin(m) => Some(m.dex.clone()),
                _ => None,
            })
            .map(|dex| {
                vec![
                    whole(Kind::Margin, dex.clone()),
                    whole(Kind::Positions, dex),
                ]
            })
            .unwrap_or_default(),
        BALANCES_CHANNEL => vec![
            whole(Kind::Balances, None),
            whole(Kind::PortfolioMargin, None),
            whole(Kind::TokenMargin, None),
        ],
        BORROW_LEND_CHANNEL => vec![whole(Kind::BorrowLend, None), whole(Kind::LendHealth, None)],
        // A streamed state: the one dex its frame names, or the account's spot.
        WS_CLEARINGHOUSE | WS_OPEN_ORDERS => {
            let dex = serde_json::from_slice::<serde_json::Value>(&payload.payload)
                .ok()
                .and_then(|v| v["frame"]["data"]["dex"].as_str().map(str::to_string))
                .filter(|d| !d.is_empty());
            if payload.channel == WS_CLEARINGHOUSE {
                vec![
                    whole(Kind::Margin, dex.clone()),
                    whole(Kind::Positions, dex),
                ]
            } else {
                vec![whole(Kind::Orders, dex)]
            }
        }
        WS_SPOT => vec![
            whole(Kind::Balances, None),
            whole(Kind::PortfolioMargin, None),
            whole(Kind::TokenMargin, None),
        ],
        // Every declared dex's open orders, read together.
        OPEN_ORDERS_CHANNEL => {
            #[derive(Deserialize)]
            struct Dexes {
                dexes: Vec<Named>,
            }
            #[derive(Deserialize)]
            struct Named {
                dex: String,
            }
            serde_json::from_slice::<Dexes>(&payload.payload)
                .map(|d| {
                    d.dexes
                        .into_iter()
                        .map(|n| whole(Kind::Orders, (!n.dex.is_empty()).then_some(n.dex)))
                        .collect()
                })
                .unwrap_or_default()
        }
        _ => Vec::new(),
    }
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
            OPEN_ORDERS_CHANNEL | ORDER_HISTORY_CHANNEL | PORTFOLIO_CHANNEL => {
                let PayloadAddress::Account(address) = &payload.address else {
                    return Err(NormaliseError::Shape {
                        kind: "orders and PnL",
                        detail: "an answer not addressed to an account".into(),
                    });
                };
                let account = Account::new(address.account.as_str())?;
                let (venue, recv, bytes) = (&self.venue, payload.recv_micros, &payload.payload);
                match payload.channel.as_str() {
                    OPEN_ORDERS_CHANNEL => open_order_rows(venue, &account, recv, bytes),
                    ORDER_HISTORY_CHANNEL => order_update_rows(venue, &account, recv, bytes),
                    _ => pnl_rows(venue, &account, recv, bytes),
                }
            }
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
            WS_CLEARINGHOUSE | WS_OPEN_ORDERS | WS_SPOT | WS_ORDER_UPDATES | WS_FILLS
            | WS_FUNDINGS | WS_LEDGER_UPDATES => {
                let PayloadAddress::Account(address) = &payload.address else {
                    return Err(NormaliseError::Shape {
                        kind: "stream",
                        detail: "a frame not addressed to an account".into(),
                    });
                };
                let account = Account::new(address.account.as_str())?;
                stream_rows(
                    &self.venue,
                    &account,
                    self.key.as_ref(),
                    &address.fingerprint,
                    &payload.channel,
                    payload.recv_micros,
                    &payload.payload,
                )
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
                    super::events::FILLS_CHANNEL => super::events::fill_rows(
                        venue,
                        &account,
                        self.key.as_ref(),
                        &address.fingerprint,
                        recv,
                        bytes,
                    ),
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

    /// The scrubbed answers of 2026-11-27 (`design/measured.md`).
    const OPEN_ORDERS_EMPTY: &[u8] =
        include_bytes!("../../../tests/fixtures/hyperliquid/ledger-open-orders-empty.json");
    const HISTORICAL_ORDERS: &[u8] =
        include_bytes!("../../../tests/fixtures/hyperliquid/ledger-historical-orders.json");

    /// Resting orders with `frontendOpenOrders`' row shape, which is
    /// `historicalOrders`' `order`: the fixture's own rows, one per status
    /// seen as open, and a spot order whose pair names no token.
    fn resting_answer() -> Vec<u8> {
        let rows: Vec<serde_json::Value> = serde_json::from_slice(HISTORICAL_ORDERS).unwrap();
        let mut open: Vec<serde_json::Value> = Vec::new();
        for want in ["Limit", "Take Profit Market"] {
            let row = rows
                .iter()
                .find(|r| r["status"] == "open" && r["order"]["orderType"] == want)
                .unwrap();
            open.push(row["order"].clone());
        }
        let mut spot = open[0].clone();
        spot["coin"] = "@107".into();
        open.push(spot);
        serde_json::to_vec(&open).unwrap()
    }

    fn open_orders(answers: &[(&str, Vec<u8>)]) -> Vec<Envelope> {
        let normaliser = LedgerNormaliser::new().unwrap();
        let payload = Payload {
            seq: 1,
            recv_micros: 77,
            address: PayloadAddress::Account(crate::record::AccountAddress {
                venue: "hyperliquid".into(),
                account: "main".into(),
                fingerprint: "0123456789abcdef".into(),
            }),
            channel: OPEN_ORDERS_CHANNEL.into(),
            kind: "orders".into(),
            symbol: None,
            origin: galata_wire::Origin::Fetched,
            payload: open_orders_bytes(answers),
        };
        normaliser.normalise(&payload).unwrap()
    }

    fn order(row: &Envelope) -> &Order {
        match &row.event {
            Event::Order(o) => o,
            other => panic!("not a resting order: {other:?}"),
        }
    }

    #[test]
    fn nothing_resting_on_any_dex_is_an_empty_set() {
        let rows = open_orders(&[
            ("", OPEN_ORDERS_EMPTY.to_vec()),
            ("xyz", OPEN_ORDERS_EMPTY.to_vec()),
        ]);
        assert!(rows.is_empty());
    }

    #[test]
    fn each_resting_order_carries_its_details_as_stated() {
        let rows = open_orders(&[("", OPEN_ORDERS_EMPTY.to_vec()), ("xyz", resting_answer())]);
        assert_eq!(rows.len(), 3);
        for row in &rows {
            assert_eq!(row.kind(), galata_wire::Kind::Orders);
            assert_eq!(row.recv_micros, 77, "the set's read");
            assert_eq!(row.at_micros, None, "the answer states no time");
            assert_eq!(row.account().map(|a| a.as_str()), Some("main"));
        }

        // A plain limit: its zero trigger price and `N/A` are the venue's
        // way of saying it has no trigger.
        let limit = order(&rows[0]);
        assert_eq!(limit.order_type, "Limit");
        assert_eq!(
            (limit.trigger_price, limit.trigger_condition.as_deref()),
            (None, None)
        );
        assert_eq!(limit.is_trigger, Some(false));
        assert_eq!(limit.time_in_force.as_deref(), Some("Gtc"));
        assert!(limit.size.is_some() && limit.remaining.is_some() && limit.limit_price.is_some());
        assert!(limit.placed_micros.is_some());
        assert_eq!(limit.status, None, "an open-orders row states no status");

        // A resting take-profit: its type, trigger price and condition, and
        // that it closes the position, each as stated.
        let tp = order(&rows[1]);
        assert_eq!(tp.order_type, "Take Profit Market");
        assert_eq!(tp.is_trigger, Some(true));
        assert_eq!(tp.position_tpsl, Some(true));
        assert_eq!(tp.reduce_only, Some(true));
        let px = tp.trigger_price.expect("a trigger price");
        assert_eq!(
            tp.trigger_condition.as_deref(),
            Some(format!("Price above {px}").as_str())
        );
        assert!(
            tp.order_id.parse::<u64>().is_ok(),
            "the venue's oid, as text"
        );

        // A spot pair, kept as the venue names it.
        let spot = order(&rows[2]);
        assert_eq!((spot.dex.as_deref(), spot.market.as_str()), (None, "@107"));
    }

    #[test]
    fn a_dex_prefix_is_kept_apart_from_the_market() {
        let rows = open_orders(&[("xyz", resting_answer())]);
        let fixture: Vec<serde_json::Value> = serde_json::from_slice(HISTORICAL_ORDERS).unwrap();
        let coin = fixture
            .iter()
            .find(|r| r["status"] == "open" && r["order"]["orderType"] == "Limit")
            .unwrap()["order"]["coin"]
            .as_str()
            .unwrap()
            .to_string();
        let limit = order(&rows[0]);
        match coin.split_once(':') {
            Some((dex, bare)) => {
                assert_eq!(limit.dex.as_deref(), Some(dex));
                assert_eq!(limit.market, bare);
            }
            None => assert_eq!(
                (limit.dex.as_deref(), limit.market.as_str()),
                (None, coin.as_str())
            ),
        }
    }

    #[test]
    fn a_side_that_is_neither_a_nor_b_is_refused() {
        let normaliser = LedgerNormaliser::new().unwrap();
        let mut answer: Vec<serde_json::Value> = serde_json::from_slice(&resting_answer()).unwrap();
        answer[0]["side"] = "S".into();
        let payload = Payload {
            seq: 1,
            recv_micros: 1,
            address: PayloadAddress::Account(crate::record::AccountAddress {
                venue: "hyperliquid".into(),
                account: "main".into(),
                fingerprint: "0123456789abcdef".into(),
            }),
            channel: OPEN_ORDERS_CHANNEL.into(),
            kind: "orders".into(),
            symbol: None,
            origin: galata_wire::Origin::Fetched,
            payload: open_orders_bytes(&[("", serde_json::to_vec(&answer).unwrap())]),
        };
        assert!(normaliser.normalise(&payload).is_err());
    }

    const PORTFOLIO: &[u8] =
        include_bytes!("../../../tests/fixtures/hyperliquid/ledger-portfolio.json");

    fn main() -> Account {
        Account::new("main").unwrap()
    }

    fn venue() -> Venue {
        Venue::new("hyperliquid").unwrap()
    }

    #[test]
    fn every_row_of_the_history_is_an_update_at_its_status_time() {
        let fixture: Vec<serde_json::Value> = serde_json::from_slice(HISTORICAL_ORDERS).unwrap();
        let rows = order_update_rows(&venue(), &main(), 5, HISTORICAL_ORDERS).unwrap();
        assert_eq!(rows.len(), fixture.len());
        for (row, stated) in rows.iter().zip(&fixture) {
            assert_eq!(row.kind(), galata_wire::Kind::OrderUpdates);
            assert_eq!(
                row.at_micros,
                Some(millis_to_micros(
                    stated["statusTimestamp"].as_i64().unwrap()
                ))
            );
            let Event::OrderUpdate(o) = &row.event else {
                panic!("not an update")
            };
            assert_eq!(o.status.as_deref(), stated["status"].as_str());
            assert_eq!(o.order_id, stated["order"]["oid"].to_string());
        }
    }

    #[test]
    fn a_take_profit_that_triggers_and_fills_is_three_updates_each_with_its_type() {
        // The scrubbed history holds one: placed, triggered, filled.
        let rows = order_update_rows(&venue(), &main(), 5, HISTORICAL_ORDERS).unwrap();
        let mut by_order: std::collections::BTreeMap<&str, Vec<(&Order, i64)>> =
            std::collections::BTreeMap::new();
        for row in &rows {
            if let Event::OrderUpdate(o) = &row.event {
                by_order
                    .entry(o.order_id.as_str())
                    .or_default()
                    .push((o, row.at_micros.unwrap()));
            }
        }
        let (_, mut updates) = by_order
            .into_iter()
            .find(|(_, u)| {
                u.iter()
                    .any(|(o, _)| o.status.as_deref() == Some("triggered"))
            })
            .expect("a take-profit that triggered");
        updates.sort_by_key(|(_, at)| *at);
        let status = |i: usize| updates[i].0.status.as_deref().unwrap();
        assert_eq!(updates.len(), 3);
        assert_eq!(status(0), "open");
        // A market take-profit fills as it triggers: the venue stamps both in
        // the same millisecond (measured 2026-11-27), and both are kept, the
        // status being part of an update's identity.
        assert_eq!(updates[1].1, updates[2].1);
        let mut last: Vec<&str> = vec![status(1), status(2)];
        last.sort();
        assert_eq!(last, ["filled", "triggered"]);
        for (o, _) in &updates {
            assert_eq!(o.order_type, "Take Profit Market", "the type on each");
            assert_eq!(o.position_tpsl, Some(true));
        }
        // Waiting on its trigger, it states the price; once filled, the venue
        // states the trigger as done.
        assert!(updates[0].0.trigger_price.is_some());
        let filled = updates
            .iter()
            .find(|(o, _)| o.status.as_deref() == Some("filled"))
            .unwrap();
        assert_eq!(filled.0.trigger_condition.as_deref(), Some("Triggered"));
    }

    #[test]
    fn every_window_the_venue_states_is_recorded_and_none_computed() {
        let fixture: Vec<(String, serde_json::Value)> = serde_json::from_slice(PORTFOLIO).unwrap();
        let rows = pnl_rows(&venue(), &main(), 5, PORTFOLIO).unwrap();
        let points = |r: &Envelope| match &r.event {
            Event::PnlPoint(p) => p.clone(),
            other => panic!("not a point: {other:?}"),
        };
        // Each window, under its own name, with as many points as it states.
        for (name, window) in &fixture {
            let mine: Vec<&Envelope> = rows.iter().filter(|r| points(r).window == *name).collect();
            let stated = window["accountValueHistory"].as_array().unwrap();
            assert_eq!(mine.len(), stated.len(), "{name}");
            // The first point as the venue states it, to the digit.
            let first = points(mine[0]);
            assert_eq!(
                first.account_value.unwrap().to_string(),
                stated[0][1].as_str().unwrap()
            );
            assert_eq!(
                mine[0].at_micros,
                Some(millis_to_micros(stated[0][0].as_i64().unwrap()))
            );
        }
        let windows: std::collections::BTreeSet<String> =
            rows.iter().map(|r| points(r).window).collect();
        assert_eq!(
            windows.len(),
            fixture.len(),
            "no window added, none dropped"
        );
        // Nothing but what the venue states.
        for row in &rows {
            let p = points(row);
            assert!(p.initial_margin.is_none() && p.maintenance_margin.is_none());
        }
    }

    #[test]
    fn a_time_with_only_a_value_has_no_pnl() {
        let answer = br#"[["day",{"accountValueHistory":[[1000,"10.5"],[2000,"11.0"]],"pnlHistory":[[2000,"0.5"]],"vlm":"0.0"}]]"#;
        let rows = pnl_rows(&venue(), &main(), 5, answer).unwrap();
        let pairs: Vec<(Option<String>, Option<String>)> = rows
            .iter()
            .map(|r| match &r.event {
                Event::PnlPoint(p) => (
                    p.account_value.map(|v| v.to_string()),
                    p.pnl.map(|v| v.to_string()),
                ),
                _ => unreachable!(),
            })
            .collect();
        assert_eq!(
            pairs,
            [
                (Some("10.5".into()), None),
                (Some("11.0".into()), Some("0.5".into()))
            ]
        );
    }

    /// The probe's frames of 2026-11-28, scrubbed (`design/measured.md`).
    fn ws(name: &str) -> Vec<u8> {
        std::fs::read(format!(
            "{}/tests/fixtures/hyperliquid/ledger-ws-{name}.json",
            env!("CARGO_MANIFEST_DIR")
        ))
        .unwrap()
    }

    fn streamed_rows(channel: &str, frame: &[u8]) -> Vec<Envelope> {
        let key = crate::ledger::FingerprintKey::new(Secret::new("deployment-key"));
        stream_rows(
            &venue(),
            &main(),
            Some(&key),
            "0123456789abcdef",
            channel,
            9,
            &stream_bytes(None, None, frame),
        )
        .unwrap()
    }

    #[test]
    fn every_measured_frame_is_classified_by_its_account() {
        for (name, channel, kind) in [
            (
                "clearinghouse-state-main",
                WS_CLEARINGHOUSE,
                galata_wire::Kind::Margin,
            ),
            (
                "clearinghouse-state-xyz",
                WS_CLEARINGHOUSE,
                galata_wire::Kind::Margin,
            ),
            (
                "open-orders-main",
                WS_OPEN_ORDERS,
                galata_wire::Kind::Orders,
            ),
            ("spot-state", WS_SPOT, galata_wire::Kind::Balances),
            ("user-fills-snapshot", WS_FILLS, galata_wire::Kind::Fills),
            (
                "user-fundings-snapshot",
                WS_FUNDINGS,
                galata_wire::Kind::FundingPayments,
            ),
            (
                "ledger-updates-snapshot",
                WS_LEDGER_UPDATES,
                galata_wire::Kind::LedgerUpdates,
            ),
        ] {
            match streamed(&ws(name)) {
                Streamed::Account {
                    channel: c,
                    kind: k,
                    user,
                } => {
                    assert_eq!((c, k), (channel, kind), "{name}");
                    assert!(user.starts_with("0x") && user.len() == 42, "{name}");
                }
                other => panic!("{name}: {other:?}"),
            }
        }
        assert_eq!(
            streamed(br#"{"channel":"subscriptionResponse","data":{}}"#),
            Streamed::Control
        );
        assert_eq!(streamed(br#"{"channel":"pong"}"#), Streamed::Control);
        assert_eq!(
            streamed(br#"{"channel":"orderUpdates","data":[]}"#),
            Streamed::OrderUpdates
        );
    }

    #[test]
    fn a_streamed_state_gives_the_rows_its_rest_answer_gives() {
        let main = streamed_rows(WS_CLEARINGHOUSE, &ws("clearinghouse-state-main"));
        assert_eq!(main.len(), 1, "a margin row, no position");
        assert!(matches!(&main[0].event, Event::Margin(m) if m.dex.is_none()));
        let xyz = streamed_rows(WS_CLEARINGHOUSE, &ws("clearinghouse-state-xyz"));
        assert!(matches!(&xyz[0].event, Event::Margin(m) if m.dex.as_deref() == Some("xyz")));
        assert!(streamed_rows(WS_OPEN_ORDERS, &ws("open-orders-xyz")).is_empty());
        let spot = streamed_rows(WS_SPOT, &ws("spot-state"));
        let balances = spot
            .iter()
            .filter(|e| matches!(e.event, Event::Balance(_)))
            .count();
        assert_eq!(balances, 3);
        assert!(
            spot.iter()
                .any(|e| matches!(e.event, Event::PortfolioMargin(_)))
        );
        // What it states whole, each scope its own.
        let payload = |channel: &str, frame: &[u8]| Payload {
            seq: 1,
            recv_micros: 9,
            address: PayloadAddress::Account(crate::record::AccountAddress {
                venue: "hyperliquid".into(),
                account: "main".into(),
                fingerprint: "0123456789abcdef".into(),
            }),
            channel: channel.into(),
            kind: "margin".into(),
            symbol: None,
            origin: galata_wire::Origin::Streamed,
            payload: stream_bytes(None, None, frame),
        };
        let stated = states(
            &payload(WS_CLEARINGHOUSE, &ws("clearinghouse-state-xyz")),
            &xyz,
        );
        let scopes: Vec<(galata_wire::Kind, Option<String>)> =
            stated.into_iter().map(|s| (s.kind, s.scope)).collect();
        assert_eq!(
            scopes,
            [
                (galata_wire::Kind::Margin, Some("xyz".to_string())),
                (galata_wire::Kind::Positions, Some("xyz".to_string()))
            ]
        );
    }

    #[test]
    fn the_fills_snapshot_tagged_as_one_gives_each_fill_spot_ones_among_them() {
        let frame = ws("user-fills-snapshot");
        let tagged: serde_json::Value = serde_json::from_slice(&frame).unwrap();
        assert_eq!(tagged["data"]["isSnapshot"], true);
        let rows = streamed_rows(WS_FILLS, &frame);
        assert_eq!(
            rows.len(),
            tagged["data"]["fills"].as_array().unwrap().len()
        );
        assert!(
            rows.iter()
                .any(|r| matches!(&r.event, Event::Fill(f) if f.market.starts_with('@')))
        );
        // And each is the row the walk gives for the same fill: one identity.
        let key = crate::ledger::FingerprintKey::new(Secret::new("deployment-key"));
        let walked = super::super::events::fill_rows(
            &venue(),
            &main(),
            Some(&key),
            "0123456789abcdef",
            9,
            &serde_json::to_vec(&tagged["data"]["fills"]).unwrap(),
        )
        .unwrap();
        assert_eq!(rows, walked);
    }

    #[test]
    fn a_streamed_funding_row_is_the_row_its_walk_gives() {
        let frame = ws("user-fundings-snapshot");
        let streamed: Vec<Envelope> = streamed_rows(WS_FUNDINGS, &frame);
        let flat: serde_json::Value = serde_json::from_slice(&frame).unwrap();
        let walked_page: Vec<serde_json::Value> = flat["data"]["fundings"]
            .as_array()
            .unwrap()
            .iter()
            .map(|r| {
                serde_json::json!({"time": r["time"], "hash": "0x0", "delta": {
                    "type": "funding", "coin": r["coin"], "usdc": r["usdc"], "szi": r["szi"],
                    "fundingRate": r["fundingRate"], "nSamples": r["nSamples"]}})
            })
            .collect();
        let walked = super::super::events::funding_rows(
            &venue(),
            &main(),
            9,
            &serde_json::to_vec(&walked_page).unwrap(),
        )
        .unwrap();
        assert_eq!(streamed.len(), 4);
        for (s, w) in streamed.iter().zip(&walked) {
            assert_eq!(
                crate::ledger::events::identity(s),
                crate::ledger::events::identity(w)
            );
        }
        let updates = streamed_rows(WS_LEDGER_UPDATES, &ws("ledger-updates-snapshot"));
        assert_eq!(updates.len(), 3);
    }

    #[test]
    fn an_account_subscribes_each_stream_and_names_a_dex_only_where_one_is_declared() {
        let frames = subscriptions("0xabc", &["".to_string(), "xyz".to_string()]);
        assert_eq!(frames.len(), 9);
        let parsed: Vec<serde_json::Value> = frames
            .iter()
            .map(|f| serde_json::from_str(f).unwrap())
            .collect();
        let with_dex = parsed
            .iter()
            .filter(|f| f["subscription"]["dex"] == "xyz")
            .count();
        assert_eq!(with_dex, 2, "clearinghouseState and openOrders on xyz");
        assert!(parsed.iter().all(|f| f["subscription"]["user"] == "0xabc"));
        assert!(parsed.iter().all(|f| f["method"] == "subscribe"));
    }
}
