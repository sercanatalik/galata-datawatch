//! The ledger, projected: an account's rows as typed Parquet, under its own
//! owner-only root.
//!
//! ```text
//!   var/ledger          the record: venue payloads, verbatim
//!     ──events::read──▶ rows (normalised, one per identity, our own aliased)
//!     ──write──▶ <tape>/venue=<v>/account=<alias>/kind=<kind>/rows.parquet
//! ```
//!
//! **Never under the market tape.** An account's rows name an account, not
//! an instrument, and the market tape is what galata-markets serves
//! (`tape/schema.rs`, "Projecting the ledger is a later change, with its own
//! schema"). This root is held as `var/ledger` is: `0700`, refused otherwise.
//!
//! **Written by the ledger process, from the rows the fold read.** `read()`
//! aliases our own counterparties through the fingerprint key, and the lane
//! holds no credential, which is why the fold runs there (roadmap Tier 14).
//!
//! **One file per kind, rewritten whole each pass** (`galata_segments::
//! write_file`): hundreds of rows today, so a rewrite is deterministic, never
//! duplicates, and needs no replacement rule. An empty kind is still a file,
//! with zero rows: *this account has no fills* is a statement a reader can
//! find.
//!
//! **`orders` is the newest read's set, and only that.** An open order is
//! state, and a read that found none leaves no row to be newest: kept with
//! every earlier set, the last non-empty one would read as resting still. So
//! the caller states when the open orders were last read, the file holds that
//! read's rows, zero where it found none, and **no file** is written for an
//! account whose open orders were never read, so that *nothing rests* and
//! *nothing known* stay apart.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::{
    ArrayRef, BooleanBuilder, Decimal128Builder, Int64Builder, StringBuilder, UInt16Builder,
    UInt32Builder, UInt64Builder,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use galata_wire::{
    Collateral, Counterparty, Effect, Envelope, Event, Fill, FundingPayment, Kind, Margin,
    MarginState, Num, Order, PnlPoint, Position, PositionState,
};

use crate::ledger::accounts::LedgerError;

/// The kinds this projection writes: every kind the fold reads, and the
/// account's state read beside them — its balances, its borrowing and the
/// margin figures the venue states with them, and its open orders. The fold
/// pass reads all of these and folds only its own.
pub const PROJECTED: [Kind; 13] = [
    Kind::Fills,
    Kind::FundingPayments,
    Kind::Margin,
    Kind::Positions,
    Kind::LedgerUpdates,
    Kind::Balances,
    Kind::BorrowLend,
    Kind::LendHealth,
    Kind::PortfolioMargin,
    Kind::TokenMargin,
    Kind::Orders,
    Kind::OrderUpdates,
    Kind::PnlHistory,
];

/// The kinds projected for a venue read by wallet (Derive): each subaccount's
/// margin, positions, collateral and open orders, as the venue states them,
/// and its order updates, fills, funding and value history.
pub const WALLET_PROJECTED: [Kind; 8] = [
    Kind::MarginState,
    Kind::PositionState,
    Kind::Collateral,
    Kind::Orders,
    Kind::OrderUpdates,
    Kind::Fills,
    Kind::FundingPayments,
    Kind::PnlHistory,
];

/// An account's newest rows of one kind, and the time they were stated: the
/// rows of the latest read. **The time is the read's receipt**, since these
/// answers state none of their own. `None` where the kind holds no rows.
///
/// What a reader of the account's state asks — the margin engine, for one —
/// so that it never takes a balance from one read beside another's.
pub fn newest(
    root: &Path,
    venue: &str,
    account: &str,
    kind: Kind,
) -> Result<Option<(i64, RecordBatch)>, ProjectError> {
    let path = path_of(root, venue, account, kind);
    let schema = schema_for(kind).ok_or(ProjectError::Arrow {
        kind,
        detail: "not a projected kind".into(),
    })?;
    let batches = galata_segments::read_segment(&path)?;
    let all =
        arrow::compute::concat_batches(&schema, &batches).map_err(|e| ProjectError::Arrow {
            kind,
            detail: e.to_string(),
        })?;
    let recv = all
        .column_by_name("recv_micros")
        .and_then(|c| c.as_any().downcast_ref::<arrow::array::Int64Array>())
        .ok_or(ProjectError::Arrow {
            kind,
            detail: "no recv_micros column".into(),
        })?;
    let Some(latest) = arrow::compute::max(recv) else {
        return Ok(None);
    };
    let mask =
        arrow::compute::kernels::cmp::eq(recv, &arrow::array::Int64Array::new_scalar(latest))
            .map_err(|e| ProjectError::Arrow {
                kind,
                detail: e.to_string(),
            })?;
    let rows =
        arrow::compute::filter_record_batch(&all, &mask).map_err(|e| ProjectError::Arrow {
            kind,
            detail: e.to_string(),
        })?;
    Ok(Some((latest, rows)))
}

/// **Every row one account's kind holds**, as one batch in the kind's
/// columns: what galata-markets answers an account's history from, up to the
/// projection's bound (`add-markets-accounts`, design D4). `None` where no
/// file was written: for open orders, never read; for any other kind, never
/// projected.
pub fn read(
    root: &Path,
    venue: &str,
    account: &str,
    kind: Kind,
) -> Result<Option<RecordBatch>, ProjectError> {
    let path = path_of(root, venue, account, kind);
    let schema = schema_for(kind).ok_or(ProjectError::Arrow {
        kind,
        detail: "not a projected kind".into(),
    })?;
    if !path.is_file() {
        return Ok(None);
    }
    let batches = galata_segments::read_segment(&path)?;
    arrow::compute::concat_batches(&schema, &batches)
        .map(Some)
        .map_err(|e| ProjectError::Arrow {
            kind,
            detail: e.to_string(),
        })
}

/// **Rows in the projection's columns, from envelopes**: what the account
/// stream carries, made to read as the projection does, so one answer holds
/// both (`add-markets-accounts`, design D4). Rows of other kinds are left
/// out, and a ledger update is one row per dex effect, as [`write_kinds`]
/// writes it. A state set is given here row by row, each in an envelope of
/// its own.
pub fn batch(
    kind: Kind,
    venue: &str,
    account: &str,
    rows: &[Envelope],
) -> Result<RecordBatch, ProjectError> {
    if schema_for(kind).is_none() {
        return Err(ProjectError::Arrow {
            kind,
            detail: "not a projected kind".into(),
        });
    }
    let expanded: Vec<std::borrow::Cow<'_, Envelope>>;
    let of_kind: Vec<&Envelope> = if kind == Kind::LedgerUpdates {
        expanded = per_effect(rows);
        expanded.iter().map(|e| e.as_ref()).collect()
    } else {
        rows.iter().filter(|e| e.kind() == kind).collect()
    };
    batch_for(kind, venue, account, &of_kind)
}

/// The file each kind is written to, under its directory.
pub const FILE: &str = "rows.parquet";

const SCALE: u32 = 18;
const MONEY: DataType = DataType::Decimal128(38, SCALE as i8);

/// Why a projection was not written.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ProjectError {
    /// The root is missing and cannot be made, or others can read it.
    #[error(transparent)]
    Root(#[from] LedgerError),
    /// A number with more decimal places than the column holds. Refused, not
    /// rounded: a rounded figure in a ledger is a wrong figure nobody sees.
    #[error("{value} has more than {SCALE} decimal places; the projection does not round money")]
    Precision {
        /// The value.
        value: String,
    },
    /// Arrow refused the batch.
    #[error("the {kind} batch could not be built: {detail}")]
    Arrow {
        /// The dataset.
        kind: Kind,
        /// Arrow's reason.
        detail: String,
    },
    /// The file could not be written.
    #[error(transparent)]
    Write(#[from] galata_segments::SegmentError),
}

/// Where one account's kind is written.
pub fn path_of(root: &Path, venue: &str, account: &str, kind: Kind) -> PathBuf {
    root.join(format!("venue={venue}"))
        .join(format!("account={account}"))
        .join(format!("kind={}", kind.as_str()))
        .join(FILE)
}

/// Write every projected kind for one account, each file whole.
///
/// `rows` are what `events::read` returned for that account; rows of other
/// kinds are ignored. Returns the files written.
pub fn write(
    root: &Path,
    venue: &str,
    account: &str,
    rows: &[Envelope],
) -> Result<Vec<PathBuf>, ProjectError> {
    write_kinds(root, venue, account, rows, &PROJECTED, None)
}

/// Write the given kinds for one account, each file whole.
///
/// `orders_read_micros` is the receipt of the newest answer that stated the
/// account's open orders whole ([`crate::ledger::events::History::read_at`]
/// of the venue's `orders_answer` channel);
/// `None` where none was read, and then no `orders` file is written.
pub fn write_kinds(
    root: &Path,
    venue: &str,
    account: &str,
    rows: &[Envelope],
    kinds: &[Kind],
    orders_read_micros: Option<i64>,
) -> Result<Vec<PathBuf>, ProjectError> {
    crate::ledger::accounts::check_root(root)?;
    let mut written = Vec::new();
    for &kind in kinds {
        let expanded: Vec<std::borrow::Cow<'_, Envelope>>;
        let of_kind: Vec<&Envelope> = if kind == Kind::LedgerUpdates {
            expanded = per_effect(rows);
            expanded.iter().map(|e| e.as_ref()).collect()
        } else if kind == Kind::Orders {
            // The newest read's set only, and nothing where none was read.
            let Some(read) = orders_read_micros else {
                continue;
            };
            rows.iter()
                .filter(|e| e.kind() == kind && e.recv_micros == read)
                .collect()
        } else {
            rows.iter().filter(|e| e.kind() == kind).collect()
        };
        let batch = batch_for(kind, venue, account, &of_kind)?;
        let path = path_of(root, venue, account, kind);
        galata_segments::write_file(&path, &batch, galata_segments::Codec::Zstd)?;
        written.push(path);
    }
    Ok(written)
}

/// Write the given kinds for **each account the rows name**, each under its
/// own alias: a wallet's rows, read in one answer, are its subaccounts' rows
/// (design D1 of `add-derive-account-ledger`). An account the rows do not
/// name is not written.
pub fn write_by_account(
    root: &Path,
    venue: &str,
    rows: &[Envelope],
    kinds: &[Kind],
    orders_read_micros: Option<i64>,
) -> Result<Vec<PathBuf>, ProjectError> {
    let mut by_account: std::collections::BTreeMap<&str, Vec<Envelope>> =
        std::collections::BTreeMap::new();
    for row in rows {
        if let Some(account) = row.account() {
            by_account
                .entry(account.as_str())
                .or_default()
                .push(row.clone());
        }
    }
    let mut written = Vec::new();
    for (account, rows) in by_account {
        // One wallet answer states every subaccount's open orders, so one
        // receipt is each subaccount's read.
        written.extend(write_kinds(
            root,
            venue,
            account,
            &rows,
            kinds,
            orders_read_micros,
        )?);
    }
    Ok(written)
}

/// The columns every account dataset opens with.
fn common() -> Vec<Field> {
    vec![
        Field::new("venue", DataType::Utf8, false),
        Field::new("account", DataType::Utf8, false),
        // `None` is the venue's main dex.
        Field::new("dex", DataType::Utf8, true),
        // The instrument, where the row names one: margin and ledger updates
        // do not.
        Field::new("ticker", DataType::Utf8, true),
        // The venue's own time; null where it stated none.
        Field::new("at_micros", DataType::Int64, true),
        Field::new("recv_micros", DataType::Int64, false),
        Field::new("schema_version", DataType::UInt16, false),
    ]
}

/// A projected kind's schema.
pub fn schema_for(kind: Kind) -> Option<SchemaRef> {
    let mut fields = common();
    match kind {
        Kind::Fills => fields.extend([
            // `bid` bought, `ask` sold.
            Field::new("side", DataType::Utf8, false),
            Field::new("price", MONEY, false),
            Field::new("size", MONEY, false),
            Field::new("start_position", MONEY, true),
            Field::new("direction", DataType::Utf8, true),
            Field::new("closed_pnl", MONEY, true),
            Field::new("fee", MONEY, true),
            Field::new("fee_token", DataType::Utf8, true),
            Field::new("builder_fee", MONEY, true),
            Field::new("crossed", DataType::Boolean, true),
            // Text: Derive's are UUIDs.
            Field::new("order_id", DataType::Utf8, false),
            // Not unique alone: identity is (trade_id, order_id).
            Field::new("trade_id", DataType::Utf8, false),
            Field::new("twap_id", DataType::UInt64, true),
            // Whether the fill liquidated the account; null where the venue
            // does not say.
            Field::new("liquidated", DataType::Boolean, true),
        ]),
        Kind::FundingPayments => fields.extend([
            // As the venue signs it: negative when the position paid.
            Field::new("usdc", MONEY, false),
            // Null where the venue states none (Derive).
            Field::new("size", MONEY, true),
            Field::new("rate", MONEY, true),
            Field::new("samples", DataType::UInt32, true),
        ]),
        Kind::Margin => fields.extend([
            // The AccountMode, as the wire spells it: `unified`, `default`, …
            Field::new("mode", DataType::Utf8, false),
            // Why this account's equity is not in these figures, where it is
            // not: a unified account's collateral is spot USDC.
            Field::new("equity_not_held", DataType::Utf8, true),
            Field::new("account_value", MONEY, true),
            Field::new("total_notional", MONEY, true),
            Field::new("total_raw_usd", MONEY, true),
            Field::new("margin_used", MONEY, true),
            Field::new("maintenance_margin_used", MONEY, true),
            Field::new("withdrawable", MONEY, true),
        ]),
        Kind::Positions => fields.extend([
            // Signed.
            Field::new("size", MONEY, false),
            Field::new("entry_price", MONEY, true),
            Field::new("mark", MONEY, true),
            Field::new("position_value", MONEY, true),
            Field::new("unrealised_pnl", MONEY, true),
            Field::new("return_on_equity", MONEY, true),
            Field::new("liquidation_price", MONEY, true),
            Field::new("leverage", MONEY, true),
            Field::new("leverage_type", DataType::Utf8, true),
            Field::new("max_leverage", DataType::UInt32, true),
            Field::new("margin_used", MONEY, true),
            Field::new("funding_all_time", MONEY, true),
            Field::new("funding_since_open", MONEY, true),
            Field::new("funding_since_change", MONEY, true),
        ]),
        // Long: one row per (update, dex it moved); `dex` above is that dex.
        Kind::LedgerUpdates => fields.extend([
            // The venue's type, verbatim.
            Field::new("update_kind", DataType::Utf8, false),
            // False where this build does not know what the update did.
            Field::new("effect_known", DataType::Boolean, false),
            // What it moved on this row's dex; null where it moved nothing.
            Field::new("effect_usdc", MONEY, true),
            // `account` (our own, by alias) or `fingerprint`. Never an address.
            Field::new("counterparty_kind", DataType::Utf8, true),
            Field::new("counterparty", DataType::Utf8, true),
            Field::new("token", DataType::Utf8, true),
            Field::new("amount", MONEY, true),
            Field::new("fee", MONEY, true),
        ]),
        // One coin held in spot per row, as stated: a negative hold included.
        Kind::Balances => fields.extend([
            Field::new("asset", DataType::Utf8, false),
            // The venue's index, the key the borrowing and token rows name.
            Field::new("token_index", DataType::UInt32, true),
            Field::new("total", MONEY, false),
            Field::new("hold", MONEY, true),
            Field::new("entry_notional", MONEY, true),
            // Stated on a collateral token only.
            Field::new("spot_hold", MONEY, true),
            Field::new("ltv", MONEY, true),
            Field::new("supplied", MONEY, true),
        ]),
        Kind::BorrowLend => fields.extend([
            Field::new("token_index", DataType::UInt32, false),
            // From the venue's reference data; null where it named none.
            Field::new("token", DataType::Utf8, true),
            Field::new("borrow_basis", MONEY, true),
            Field::new("borrow_value", MONEY, true),
            Field::new("supply_basis", MONEY, true),
            Field::new("supply_value", MONEY, true),
        ]),
        Kind::LendHealth => fields.extend([
            Field::new("health", DataType::Utf8, true),
            // Null where the venue stated null: absent, not zero.
            Field::new("health_factor", MONEY, true),
        ]),
        // The venue's own figures; none is computed here.
        Kind::PortfolioMargin => fields.extend([
            Field::new("enabled", DataType::Boolean, true),
            Field::new("ratio", MONEY, true),
        ]),
        Kind::TokenMargin => fields.extend([
            Field::new("token_index", DataType::UInt32, false),
            Field::new("token", DataType::Utf8, true),
            Field::new("borrow_ratio", MONEY, true),
            Field::new("supply_ratio", MONEY, true),
            Field::new("available_after_maintenance", MONEY, true),
        ]),
        // A subaccount's margin as the venue states it: on Derive, net of the
        // requirement, positive being headroom (`galata_wire::MarginState`).
        Kind::MarginState => fields.extend([
            Field::new("margin_type", DataType::Utf8, true),
            Field::new("manager", DataType::Utf8, true),
            Field::new("risk_universe", DataType::Utf8, true),
            Field::new("value", MONEY, true),
            Field::new("initial_margin", MONEY, true),
            Field::new("maintenance_margin", MONEY, true),
            Field::new("positions_value", MONEY, true),
            Field::new("collateral_value", MONEY, true),
            Field::new("open_orders_margin", MONEY, true),
            Field::new("under_liquidation", DataType::Boolean, true),
        ]),
        // One position per row, options and perpetuals alike; `ticker` above
        // is the venue's instrument name. The greeks are the venue's.
        Kind::PositionState => fields.extend([
            Field::new("instrument_type", DataType::Utf8, true),
            // Signed: positive is long.
            Field::new("amount", MONEY, true),
            Field::new("average_price", MONEY, true),
            Field::new("mark_price", MONEY, true),
            Field::new("mark_value", MONEY, true),
            Field::new("unrealised_pnl", MONEY, true),
            Field::new("delta", MONEY, true),
            Field::new("gamma", MONEY, true),
            Field::new("theta", MONEY, true),
            Field::new("vega", MONEY, true),
            // Its contribution to the subaccount's margin: negative where it
            // uses margin.
            Field::new("initial_margin", MONEY, true),
            Field::new("maintenance_margin", MONEY, true),
            Field::new("liquidation_price", MONEY, true),
            Field::new("cumulative_funding", MONEY, true),
        ]),
        // One collateral asset per row.
        Kind::Collateral => fields.extend([
            Field::new("asset", DataType::Utf8, false),
            Field::new("asset_type", DataType::Utf8, true),
            Field::new("amount", MONEY, true),
            Field::new("mark_price", MONEY, true),
            Field::new("mark_value", MONEY, true),
            // Its contribution to the subaccount's margin: positive where it
            // adds headroom.
            Field::new("initial_margin", MONEY, true),
            Field::new("maintenance_margin", MONEY, true),
            Field::new("cumulative_interest", MONEY, true),
            Field::new("pending_interest", MONEY, true),
        ]),
        // One order per row: a resting one in `orders`, one as it stood at a
        // change of its status in `order_updates`, whose `at_micros` is the
        // venue's time for that change. Type, trigger and status are the
        // venue's own words; `ticker` above is the instrument.
        Kind::Orders | Kind::OrderUpdates => fields.extend(order_fields()),
        // One point of a window the venue states; `at_micros` is its time.
        Kind::PnlHistory => fields.extend([
            Field::new("window", DataType::Utf8, false),
            Field::new("account_value", MONEY, true),
            Field::new("pnl", MONEY, true),
            Field::new("initial_margin", MONEY, true),
            Field::new("maintenance_margin", MONEY, true),
        ]),
        _ => return None,
    }
    Some(Arc::new(Schema::new(fields)))
}

/// An order's columns, the same in `orders` and `order_updates`.
fn order_fields() -> [Field; 16] {
    [
        // `bid` buys, `ask` sells.
        Field::new("side", DataType::Utf8, false),
        // Text: Derive's are UUIDs.
        Field::new("order_id", DataType::Utf8, false),
        Field::new("client_id", DataType::Utf8, true),
        Field::new("order_type", DataType::Utf8, false),
        Field::new("trigger_type", DataType::Utf8, true),
        Field::new("is_trigger", DataType::Boolean, true),
        Field::new("position_tpsl", DataType::Boolean, true),
        Field::new("size", MONEY, true),
        Field::new("remaining", MONEY, true),
        Field::new("limit_price", MONEY, true),
        Field::new("trigger_price", MONEY, true),
        Field::new("trigger_condition", DataType::Utf8, true),
        Field::new("reduce_only", DataType::Boolean, true),
        Field::new("time_in_force", DataType::Utf8, true),
        Field::new("status", DataType::Utf8, true),
        Field::new("placed_micros", DataType::Int64, true),
    ]
}

// **Borrowing accessors**, as plain functions so their lifetimes are the
// envelope's. Each column's closure used to clone the whole event out of the
// envelope — a fill's strings, a ledger update's effects — once per column,
// some thirteen times a row, on every pass over the account's history.
fn fill_of(e: &Envelope) -> Option<&Fill> {
    match &e.event {
        Event::Fill(f) => Some(f),
        _ => None,
    }
}
fn funding_of(e: &Envelope) -> Option<&FundingPayment> {
    match &e.event {
        Event::FundingPayment(p) => Some(p),
        _ => None,
    }
}
fn margin_of(e: &Envelope) -> Option<&Margin> {
    match &e.event {
        Event::Margin(m) => Some(m),
        _ => None,
    }
}
fn position_of(e: &Envelope) -> Option<&Position> {
    match &e.event {
        Event::Position(p) => Some(p),
        _ => None,
    }
}
fn update_of(e: &Envelope) -> Option<&galata_wire::LedgerUpdate> {
    match &e.event {
        Event::LedgerUpdate(u) => Some(u),
        _ => None,
    }
}
fn balance_of(e: &Envelope) -> Option<&galata_wire::Balance> {
    match &e.event {
        Event::Balance(b) => Some(b),
        _ => None,
    }
}
fn borrow_lend_of(e: &Envelope) -> Option<&galata_wire::BorrowLend> {
    match &e.event {
        Event::BorrowLend(b) => Some(b),
        _ => None,
    }
}
fn lend_health_of(e: &Envelope) -> Option<&galata_wire::LendHealth> {
    match &e.event {
        Event::LendHealth(h) => Some(h),
        _ => None,
    }
}
fn portfolio_margin_of(e: &Envelope) -> Option<&galata_wire::PortfolioMargin> {
    match &e.event {
        Event::PortfolioMargin(p) => Some(p),
        _ => None,
    }
}
fn token_margin_of(e: &Envelope) -> Option<&galata_wire::TokenMargin> {
    match &e.event {
        Event::TokenMargin(t) => Some(t),
        _ => None,
    }
}

fn order_of(e: &Envelope) -> Option<&Order> {
    match &e.event {
        Event::Order(o) | Event::OrderUpdate(o) => Some(o),
        _ => None,
    }
}
fn pnl_point_of(e: &Envelope) -> Option<&PnlPoint> {
    match &e.event {
        Event::PnlPoint(p) => Some(p),
        _ => None,
    }
}

fn bools(rows: &[&Envelope], f: impl Fn(&Envelope) -> Option<bool>) -> ArrayRef {
    let mut b = BooleanBuilder::with_capacity(rows.len());
    rows.iter().for_each(|e| b.append_option(f(e)));
    Arc::new(b.finish())
}

fn u32s(rows: &[&Envelope], f: impl Fn(&Envelope) -> Option<u32>) -> ArrayRef {
    let mut b = UInt32Builder::with_capacity(rows.len());
    rows.iter().for_each(|e| b.append_option(f(e)));
    Arc::new(b.finish())
}

fn dex_of(e: &Envelope) -> Option<&str> {
    match &e.event {
        Event::Fill(f) => f.dex.as_deref(),
        Event::FundingPayment(p) => p.dex.as_deref(),
        Event::Margin(m) => m.dex.as_deref(),
        Event::Position(p) => p.dex.as_deref(),
        Event::Order(o) | Event::OrderUpdate(o) => o.dex.as_deref(),
        // Expanded to one effect per row by `per_effect`.
        Event::LedgerUpdate(u) => match &u.effect {
            Effect::Known(effects) => effects.first().and_then(|d| d.dex.as_deref()),
            _ => None,
        },
        _ => None,
    }
}

/// The instrument the event names: an account-addressed envelope carries no
/// instrument address of its own.
fn ticker_of(e: &Envelope) -> Option<&str> {
    match &e.event {
        Event::Fill(f) => Some(f.market.as_str()),
        Event::FundingPayment(p) => Some(p.ticker.as_str()),
        Event::Position(p) => Some(p.ticker.as_str()),
        Event::PositionState(p) => Some(p.instrument.as_str()),
        Event::Order(o) | Event::OrderUpdate(o) => Some(o.market.as_str()),
        _ => None,
    }
}

fn margin_state_of(e: &Envelope) -> Option<&MarginState> {
    match &e.event {
        Event::MarginState(m) => Some(m),
        _ => None,
    }
}
fn position_state_of(e: &Envelope) -> Option<&PositionState> {
    match &e.event {
        Event::PositionState(p) => Some(p),
        _ => None,
    }
}
fn collateral_of(e: &Envelope) -> Option<&Collateral> {
    match &e.event {
        Event::Collateral(c) => Some(c),
        _ => None,
    }
}

fn batch_for(
    kind: Kind,
    venue: &str,
    account: &str,
    rows: &[&Envelope],
) -> Result<RecordBatch, ProjectError> {
    let schema = schema_for(kind).expect("only projected kinds are built");
    let mut columns: Vec<ArrayRef> = vec![
        constant(rows.len(), venue),
        constant(rows.len(), account),
        text(rows, dex_of),
        text(rows, ticker_of),
        int(rows, |e| e.at_micros),
        int(rows, |e| Some(e.recv_micros)),
        {
            let mut b = UInt16Builder::with_capacity(rows.len());
            rows.iter()
                .for_each(|_| b.append_value(galata_wire::SCHEMA_VERSION));
            Arc::new(b.finish())
        },
    ];
    match kind {
        Kind::Fills => {
            let each = |g: fn(&Fill) -> Option<Num>| move |e: &Envelope| fill_of(e).and_then(g);
            columns.push(text(rows, |e| fill_of(e).map(|x| x.side.as_str())));
            columns.push(dec(rows, each(|x| Some(x.price)))?);
            columns.push(dec(rows, each(|x| Some(x.size)))?);
            columns.push(dec(rows, each(|x| x.start_position))?);
            columns.push(text(rows, |e| {
                fill_of(e).and_then(|x| x.direction.as_deref())
            }));
            columns.push(dec(rows, each(|x| x.closed_pnl))?);
            columns.push(dec(rows, each(|x| x.fee))?);
            columns.push(text(rows, |e| {
                fill_of(e).and_then(|x| x.fee_token.as_deref())
            }));
            columns.push(dec(rows, each(|x| x.builder_fee))?);
            columns.push({
                let mut b = BooleanBuilder::with_capacity(rows.len());
                rows.iter()
                    .for_each(|e| b.append_option(fill_of(e).and_then(|x| x.crossed)));
                Arc::new(b.finish())
            });
            columns.push(text(rows, |e| fill_of(e).map(|x| x.order_id.as_str())));
            columns.push(text(rows, |e| fill_of(e).map(|x| x.trade_id.as_str())));
            columns.push(u64s(rows, |e| fill_of(e).and_then(|x| x.twap_id)));
            columns.push(bools(rows, |e| fill_of(e).and_then(|x| x.liquidated)));
        }
        Kind::FundingPayments => {
            let each = |g: fn(&FundingPayment) -> Option<Num>| {
                move |e: &Envelope| funding_of(e).and_then(g)
            };
            columns.push(dec(rows, each(|x| Some(x.usdc)))?);
            columns.push(dec(rows, each(|x| x.size))?);
            columns.push(dec(rows, each(|x| x.rate))?);
            columns.push({
                let mut b = UInt32Builder::with_capacity(rows.len());
                rows.iter()
                    .for_each(|e| b.append_option(funding_of(e).and_then(|x| x.samples)));
                Arc::new(b.finish())
            });
        }
        Kind::Margin => {
            let each = |g: fn(&Margin) -> Option<Num>| move |e: &Envelope| margin_of(e).and_then(g);
            // The mode's wire spelling, read once per distinct mode rather
            // than serialised per row.
            let mut spelled: Vec<(galata_wire::AccountMode, Option<String>)> = Vec::new();
            let mut spell = |mode: galata_wire::AccountMode| -> Option<String> {
                if let Some((_, s)) = spelled.iter().find(|(m, _)| *m == mode) {
                    return s.clone();
                }
                let s = serde_json::to_value(mode)
                    .ok()
                    .and_then(|v| v.as_str().map(str::to_string));
                spelled.push((mode, s.clone()));
                s
            };
            let mut b = StringBuilder::with_capacity(rows.len(), rows.len() * 8);
            for e in rows {
                b.append_option(margin_of(e).and_then(|x| spell(x.mode)));
            }
            columns.push(Arc::new(b.finish()));
            columns.push(text(rows, |e| {
                margin_of(e).and_then(|x| x.equity_not_held.as_deref())
            }));
            columns.push(dec(rows, each(|x| x.account_value))?);
            columns.push(dec(rows, each(|x| x.total_notional))?);
            columns.push(dec(rows, each(|x| x.total_raw_usd))?);
            columns.push(dec(rows, each(|x| x.margin_used))?);
            columns.push(dec(rows, each(|x| x.maintenance_margin_used))?);
            columns.push(dec(rows, each(|x| x.withdrawable))?);
        }
        Kind::Positions => {
            let each =
                |g: fn(&Position) -> Option<Num>| move |e: &Envelope| position_of(e).and_then(g);
            columns.push(dec(rows, each(|x| Some(x.size)))?);
            columns.push(dec(rows, each(|x| x.entry_price))?);
            columns.push(dec(rows, each(|x| x.mark))?);
            columns.push(dec(rows, each(|x| x.position_value))?);
            columns.push(dec(rows, each(|x| x.unrealised_pnl))?);
            columns.push(dec(rows, each(|x| x.return_on_equity))?);
            columns.push(dec(rows, each(|x| x.liquidation_price))?);
            columns.push(dec(rows, each(|x| x.leverage))?);
            columns.push(text(rows, |e| {
                position_of(e).and_then(|x| x.leverage_type.as_deref())
            }));
            columns.push({
                let mut b = UInt32Builder::with_capacity(rows.len());
                rows.iter()
                    .for_each(|e| b.append_option(position_of(e).and_then(|x| x.max_leverage)));
                Arc::new(b.finish())
            });
            columns.push(dec(rows, each(|x| x.margin_used))?);
            columns.push(dec(rows, each(|x| x.funding_all_time))?);
            columns.push(dec(rows, each(|x| x.funding_since_open))?);
            columns.push(dec(rows, each(|x| x.funding_since_change))?);
        }
        Kind::LedgerUpdates => {
            columns.push(text(rows, |e| update_of(e).map(|x| x.kind.as_str())));
            columns.push({
                let mut b = BooleanBuilder::with_capacity(rows.len());
                rows.iter().for_each(|e| {
                    b.append_option(update_of(e).map(|x| matches!(x.effect, Effect::Known(_))))
                });
                Arc::new(b.finish())
            });
            columns.push(dec(rows, |e| match update_of(e).map(|x| &x.effect) {
                Some(Effect::Known(effects)) => effects.first().map(|d| d.usdc),
                _ => None,
            })?);
            columns.push(text(rows, |e| {
                update_of(e)
                    .and_then(|x| x.counterparty.as_ref())
                    .map(|c| match c {
                        Counterparty::Account(_) => "account",
                        Counterparty::Fingerprint(_) => "fingerprint",
                    })
            }));
            columns.push(text(rows, |e| {
                update_of(e)
                    .and_then(|x| x.counterparty.as_ref())
                    .map(|c| match c {
                        Counterparty::Account(alias) => alias.as_str(),
                        Counterparty::Fingerprint(fp) => fp.as_str(),
                    })
            }));
            columns.push(text(rows, |e| {
                update_of(e).and_then(|x| x.token.as_deref())
            }));
            columns.push(dec(rows, |e| update_of(e).and_then(|x| x.amount))?);
            columns.push(dec(rows, |e| update_of(e).and_then(|x| x.fee))?);
        }
        Kind::Balances => {
            let each = |g: fn(&galata_wire::Balance) -> Option<Num>| {
                move |e: &Envelope| balance_of(e).and_then(g)
            };
            columns.push(text(rows, |e| balance_of(e).map(|x| x.asset.as_str())));
            columns.push(u32s(rows, |e| balance_of(e).and_then(|x| x.token_index)));
            columns.push(dec(rows, each(|x| Some(x.total)))?);
            columns.push(dec(rows, each(|x| x.hold))?);
            columns.push(dec(rows, each(|x| x.entry_notional))?);
            columns.push(dec(rows, each(|x| x.spot_hold))?);
            columns.push(dec(rows, each(|x| x.ltv))?);
            columns.push(dec(rows, each(|x| x.supplied))?);
        }
        Kind::BorrowLend => {
            let each = |g: fn(&galata_wire::BorrowLend) -> Option<Num>| {
                move |e: &Envelope| borrow_lend_of(e).and_then(g)
            };
            columns.push(u32s(rows, |e| borrow_lend_of(e).map(|x| x.token_index)));
            columns.push(text(rows, |e| {
                borrow_lend_of(e).and_then(|x| x.token.as_deref())
            }));
            columns.push(dec(rows, each(|x| x.borrow_basis))?);
            columns.push(dec(rows, each(|x| x.borrow_value))?);
            columns.push(dec(rows, each(|x| x.supply_basis))?);
            columns.push(dec(rows, each(|x| x.supply_value))?);
        }
        Kind::LendHealth => {
            columns.push(text(rows, |e| {
                lend_health_of(e).and_then(|x| x.health.as_deref())
            }));
            columns.push(dec(rows, |e| {
                lend_health_of(e).and_then(|x| x.health_factor)
            })?);
        }
        Kind::PortfolioMargin => {
            columns.push({
                let mut b = BooleanBuilder::with_capacity(rows.len());
                rows.iter()
                    .for_each(|e| b.append_option(portfolio_margin_of(e).and_then(|x| x.enabled)));
                Arc::new(b.finish())
            });
            columns.push(dec(rows, |e| portfolio_margin_of(e).and_then(|x| x.ratio))?);
        }
        Kind::TokenMargin => {
            let each = |g: fn(&galata_wire::TokenMargin) -> Option<Num>| {
                move |e: &Envelope| token_margin_of(e).and_then(g)
            };
            columns.push(u32s(rows, |e| token_margin_of(e).map(|x| x.token_index)));
            columns.push(text(rows, |e| {
                token_margin_of(e).and_then(|x| x.token.as_deref())
            }));
            columns.push(dec(rows, each(|x| x.borrow_ratio))?);
            columns.push(dec(rows, each(|x| x.supply_ratio))?);
            columns.push(dec(rows, each(|x| x.available_after_maintenance))?);
        }
        Kind::MarginState => {
            let each = |g: fn(&MarginState) -> Option<Num>| {
                move |e: &Envelope| margin_state_of(e).and_then(g)
            };
            columns.push(text(rows, |e| {
                margin_state_of(e).and_then(|x| x.margin_type.as_deref())
            }));
            columns.push(text(rows, |e| {
                margin_state_of(e).and_then(|x| x.manager.as_deref())
            }));
            columns.push(text(rows, |e| {
                margin_state_of(e).and_then(|x| x.risk_universe.as_deref())
            }));
            columns.push(dec(rows, each(|x| x.value))?);
            columns.push(dec(rows, each(|x| x.initial_margin))?);
            columns.push(dec(rows, each(|x| x.maintenance_margin))?);
            columns.push(dec(rows, each(|x| x.positions_value))?);
            columns.push(dec(rows, each(|x| x.collateral_value))?);
            columns.push(dec(rows, each(|x| x.open_orders_margin))?);
            columns.push({
                let mut b = BooleanBuilder::with_capacity(rows.len());
                rows.iter().for_each(|e| {
                    b.append_option(margin_state_of(e).and_then(|x| x.under_liquidation))
                });
                Arc::new(b.finish())
            });
        }
        Kind::PositionState => {
            let each = |g: fn(&PositionState) -> Option<Num>| {
                move |e: &Envelope| position_state_of(e).and_then(g)
            };
            columns.push(text(rows, |e| {
                position_state_of(e).and_then(|x| x.instrument_type.as_deref())
            }));
            columns.push(dec(rows, each(|x| x.amount))?);
            columns.push(dec(rows, each(|x| x.average_price))?);
            columns.push(dec(rows, each(|x| x.mark_price))?);
            columns.push(dec(rows, each(|x| x.mark_value))?);
            columns.push(dec(rows, each(|x| x.unrealised_pnl))?);
            columns.push(dec(rows, each(|x| x.delta))?);
            columns.push(dec(rows, each(|x| x.gamma))?);
            columns.push(dec(rows, each(|x| x.theta))?);
            columns.push(dec(rows, each(|x| x.vega))?);
            columns.push(dec(rows, each(|x| x.initial_margin))?);
            columns.push(dec(rows, each(|x| x.maintenance_margin))?);
            columns.push(dec(rows, each(|x| x.liquidation_price))?);
            columns.push(dec(rows, each(|x| x.cumulative_funding))?);
        }
        Kind::Collateral => {
            let each = |g: fn(&Collateral) -> Option<Num>| {
                move |e: &Envelope| collateral_of(e).and_then(g)
            };
            columns.push(text(rows, |e| collateral_of(e).map(|x| x.asset.as_str())));
            columns.push(text(rows, |e| {
                collateral_of(e).and_then(|x| x.asset_type.as_deref())
            }));
            columns.push(dec(rows, each(|x| x.amount))?);
            columns.push(dec(rows, each(|x| x.mark_price))?);
            columns.push(dec(rows, each(|x| x.mark_value))?);
            columns.push(dec(rows, each(|x| x.initial_margin))?);
            columns.push(dec(rows, each(|x| x.maintenance_margin))?);
            columns.push(dec(rows, each(|x| x.cumulative_interest))?);
            columns.push(dec(rows, each(|x| x.pending_interest))?);
        }
        Kind::Orders | Kind::OrderUpdates => {
            let each = |g: fn(&Order) -> Option<Num>| move |e: &Envelope| order_of(e).and_then(g);
            columns.push(text(rows, |e| order_of(e).map(|x| x.side.as_str())));
            columns.push(text(rows, |e| order_of(e).map(|x| x.order_id.as_str())));
            columns.push(text(rows, |e| {
                order_of(e).and_then(|x| x.client_id.as_deref())
            }));
            columns.push(text(rows, |e| order_of(e).map(|x| x.order_type.as_str())));
            columns.push(text(rows, |e| {
                order_of(e).and_then(|x| x.trigger_type.as_deref())
            }));
            columns.push(bools(rows, |e| order_of(e).and_then(|x| x.is_trigger)));
            columns.push(bools(rows, |e| order_of(e).and_then(|x| x.position_tpsl)));
            columns.push(dec(rows, each(|x| x.size))?);
            columns.push(dec(rows, each(|x| x.remaining))?);
            columns.push(dec(rows, each(|x| x.limit_price))?);
            columns.push(dec(rows, each(|x| x.trigger_price))?);
            columns.push(text(rows, |e| {
                order_of(e).and_then(|x| x.trigger_condition.as_deref())
            }));
            columns.push(bools(rows, |e| order_of(e).and_then(|x| x.reduce_only)));
            columns.push(text(rows, |e| {
                order_of(e).and_then(|x| x.time_in_force.as_deref())
            }));
            columns.push(text(rows, |e| {
                order_of(e).and_then(|x| x.status.as_deref())
            }));
            columns.push(int(rows, |e| order_of(e).and_then(|x| x.placed_micros)));
        }
        Kind::PnlHistory => {
            let each =
                |g: fn(&PnlPoint) -> Option<Num>| move |e: &Envelope| pnl_point_of(e).and_then(g);
            columns.push(text(rows, |e| pnl_point_of(e).map(|x| x.window.as_str())));
            columns.push(dec(rows, each(|x| x.account_value))?);
            columns.push(dec(rows, each(|x| x.pnl))?);
            columns.push(dec(rows, each(|x| x.initial_margin))?);
            columns.push(dec(rows, each(|x| x.maintenance_margin))?);
        }
        _ => unreachable!("only projected kinds are built"),
    }
    RecordBatch::try_new(schema, columns).map_err(|e| ProjectError::Arrow {
        kind,
        detail: e.to_string(),
    })
}

/// Ledger updates, one per dex effect: an update that moved margin on two
/// dexes becomes two, one that moved nothing (or in a way this build does not
/// know) stays one.
///
/// Borrowed where it is already one row; only an update that splits is copied.
fn per_effect(rows: &[Envelope]) -> Vec<std::borrow::Cow<'_, Envelope>> {
    use std::borrow::Cow;
    let mut out = Vec::new();
    for row in rows {
        let Event::LedgerUpdate(update) = &row.event else {
            continue;
        };
        match &update.effect {
            Effect::Known(effects) if effects.len() > 1 => {
                for effect in effects {
                    let mut one = row.clone();
                    if let Event::LedgerUpdate(u) = &mut one.event {
                        u.effect = Effect::Known(vec![effect.clone()]);
                    }
                    out.push(Cow::Owned(one));
                }
            }
            _ => out.push(Cow::Borrowed(row)),
        }
    }
    out
}

fn scaled(value: Num) -> Result<i128, ProjectError> {
    let scale = value.scale();
    if scale > SCALE {
        return Err(ProjectError::Precision {
            value: value.to_string(),
        });
    }
    value
        .mantissa()
        .checked_mul(10i128.pow(SCALE - scale))
        .ok_or_else(|| ProjectError::Precision {
            value: value.to_string(),
        })
}

fn dec(rows: &[&Envelope], f: impl Fn(&Envelope) -> Option<Num>) -> Result<ArrayRef, ProjectError> {
    let mut b = Decimal128Builder::with_capacity(rows.len())
        .with_precision_and_scale(38, SCALE as i8)
        .map_err(|e| ProjectError::Arrow {
            kind: Kind::Fills,
            detail: e.to_string(),
        })?;
    for row in rows {
        match f(row) {
            Some(value) => b.append_value(scaled(value)?),
            None => b.append_null(),
        }
    }
    Ok(Arc::new(b.finish()))
}

/// A string column borrowed from the rows, sized by a first pass.
fn text(rows: &[&Envelope], f: impl Fn(&Envelope) -> Option<&str>) -> ArrayRef {
    let bytes: usize = rows.iter().filter_map(|e| f(e)).map(str::len).sum();
    let mut b = StringBuilder::with_capacity(rows.len(), bytes);
    rows.iter().for_each(|e| b.append_option(f(e)));
    Arc::new(b.finish())
}

/// One value on every row.
fn constant(rows: usize, value: &str) -> ArrayRef {
    let mut b = StringBuilder::with_capacity(rows, rows * value.len());
    (0..rows).for_each(|_| b.append_value(value));
    Arc::new(b.finish())
}

fn int(rows: &[&Envelope], f: impl Fn(&Envelope) -> Option<i64>) -> ArrayRef {
    let mut b = Int64Builder::with_capacity(rows.len());
    rows.iter().for_each(|e| b.append_option(f(e)));
    Arc::new(b.finish())
}

fn u64s(rows: &[&Envelope], f: impl Fn(&Envelope) -> Option<u64>) -> ArrayRef {
    let mut b = UInt64Builder::with_capacity(rows.len());
    rows.iter().for_each(|e| b.append_option(f(e)));
    Arc::new(b.finish())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Array;
    use galata_wire::{Side, Ticker, Venue};
    use std::str::FromStr;

    fn fill(trade_id: u64, order_id: u64, recv: i64) -> Envelope {
        let (trade_id, order_id) = (trade_id.to_string(), order_id.to_string());
        Envelope::new(
            Venue::new("hyperliquid").unwrap(),
            Ticker::new("BTC").unwrap(),
            Some(recv - 1),
            recv,
            Event::Fill(Fill {
                dex: None,
                market: "BTC".into(),
                side: Side::Bid,
                price: Num::from_str("81213.5").unwrap(),
                size: Num::from_str("0.01").unwrap(),
                start_position: Some(Num::from_str("0").unwrap()),
                direction: Some("Open Long".into()),
                closed_pnl: Some(Num::from_str("0").unwrap()),
                fee: Some(Num::from_str("0.365461").unwrap()),
                fee_token: Some("USDC".into()),
                builder_fee: None,
                crossed: Some(true),
                order_id,
                trade_id,
                twap_id: None,
                liquidated: None,
            }),
        )
    }

    #[test]
    fn a_fills_liquidation_mark_is_a_column_null_where_unsaid() {
        let mut marked = fill(1, 1, 10);
        let mut ordinary = fill(2, 2, 20);
        if let Event::Fill(f) = &mut marked.event {
            f.liquidated = Some(true);
        }
        if let Event::Fill(f) = &mut ordinary.event {
            f.liquidated = Some(false);
        }
        let unsaid = fill(3, 3, 30);
        let b = batch(
            Kind::Fills,
            "hyperliquid",
            "main",
            &[marked, ordinary, unsaid],
        )
        .unwrap();
        let column = b
            .column_by_name("liquidated")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::BooleanArray>()
            .unwrap()
            .clone();
        assert_eq!(
            column.iter().collect::<Vec<_>>(),
            [Some(true), Some(false), None]
        );
    }

    fn owner_only() -> tempfile::TempDir {
        let dir = tempfile::tempdir().unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        }
        dir
    }

    #[test]
    fn fills_are_projected_once_each() {
        // `read()` has already collapsed receipts to identities; the
        // projection writes what it was given, typed, and nothing twice.
        let root = owner_only();
        let rows = vec![fill(7, 100, 10), fill(7, 101, 11)];
        write(root.path(), "hyperliquid", "main", &rows).unwrap();
        let path = path_of(root.path(), "hyperliquid", "main", Kind::Fills);
        let batch = galata_segments::read_segment(&path).unwrap().remove(0);
        assert_eq!(batch.num_rows(), 2);
        let names: Vec<String> = batch
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();
        assert_eq!(&names[..4], ["venue", "account", "dex", "ticker"]);
        let orders = batch
            .column_by_name("order_id")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        assert_eq!((orders.value(0), orders.value(1)), ("100", "101"));
        let price = batch
            .column_by_name("price")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::Decimal128Array>()
            .unwrap();
        // Exactly 81213.5 at scale 18.
        assert_eq!(price.value(0), 81_213_500_000_000_000_000_000);
    }

    fn resting(order_id: &str, recv: i64) -> Envelope {
        Envelope::for_account(
            Venue::new("hyperliquid").unwrap(),
            galata_wire::Account::new("main").unwrap(),
            None,
            recv,
            Event::Order(Order {
                dex: Some("xyz".into()),
                market: "GOLD".into(),
                side: Side::Ask,
                order_id: order_id.into(),
                client_id: None,
                order_type: "Stop Market".into(),
                trigger_type: None,
                is_trigger: Some(true),
                position_tpsl: Some(false),
                size: Some(Num::from_str("0.5").unwrap()),
                remaining: Some(Num::from_str("0.5").unwrap()),
                limit_price: Some(Num::from_str("4100").unwrap()),
                trigger_price: Some(Num::from_str("4120.5").unwrap()),
                trigger_condition: Some("Price below 4120.5".into()),
                reduce_only: Some(true),
                time_in_force: None,
                status: None,
                placed_micros: Some(3),
            }),
        )
    }

    fn rows_of(root: &Path, kind: Kind) -> usize {
        galata_segments::read_segment(&path_of(root, "hyperliquid", "main", kind))
            .unwrap()
            .iter()
            .map(|b| b.num_rows())
            .sum()
    }

    #[test]
    fn the_open_orders_are_the_newest_reads_set() {
        let root = owner_only();
        // Two reads: two orders at 10, one of them still resting at 20.
        let rows = vec![resting("1", 10), resting("2", 10), resting("2", 20)];
        write_kinds(
            root.path(),
            "hyperliquid",
            "main",
            &rows,
            &[Kind::Orders],
            Some(20),
        )
        .unwrap();
        let batch = read_kind(root.path(), Kind::Orders);
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(strings(&batch, "order_id"), [Some("2".to_string())]);
        assert_eq!(strings(&batch, "dex"), [Some("xyz".to_string())]);
        assert_eq!(strings(&batch, "ticker"), [Some("GOLD".to_string())]);
        assert_eq!(
            strings(&batch, "order_type"),
            [Some("Stop Market".to_string())]
        );
        assert_eq!(
            strings(&batch, "trigger_condition"),
            [Some("Price below 4120.5".to_string())]
        );
        assert_eq!(
            decimals(&batch, "trigger_price"),
            [Some(4_120_500_000_000_000_000_000)]
        );
    }

    #[test]
    fn a_read_that_found_nothing_resting_is_a_file_of_zero_rows() {
        // The newest read stated no open order: the earlier set must not read
        // as resting still.
        let root = owner_only();
        let rows = vec![resting("1", 10), resting("2", 10)];
        write_kinds(
            root.path(),
            "hyperliquid",
            "main",
            &rows,
            &[Kind::Orders],
            Some(30),
        )
        .unwrap();
        assert_eq!(rows_of(root.path(), Kind::Orders), 0);
    }

    #[test]
    fn open_orders_never_read_write_no_file() {
        // Nothing known is not nothing resting.
        let root = owner_only();
        let rows = vec![resting("1", 10)];
        let written = write_kinds(
            root.path(),
            "hyperliquid",
            "main",
            &rows,
            &[Kind::Orders],
            None,
        )
        .unwrap();
        assert!(written.is_empty());
        assert!(!path_of(root.path(), "hyperliquid", "main", Kind::Orders).exists());
    }

    #[test]
    fn order_updates_and_pnl_points_are_written_as_given() {
        // Each held once by identity before it arrives here (`events::read`);
        // the projection writes every one, typed.
        let root = owner_only();
        let mut update = resting("7", 10);
        if let Event::Order(o) = update.event.clone() {
            update.event = Event::OrderUpdate(Order {
                status: Some("triggered".into()),
                ..o
            });
            update.at_micros = Some(9);
        }
        let point = Envelope::for_account(
            Venue::new("hyperliquid").unwrap(),
            galata_wire::Account::new("main").unwrap(),
            Some(8),
            10,
            Event::PnlPoint(PnlPoint {
                window: "perpWeek".into(),
                account_value: Some(Num::from_str("1520.25").unwrap()),
                pnl: Some(Num::from_str("-12.5").unwrap()),
                initial_margin: None,
                maintenance_margin: None,
            }),
        );
        write_kinds(
            root.path(),
            "hyperliquid",
            "main",
            &[update, point],
            &[Kind::OrderUpdates, Kind::PnlHistory],
            None,
        )
        .unwrap();
        let updates = read_kind(root.path(), Kind::OrderUpdates);
        assert_eq!(strings(&updates, "status"), [Some("triggered".to_string())]);
        let points = read_kind(root.path(), Kind::PnlHistory);
        assert_eq!(strings(&points, "window"), [Some("perpWeek".to_string())]);
        assert_eq!(
            decimals(&points, "pnl"),
            [Some(-12_500_000_000_000_000_000)]
        );
        assert_eq!(decimals(&points, "maintenance_margin"), [None]);
    }

    #[test]
    fn a_kind_with_no_rows_still_writes_its_file() {
        let root = owner_only();
        write(root.path(), "hyperliquid", "main", &[fill(1, 1, 5)]).unwrap();
        let path = path_of(root.path(), "hyperliquid", "main", Kind::FundingPayments);
        assert!(path.exists(), "no funding payments is a file saying so");
        let rows: usize = galata_segments::read_segment(&path)
            .unwrap()
            .iter()
            .map(|b| b.num_rows())
            .sum();
        assert_eq!(rows, 0);
    }

    #[cfg(unix)]
    #[test]
    fn a_root_others_can_read_is_refused() {
        use std::os::unix::fs::PermissionsExt;
        let root = tempfile::tempdir().unwrap();
        std::fs::set_permissions(root.path(), std::fs::Permissions::from_mode(0o755)).unwrap();
        let err = write(root.path(), "hyperliquid", "main", &[]).unwrap_err();
        assert!(matches!(err, ProjectError::Root(_)), "{err}");
    }

    fn account_row(event: Event, recv: i64) -> Envelope {
        Envelope::new(
            Venue::new("hyperliquid").unwrap(),
            Ticker::new("BTC").unwrap(),
            Some(recv),
            recv,
            event,
        )
    }

    fn margin(dex: Option<&str>, value: &str) -> Envelope {
        account_row(
            Event::Margin(galata_wire::Margin {
                dex: dex.map(str::to_string),
                mode: galata_wire::AccountMode::Default,
                equity_not_held: None,
                account_value: Some(Num::from_str(value).unwrap()),
                total_notional: None,
                total_raw_usd: None,
                margin_used: None,
                maintenance_margin_used: None,
                withdrawable: None,
            }),
            5,
        )
    }

    fn update(effect: Effect, counterparty: Option<Counterparty>) -> Envelope {
        account_row(
            Event::LedgerUpdate(galata_wire::LedgerUpdate {
                kind: "accountClassTransfer".into(),
                effect,
                counterparty,
                token: None,
                amount: Some(Num::from_str("10").unwrap()),
                fee: None,
            }),
            7,
        )
    }

    fn read_kind(root: &Path, kind: Kind) -> RecordBatch {
        let batches =
            galata_segments::read_segment(&path_of(root, "hyperliquid", "main", kind)).unwrap();
        arrow::compute::concat_batches(&schema_for(kind).unwrap(), &batches).unwrap()
    }

    fn strings(batch: &RecordBatch, name: &str) -> Vec<Option<String>> {
        let a = batch
            .column_by_name(name)
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        (0..a.len())
            .map(|i| (!arrow::array::Array::is_null(a, i)).then(|| a.value(i).to_string()))
            .collect()
    }

    fn decimals(batch: &RecordBatch, name: &str) -> Vec<Option<i128>> {
        let a = batch
            .column_by_name(name)
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::Decimal128Array>()
            .unwrap();
        (0..a.len())
            .map(|i| (!arrow::array::Array::is_null(a, i)).then(|| a.value(i)))
            .collect()
    }

    const ONE: i128 = 1_000_000_000_000_000_000;

    #[test]
    fn margin_is_one_row_per_dex() {
        let root = owner_only();
        write(
            root.path(),
            "hyperliquid",
            "main",
            &[margin(None, "0"), margin(Some("xyz"), "238.85")],
        )
        .unwrap();
        let b = read_kind(root.path(), Kind::Margin);
        assert_eq!(strings(&b, "dex"), [None, Some("xyz".into())]);
        assert_eq!(
            strings(&b, "mode"),
            [Some("default".into()), Some("default".into())]
        );
        assert_eq!(
            strings(&b, "ticker"),
            [None, None],
            "margin names no instrument"
        );
        assert_eq!(
            decimals(&b, "account_value")[1],
            Some(238_850_000_000_000_000_000)
        );
    }

    #[test]
    fn a_position_carries_its_figures_and_absences() {
        let root = owner_only();
        let position = account_row(
            Event::Position(galata_wire::Position {
                dex: Some("xyz".into()),
                ticker: Ticker::new("GOLD").unwrap(),
                size: Num::from_str("-2").unwrap(),
                entry_price: Some(Num::from_str("4284").unwrap()),
                mark: None,
                position_value: None,
                unrealised_pnl: None,
                return_on_equity: None,
                liquidation_price: None,
                leverage: Some(Num::from_str("5").unwrap()),
                leverage_type: Some("cross".into()),
                max_leverage: Some(20),
                margin_used: None,
                funding_all_time: None,
                funding_since_open: None,
                funding_since_change: None,
            }),
            5,
        );
        write(root.path(), "hyperliquid", "main", &[position]).unwrap();
        let b = read_kind(root.path(), Kind::Positions);
        assert_eq!(strings(&b, "ticker"), [Some("GOLD".into())]);
        assert_eq!(decimals(&b, "size"), [Some(-2 * ONE)], "signed");
        assert_eq!(
            decimals(&b, "mark"),
            [None],
            "not stated is null, never zero"
        );
    }

    #[test]
    fn a_transfer_between_dexes_is_two_rows() {
        let root = owner_only();
        let moved = Effect::Known(vec![
            galata_wire::DexEffect {
                dex: None,
                usdc: Num::from_str("-10").unwrap(),
            },
            galata_wire::DexEffect {
                dex: Some("xyz".into()),
                usdc: Num::from_str("10").unwrap(),
            },
        ]);
        write(root.path(), "hyperliquid", "main", &[update(moved, None)]).unwrap();
        let b = read_kind(root.path(), Kind::LedgerUpdates);
        assert_eq!(strings(&b, "dex"), [None, Some("xyz".into())]);
        assert_eq!(
            decimals(&b, "effect_usdc"),
            [Some(-10 * ONE), Some(10 * ONE)]
        );
        assert_eq!(
            strings(&b, "update_kind"),
            [
                Some("accountClassTransfer".into()),
                Some("accountClassTransfer".into())
            ]
        );
    }

    #[test]
    fn an_update_that_moved_nothing_is_one_row() {
        let root = owner_only();
        write(
            root.path(),
            "hyperliquid",
            "main",
            &[update(Effect::Known(vec![]), None)],
        )
        .unwrap();
        let b = read_kind(root.path(), Kind::LedgerUpdates);
        assert_eq!(b.num_rows(), 1);
        assert_eq!(decimals(&b, "effect_usdc"), [None]);
        assert_eq!(strings(&b, "dex"), [None]);
    }

    #[test]
    fn an_unknown_effect_says_so() {
        let root = owner_only();
        write(
            root.path(),
            "hyperliquid",
            "main",
            &[update(Effect::Unknown, None)],
        )
        .unwrap();
        let b = read_kind(root.path(), Kind::LedgerUpdates);
        let known = b
            .column_by_name("effect_known")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::BooleanArray>()
            .unwrap();
        assert!(!known.value(0));
    }

    #[test]
    fn a_counterparty_is_an_alias_or_a_fingerprint() {
        let root = owner_only();
        let ours = update(
            Effect::Known(vec![]),
            Some(Counterparty::Account(
                crate::ledger::accounts::sub_alias(&galata_wire::Account::new("main").unwrap(), 1)
                    .unwrap(),
            )),
        );
        let theirs = update(
            Effect::Known(vec![]),
            Some(Counterparty::Fingerprint("fp_3a9c".into())),
        );
        write(root.path(), "hyperliquid", "main", &[ours, theirs]).unwrap();
        let b = read_kind(root.path(), Kind::LedgerUpdates);
        assert_eq!(
            strings(&b, "counterparty_kind"),
            [Some("account".into()), Some("fingerprint".into())]
        );
        assert_eq!(
            strings(&b, "counterparty"),
            [Some("main_s1".into()), Some("fp_3a9c".into())]
        );
    }

    #[test]
    fn a_second_pass_replaces_the_first() {
        let root = owner_only();
        write(root.path(), "hyperliquid", "main", &[fill(1, 1, 5)]).unwrap();
        write(
            root.path(),
            "hyperliquid",
            "main",
            &[fill(1, 1, 5), fill(2, 2, 6)],
        )
        .unwrap();
        let path = path_of(root.path(), "hyperliquid", "main", Kind::Fills);
        let rows: usize = galata_segments::read_segment(&path)
            .unwrap()
            .iter()
            .map(|b| b.num_rows())
            .sum();
        assert_eq!(rows, 2, "every fill once, the new one included");
    }
}
