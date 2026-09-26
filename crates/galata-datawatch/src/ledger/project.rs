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
//! an instrument, and the market tape is what the tower serves
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

use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::{
    ArrayRef, BooleanBuilder, Decimal128Builder, Int64Builder, StringBuilder, UInt16Builder,
    UInt32Builder, UInt64Builder,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use galata_wire::{
    Counterparty, Effect, Envelope, Event, Fill, FundingPayment, Kind, Margin, Num, Position,
};

use crate::ledger::accounts::LedgerError;

/// The kinds this projection writes: every kind the fold reads.
pub const PROJECTED: [Kind; 5] = [
    Kind::Fills,
    Kind::FundingPayments,
    Kind::Margin,
    Kind::Positions,
    Kind::LedgerUpdates,
];

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
    crate::ledger::accounts::check_root(root)?;
    let mut written = Vec::new();
    for kind in PROJECTED {
        let expanded: Vec<Envelope>;
        let of_kind: Vec<&Envelope> = if kind == Kind::LedgerUpdates {
            expanded = per_effect(rows);
            expanded.iter().collect()
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
            Field::new("order_id", DataType::UInt64, false),
            // Not unique alone: identity is (trade_id, order_id).
            Field::new("trade_id", DataType::UInt64, false),
            Field::new("twap_id", DataType::UInt64, true),
        ]),
        Kind::FundingPayments => fields.extend([
            // As the venue signs it: negative when the position paid.
            Field::new("usdc", MONEY, false),
            Field::new("size", MONEY, false),
            Field::new("rate", MONEY, false),
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
        _ => return None,
    }
    Some(Arc::new(Schema::new(fields)))
}

fn batch_for(
    kind: Kind,
    venue: &str,
    account: &str,
    rows: &[&Envelope],
) -> Result<RecordBatch, ProjectError> {
    let schema = schema_for(kind).expect("only projected kinds are built");
    let dex = |e: &Envelope| match &e.event {
        Event::Fill(f) => f.dex.clone(),
        Event::FundingPayment(p) => p.dex.clone(),
        Event::Margin(m) => m.dex.clone(),
        Event::Position(p) => p.dex.clone(),
        // Expanded to one effect per row by `per_effect`.
        Event::LedgerUpdate(u) => match &u.effect {
            Effect::Known(effects) => effects.first().and_then(|d| d.dex.clone()),
            _ => None,
        },
        _ => None,
    };
    // The instrument the event names: an account-addressed envelope carries
    // no instrument address of its own.
    let ticker = |e: &Envelope| match &e.event {
        Event::Fill(f) => Some(f.ticker.as_str().to_string()),
        Event::FundingPayment(p) => Some(p.ticker.as_str().to_string()),
        Event::Position(p) => Some(p.ticker.as_str().to_string()),
        _ => None,
    };
    let mut columns: Vec<ArrayRef> = vec![
        text(rows, |_| Some(venue.to_string())),
        text(rows, |_| Some(account.to_string())),
        text(rows, dex),
        text(rows, ticker),
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
            let f = |e: &Envelope| match &e.event {
                Event::Fill(f) => Some(f.clone()),
                _ => None,
            };
            let each = |g: fn(&Fill) -> Option<Num>| move |e: &Envelope| f(e).and_then(|x| g(&x));
            columns.push(text(rows, |e| f(e).map(|x| x.side.as_str().to_string())));
            columns.push(dec(rows, each(|x| Some(x.price)))?);
            columns.push(dec(rows, each(|x| Some(x.size)))?);
            columns.push(dec(rows, each(|x| x.start_position))?);
            columns.push(text(rows, |e| f(e).and_then(|x| x.direction)));
            columns.push(dec(rows, each(|x| x.closed_pnl))?);
            columns.push(dec(rows, each(|x| x.fee))?);
            columns.push(text(rows, |e| f(e).and_then(|x| x.fee_token)));
            columns.push(dec(rows, each(|x| x.builder_fee))?);
            columns.push({
                let mut b = BooleanBuilder::with_capacity(rows.len());
                rows.iter()
                    .for_each(|e| b.append_option(f(e).and_then(|x| x.crossed)));
                Arc::new(b.finish())
            });
            columns.push(u64s(rows, |e| f(e).map(|x| x.order_id)));
            columns.push(u64s(rows, |e| f(e).map(|x| x.trade_id)));
            columns.push(u64s(rows, |e| f(e).and_then(|x| x.twap_id)));
        }
        Kind::FundingPayments => {
            let p = |e: &Envelope| match &e.event {
                Event::FundingPayment(p) => Some(p.clone()),
                _ => None,
            };
            let each = |g: fn(&FundingPayment) -> Option<Num>| {
                move |e: &Envelope| p(e).and_then(|x| g(&x))
            };
            columns.push(dec(rows, each(|x| Some(x.usdc)))?);
            columns.push(dec(rows, each(|x| Some(x.size)))?);
            columns.push(dec(rows, each(|x| Some(x.rate)))?);
            columns.push({
                let mut b = UInt32Builder::with_capacity(rows.len());
                rows.iter()
                    .for_each(|e| b.append_option(p(e).and_then(|x| x.samples)));
                Arc::new(b.finish())
            });
        }
        Kind::Margin => {
            let m = |e: &Envelope| match &e.event {
                Event::Margin(m) => Some(m.clone()),
                _ => None,
            };
            let each = |g: fn(&Margin) -> Option<Num>| move |e: &Envelope| m(e).and_then(|x| g(&x));
            columns.push(text(rows, |e| {
                m(e).and_then(|x| serde_json::to_value(x.mode).ok())
                    .and_then(|v| v.as_str().map(str::to_string))
            }));
            columns.push(text(rows, |e| m(e).and_then(|x| x.equity_not_held)));
            columns.push(dec(rows, each(|x| x.account_value))?);
            columns.push(dec(rows, each(|x| x.total_notional))?);
            columns.push(dec(rows, each(|x| x.total_raw_usd))?);
            columns.push(dec(rows, each(|x| x.margin_used))?);
            columns.push(dec(rows, each(|x| x.maintenance_margin_used))?);
            columns.push(dec(rows, each(|x| x.withdrawable))?);
        }
        Kind::Positions => {
            let p = |e: &Envelope| match &e.event {
                Event::Position(p) => Some(p.clone()),
                _ => None,
            };
            let each =
                |g: fn(&Position) -> Option<Num>| move |e: &Envelope| p(e).and_then(|x| g(&x));
            columns.push(dec(rows, each(|x| Some(x.size)))?);
            columns.push(dec(rows, each(|x| x.entry_price))?);
            columns.push(dec(rows, each(|x| x.mark))?);
            columns.push(dec(rows, each(|x| x.position_value))?);
            columns.push(dec(rows, each(|x| x.unrealised_pnl))?);
            columns.push(dec(rows, each(|x| x.return_on_equity))?);
            columns.push(dec(rows, each(|x| x.liquidation_price))?);
            columns.push(dec(rows, each(|x| x.leverage))?);
            columns.push(text(rows, |e| p(e).and_then(|x| x.leverage_type)));
            columns.push({
                let mut b = UInt32Builder::with_capacity(rows.len());
                rows.iter()
                    .for_each(|e| b.append_option(p(e).and_then(|x| x.max_leverage)));
                Arc::new(b.finish())
            });
            columns.push(dec(rows, each(|x| x.margin_used))?);
            columns.push(dec(rows, each(|x| x.funding_all_time))?);
            columns.push(dec(rows, each(|x| x.funding_since_open))?);
            columns.push(dec(rows, each(|x| x.funding_since_change))?);
        }
        Kind::LedgerUpdates => {
            let u = |e: &Envelope| match &e.event {
                Event::LedgerUpdate(u) => Some(u.clone()),
                _ => None,
            };
            columns.push(text(rows, |e| u(e).map(|x| x.kind)));
            columns.push({
                let mut b = BooleanBuilder::with_capacity(rows.len());
                rows.iter().for_each(|e| {
                    b.append_option(u(e).map(|x| matches!(x.effect, Effect::Known(_))))
                });
                Arc::new(b.finish())
            });
            columns.push(dec(rows, |e| match u(e).map(|x| x.effect) {
                Some(Effect::Known(effects)) => effects.first().map(|d| d.usdc),
                _ => None,
            })?);
            columns.push(text(rows, |e| {
                u(e).and_then(|x| x.counterparty).map(|c| match c {
                    Counterparty::Account(_) => "account".to_string(),
                    Counterparty::Fingerprint(_) => "fingerprint".to_string(),
                })
            }));
            columns.push(text(rows, |e| {
                u(e).and_then(|x| x.counterparty).map(|c| match c {
                    Counterparty::Account(alias) => alias.as_str().to_string(),
                    Counterparty::Fingerprint(fp) => fp,
                })
            }));
            columns.push(text(rows, |e| u(e).and_then(|x| x.token)));
            columns.push(dec(rows, |e| u(e).and_then(|x| x.amount))?);
            columns.push(dec(rows, |e| u(e).and_then(|x| x.fee))?);
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
fn per_effect(rows: &[Envelope]) -> Vec<Envelope> {
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
                    out.push(one);
                }
            }
            _ => out.push(row.clone()),
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

fn text(rows: &[&Envelope], f: impl Fn(&Envelope) -> Option<String>) -> ArrayRef {
    let mut b = StringBuilder::with_capacity(rows.len(), rows.len() * 8);
    rows.iter().for_each(|e| b.append_option(f(e)));
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
        Envelope::new(
            Venue::new("hyperliquid").unwrap(),
            Ticker::new("BTC").unwrap(),
            Some(recv - 1),
            recv,
            Event::Fill(Fill {
                dex: None,
                ticker: Ticker::new("BTC").unwrap(),
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
            }),
        )
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
            .downcast_ref::<arrow::array::UInt64Array>()
            .unwrap();
        assert_eq!((orders.value(0), orders.value(1)), (100, 101));
        let price = batch
            .column_by_name("price")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::Decimal128Array>()
            .unwrap();
        // Exactly 81213.5 at scale 18.
        assert_eq!(price.value(0), 81_213_500_000_000_000_000_000);
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
