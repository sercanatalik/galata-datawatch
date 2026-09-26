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
use galata_wire::{Envelope, Event, Fill, FundingPayment, Kind, Num};

use crate::ledger::accounts::LedgerError;

/// The kinds this projection writes.
pub const PROJECTED: [Kind; 2] = [Kind::Fills, Kind::FundingPayments];

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
        let of_kind: Vec<&Envelope> = rows.iter().filter(|e| e.kind() == kind).collect();
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
        Field::new("ticker", DataType::Utf8, false),
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
        _ => None,
    };
    // The instrument the event names: an account-addressed envelope carries
    // no instrument address of its own.
    let ticker = |e: &Envelope| match &e.event {
        Event::Fill(f) => Some(f.ticker.as_str().to_string()),
        Event::FundingPayment(p) => Some(p.ticker.as_str().to_string()),
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
        _ => unreachable!("only projected kinds are built"),
    }
    RecordBatch::try_new(schema, columns).map_err(|e| ProjectError::Arrow {
        kind,
        detail: e.to_string(),
    })
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
