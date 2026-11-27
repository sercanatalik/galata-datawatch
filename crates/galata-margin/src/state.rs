//! **The owner's accounts as the ledger last read them** (task 5.1), from its
//! projection: `<tape>/venue=<v>/account=<alias>/kind=<kind>/rows.parquet`,
//! one file per kind, rewritten whole each pass (`ledger::project`).
//!
//! **A snapshot is the newest read of each of its parts**, never a row of one
//! read beside a row of another of the same part:
//! - **Hyperliquid answers per dex**, each with its own receipt time. So each
//!   dex's margin is its newest read, and its positions are the rows of that
//!   same answer: none there means none held. The spot state, the borrows and
//!   the ratio are each their newest read.
//! - **Derive answers every subaccount at once**: one read gives a
//!   subaccount's margin, positions and collateral.
//!
//! **The snapshot's time is its oldest part's**, so an age is never flattered
//! by the part read last (design D8).

use std::collections::BTreeMap;
use std::path::Path;

use arrow::array::{Array, BooleanArray, Decimal128Array, Int64Array, StringArray, UInt32Array};
use arrow::record_batch::RecordBatch;
use galata_datawatch::ledger::project;
use galata_wire::Kind;
use galata_wire::event::AccountMode;
use rust_decimal::Decimal;

use crate::hyperliquid::PmState;

/// Why an account has no snapshot. Names the account and the kind, never a
/// figure.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum StateError {
    /// The ledger has projected nothing of this kind for the account.
    #[error("the ledger holds no {kind} for {venue} `{account}`")]
    NoRecord {
        /// The venue.
        venue: &'static str,
        /// The account's alias.
        account: String,
        /// The kind, by the projection's name.
        kind: String,
    },
    /// The projection could not be read.
    #[error("the ledger's {kind} for `{account}` could not be read: {detail}")]
    Unreadable {
        /// The account's alias.
        account: String,
        /// The kind.
        kind: String,
        /// What was wrong.
        detail: String,
    },
}

/// One Hyperliquid position, as the ledger read it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HlPosition {
    /// The dex: `""` for the main one.
    pub dex: String,
    /// The market.
    pub market: String,
    /// Signed size.
    pub size: Decimal,
    /// The mark at the read.
    pub mark: Option<Decimal>,
    /// The leverage in force.
    pub leverage: Option<Decimal>,
    /// The market's maximum leverage.
    pub max_leverage: Option<u32>,
}

/// A Hyperliquid account, as the ledger last read it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HyperliquidState {
    /// The account's alias.
    pub account: String,
    /// Its mode.
    pub mode: AccountMode,
    /// Its positions, every dex.
    pub positions: Vec<HlPosition>,
    /// Its portfolio-margin inputs, prices aside: those are the market's.
    pub pm: PmState,
    /// The portfolio margin ratio the venue stated.
    pub stated_ratio: Option<Decimal>,
    /// The time of its oldest part.
    pub at_micros: i64,
}

/// One Derive position.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DerivePosition {
    /// The instrument.
    pub instrument: String,
    /// Signed amount.
    pub amount: Option<Decimal>,
    /// The mark at the read.
    pub mark_price: Option<Decimal>,
}

/// One Derive collateral holding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeriveCollateral {
    /// The asset.
    pub asset: String,
    /// The amount held.
    pub amount: Option<Decimal>,
}

/// A Derive subaccount, as the ledger last read it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeriveState {
    /// The subaccount's alias.
    pub account: String,
    /// Its margin type: `SM` or `PM2`.
    pub margin_type: Option<String>,
    /// Its net initial margin, as stated.
    pub initial_margin: Option<Decimal>,
    /// Its net maintenance margin, as stated.
    pub maintenance_margin: Option<Decimal>,
    /// Its positions.
    pub positions: Vec<DerivePosition>,
    /// Its collateral.
    pub collaterals: Vec<DeriveCollateral>,
    /// The read's time.
    pub at_micros: i64,
}

/// Every row of one account's kind.
fn rows(
    root: &Path,
    venue: &'static str,
    account: &str,
    kind: Kind,
) -> Result<RecordBatch, StateError> {
    let path = project::path_of(root, venue, account, kind);
    let name = kind_name(kind);
    if !path.exists() {
        return Err(StateError::NoRecord {
            venue,
            account: account.into(),
            kind: name,
        });
    }
    let unreadable = |detail: String| StateError::Unreadable {
        account: account.into(),
        kind: kind_name(kind),
        detail,
    };
    let schema =
        project::schema_for(kind).ok_or_else(|| unreadable("not a projected kind".into()))?;
    let batches = galata_segments::read_segment(&path).map_err(|e| unreadable(e.to_string()))?;
    arrow::compute::concat_batches(&schema, &batches).map_err(|e| unreadable(e.to_string()))
}

fn kind_name(kind: Kind) -> String {
    format!("{kind:?}").to_lowercase()
}

/// A column's values by row, read through one accessor.
struct Columns<'a> {
    batch: &'a RecordBatch,
}

impl Columns<'_> {
    fn recv(&self, row: usize) -> i64 {
        self.int("recv_micros", row).unwrap_or_default()
    }

    fn int(&self, name: &str, row: usize) -> Option<i64> {
        let c = self
            .batch
            .column_by_name(name)?
            .as_any()
            .downcast_ref::<Int64Array>()?;
        (!c.is_null(row)).then(|| c.value(row))
    }

    fn uint(&self, name: &str, row: usize) -> Option<u32> {
        let c = self
            .batch
            .column_by_name(name)?
            .as_any()
            .downcast_ref::<UInt32Array>()?;
        (!c.is_null(row)).then(|| c.value(row))
    }

    fn text(&self, name: &str, row: usize) -> Option<String> {
        let c = self
            .batch
            .column_by_name(name)?
            .as_any()
            .downcast_ref::<StringArray>()?;
        (!c.is_null(row)).then(|| c.value(row).to_string())
    }

    fn flag(&self, name: &str, row: usize) -> Option<bool> {
        let c = self
            .batch
            .column_by_name(name)?
            .as_any()
            .downcast_ref::<BooleanArray>()?;
        (!c.is_null(row)).then(|| c.value(row))
    }

    fn num(&self, name: &str, row: usize) -> Option<Decimal> {
        let c = self
            .batch
            .column_by_name(name)?
            .as_any()
            .downcast_ref::<Decimal128Array>()?;
        if c.is_null(row) {
            return None;
        }
        exact(c.value(row), c.scale().max(0) as u32)
    }

    fn dex(&self, row: usize) -> String {
        self.text("dex", row).unwrap_or_default()
    }

    /// Rows read at `recv`.
    fn at(&self, recv: i64) -> impl Iterator<Item = usize> + '_ {
        (0..self.batch.num_rows()).filter(move |r| self.recv(*r) == recv)
    }

    /// The newest receipt, overall.
    fn newest(&self) -> Option<i64> {
        (0..self.batch.num_rows()).map(|r| self.recv(r)).max()
    }
}

/// A projected decimal as an exact `Decimal`, trailing zeros stripped first
/// so a figure the 96-bit mantissa can hold is never refused for its scale.
fn exact(mut value: i128, mut scale: u32) -> Option<Decimal> {
    while scale > 0 && value % 10 == 0 {
        value /= 10;
        scale -= 1;
    }
    Decimal::try_from_i128_with_scale(value, scale).ok()
}

/// A Hyperliquid account's newest snapshot.
pub fn read_hyperliquid(root: &Path, account: &str) -> Result<HyperliquidState, StateError> {
    const VENUE: &str = "hyperliquid";
    let margin_batch = rows(root, VENUE, account, Kind::Margin)?;
    let margin = Columns {
        batch: &margin_batch,
    };
    // Each dex's newest read.
    let mut dex_read: BTreeMap<String, (i64, usize)> = BTreeMap::new();
    for r in 0..margin_batch.num_rows() {
        let recv = margin.recv(r);
        let entry = dex_read.entry(margin.dex(r)).or_insert((recv, r));
        if recv > entry.0 {
            *entry = (recv, r);
        }
    }
    if dex_read.is_empty() {
        return Err(StateError::NoRecord {
            venue: VENUE,
            account: account.into(),
            kind: "margin".into(),
        });
    }
    let mut oldest = i64::MAX;
    let mut mode = AccountMode::Unknown;
    let mut pm = PmState::default();
    for (dex, (recv, row)) in &dex_read {
        oldest = oldest.min(*recv);
        if let Some(m) = margin.text("mode", *row) {
            mode = serde_json::from_value(serde_json::Value::String(m))
                .unwrap_or(AccountMode::Unknown);
        }
        if let Some(mm) = margin.num("maintenance_margin_used", *row) {
            pm.dex_maintenance.insert(dex.clone(), mm);
        }
    }

    // Each dex's positions are the rows of its margin's own read.
    let positions_batch = rows(root, VENUE, account, Kind::Positions).ok();
    let mut positions = Vec::new();
    if let Some(batch) = &positions_batch {
        let p = Columns { batch };
        for (dex, (recv, _)) in &dex_read {
            for r in p.at(*recv).filter(|r| &p.dex(*r) == dex) {
                positions.push(HlPosition {
                    dex: dex.clone(),
                    market: p.text("ticker", r).unwrap_or_default(),
                    size: p.num("size", r).unwrap_or_default(),
                    mark: p.num("mark", r),
                    leverage: p.num("leverage", r),
                    max_leverage: p.uint("max_leverage", r),
                });
            }
        }
    }

    // The spot state, the borrows and the ratio: each its newest read.
    let mut part = |kind: Kind| -> Result<Option<(RecordBatch, i64)>, StateError> {
        match rows(root, VENUE, account, kind) {
            Ok(batch) => {
                let Some(recv) = (Columns { batch: &batch }).newest() else {
                    return Ok(None);
                };
                oldest = oldest.min(recv);
                Ok(Some((batch, recv)))
            }
            Err(StateError::NoRecord { .. }) => Ok(None),
            Err(e) => Err(e),
        }
    };
    if let Some((batch, recv)) = part(Kind::Balances)? {
        let b = Columns { batch: &batch };
        for r in b.at(recv) {
            let asset = b.text("asset", r).unwrap_or_default();
            if let Some(total) = b.num("total", r) {
                pm.balances.insert(asset.clone(), total);
            }
            if let Some(ltv) = b.num("ltv", r).filter(|l| *l > Decimal::ZERO) {
                pm.ltv.insert(asset, ltv);
            }
        }
    }
    if let Some((batch, recv)) = part(Kind::BorrowLend)? {
        let b = Columns { batch: &batch };
        for r in b.at(recv) {
            if let (Some(token), Some(value)) = (b.text("token", r), b.num("borrow_value", r))
                && value > Decimal::ZERO
            {
                pm.borrowed.insert(token, value);
            }
        }
    }
    let mut stated_ratio = None;
    if let Some((batch, recv)) = part(Kind::PortfolioMargin)? {
        let b = Columns { batch: &batch };
        stated_ratio = b
            .at(recv)
            .filter(|r| b.flag("enabled", *r) != Some(false))
            .find_map(|r| b.num("ratio", r));
    }
    // The main dex and the builder dexes the owner trades are USDC's.
    for dex in dex_read.keys() {
        pm.dex_collateral.insert(dex.clone(), "USDC".into());
    }
    Ok(HyperliquidState {
        account: account.into(),
        mode,
        positions,
        pm,
        stated_ratio,
        at_micros: oldest,
    })
}

/// A Derive subaccount's newest snapshot.
pub fn read_derive(root: &Path, account: &str) -> Result<DeriveState, StateError> {
    const VENUE: &str = "derive-xyz";
    let margin_batch = rows(root, VENUE, account, Kind::MarginState)?;
    let m = Columns {
        batch: &margin_batch,
    };
    let recv = m.newest().ok_or_else(|| StateError::NoRecord {
        venue: VENUE,
        account: account.into(),
        kind: "margin_state".into(),
    })?;
    let row = m.at(recv).next().expect("a row at the newest receipt");
    let at = |kind: Kind| -> Result<Vec<(RecordBatch, usize)>, StateError> {
        match rows(root, VENUE, account, kind) {
            Ok(batch) => {
                let rows: Vec<usize> = Columns { batch: &batch }.at(recv).collect();
                Ok(rows.into_iter().map(|r| (batch.clone(), r)).collect())
            }
            Err(StateError::NoRecord { .. }) => Ok(Vec::new()),
            Err(e) => Err(e),
        }
    };
    let positions = at(Kind::PositionState)?
        .iter()
        .map(|(batch, r)| {
            let c = Columns { batch };
            DerivePosition {
                // The projection keeps a position's instrument in `ticker`.
                instrument: c.text("ticker", *r).unwrap_or_default(),
                amount: c.num("amount", *r),
                mark_price: c.num("mark_price", *r),
            }
        })
        .collect();
    let collaterals = at(Kind::Collateral)?
        .iter()
        .map(|(batch, r)| {
            let c = Columns { batch };
            DeriveCollateral {
                asset: c.text("asset", *r).unwrap_or_default(),
                amount: c.num("amount", *r),
            }
        })
        .collect();
    Ok(DeriveState {
        account: account.into(),
        margin_type: m.text("margin_type", row),
        initial_margin: m.num("initial_margin", row),
        maintenance_margin: m.num("maintenance_margin", row),
        positions,
        collaterals,
        at_micros: recv,
    })
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use galata_wire::{
        Account, Balance, Collateral, Envelope, Event, Margin, MarginState, PortfolioMargin,
        Position, PositionState, Ticker, Venue,
    };

    use super::*;

    fn d(s: &str) -> Decimal {
        Decimal::from_str(s).unwrap()
    }

    /// A ledger projection root, owner-only as the ledger requires.
    fn root() -> tempfile::TempDir {
        let dir = tempfile::tempdir().unwrap();
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        dir
    }

    fn envelope(venue: &str, account: &str, recv: i64, event: Event) -> Envelope {
        Envelope::for_account(
            Venue::new(venue).unwrap(),
            Account::new(account).unwrap(),
            None,
            recv,
            event,
        )
    }

    fn margin(dex: Option<&str>, mm: &str) -> Event {
        Event::Margin(Margin {
            dex: dex.map(str::to_string),
            mode: AccountMode::PortfolioMargin,
            equity_not_held: None,
            account_value: Some(d("0")),
            total_notional: Some(d("0")),
            total_raw_usd: None,
            margin_used: Some(d("0")),
            maintenance_margin_used: Some(d(mm)),
            withdrawable: None,
        })
    }

    fn position(dex: Option<&str>, ticker: &str, size: &str) -> Event {
        Event::Position(Position {
            dex: dex.map(str::to_string),
            ticker: Ticker::new(ticker).unwrap(),
            size: d(size),
            entry_price: None,
            mark: Some(d("100")),
            position_value: None,
            unrealised_pnl: None,
            return_on_equity: None,
            liquidation_price: None,
            leverage: Some(d("20")),
            leverage_type: None,
            max_leverage: Some(25),
            margin_used: None,
            funding_all_time: None,
            funding_since_open: None,
            funding_since_change: None,
        })
    }

    fn balance(asset: &str, total: &str, ltv: Option<&str>) -> Event {
        Event::Balance(Balance {
            asset: asset.into(),
            token_index: None,
            total: d(total),
            hold: None,
            entry_notional: None,
            spot_hold: None,
            ltv: ltv.map(d),
            supplied: None,
        })
    }

    #[test]
    fn a_hyperliquid_snapshot_is_each_parts_newest_read_and_its_oldest_time() {
        let dir = root();
        let rows = vec![
            // An old pass: both dexes held positions.
            envelope("hyperliquid", "main", 100, margin(None, "50")),
            envelope("hyperliquid", "main", 100, position(None, "BTC", "-0.5")),
            envelope("hyperliquid", "main", 101, margin(Some("xyz"), "30")),
            envelope(
                "hyperliquid",
                "main",
                101,
                position(Some("xyz"), "GOLD", "2"),
            ),
            // The newest pass: the main dex is flat, xyz still holds GOLD.
            envelope("hyperliquid", "main", 200, margin(None, "0")),
            envelope("hyperliquid", "main", 203, margin(Some("xyz"), "31")),
            envelope(
                "hyperliquid",
                "main",
                203,
                position(Some("xyz"), "GOLD", "2.5"),
            ),
            envelope("hyperliquid", "main", 100, balance("USDC", "900", None)),
            envelope("hyperliquid", "main", 201, balance("USDC", "1000", None)),
            envelope(
                "hyperliquid",
                "main",
                201,
                balance("HYPE", "10", Some("0.65")),
            ),
            envelope(
                "hyperliquid",
                "main",
                202,
                Event::PortfolioMargin(PortfolioMargin {
                    enabled: Some(true),
                    ratio: Some(d("0.031")),
                }),
            ),
        ];
        project::write(dir.path(), "hyperliquid", "main", &rows).unwrap();
        let state = read_hyperliquid(dir.path(), "main").unwrap();
        assert_eq!(state.mode, AccountMode::PortfolioMargin);
        assert_eq!(state.pm.dex_maintenance[""], d("0"));
        assert_eq!(state.pm.dex_maintenance["xyz"], d("31"));
        // The flat main dex holds nothing; the old BTC short is not carried.
        assert_eq!(state.positions.len(), 1, "{:?}", state.positions);
        assert_eq!(state.positions[0].market, "GOLD");
        assert_eq!(state.positions[0].size, d("2.5"));
        assert_eq!(state.positions[0].leverage, Some(d("20")));
        assert_eq!(state.pm.balances["USDC"], d("1000"));
        assert_eq!(state.pm.ltv["HYPE"], d("0.65"));
        assert_eq!(state.stated_ratio, Some(d("0.031")));
        // The oldest part, the main dex's read at 200.
        assert_eq!(state.at_micros, 200);
    }

    #[test]
    fn a_derive_snapshot_is_one_reads_margin_positions_and_collateral() {
        let dir = root();
        let state = |recv, im: &str| {
            envelope(
                "derive-xyz",
                "owner_s3",
                recv,
                Event::MarginState(MarginState {
                    margin_type: Some("PM2".into()),
                    manager: None,
                    risk_universe: None,
                    value: None,
                    initial_margin: Some(d(im)),
                    maintenance_margin: Some(d("200")),
                    positions_value: None,
                    collateral_value: None,
                    open_orders_margin: None,
                    under_liquidation: Some(false),
                }),
            )
        };
        let put = |recv, amount: &str| {
            envelope(
                "derive-xyz",
                "owner_s3",
                recv,
                Event::PositionState(PositionState {
                    instrument: "BTC-20261225-60000-P".into(),
                    instrument_type: Some("option".into()),
                    amount: Some(d(amount)),
                    average_price: None,
                    mark_price: Some(d("2100.5")),
                    mark_value: None,
                    unrealised_pnl: None,
                    delta: None,
                    gamma: None,
                    theta: None,
                    vega: None,
                    initial_margin: None,
                    maintenance_margin: None,
                    liquidation_price: None,
                    cumulative_funding: None,
                }),
            )
        };
        let usdc = |recv| {
            envelope(
                "derive-xyz",
                "owner_s3",
                recv,
                Event::Collateral(Collateral {
                    asset: "USDC".into(),
                    asset_type: None,
                    amount: Some(d("5000")),
                    mark_price: None,
                    mark_value: None,
                    initial_margin: None,
                    maintenance_margin: None,
                    cumulative_interest: None,
                    pending_interest: None,
                }),
            )
        };
        let rows = vec![
            state(100, "900"),
            put(100, "-1"),
            usdc(100),
            state(200, "850.25"),
            put(200, "-2"),
            usdc(200),
        ];
        project::write_kinds(
            dir.path(),
            "derive-xyz",
            "owner_s3",
            &rows,
            &project::WALLET_PROJECTED,
        )
        .unwrap();
        let s = read_derive(dir.path(), "owner_s3").unwrap();
        assert_eq!(s.at_micros, 200);
        assert_eq!(s.margin_type.as_deref(), Some("PM2"));
        assert_eq!(s.initial_margin, Some(d("850.25")));
        assert_eq!(s.positions.len(), 1);
        assert_eq!(s.positions[0].instrument, "BTC-20261225-60000-P");
        assert_eq!(s.positions[0].amount, Some(d("-2")));
        assert_eq!(s.positions[0].mark_price, Some(d("2100.5")));
        assert_eq!(s.collaterals[0].amount, Some(d("5000")));
    }

    #[test]
    fn an_account_the_ledger_never_projected_is_named() {
        let dir = root();
        let err = read_derive(dir.path(), "owner_s9").unwrap_err();
        assert!(err.to_string().contains("owner_s9"), "{err}");
        let err = read_hyperliquid(dir.path(), "main").unwrap_err();
        assert!(matches!(err, StateError::NoRecord { .. }), "{err}");
    }

    #[test]
    fn a_projected_decimal_is_exact_whatever_its_scale() {
        // 24738.08 at scale 18, as the projection stores it.
        assert_eq!(
            exact(24_738_080_000_000_000_000_000, 18),
            Some(d("24738.08"))
        );
        assert_eq!(exact(-500_000_000_000_000_000, 18), Some(d("-0.5")));
    }
}
