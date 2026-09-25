//! The fold: an account's positions, basis and realised P&L, **derived from
//! its fills and checked three ways** — never asserted.
//!
//! ```text
//!   ledger::events::read ──▶ fills · funding · updates · snapshots, in venue time
//!        │
//!        ├─ books per (dex, ticker)     weighted average; a flip realises fully
//!        ├─ continuity                  each fill's startPosition vs the book
//!        ├─ realised                    the fold vs the venue's closedPnl
//!        └─ snapshots                   the book vs szi and entryPx at that moment
//! ```
//!
//! **The arithmetic is legacy's** (`crates/fold/src/lib.rs`), carried with its
//! rules: flat has no basis, a fill is applied once, an uninterpretable fill
//! poisons its book and nothing resurrects it, and absence is kept apart from
//! zero. **The anchor is the venue's**: a book opens at its first held fill's
//! `startPosition`, and where that is not zero the basis is *unknown* until the
//! book is next flat, rather than invented.
//!
//! **The checks report, never judge** (legacy `reconcile`): agreement within
//! a declared tolerance is agreement and is counted; a difference carries both
//! figures. Measured 2026-09-25 (`design/measured.md`, *what a fold can check a
//! fill against*): `closedPnl` excludes the fee, contrary to the venue's
//! documentation, and the venue's rounding is relative to notional.

use std::collections::{BTreeMap, BTreeSet};

use galata_wire::{AccountMode, Effect, Envelope, Event, Fill, Num, Side, Ticker};

/// What the realised figure does **not** include, stated by the type.
pub const REALISED_EXCLUDES: &[&str] = &["fees", "funding"];

/// The declared tolerances. **Undeclared is zero** (legacy `reconcile`): a
/// fold without them refuses rather than defaulting.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Tolerances {
    /// An absolute size: positions within it agree.
    pub position: Num,
    /// A fraction: realised P&L within it times the fill's closed notional
    /// agrees, and a basis within it times the price.
    pub relative: Num,
}

/// A book's cost basis. **Three states, not an option**: a flat book has no
/// basis because zero is a price; an inherited position has one the fold
/// cannot know.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case", tag = "state", content = "price")]
pub enum Basis {
    /// No position.
    Flat,
    /// The weighted-average entry of the open position.
    Known(Num),
    /// A position the fold inherited from before its first held fill, or
    /// re-anchored to after a break. Known again once the book is flat.
    Unknown,
}

/// One book, folded.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Book {
    position: Num,
    basis: Option<Basis>,
    realised: Num,
    realised_not_held: u32,
    fees: Num,
    fees_unrecorded: u32,
    funding: Num,
    seen: BTreeSet<(u64, u64)>,
    poisoned_by: Option<(u64, u64)>,
    after_poison: u32,
}

impl Book {
    fn basis(&self) -> Basis {
        self.basis.unwrap_or(Basis::Flat)
    }
}

/// Where a fill's stated start position disagreed with the book.
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct Break {
    /// The dex. `None` is the main one.
    pub dex: Option<String>,
    /// The instrument.
    pub ticker: String,
    /// The fill whose `startPosition` disagreed: the first after the gap.
    pub trade_id: u64,
    /// Its order.
    pub order_id: u64,
    /// When.
    pub at_micros: Option<i64>,
    /// The book's position before it.
    pub folded: Num,
    /// What the venue said the position was.
    pub stated: Num,
}

/// Where the fold's realised P&L and the venue's disagreed beyond tolerance.
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct Skew {
    /// The dex.
    pub dex: Option<String>,
    /// The instrument.
    pub ticker: String,
    /// The fill.
    pub trade_id: u64,
    /// Its order.
    pub order_id: u64,
    /// Realised by the fold, fees excluded.
    pub folded: Num,
    /// The venue's `closedPnl`.
    pub stated: Num,
    /// `stated − folded`.
    pub difference: Num,
}

/// Where a book and a snapshot disagreed beyond tolerance.
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct SnapshotDifference {
    /// The dex.
    pub dex: Option<String>,
    /// The instrument.
    pub ticker: String,
    /// The snapshot's venue time.
    pub at_micros: i64,
    /// `position` or `basis`.
    pub quantity: &'static str,
    /// The fold's figure; absent where the fold holds no such book.
    pub folded: Option<Num>,
    /// The snapshot's; absent where the snapshot holds none.
    pub stated: Option<Num>,
}

/// One book, as reported.
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct BookReport {
    /// The dex.
    pub dex: Option<String>,
    /// The instrument.
    pub ticker: String,
    /// Signed position. Absent for a poisoned book.
    pub position: Option<Num>,
    /// The basis.
    pub basis: Basis,
    /// Realised P&L, excluding [`REALISED_EXCLUDES`].
    pub realised: Num,
    /// Reducing fills whose realised P&L the fold could not know (inherited basis).
    pub realised_not_held: u32,
    /// Fees paid; absent while any fill's fee is unrecorded.
    pub fees: Option<Num>,
    /// Funding, as the venue signs it: negative when paid.
    pub funding: Num,
    /// The fill that poisoned this book, where one did.
    pub poisoned_by: Option<(u64, u64)>,
    /// Fills counted after the poisoning, never applied.
    pub after_poison: u32,
}

/// What an account's ledger updates moved in and out of one dex's perp margin.
#[derive(Debug, Clone, Default, PartialEq, serde::Serialize)]
pub struct CashFlow {
    /// The sum of known effects.
    pub usdc: Num,
    /// Updates whose effect is unknown: while non-zero, `usdc` is partial.
    pub unknown: u32,
}

/// One account, folded and checked.
#[derive(Debug, Clone, Default, PartialEq, serde::Serialize)]
pub struct AccountFold {
    /// Every book.
    pub books: Vec<BookReport>,
    /// What realised excludes.
    pub realised_excludes: &'static [&'static str],
    /// Continuity breaks.
    pub breaks: Vec<Break>,
    /// Realised skews.
    pub skews: Vec<Skew>,
    /// Realised checks that agreed.
    pub realised_agreements: u64,
    /// Snapshot differences.
    pub snapshot_differences: Vec<SnapshotDifference>,
    /// Snapshot checks that agreed.
    pub snapshot_agreements: u64,
    /// Per dex (`""` is the main one).
    pub cash: BTreeMap<String, CashFlow>,
    /// Why no equity figure is composed. Always stated: a composed equity
    /// needs a mark, which the venue's snapshot does not carry.
    pub equity_not_held: Vec<String>,
}

/// Every polled account, folded: the report the ledger writes after each
/// events pass. **A cache**: deleting it loses nothing, and the next pass
/// writes it again.
#[derive(Debug, Clone, Default, PartialEq, serde::Serialize)]
pub struct FoldReport {
    /// Which venue.
    pub venue: String,
    /// When it was folded.
    pub at_micros: i64,
    /// The tolerances it was checked at.
    pub position_tolerance: Num,
    /// The relative tolerance.
    pub relative_tolerance: Num,
    /// By alias.
    pub accounts: BTreeMap<String, AccountFold>,
}

/// The kinds a fold reads.
pub const FOLD_KINDS: [galata_wire::Kind; 5] = [
    galata_wire::Kind::Fills,
    galata_wire::Kind::FundingPayments,
    galata_wire::Kind::LedgerUpdates,
    galata_wire::Kind::Margin,
    galata_wire::Kind::Positions,
];

type Key = (Option<String>, Ticker);

fn signed(fill: &Fill) -> Num {
    match fill.side {
        Side::Bid => fill.size,
        Side::Ask => -fill.size,
    }
}

fn within(difference: Num, scale: Num, relative: Num) -> bool {
    difference.abs() <= (scale.abs() * relative)
}

/// Fold one account's rows: its fills, funding payments, ledger updates and
/// margin/position snapshots, as `ledger::events::read` returns them.
pub fn fold(rows: &[Envelope], tolerances: &Tolerances) -> AccountFold {
    let mut books: BTreeMap<Key, Book> = BTreeMap::new();
    let mut out = AccountFold {
        realised_excludes: REALISED_EXCLUDES,
        ..AccountFold::default()
    };
    let mut modes: BTreeMap<String, AccountMode> = BTreeMap::new();

    // Venue time order, snapshots after the fills of the same moment. A
    // snapshot's rows share one venue time per dex, and are compared as a set.
    let mut ordered: Vec<&Envelope> = rows.iter().collect();
    ordered.sort_by_key(|e| {
        let snapshot = matches!(e.event, Event::Margin(_) | Event::Position(_));
        (
            e.at_micros.unwrap_or(e.recv_micros),
            snapshot,
            e.recv_micros,
        )
    });

    let mut i = 0;
    while i < ordered.len() {
        let row = ordered[i];
        match &row.event {
            Event::Fill(fill) => apply(&mut books, &mut out, row, fill, tolerances),
            Event::FundingPayment(p) => {
                books
                    .entry((p.dex.clone(), p.ticker.clone()))
                    .or_default()
                    .funding += p.usdc;
            }
            Event::LedgerUpdate(u) => match &u.effect {
                Effect::Known(effects) => {
                    for e in effects {
                        out.cash
                            .entry(e.dex.clone().unwrap_or_default())
                            .or_default()
                            .usdc += e.usdc;
                    }
                }
                Effect::Unknown => {
                    out.cash.entry(String::new()).or_default().unknown += 1;
                }
            },
            Event::Margin(m) => {
                let dex = m.dex.clone();
                modes.insert(dex.clone().unwrap_or_default(), m.mode);
                // The positions of this snapshot: the rows sharing its venue
                // time and dex that follow it.
                let at = row.at_micros.unwrap_or(row.recv_micros);
                let mut held: BTreeMap<Ticker, (Num, Option<Num>)> = BTreeMap::new();
                let mut j = i + 1;
                while j < ordered.len()
                    && ordered[j].at_micros.unwrap_or(ordered[j].recv_micros) == at
                {
                    if let Event::Position(p) = &ordered[j].event
                        && p.dex == dex
                    {
                        held.insert(p.ticker.clone(), (p.size, p.entry_price));
                    }
                    j += 1;
                }
                check_snapshot(&books, &mut out, &dex, at, &held, tolerances);
            }
            _ => {}
        }
        i += 1;
    }

    for ((dex, ticker), book) in &books {
        out.books.push(BookReport {
            dex: dex.clone(),
            ticker: ticker.to_string(),
            position: book.poisoned_by.is_none().then_some(book.position),
            basis: book.basis(),
            realised: book.realised,
            realised_not_held: book.realised_not_held,
            fees: (book.fees_unrecorded == 0).then_some(book.fees),
            funding: book.funding,
            poisoned_by: book.poisoned_by,
            after_poison: book.after_poison,
        });
    }

    out.equity_not_held.push(
        "no mark: the venue's snapshot carries none, and unrealised P&L without a mark is absent"
            .to_string(),
    );
    for (dex, mode) in &modes {
        if let Some(reason) = mode.equity_not_held() {
            out.equity_not_held.push(format!("dex {dex:?}: {reason}"));
        }
    }
    out
}

fn apply(
    books: &mut BTreeMap<Key, Book>,
    out: &mut AccountFold,
    row: &Envelope,
    fill: &Fill,
    tolerances: &Tolerances,
) {
    let key = (fill.dex.clone(), fill.ticker.clone());
    let fresh = !books.contains_key(&key);
    let book = books.entry(key).or_default();
    let id = (fill.trade_id, fill.order_id);

    if book.poisoned_by.is_some() {
        book.after_poison = book.after_poison.saturating_add(1);
        return;
    }
    if !book.seen.insert(id) {
        return;
    }
    if fill.size.is_zero() || fill.price <= Num::ZERO {
        book.poisoned_by = Some(id);
        return;
    }
    match fill.fee {
        Some(fee) => book.fees += fee,
        None => book.fees_unrecorded += 1,
    }

    // The anchor, and continuity: the venue says what the position was.
    if let Some(stated) = fill.start_position {
        if fresh {
            book.position = stated;
            book.basis = Some(if stated.is_zero() {
                Basis::Flat
            } else {
                Basis::Unknown
            });
        } else if (book.position - stated).abs() > tolerances.position {
            out.breaks.push(Break {
                dex: fill.dex.clone(),
                ticker: fill.ticker.to_string(),
                trade_id: fill.trade_id,
                order_id: fill.order_id,
                at_micros: row.at_micros,
                folded: book.position,
                stated,
            });
            book.position = stated;
            book.basis = Some(if stated.is_zero() {
                Basis::Flat
            } else {
                Basis::Unknown
            });
        }
    }

    let quantity = signed(fill);
    let position = book.position;
    let price = fill.price;
    let same_side =
        position.is_zero() || position.is_sign_positive() == quantity.is_sign_positive();

    if same_side {
        book.basis = Some(match book.basis() {
            Basis::Flat => Basis::Known(price),
            Basis::Known(basis) => {
                let (open, added) = (position.abs(), quantity.abs());
                Basis::Known((open * basis + added * price) / (open + added))
            }
            Basis::Unknown => Basis::Unknown,
        });
        book.position = position + quantity;
        return;
    }

    let closing = quantity.abs().min(position.abs());
    let direction = if position.is_sign_positive() {
        Num::ONE
    } else {
        -Num::ONE
    };
    match book.basis() {
        Basis::Known(basis) => {
            let realised = closing * (price - basis) * direction;
            book.realised += realised;
            if let Some(stated) = fill.closed_pnl {
                let difference = stated - realised;
                if within(difference, closing * price, tolerances.relative) {
                    out.realised_agreements += 1;
                } else {
                    out.skews.push(Skew {
                        dex: fill.dex.clone(),
                        ticker: fill.ticker.to_string(),
                        trade_id: fill.trade_id,
                        order_id: fill.order_id,
                        folded: realised,
                        stated,
                        difference,
                    });
                }
            }
        }
        _ => book.realised_not_held += 1,
    }
    book.position = position + quantity;
    if book.position.is_zero() {
        book.basis = Some(Basis::Flat);
    } else if book.position.is_sign_positive() != position.is_sign_positive() {
        // A flip realised the whole old position; the rest opens here.
        book.basis = Some(Basis::Known(price));
    }
}

fn check_snapshot(
    books: &BTreeMap<Key, Book>,
    out: &mut AccountFold,
    dex: &Option<String>,
    at: i64,
    held: &BTreeMap<Ticker, (Num, Option<Num>)>,
    tolerances: &Tolerances,
) {
    let mut tickers: BTreeSet<&Ticker> = held.keys().collect();
    tickers.extend(
        books
            .iter()
            .filter(|((d, _), b)| d == dex && b.poisoned_by.is_none() && !b.position.is_zero())
            .map(|((_, t), _)| t),
    );
    for ticker in tickers {
        let book = books
            .get(&(dex.clone(), ticker.clone()))
            .filter(|b| b.poisoned_by.is_none());
        let folded = book.map(|b| b.position).unwrap_or(Num::ZERO);
        let stated = held.get(ticker).map(|(size, _)| *size).unwrap_or(Num::ZERO);
        if (folded - stated).abs() <= tolerances.position {
            out.snapshot_agreements += 1;
        } else {
            out.snapshot_differences.push(SnapshotDifference {
                dex: dex.clone(),
                ticker: ticker.to_string(),
                at_micros: at,
                quantity: "position",
                folded: book.map(|b| b.position),
                stated: held.get(ticker).map(|(size, _)| *size),
            });
            continue;
        }
        if let (Some(Basis::Known(basis)), Some((_, Some(entry)))) =
            (book.map(Book::basis), held.get(ticker))
        {
            if within(basis - entry, *entry, tolerances.relative) {
                out.snapshot_agreements += 1;
            } else {
                out.snapshot_differences.push(SnapshotDifference {
                    dex: dex.clone(),
                    ticker: ticker.to_string(),
                    at_micros: at,
                    quantity: "basis",
                    folded: Some(basis),
                    stated: Some(*entry),
                });
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use galata_wire::{Account, Margin, Position, Venue};
    use std::str::FromStr;

    fn n(s: &str) -> Num {
        Num::from_str(s).unwrap()
    }
    fn tol() -> Tolerances {
        Tolerances {
            position: n("0"),
            relative: n("0.00002"),
        }
    }
    fn env(at: i64, event: Event) -> Envelope {
        Envelope::for_account(
            Venue::new("hyperliquid").unwrap(),
            Account::new("main").unwrap(),
            Some(at),
            at,
            event,
        )
    }
    /// A fill: `size` signed (buy positive), at `price`, stating `start`.
    fn fill(id: u64, at: i64, size: &str, price: &str, start: &str) -> Envelope {
        let signed = n(size);
        env(
            at,
            Event::Fill(Fill {
                dex: None,
                ticker: Ticker::new("BTC").unwrap(),
                side: if signed.is_sign_positive() {
                    Side::Bid
                } else {
                    Side::Ask
                },
                price: n(price),
                size: signed.abs(),
                start_position: Some(n(start)),
                direction: None,
                closed_pnl: None,
                fee: Some(n("0")),
                fee_token: None,
                builder_fee: None,
                crossed: None,
                order_id: id,
                trade_id: id,
                twap_id: None,
            }),
        )
    }
    fn with_pnl(mut e: Envelope, pnl: &str) -> Envelope {
        if let Event::Fill(f) = &mut e.event {
            f.closed_pnl = Some(n(pnl));
        }
        e
    }
    fn book(f: &AccountFold) -> &BookReport {
        &f.books[0]
    }

    #[test]
    fn cost_basis_is_a_weighted_average() {
        let f = fold(
            &[fill(1, 1, "1", "100", "0"), fill(2, 2, "3", "200", "1")],
            &tol(),
        );
        assert_eq!(book(&f).position, Some(n("4")));
        assert_eq!(book(&f).basis, Basis::Known(n("175")));
    }

    #[test]
    fn a_partial_reduction_realises_partially_and_keeps_the_basis() {
        let f = fold(
            &[fill(1, 1, "2", "100", "0"), fill(2, 2, "-1", "110", "2")],
            &tol(),
        );
        assert_eq!(book(&f).realised, n("10"));
        assert_eq!(book(&f).basis, Basis::Known(n("100")));
    }

    #[test]
    fn a_flip_realises_fully_and_opens_at_the_fills_price() {
        let f = fold(
            &[fill(1, 1, "2", "100", "0"), fill(2, 2, "-5", "110", "2")],
            &tol(),
        );
        assert_eq!(book(&f).realised, n("20"));
        assert_eq!(book(&f).position, Some(n("-3")));
        assert_eq!(book(&f).basis, Basis::Known(n("110")));
    }

    #[test]
    fn a_short_reduction_realises_with_the_right_sign() {
        let f = fold(
            &[fill(1, 1, "-2", "100", "0"), fill(2, 2, "1", "90", "-2")],
            &tol(),
        );
        assert_eq!(
            book(&f).realised,
            n("10"),
            "a short bought back lower made money"
        );
    }

    #[test]
    fn flat_has_no_basis() {
        let f = fold(
            &[fill(1, 1, "1", "100", "0"), fill(2, 2, "-1", "100", "1")],
            &tol(),
        );
        assert_eq!(book(&f).basis, Basis::Flat);
    }

    #[test]
    fn a_fill_heard_twice_moves_nothing_twice() {
        let f = fold(
            &[fill(1, 1, "1", "100", "0"), fill(1, 1, "1", "100", "0")],
            &tol(),
        );
        assert_eq!(book(&f).position, Some(n("1")));
    }

    #[test]
    fn an_unrecorded_fee_is_not_a_fee_of_zero() {
        let mut unfeed = fill(2, 2, "1", "100", "1");
        if let Event::Fill(f) = &mut unfeed.event {
            f.fee = None;
        }
        let f = fold(&[fill(1, 1, "1", "100", "0"), unfeed], &tol());
        assert_eq!(book(&f).fees, None);
    }

    #[test]
    fn poisoning_is_scoped_to_its_book_and_nothing_resurrects_it() {
        let mut eth = fill(9, 1, "1", "10", "0");
        if let Event::Fill(f) = &mut eth.event {
            f.ticker = Ticker::new("ETH").unwrap();
        }
        let f = fold(
            &[
                fill(1, 1, "0", "100", "0"),
                fill(2, 2, "1", "100", "0"),
                eth,
            ],
            &tol(),
        );
        let btc = f.books.iter().find(|b| b.ticker == "BTC").unwrap();
        assert_eq!(btc.poisoned_by, Some((1, 1)));
        assert_eq!(btc.after_poison, 1, "counted, never applied");
        assert_eq!(btc.position, None);
        let eth = f.books.iter().find(|b| b.ticker == "ETH").unwrap();
        assert_eq!(eth.position, Some(n("1")));
    }

    #[test]
    fn history_that_began_mid_position_has_an_unknown_basis() {
        let f = fold(&[fill(1, 1, "-2", "110", "5")], &tol());
        assert_eq!(book(&f).position, Some(n("3")), "opens at the venue's 5");
        assert_eq!(book(&f).basis, Basis::Unknown);
        assert_eq!(book(&f).realised_not_held, 1);
        assert_eq!(book(&f).realised, n("0"));
    }

    #[test]
    fn flat_makes_everything_known() {
        let f = fold(
            &[
                fill(1, 1, "-5", "110", "5"),
                fill(2, 2, "2", "100", "0"),
                fill(3, 3, "-2", "120", "2"),
            ],
            &tol(),
        );
        assert_eq!(book(&f).realised, n("40"), "known again after flat");
        assert_eq!(book(&f).realised_not_held, 1);
    }

    #[test]
    fn a_missing_fill_is_found_at_the_fill_after_it() {
        let f = fold(
            &[fill(1, 1, "2", "100", "0"), fill(3, 3, "1", "100", "3")],
            &tol(),
        );
        assert_eq!(f.breaks.len(), 1);
        let b = &f.breaks[0];
        assert_eq!((b.trade_id, b.folded, b.stated), (3, n("2"), n("3")));
        assert_eq!(
            book(&f).position,
            Some(n("4")),
            "continues from the venue's 3"
        );
        assert_eq!(book(&f).basis, Basis::Unknown);
    }

    #[test]
    fn rounding_within_the_tolerance_is_agreement() {
        // Measured: the fold realised 16.895924 and the venue stated 16.89658.
        let f = fold(
            &[
                fill(1, 1, "1", "8000", "0"),
                with_pnl(fill(2, 2, "-1", "8016.895924", "1"), "16.89658"),
            ],
            &tol(),
        );
        assert!(f.skews.is_empty(), "{:?}", f.skews);
        assert_eq!(f.realised_agreements, 1);
    }

    #[test]
    fn a_zero_tolerance_reports_every_difference() {
        let exact = Tolerances {
            position: n("0"),
            relative: n("0"),
        };
        let f = fold(
            &[
                fill(1, 1, "1", "8000", "0"),
                with_pnl(fill(2, 2, "-1", "8016.895924", "1"), "16.89658"),
            ],
            &exact,
        );
        assert_eq!(f.skews.len(), 1);
        assert_eq!(f.skews[0].difference, n("0.000656"));
    }

    fn snapshot(at: i64, positions: &[(&str, &str, &str)], mode: AccountMode) -> Vec<Envelope> {
        let mut rows = vec![env(
            at,
            Event::Margin(Margin {
                dex: None,
                mode,
                equity_not_held: mode.equity_not_held().map(str::to_string),
                account_value: None,
                total_notional: None,
                total_raw_usd: None,
                margin_used: None,
                maintenance_margin_used: None,
                withdrawable: None,
            }),
        )];
        for (ticker, size, entry) in positions {
            rows.push(env(
                at,
                Event::Position(Position {
                    dex: None,
                    ticker: Ticker::new(*ticker).unwrap(),
                    size: n(size),
                    entry_price: Some(n(entry)),
                    mark: None,
                    position_value: None,
                    unrealised_pnl: None,
                    return_on_equity: None,
                    liquidation_price: None,
                    leverage: None,
                    leverage_type: None,
                    max_leverage: None,
                    margin_used: None,
                    funding_all_time: None,
                    funding_since_open: None,
                    funding_since_change: None,
                }),
            ));
        }
        rows
    }

    #[test]
    fn a_position_the_snapshot_holds_and_the_fold_does_not_is_named() {
        let rows = snapshot(10, &[("BTC", "1", "100")], AccountMode::Disabled);
        let f = fold(&rows, &tol());
        assert_eq!(f.snapshot_differences.len(), 1);
        let d = &f.snapshot_differences[0];
        assert_eq!(
            (d.ticker.as_str(), d.folded, d.stated),
            ("BTC", None, Some(n("1")))
        );
    }

    #[test]
    fn a_book_that_agrees_with_its_snapshot_is_counted() {
        let mut rows = vec![fill(1, 1, "1", "100", "0")];
        rows.extend(snapshot(10, &[("BTC", "1", "100")], AccountMode::Disabled));
        let f = fold(&rows, &tol());
        assert!(
            f.snapshot_differences.is_empty(),
            "{:?}",
            f.snapshot_differences
        );
        assert_eq!(f.snapshot_agreements, 2, "position and basis");
    }

    #[test]
    fn a_unified_account_has_no_equity_figure() {
        let f = fold(&snapshot(10, &[], AccountMode::Unified), &tol());
        assert!(
            f.equity_not_held
                .iter()
                .any(|r| r.contains("collateral is spot")),
            "{:?}",
            f.equity_not_held
        );
    }

    #[test]
    fn an_unknown_effect_makes_a_cash_flow_partial() {
        let update = |effect| {
            env(
                1,
                Event::LedgerUpdate(galata_wire::LedgerUpdate {
                    kind: "x".into(),
                    effect,
                    counterparty: None,
                    token: None,
                    amount: None,
                    fee: None,
                }),
            )
        };
        let f = fold(
            &[
                update(Effect::Known(vec![galata_wire::DexEffect {
                    dex: None,
                    usdc: n("100"),
                }])),
                update(Effect::Unknown),
            ],
            &tol(),
        );
        let main = &f.cash[""];
        assert_eq!((main.usdc, main.unknown), (n("100"), 1));
    }
}
