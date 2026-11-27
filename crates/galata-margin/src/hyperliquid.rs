//! **Hyperliquid's margin, computed by galata** from the venue's published
//! formulas (design D4), as pure functions of recorded account state.
//!
//! Read from the venue's pages on 2026-11-27 (`design/measured.md`,
//! "Hyperliquid's margin, read from its pages"):
//!
//! - **Initial margin is the same in every mode**: a position needs
//!   `size * mark / leverage` (`trading/margining`). Portfolio margin changes
//!   how that is funded, not what it is. [`initial_requirement`].
//! - **Portfolio margin defines maintenance**, through the portfolio margin
//!   ratio, liquidatable above 0.95. [`portfolio_margin`].
//!
//! The page leaves two things open, and the record settles them as far as it
//! can (221 recorded snapshots with open positions and no borrow, 2026-11-26
//! to 27, `design/measured.md`):
//! - **A token's requirement counts the maintenance of the dexes it
//!   collateralises**, not of every dex. Read literally, USDT0 with no
//!   balance would carry USDC's dexes against HYPE's credit alone and set
//!   the maximum, above what the venue states.
//! - **The 20 USDC offset applies only while the token is borrowed.** Without
//!   it the ratio is the venue's to the fourth significant figure; with it,
//!   never, and a flat account would read above zero where the venue states 0.
//!
//! What it does not model, and why a computed figure may disagree with the
//! venue's: margin tiers above a market's first notional band; borrowing to
//! fund a new position's initial margin; and the spot price in the borrow
//! oracle median, which galata does not capture, so the median of the mark
//! and the oracle stands for it. The agreement check (task 3.3) measures the
//! result against the venue on every snapshot.

use std::collections::BTreeMap;

use galata_wire::event::AccountMode;
use rust_decimal::Decimal;

/// Why there is no figure. Stated to the owner as written.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct NoFigure(pub String);

/// One recorded perpetual position, or one proposed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PerpPosition {
    /// The dex: `""` for the main one.
    pub dex: String,
    /// The market, by the venue's name.
    pub market: String,
    /// Signed size, in the market's base asset.
    pub size: Decimal,
    /// The mark the requirement is computed at.
    pub mark: Decimal,
    /// The leverage in force for the market. `None` when none is recorded:
    /// the account has no position there and the venue's setting is not read.
    pub leverage: Option<Decimal>,
}

/// The initial margin one position needs: its notional over its leverage.
pub fn initial_margin(notional: Decimal, leverage: Decimal) -> Result<Decimal, NoFigure> {
    if leverage <= Decimal::ZERO {
        return Err(NoFigure(format!(
            "a leverage of {leverage} is not a leverage"
        )));
    }
    Ok(notional.abs() / leverage)
}

/// The initial margin an account's positions need, in its mode. The formula
/// is the venue's for every mode it names; an unknown mode is not assumed to
/// be one of them.
pub fn initial_requirement(
    mode: AccountMode,
    positions: &[PerpPosition],
) -> Result<Decimal, NoFigure> {
    match mode {
        AccountMode::Unknown => {
            return Err(NoFigure(
                "the account's mode is unknown, so the venue's margin formula for it is too".into(),
            ));
        }
        AccountMode::Unified
        | AccountMode::PortfolioMargin
        | AccountMode::Disabled
        | AccountMode::Default => {}
        // `#[non_exhaustive]`: a mode added to the vocabulary later is not
        // assumed to margin as these do.
        #[allow(unreachable_patterns)]
        _ => {
            return Err(NoFigure(format!(
                "the account's mode ({mode:?}) has no margin formula here"
            )));
        }
    }
    let mut total = Decimal::ZERO;
    for p in positions {
        let leverage = p.leverage.ok_or_else(|| {
            NoFigure(format!(
                "no leverage is recorded for {}: the account holds no position there",
                p.market
            ))
        })?;
        total += initial_margin(p.size * p.mark, leverage)?;
    }
    Ok(total)
}

/// The venue's user caps, from `trading/portfolio-margin` (2026-11-27), by
/// the token names the venue's answers use: its USDT is `USDT0`, its BTC
/// `UBTC`.
fn borrow_cap(token: &str) -> Option<Decimal> {
    match token {
        "USDC" => Some(Decimal::from(50_000_000)),
        "USDT0" => Some(Decimal::from(1_000_000)),
        _ => None,
    }
}

fn supply_cap(token: &str) -> Option<Decimal> {
    match token {
        "USDC" => Some(Decimal::from(250_000_000)),
        "USDT0" => Some(Decimal::from(5_000_000)),
        "UBTC" => Some(Decimal::from(200)),
        "HYPE" => Some(Decimal::from(1_000_000)),
        _ => None,
    }
}

/// The tokens the ratio's maximum runs over: those the venue lends.
pub const BORROWABLE: [&str; 2] = ["USDC", "USDT0"];
/// The portfolio margin ratio above which the account is liquidatable.
pub const LIQUIDATABLE_ABOVE: &str = "0.95";
/// The offset added to a borrowed token's requirement, in USDC.
const MIN_BORROW_OFFSET: u32 = 20;

/// A portfolio-margin account as recorded at one snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct PmState {
    /// Each dex's cross maintenance margin used, as the venue stated it.
    pub dex_maintenance: BTreeMap<String, Decimal>,
    /// Each dex's collateral token. The main dex and `xyz` are USDC's.
    pub dex_collateral: BTreeMap<String, String>,
    /// Each token's spot balance.
    pub balances: BTreeMap<String, Decimal>,
    /// Each collateral token's loan-to-value, as the venue states it.
    pub ltv: BTreeMap<String, Decimal>,
    /// Each token's borrowed size.
    pub borrowed: BTreeMap<String, Decimal>,
    /// Each token's borrow oracle price, in USDC. USDC's is 1.
    pub prices: BTreeMap<String, Decimal>,
}

/// galata's portfolio-margin figure for an account.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PmFigure {
    /// The ratio: the largest over the borrowable tokens.
    pub ratio: Decimal,
    /// The maintenance requirement of the token that sets the ratio, in USDC.
    pub requirement: Decimal,
    /// That token's liquidation value, in USDC.
    pub liquidation_value: Decimal,
    /// The token that sets it; `None` when every token's requirement is zero.
    pub binding: Option<String>,
}

impl PmState {
    fn price(&self, token: &str) -> Result<Decimal, NoFigure> {
        if token == "USDC" {
            return Ok(Decimal::ONE);
        }
        self.prices
            .get(token)
            .copied()
            .ok_or_else(|| NoFigure(format!("no borrow oracle price for {token}")))
    }

    /// One token's maintenance requirement and liquidation value.
    fn token(&self, token: &str) -> Result<(Decimal, Decimal), NoFigure> {
        let mut requirement: Decimal = self
            .dex_maintenance
            .iter()
            .filter(|(dex, _)| self.dex_collateral.get(*dex).map(String::as_str) == Some(token))
            .map(|(_, mm)| *mm)
            .sum();
        let borrowed = self.borrowed.get(token).copied().unwrap_or_default();
        if borrowed > Decimal::ZERO {
            requirement += Decimal::from(MIN_BORROW_OFFSET) + borrowed * self.price(token)?;
        }
        if requirement.is_zero() {
            return Ok((Decimal::ZERO, Decimal::ZERO));
        }
        let half = Decimal::new(5, 1);
        let mut value = self.balances.get(token).copied().unwrap_or_default();
        for (collateral, balance) in &self.balances {
            let ltv = self.ltv.get(collateral).copied().unwrap_or_default();
            if collateral == token || ltv <= Decimal::ZERO || *balance <= Decimal::ZERO {
                continue;
            }
            let held = supply_cap(collateral).map_or(*balance, |cap| (*balance).min(cap));
            let credit = held * self.price(collateral)? * (half + half * ltv);
            value += borrow_cap(token).map_or(credit, |cap| credit.min(cap));
        }
        Ok((requirement, value))
    }
}

/// The portfolio margin ratio, and the requirement behind it.
pub fn portfolio_margin(state: &PmState) -> Result<PmFigure, NoFigure> {
    let mut figure = PmFigure {
        ratio: Decimal::ZERO,
        requirement: Decimal::ZERO,
        liquidation_value: Decimal::ZERO,
        binding: None,
    };
    for token in BORROWABLE {
        let (requirement, value) = state.token(token)?;
        if requirement.is_zero() {
            continue;
        }
        if value <= Decimal::ZERO {
            return Err(NoFigure(format!(
                "{token} carries a requirement of {requirement} and no liquidation value"
            )));
        }
        let ratio = requirement / value;
        if figure.binding.is_none() || ratio > figure.ratio {
            figure = PmFigure {
                ratio,
                requirement,
                liquidation_value: value,
                binding: Some(token.to_string()),
            };
        }
    }
    Ok(figure)
}

/// The state with a perpetual added on a dex: the dex's maintenance grows by
/// the market's change in maintenance, half the initial margin at the
/// market's maximum leverage (`trading/margining`), on the net position.
pub fn with_perp(
    state: &PmState,
    dex: &str,
    held: Decimal,
    added: Decimal,
    mark: Decimal,
    max_leverage: u32,
) -> Result<PmState, NoFigure> {
    if max_leverage == 0 {
        return Err(NoFigure("a maximum leverage of 0".into()));
    }
    let rate = Decimal::ONE / (Decimal::from(2) * Decimal::from(max_leverage));
    let change = ((held + added).abs() - held.abs()) * mark * rate;
    let mut after = state.clone();
    *after.dex_maintenance.entry(dex.to_string()).or_default() += change;
    Ok(after)
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;

    fn d(s: &str) -> Decimal {
        Decimal::from_str(s).unwrap()
    }

    #[test]
    fn twenty_eight_thousand_at_ten_times_is_two_thousand_eight_hundred() {
        assert_eq!(initial_margin(d("28000"), d("10")).unwrap(), d("2800"));
        let short = PerpPosition {
            dex: String::new(),
            market: "BTC".into(),
            size: d("-0.5"),
            mark: d("56000"),
            leverage: Some(d("10")),
        };
        for mode in [AccountMode::Default, AccountMode::PortfolioMargin] {
            assert_eq!(
                initial_requirement(mode, std::slice::from_ref(&short)).unwrap(),
                d("2800")
            );
        }
    }

    #[test]
    fn an_unknown_account_mode_has_no_figure_and_says_why() {
        let err = initial_requirement(AccountMode::Unknown, &[]).unwrap_err();
        assert!(err.0.contains("unknown"), "{err}");
    }

    #[test]
    fn a_market_with_no_recorded_leverage_has_no_figure_and_names_it() {
        let p = PerpPosition {
            dex: String::new(),
            market: "ETH".into(),
            size: d("1"),
            mark: d("3000"),
            leverage: None,
        };
        let err = initial_requirement(AccountMode::Default, &[p]).unwrap_err();
        assert!(err.0.contains("ETH"), "{err}");
    }

    /// The recorded snapshots, amounts scaled by one factor kept out of the
    /// repository; a ratio is unchanged by it.
    fn snapshots() -> Vec<(String, PmState, Decimal)> {
        let text = include_str!("../tests/fixtures/hyperliquid_pm_snapshots.json");
        let rows: serde_json::Value = serde_json::from_str(text).unwrap();
        let map = |v: &serde_json::Value| -> BTreeMap<String, Decimal> {
            v.as_object()
                .unwrap()
                .iter()
                .map(|(k, v)| (k.clone(), d(v.as_str().unwrap())))
                .collect()
        };
        rows.as_array()
            .unwrap()
            .iter()
            .map(|row| {
                let mut prices = BTreeMap::new();
                for (token, p) in row["hyperliquid_prices"].as_object().unwrap() {
                    // The median of the spot price, the mark and the oracle;
                    // with no spot price captured, the median of the two.
                    let (mark, oracle) = (
                        d(p["mark"].as_str().unwrap()),
                        d(p["oracle"].as_str().unwrap()),
                    );
                    prices.insert(token.clone(), (mark + oracle) / Decimal::from(2));
                }
                let state = PmState {
                    dex_maintenance: map(&row["dex_maintenance"]),
                    dex_collateral: [("", "USDC"), ("xyz", "USDC")]
                        .map(|(k, v)| (k.to_string(), v.to_string()))
                        .into(),
                    balances: map(&row["balances"]),
                    ltv: map(&row["ltv"]),
                    borrowed: map(&row["borrowed"]),
                    prices,
                };
                (
                    row["what"].as_str().unwrap().to_string(),
                    state,
                    d(row["stated_ratio"].as_str().unwrap()),
                )
            })
            .collect()
    }

    #[test]
    fn the_ratio_of_each_recorded_snapshot_is_the_venues() {
        for (what, state, stated) in snapshots() {
            let figure = portfolio_margin(&state).unwrap();
            let gap = (figure.ratio - stated).abs();
            // USDC only: within 0.00001. The dexes' state and the spot state
            // are separate answers, read moments apart, and a dex's
            // maintenance moves with its marks between them (measured: 6.5e-7
            // on the first snapshot). With HYPE, also the spot price the
            // venue's median uses, which galata does not capture.
            let within = if state
                .balances
                .get("HYPE")
                .is_some_and(|h| *h > Decimal::ZERO)
            {
                d("0.001")
            } else {
                d("0.00001")
            };
            assert!(
                gap <= within,
                "{what}: galata {} venue {stated}, gap {gap}",
                figure.ratio
            );
            if stated.is_zero() {
                assert_eq!(figure.binding, None, "{what}");
            } else {
                assert_eq!(figure.binding.as_deref(), Some("USDC"), "{what}");
            }
        }
    }

    #[test]
    fn a_perp_added_raises_the_requirement_by_its_maintenance() {
        let (_, state, _) = snapshots().remove(0);
        let before = portfolio_margin(&state).unwrap();
        // 0.5 BTC short at 56,000 on the main dex, max leverage 40: 28,000
        // notional, maintenance 1/80 of it, 350.
        let after = with_perp(&state, "", Decimal::ZERO, d("-0.5"), d("56000"), 40).unwrap();
        let after = portfolio_margin(&after).unwrap();
        assert_eq!(after.requirement - before.requirement, d("350"));
        assert_eq!(after.liquidation_value, before.liquidation_value);
        assert!(after.ratio > before.ratio);
        // Closing what is held lowers it.
        let closed = with_perp(&state, "", d("-0.5"), d("0.5"), d("56000"), 40).unwrap();
        assert_eq!(
            closed.dex_maintenance[""],
            state.dex_maintenance[""] - d("350")
        );
    }

    #[test]
    fn a_borrow_adds_the_offset_and_the_borrowed_value() {
        let (_, mut state, _) = snapshots().remove(0);
        let before = portfolio_margin(&state).unwrap();
        state.borrowed.insert("USDC".into(), d("1000"));
        let after = portfolio_margin(&state).unwrap();
        assert_eq!(after.requirement - before.requirement, d("1020"));
    }

    #[test]
    fn collateral_with_no_price_has_no_figure_and_names_it() {
        let (_, mut state, _) = snapshots().remove(1);
        state.prices.clear();
        let err = portfolio_margin(&state).unwrap_err();
        assert!(err.0.contains("HYPE"), "{err}");
    }
}
