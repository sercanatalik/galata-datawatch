//! **Each venue's estimate**: its margin before and after a placement's
//! positions are added, with the mode it is computed in, where each figure
//! came from, its estimate label and its time (tasks 5.2 and 5.3).
//!
//! **A venue with no figure says why and stops nothing.** A stale snapshot, a
//! venue that did not answer and a mode with no formula each give
//! [`VenueEstimate::Missing`] with the reason, and the other venue's estimate
//! is made as if it were alone.
//!
//! **No clock is read here.** The time is passed in, so every bound is
//! asserted by a test that sets it (`scripts/check-clock-discipline.sh`
//! covers this crate; its binary is its clock).

use rust_decimal::Decimal;

use crate::advice::{Estimate, EstimateLabel, Figure, Margins, Origin, Venue, VenueEstimate};
use crate::derive::MarginAnswer;
use crate::hyperliquid::{
    NoFigure, PerpPosition, initial_requirement, portfolio_margin, with_perp,
};
use crate::state::{DeriveState, HyperliquidState};

/// The snapshot is within the bound, or the reason it is refused, with its
/// age (design D8).
pub fn fresh(snapshot_micros: i64, now_micros: i64, bound_secs: u64) -> Result<(), String> {
    let age = (now_micros - snapshot_micros).max(0) / 1_000_000;
    if age > bound_secs as i64 {
        return Err(format!(
            "its snapshot is {age} s old, past the {bound_secs} s the configuration allows"
        ));
    }
    Ok(())
}

/// The part of a hedge placed on Hyperliquid.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HedgeOnHyperliquid {
    /// The dex: `""` for the main one.
    pub dex: String,
    /// The market.
    pub market: String,
    /// Signed size.
    pub size: Decimal,
    /// The mark it is computed at, read when advice is asked for.
    pub mark: Decimal,
    /// The leverage it would be opened at: the market's own position's where
    /// one is held, else the one the caller read from the venue.
    pub leverage: Decimal,
    /// The market's maximum leverage, which sets its maintenance.
    pub max_leverage: u32,
}

/// Hyperliquid's estimate for an account, with the hedge added if any.
pub fn hyperliquid_estimate(
    state: &HyperliquidState,
    hedge: Option<&HedgeOnHyperliquid>,
    prices: &std::collections::BTreeMap<String, Decimal>,
    label: &Option<EstimateLabel>,
    now_micros: i64,
    bound_secs: u64,
) -> VenueEstimate {
    let missing = |reason: String| VenueEstimate::Missing {
        venue: Venue::Hyperliquid,
        account: state.account.clone(),
        reason,
    };
    if let Err(reason) = fresh(state.at_micros, now_micros, bound_secs) {
        return missing(reason);
    }
    match hyperliquid_margins(state, hedge, prices, label) {
        Ok((before, after)) => VenueEstimate::Estimate(Estimate {
            venue: Venue::Hyperliquid,
            account: state.account.clone(),
            mode: serde_json::to_value(state.mode)
                .ok()
                .and_then(|v| v.as_str().map(str::to_string))
                .unwrap_or_else(|| "unknown".into()),
            before,
            after,
            snapshot_micros: state.at_micros,
        }),
        Err(NoFigure(reason)) => missing(reason),
    }
}

fn hyperliquid_margins(
    state: &HyperliquidState,
    hedge: Option<&HedgeOnHyperliquid>,
    prices: &std::collections::BTreeMap<String, Decimal>,
    label: &Option<EstimateLabel>,
) -> Result<(Margins, Margins), NoFigure> {
    let held: Vec<PerpPosition> = state
        .positions
        .iter()
        .map(|p| {
            Ok(PerpPosition {
                dex: p.dex.clone(),
                market: p.market.clone(),
                size: p.size,
                mark: p
                    .mark
                    .ok_or_else(|| NoFigure(format!("no mark is recorded for {}", p.market)))?,
                leverage: p.leverage,
            })
        })
        .collect::<Result<_, NoFigure>>()?;
    let initial_before = initial_requirement(state.mode, &held)?;

    // With the hedge: the market's position netted, at the hedge's leverage
    // where none is held.
    let mut after_positions = held.clone();
    let mut held_in_market = Decimal::ZERO;
    if let Some(h) = hedge {
        match after_positions
            .iter_mut()
            .find(|p| p.dex == h.dex && p.market == h.market)
        {
            Some(p) => {
                held_in_market = p.size;
                p.size += h.size;
                p.mark = h.mark;
            }
            None => after_positions.push(PerpPosition {
                dex: h.dex.clone(),
                market: h.market.clone(),
                size: h.size,
                mark: h.mark,
                leverage: Some(h.leverage),
            }),
        }
    }
    let initial_after = initial_requirement(state.mode, &after_positions)?;

    let (maintenance_before, maintenance_after) = if state.mode
        == galata_wire::event::AccountMode::PortfolioMargin
    {
        let mut pm = state.pm.clone();
        pm.prices = prices.clone();
        let before = portfolio_margin(&pm)?.requirement;
        let after = match hedge {
            Some(h) => {
                portfolio_margin(&with_perp(
                    &pm,
                    &h.dex,
                    held_in_market,
                    h.size,
                    h.mark,
                    h.max_leverage,
                )?)?
                .requirement
            }
            None => before,
        };
        (before, after)
    } else {
        // Outside portfolio margin, maintenance is half the initial margin
        // at the market's maximum leverage (`trading/margining`).
        let maintenance =
            |positions: &[PerpPosition], hedge_max: Option<u32>| -> Result<Decimal, NoFigure> {
                let mut total = Decimal::ZERO;
                for p in positions {
                    let max = state
                        .positions
                        .iter()
                        .find(|s| s.dex == p.dex && s.market == p.market)
                        .and_then(|s| s.max_leverage)
                        .or(hedge_max)
                        .ok_or_else(|| {
                            NoFigure(format!("no maximum leverage is recorded for {}", p.market))
                        })?;
                    total += (p.size * p.mark).abs() / (Decimal::from(2) * Decimal::from(max));
                }
                Ok(total)
            };
        (
            maintenance(&held, None)?,
            maintenance(&after_positions, hedge.map(|h| h.max_leverage))?,
        )
    };

    let figure = |value| Figure {
        value,
        origin: Origin::Computed,
        estimate: label.clone(),
        at_micros: state.at_micros,
    };
    Ok((
        Margins {
            initial: figure(initial_before),
            maintenance: figure(maintenance_before),
        },
        Margins {
            initial: figure(initial_after),
            maintenance: figure(maintenance_after),
        },
    ))
}

/// Derive's estimate for a subaccount: the venue's simulation, carried as it
/// answered, or why there is none.
pub fn derive_estimate(
    state: &DeriveState,
    simulation: Result<&MarginAnswer, String>,
    simulated_at_micros: i64,
    now_micros: i64,
    bound_secs: u64,
) -> VenueEstimate {
    let missing = |reason: String| VenueEstimate::Missing {
        venue: Venue::Derive,
        account: state.account.clone(),
        reason,
    };
    if let Err(reason) = fresh(state.at_micros, now_micros, bound_secs) {
        return missing(reason);
    }
    let answer = match simulation {
        Ok(answer) => answer,
        Err(reason) => return missing(reason),
    };
    let figure = |value| Figure {
        value,
        origin: Origin::Venue,
        estimate: None,
        at_micros: simulated_at_micros,
    };
    VenueEstimate::Estimate(Estimate {
        venue: Venue::Derive,
        account: state.account.clone(),
        mode: state
            .margin_type
            .clone()
            .unwrap_or_else(|| "unknown".into()),
        before: Margins {
            initial: figure(answer.pre_initial),
            maintenance: figure(answer.pre_maintenance),
        },
        after: Margins {
            initial: figure(answer.post_initial),
            maintenance: figure(answer.post_maintenance),
        },
        snapshot_micros: state.at_micros,
    })
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::str::FromStr;

    use galata_wire::event::AccountMode;

    use super::*;
    use crate::hyperliquid::PmState;
    use crate::state::HlPosition;

    fn d(s: &str) -> Decimal {
        Decimal::from_str(s).unwrap()
    }

    const S: i64 = 1_000_000;
    const NOW: i64 = 1_795_800_000 * S;
    const BOUND: u64 = 900;

    #[test]
    fn a_snapshot_inside_the_bound_is_taken_and_one_past_it_is_refused_with_its_age() {
        assert!(fresh(NOW - 900 * S, NOW, BOUND).is_ok());
        let refused = fresh(NOW - 901 * S, NOW, BOUND).unwrap_err();
        assert!(
            refused.contains("901 s old") && refused.contains("900 s"),
            "{refused}"
        );
    }

    fn hyperliquid(at: i64) -> HyperliquidState {
        HyperliquidState {
            account: "main".into(),
            mode: AccountMode::PortfolioMargin,
            positions: vec![HlPosition {
                dex: "xyz".into(),
                market: "GOLD".into(),
                size: d("2"),
                mark: Some(d("4000")),
                leverage: Some(d("20")),
                max_leverage: Some(25),
            }],
            pm: PmState {
                dex_maintenance: [("xyz".to_string(), d("160"))].into(),
                dex_collateral: [
                    ("".to_string(), "USDC".to_string()),
                    ("xyz".to_string(), "USDC".to_string()),
                ]
                .into(),
                balances: [("USDC".to_string(), d("20000"))].into(),
                ..PmState::default()
            },
            stated_ratio: Some(d("0.008")),
            at_micros: at,
        }
    }

    fn derive(at: i64) -> DeriveState {
        DeriveState {
            account: "owner_s3".into(),
            margin_type: Some("PM2".into()),
            initial_margin: Some(d("5000")),
            maintenance_margin: Some(d("6000")),
            positions: vec![],
            collaterals: vec![],
            at_micros: at,
        }
    }

    fn answer() -> MarginAnswer {
        MarginAnswer {
            subaccount_id: 1003,
            pre_initial: d("100000.123456789012"),
            pre_maintenance: d("120000.123456789012"),
            post_initial: d("99907.412814464023"),
            post_maintenance: d("119928.573441920621"),
            is_valid_trade: true,
        }
    }

    fn btc_short() -> HedgeOnHyperliquid {
        HedgeOnHyperliquid {
            dex: String::new(),
            market: "BTC".into(),
            size: d("-0.5"),
            mark: d("56000"),
            leverage: d("10"),
            max_leverage: 40,
        }
    }

    #[test]
    fn derive_answers_and_a_stale_hyperliquid_says_why_without_stopping_it() {
        let derive = derive_estimate(&derive(NOW - 60 * S), Ok(&answer()), NOW, NOW, BOUND);
        let hyperliquid = hyperliquid_estimate(
            &hyperliquid(NOW - 1_200 * S),
            Some(&btc_short()),
            &BTreeMap::new(),
            &None,
            NOW,
            BOUND,
        );
        let VenueEstimate::Estimate(e) = derive else {
            panic!("{derive:?}")
        };
        assert_eq!(e.mode, "PM2");
        assert_eq!(e.after.initial.value, d("99907.412814464023"));
        assert_eq!(e.after.initial.origin, Origin::Venue);
        assert_eq!(e.before.initial.at_micros, NOW);
        let VenueEstimate::Missing { reason, .. } = hyperliquid else {
            panic!("{hyperliquid:?}")
        };
        assert!(reason.contains("1200 s old"), "{reason}");
    }

    #[test]
    fn hyperliquid_answers_and_a_derive_that_did_not_says_why_without_stopping_it() {
        let derive = derive_estimate(
            &derive(NOW - 60 * S),
            Err("Derive refused private/get_margin: HTTP 429".into()),
            NOW,
            NOW,
            BOUND,
        );
        let label = Some(EstimateLabel {
            disagreement: Some(d("-0.0001")),
            measured_at_micros: Some(NOW - 300 * S),
        });
        let hyperliquid = hyperliquid_estimate(
            &hyperliquid(NOW - 60 * S),
            Some(&btc_short()),
            &BTreeMap::new(),
            &label,
            NOW,
            BOUND,
        );
        let VenueEstimate::Missing { reason, venue, .. } = derive else {
            panic!("{derive:?}")
        };
        assert_eq!(venue, Venue::Derive);
        assert!(reason.contains("429"), "{reason}");
        let VenueEstimate::Estimate(e) = hyperliquid else {
            panic!("{hyperliquid:?}")
        };
        assert_eq!(e.mode, "portfolio_margin");
        // Initial: GOLD 2 × 4,000 / 20 = 400; with 0.5 BTC at 56,000 / 10, +2,800.
        assert_eq!(e.before.initial.value, d("400"));
        assert_eq!(e.after.initial.value, d("3200"));
        // Maintenance: 160 on xyz; the BTC short adds 28,000 / 80 = 350.
        assert_eq!(e.before.maintenance.value, d("160"));
        assert_eq!(e.after.maintenance.value, d("510"));
        assert_eq!(e.after.initial.origin, Origin::Computed);
        assert_eq!(e.after.initial.estimate, label);
        assert_eq!(e.snapshot_micros, NOW - 60 * S);
    }

    #[test]
    fn a_hedge_in_a_market_already_held_is_netted() {
        let mut state = hyperliquid(NOW);
        state.positions[0].market = "BTC".into();
        state.positions[0].dex = String::new();
        state.positions[0].size = d("0.5");
        state.positions[0].mark = Some(d("56000"));
        state.positions[0].leverage = Some(d("20"));
        let e = hyperliquid_estimate(
            &state,
            Some(&btc_short()),
            &BTreeMap::new(),
            &None,
            NOW,
            BOUND,
        );
        let VenueEstimate::Estimate(e) = e else {
            panic!("{e:?}")
        };
        // 0.5 long and 0.5 short: flat after.
        assert_eq!(e.before.initial.value, d("1400"));
        assert_eq!(e.after.initial.value, d("0"));
    }

    #[test]
    fn an_unknown_mode_gives_no_hyperliquid_figure_and_says_why() {
        let mut state = hyperliquid(NOW);
        state.mode = AccountMode::Unknown;
        let e = hyperliquid_estimate(&state, None, &BTreeMap::new(), &None, NOW, BOUND);
        let VenueEstimate::Missing { reason, .. } = e else {
            panic!("{e:?}")
        };
        assert!(reason.contains("unknown"), "{reason}");
    }
}
