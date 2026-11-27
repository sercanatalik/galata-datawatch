//! **Whether galata's Hyperliquid figure can be trusted**, account by account
//! (design D4).
//!
//! On every recorded snapshot galata computes the account's portfolio margin
//! ratio from the same state and compares it with the ratio the venue states
//! for it (`portfolioMarginRatio`, recorded by the ledger). The latest
//! comparison per account is held. While it is outside the declared tolerance,
//! or there is none yet, every figure galata computes for that account is
//! labelled an estimate and carries the disagreement measured.

use std::collections::BTreeMap;

use rust_decimal::Decimal;

use crate::advice::{Estimate, EstimateLabel, Figure, Origin};
use crate::hyperliquid::{NoFigure, PmState, portfolio_margin};

/// One comparison of galata's ratio with the venue's.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Comparison {
    /// The snapshot's time.
    pub at_micros: i64,
    /// galata's ratio.
    pub computed: Decimal,
    /// The venue's.
    pub stated: Decimal,
}

impl Comparison {
    /// galata's ratio less the venue's.
    pub fn disagreement(&self) -> Decimal {
        self.computed - self.stated
    }

    /// Whether the two agree within the tolerance, either way.
    pub fn agrees(&self, tolerance: Decimal) -> bool {
        self.disagreement().abs() <= tolerance
    }
}

/// The latest comparison for each account.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Agreements {
    latest: BTreeMap<String, Comparison>,
}

impl Agreements {
    /// Compare one snapshot: compute the ratio from its state and hold the
    /// comparison if it is the account's newest.
    pub fn compare(
        &mut self,
        account: &str,
        at_micros: i64,
        state: &PmState,
        stated: Decimal,
    ) -> Result<Comparison, NoFigure> {
        let comparison = Comparison {
            at_micros,
            computed: portfolio_margin(state)?.ratio,
            stated,
        };
        self.record(account, comparison.clone());
        Ok(comparison)
    }

    /// Hold a comparison, unless the account already has a newer one.
    pub fn record(&mut self, account: &str, comparison: Comparison) {
        match self.latest.get(account) {
            Some(held) if held.at_micros > comparison.at_micros => {}
            _ => {
                self.latest.insert(account.to_string(), comparison);
            }
        }
    }

    /// The account's latest comparison, if any.
    pub fn latest(&self, account: &str) -> Option<&Comparison> {
        self.latest.get(account)
    }

    /// The label for the account's computed figures: `None` while its latest
    /// comparison agrees, the disagreement while it does not, and an empty
    /// label when it has never been compared.
    pub fn label(&self, account: &str, tolerance: Decimal) -> Option<EstimateLabel> {
        match self.latest(account) {
            Some(c) if c.agrees(tolerance) => None,
            Some(c) => Some(EstimateLabel {
                disagreement: Some(c.disagreement()),
                measured_at_micros: Some(c.at_micros),
            }),
            None => Some(EstimateLabel {
                disagreement: None,
                measured_at_micros: None,
            }),
        }
    }
}

/// Put the label on a figure if galata computed it. A venue's own figure is
/// never an estimate.
pub fn label_figure(figure: &mut Figure, label: &Option<EstimateLabel>) {
    if figure.origin == Origin::Computed {
        figure.estimate = label.clone();
    }
}

/// Put the label on every computed figure of an estimate.
pub fn label_estimate(estimate: &mut Estimate, label: &Option<EstimateLabel>) {
    for margins in [&mut estimate.before, &mut estimate.after] {
        label_figure(&mut margins.initial, label);
        label_figure(&mut margins.maintenance, label);
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;
    use crate::advice::{Margins, Venue};

    fn d(s: &str) -> Decimal {
        Decimal::from_str(s).unwrap()
    }

    /// 600 of maintenance on the main dex against 20,000 USDC: 0.03.
    fn state() -> PmState {
        PmState {
            dex_maintenance: [(String::new(), d("600"))].into(),
            dex_collateral: [(String::new(), "USDC".to_string())].into(),
            balances: [("USDC".to_string(), d("20000"))].into(),
            ..PmState::default()
        }
    }

    const TOLERANCE: &str = "0.005";

    #[test]
    fn agreement_inside_the_tolerance_labels_nothing() {
        let mut book = Agreements::default();
        let c = book.compare("main", 10, &state(), d("0.031")).unwrap();
        assert_eq!(c.computed, d("0.03"));
        assert_eq!(c.disagreement(), d("-0.001"));
        assert_eq!(book.label("main", d(TOLERANCE)), None);
    }

    #[test]
    fn disagreement_outside_it_labels_with_the_disagreement_and_its_time() {
        let mut book = Agreements::default();
        book.compare("main", 10, &state(), d("0.04")).unwrap();
        let label = book.label("main", d(TOLERANCE)).expect("labelled");
        assert_eq!(label.disagreement, Some(d("-0.01")));
        assert_eq!(label.measured_at_micros, Some(10));
    }

    #[test]
    fn no_comparison_yet_is_labelled_with_no_disagreement() {
        let book = Agreements::default();
        let label = book.label("main", d(TOLERANCE)).expect("labelled");
        assert_eq!(label.disagreement, None);
        assert_eq!(label.measured_at_micros, None);
    }

    #[test]
    fn the_latest_comparison_is_held_whatever_order_they_arrive_in() {
        let mut book = Agreements::default();
        book.compare("main", 20, &state(), d("0.031")).unwrap();
        book.compare("main", 10, &state(), d("0.09")).unwrap();
        assert_eq!(book.latest("main").unwrap().at_micros, 20);
        assert_eq!(book.label("main", d(TOLERANCE)), None);
        // Accounts are held apart.
        assert!(book.latest("other").is_none());
    }

    #[test]
    fn the_label_appears_on_computed_figures_and_goes_once_a_comparison_agrees() {
        let figure = |origin| Figure {
            value: d("2800"),
            origin,
            estimate: None,
            at_micros: 30,
        };
        let mut estimate = Estimate {
            venue: Venue::Hyperliquid,
            account: "main".into(),
            mode: "portfolio_margin".into(),
            before: Margins {
                initial: figure(Origin::Computed),
                maintenance: figure(Origin::Computed),
            },
            after: Margins {
                initial: figure(Origin::Computed),
                maintenance: figure(Origin::Venue),
            },
            snapshot_micros: 30,
        };
        let mut book = Agreements::default();
        book.compare("main", 10, &state(), d("0.04")).unwrap();
        label_estimate(&mut estimate, &book.label("main", d(TOLERANCE)));
        let shown = estimate.before.initial.estimate.clone().expect("labelled");
        assert_eq!(shown.disagreement, Some(d("-0.01")));
        assert!(estimate.after.initial.estimate.is_some());
        assert!(
            estimate.after.maintenance.estimate.is_none(),
            "a venue's figure is not an estimate"
        );

        // A newer comparison agrees: the label goes.
        book.compare("main", 20, &state(), d("0.0301")).unwrap();
        label_estimate(&mut estimate, &book.label("main", d(TOLERANCE)));
        assert!(estimate.before.initial.estimate.is_none());
        assert!(estimate.after.initial.estimate.is_none());
    }
}
