//! What an estimate and an advice hold (design D8).
//!
//! **Every figure says where it came from and how old it is.** A figure is an
//! exact decimal, as the venue stated it or as exact arithmetic on such
//! values, with its origin (the venue's or galata's), the time of the
//! snapshot or market read it rests on, and, for galata's Hyperliquid figure
//! while it does not agree with the venue, an estimate label carrying the
//! disagreement measured (design D4).
//!
//! **Nothing here is a float**, serialised or held: a decimal serialises as a
//! string, and the only numbers are times and percentages, which a test holds.

use rust_decimal::Decimal;
use serde::Serialize;

/// The two venues a placement can use.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Venue {
    /// Derive: the option, and the hedge beside it.
    Derive,
    /// Hyperliquid: the hedge, held apart from the option.
    Hyperliquid,
}

/// Where a figure came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Origin {
    /// The venue stated it: Derive's simulation, or a venue's book or rate.
    Venue,
    /// galata computed it, from recorded state and the venue's formula.
    Computed,
}

/// A computed figure for an account whose latest comparison with the venue
/// is outside the tolerance, or that has not been compared yet (design D4).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct EstimateLabel {
    /// The latest disagreement measured: galata's portfolio margin ratio less
    /// the venue's, as stated. `None` when the account has not been compared.
    pub disagreement: Option<Decimal>,
    /// When that comparison was made.
    pub measured_at_micros: Option<i64>,
}

/// One figure: its value, its origin, its label if it is an estimate, and the
/// time it rests on.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Figure {
    /// The figure, in USD unless its name says otherwise.
    pub value: Decimal,
    /// Whether the venue stated it or galata computed it.
    pub origin: Origin,
    /// Present while galata's figure does not agree with the venue's.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub estimate: Option<EstimateLabel>,
    /// The time of the account snapshot or market read it rests on.
    pub at_micros: i64,
}

/// A term of a placement's cost: a figure, or why there is none. A placement
/// with a missing term is incomplete and is not proposed (design D7).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Term {
    /// The term's figure.
    Figure(Figure),
    /// No figure, and the reason.
    Missing {
        /// Why: the venue did not answer, the snapshot was stale, and so on.
        reason: String,
    },
}

/// Initial and maintenance margin, as each venue states them: Derive's are
/// net (value less requirement), Hyperliquid's are requirements. The
/// estimate's `mode` says which.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Margins {
    /// Initial margin.
    pub initial: Figure,
    /// Maintenance margin.
    pub maintenance: Figure,
}

/// One venue's margin for one account, before and after a placement's
/// positions are added.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Estimate {
    /// The venue.
    pub venue: Venue,
    /// The account, by the ledger's alias.
    pub account: String,
    /// Derive's margin type (`SM`, `PM2`) or Hyperliquid's account mode.
    pub mode: String,
    /// The account as it stands.
    pub before: Margins,
    /// With the placement's positions added.
    pub after: Margins,
    /// The time of the account snapshot it was computed from.
    pub snapshot_micros: i64,
}

/// An estimate, or why a venue has none. One venue's missing figure does not
/// stop the other's.
// A handful are built per request, so boxing the estimate would save nothing
// that could be measured and cost every reader a dereference.
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum VenueEstimate {
    /// The venue's estimate.
    Estimate(Estimate),
    /// No estimate, and the reason.
    Missing {
        /// The venue.
        venue: Venue,
        /// The account asked about.
        account: String,
        /// Why there is no figure.
        reason: String,
    },
}

/// One way of placing the hedge, and each term of what it costs. The option
/// is on Derive in every placement.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Placement {
    /// The share of the hedge placed on Derive, in percent: 100 is all beside
    /// the option, 0 all on Hyperliquid.
    pub derive_pct: u32,
    /// The hedge's size on Derive, signed.
    pub hedge_on_derive: Decimal,
    /// The hedge's size on Hyperliquid, signed.
    pub hedge_on_hyperliquid: Decimal,
    /// The initial margin the placement uses on Derive: the option and the
    /// hedge's part there, as Derive simulates them.
    pub derive_margin: Term,
    /// The initial margin the hedge's part on Hyperliquid adds there.
    pub hyperliquid_margin: Term,
    /// The cash buffer for the part of the hedge held apart from its option.
    pub buffer: Term,
    /// The cost of executing the hedge's part on Derive, at its size.
    pub derive_execution: Term,
    /// The cost of executing the hedge's part on Hyperliquid, at its size.
    pub hyperliquid_execution: Term,
    /// The funding the hedge pays (positive) or receives (negative) over the
    /// declared holding period, at each venue's current rate.
    pub funding: Term,
    /// The sum of the terms, or `None` when one is missing.
    pub total: Option<Decimal>,
    /// Whether each venue's share of the hedge is at or under the cap.
    pub inside_cap: bool,
}

/// What is proposed, if anything.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Proposal {
    /// The cheapest complete placement inside the venue cap.
    Placement {
        /// Which placement, by its share on Derive.
        derive_pct: u32,
        /// Whether a figure it rests on is labelled an estimate.
        rests_on_estimate: bool,
        /// The label of that figure: the disagreement measured for the
        /// account, and when.
        #[serde(skip_serializing_if = "Option::is_none")]
        estimate: Option<EstimateLabel>,
    },
    /// Nothing is proposed, and why.
    None {
        /// No placement is complete, or none is inside the cap.
        reason: String,
    },
}

/// The advice for one option and its hedge. It acts on nothing.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Advice {
    /// Each venue's estimate, or why it has none.
    pub estimates: Vec<VenueEstimate>,
    /// Every placement compared.
    pub placements: Vec<Placement>,
    /// The margin with the hedge on Hyperliquid less the margin with it on
    /// Derive.
    pub netting_benefit: Term,
    /// Why the benefit is what it is, where that needs saying: standard
    /// margin, which does not net.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub netting_reason: Option<String>,
    /// What is proposed.
    pub proposal: Proposal,
    /// What the cap cost: the proposed placement's total less the cheapest
    /// complete placement's, when the cap excluded the cheapest.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cap_cost: Option<Decimal>,
    /// Anything else the owner should read: a placement not compared, and why.
    pub notes: Vec<String>,
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;

    fn d(s: &str) -> Decimal {
        Decimal::from_str(s).unwrap()
    }

    fn figure(value: &str, origin: Origin, estimate: Option<EstimateLabel>) -> Figure {
        Figure {
            value: d(value),
            origin,
            estimate,
            at_micros: 1_795_800_000_000_000,
        }
    }

    /// An advice with every variant in it.
    fn advice() -> Advice {
        let label = EstimateLabel {
            disagreement: Some(d("0.0123")),
            measured_at_micros: Some(1_795_799_000_000_000),
        };
        let margins = |i: &str, m: &str, origin, estimate: Option<EstimateLabel>| Margins {
            initial: figure(i, origin, estimate.clone()),
            maintenance: figure(m, origin, estimate),
        };
        Advice {
            estimates: vec![
                VenueEstimate::Estimate(Estimate {
                    venue: Venue::Derive,
                    account: "owner_s3".into(),
                    mode: "PM2".into(),
                    before: margins(
                        "100000.123456789012",
                        "120000.123456789012",
                        Origin::Venue,
                        None,
                    ),
                    after: margins(
                        "99907.412814464023",
                        "119928.573441920621",
                        Origin::Venue,
                        None,
                    ),
                    snapshot_micros: 1_795_800_000_000_000,
                }),
                VenueEstimate::Estimate(Estimate {
                    venue: Venue::Hyperliquid,
                    account: "main".into(),
                    mode: "portfolio_margin".into(),
                    before: margins("2800", "1400", Origin::Computed, Some(label.clone())),
                    after: margins("3504.5", "1752.25", Origin::Computed, Some(label)),
                    snapshot_micros: 1_795_800_000_000_000,
                }),
                VenueEstimate::Missing {
                    venue: Venue::Hyperliquid,
                    account: "other".into(),
                    reason: "the snapshot is 1,200 s old".into(),
                },
            ],
            placements: vec![Placement {
                derive_pct: 25,
                hedge_on_derive: d("-0.1175"),
                hedge_on_hyperliquid: d("-0.3525"),
                derive_margin: Term::Figure(figure("6365.02", Origin::Venue, None)),
                hyperliquid_margin: Term::Figure(figure("1973.4", Origin::Computed, None)),
                buffer: Term::Figure(figure("2959.0", Origin::Computed, None)),
                derive_execution: Term::Missing {
                    reason: "Derive's book did not answer".into(),
                },
                hyperliquid_execution: Term::Figure(figure("3.52", Origin::Venue, None)),
                funding: Term::Figure(figure("-12.40", Origin::Computed, None)),
                total: None,
                inside_cap: true,
            }],
            netting_benefit: Term::Figure(figure("7822", Origin::Venue, None)),
            netting_reason: None,
            cap_cost: Some(d("120.5")),
            notes: vec!["a note".into()],
            proposal: Proposal::Placement {
                derive_pct: 25,
                rests_on_estimate: true,
                estimate: Some(EstimateLabel {
                    disagreement: Some(d("0.0123")),
                    measured_at_micros: Some(1_795_799_000_000_000),
                }),
            },
        }
    }

    #[test]
    fn a_serialised_advice_carries_no_float() {
        // The only numbers are times and shares; every amount is a string.
        let integers = [
            "at_micros",
            "measured_at_micros",
            "snapshot_micros",
            "derive_pct",
        ];
        fn walk(v: &serde_json::Value, key: &str, ints: &[&str], bad: &mut Vec<String>) {
            match v {
                serde_json::Value::Number(n) if !ints.contains(&key) || n.is_f64() => {
                    bad.push(format!("{key} = {v}"))
                }
                serde_json::Value::Object(map) => {
                    for (k, v) in map {
                        walk(v, k, ints, bad);
                    }
                }
                serde_json::Value::Array(items) => {
                    items.iter().for_each(|v| walk(v, key, ints, bad))
                }
                _ => {}
            }
        }
        let value = serde_json::to_value(advice()).unwrap();
        let mut bad = Vec::new();
        walk(&value, "", &integers, &mut bad);
        assert!(
            bad.is_empty(),
            "numbers that are not times or shares: {bad:?}"
        );
        // And the venue's digits are carried as written.
        let derive = &value["estimates"][0]["estimate"]["after"]["initial"]["value"];
        assert_eq!(derive, "99907.412814464023");
    }

    #[test]
    fn an_estimate_label_shows_and_a_venue_figure_has_none() {
        let value = serde_json::to_value(advice()).unwrap();
        let hyperliquid = &value["estimates"][1]["estimate"]["before"]["initial"];
        assert_eq!(hyperliquid["origin"], "computed");
        assert_eq!(hyperliquid["estimate"]["disagreement"], "0.0123");
        let derive = &value["estimates"][0]["estimate"]["before"]["initial"];
        assert_eq!(derive["origin"], "venue");
        assert!(derive.get("estimate").is_none(), "{derive}");
    }
}
