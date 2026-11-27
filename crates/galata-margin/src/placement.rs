//! **The placement comparison** (tasks 6.1 to 6.8): the placements of the
//! hedge, every term of what each costs, the netting benefit, and the
//! proposal.
//!
//! The option is on Derive in every placement. The hedge is placed all on
//! Derive, all on Hyperliquid, and split at each declared fraction (design
//! D7). A placement's cost is the sum of:
//! - **initial margin on each venue**: on Derive, what the venue's own
//!   simulation of the option and the hedge's part there uses; on
//!   Hyperliquid, what the hedge's part adds;
//! - **the buffer** for the part held apart from its option: its notional
//!   times the stress move Derive applies to the underlying;
//! - **execution cost** on each venue, from the book at the part's size;
//! - **funding** over the declared holding period, at each venue's current
//!   rate.
//!
//! A placement missing any term is incomplete, shown with the reason, and
//! never proposed.
//!
//! **Derive's margin is the fall in net initial margin** the simulation
//! states. For a short option that fall includes the option's mark, since the
//! venue credits no premium in a simulation (design.md, "One trade,
//! measured"). The option is the same in every placement, so it moves every
//! placement's Derive figure alike and the comparison holds.

use std::str::FromStr;

use rust_decimal::Decimal;

use crate::advice::{
    Advice, Estimate, EstimateLabel, Figure, Origin, Placement, Proposal, Term, VenueEstimate,
};
use crate::config::Accounts;

/// A request for advice, as given.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawRequest {
    /// The Derive option, by its name: `BTC-20261225-60000-P`.
    pub option: String,
    /// Its signed size: negative sells.
    pub option_size: Decimal,
    /// Its price.
    pub option_price: Decimal,
    /// The hedge's perpetual, if named: `BTC-PERP` or `BTC`. Absent means
    /// the option's underlying's.
    pub hedge: Option<String>,
    /// The hedge's signed size, in the underlying.
    pub hedge_size: Decimal,
    /// The Derive subaccount, by alias.
    pub derive_account: String,
    /// The Hyperliquid account, by alias.
    pub hyperliquid_account: String,
}

/// A request found well formed (task 6.1).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Request {
    /// The option.
    pub option: String,
    /// Its underlying: `BTC`.
    pub underlying: String,
    /// Its signed size.
    pub option_size: Decimal,
    /// Its price.
    pub option_price: Decimal,
    /// The hedge's signed size.
    pub hedge_size: Decimal,
    /// The Derive subaccount.
    pub derive_account: String,
    /// The Hyperliquid account.
    pub hyperliquid_account: String,
}

impl Request {
    /// The hedge's perpetual on Derive.
    pub fn derive_perp(&self) -> String {
        format!("{}-PERP", self.underlying)
    }
}

/// The underlying of a Derive option's name, or why it is not one:
/// `<UNDERLYING>-<YYYYMMDD>-<STRIKE>-<P|C>`.
fn underlying_of(option: &str) -> Result<String, String> {
    let parts: Vec<&str> = option.split('-').collect();
    let well_formed = parts.len() == 4
        && !parts[0].is_empty()
        && parts[0]
            .chars()
            .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit())
        && parts[1].len() == 8
        && parts[1].chars().all(|c| c.is_ascii_digit())
        && Decimal::from_str(parts[2]).is_ok_and(|s| s > Decimal::ZERO)
        && matches!(parts[3], "P" | "C");
    if !well_formed {
        return Err(format!(
            "`{option}` is not a Derive option's name, such as BTC-20261225-60000-P"
        ));
    }
    Ok(parts[0].to_string())
}

/// Check a request (task 6.1): one Derive option with a signed size and a
/// price, a signed hedge that is the perpetual of the option's underlying,
/// and an account on each venue the configuration names.
pub fn validate(raw: &RawRequest, accounts: &Accounts) -> Result<Request, String> {
    let underlying = underlying_of(&raw.option)?;
    if raw.option_size.is_zero() {
        return Err("the option's size is zero; a sale is negative, a purchase positive".into());
    }
    if raw.option_price <= Decimal::ZERO {
        return Err(format!(
            "the option's price must be above zero: {}",
            raw.option_price
        ));
    }
    if raw.hedge_size.is_zero() {
        return Err("the hedge's size is zero; a short is negative, a long positive".into());
    }
    if let Some(hedge) = &raw.hedge {
        let named = hedge.strip_suffix("-PERP").unwrap_or(hedge);
        if named != underlying {
            return Err(format!(
                "the hedge must be the perpetual of the option's underlying, {underlying}-PERP; `{hedge}` is not"
            ));
        }
    }
    if !accounts.derive.contains(&raw.derive_account) {
        return Err(format!(
            "`{}` is not a Derive account the [margin] block names ({})",
            raw.derive_account,
            accounts.derive.join(", ")
        ));
    }
    if !accounts.hyperliquid.contains(&raw.hyperliquid_account) {
        return Err(format!(
            "`{}` is not a Hyperliquid account the [margin] block names ({})",
            raw.hyperliquid_account,
            accounts.hyperliquid.join(", ")
        ));
    }
    Ok(Request {
        option: raw.option.clone(),
        underlying,
        option_size: raw.option_size,
        option_price: raw.option_price,
        hedge_size: raw.hedge_size,
        derive_account: raw.derive_account.clone(),
        hyperliquid_account: raw.hyperliquid_account.clone(),
    })
}

/// One way of placing the hedge.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Split {
    /// The share on Derive, in percent.
    pub derive_pct: u32,
    /// The size on Derive.
    pub on_derive: Decimal,
    /// The size on Hyperliquid.
    pub on_hyperliquid: Decimal,
}

/// The placements compared (task 6.2): all on Derive, each declared split,
/// all on Hyperliquid. Only the first when Hyperliquid lists no perpetual
/// for the underlying. The two parts always sum to the hedge exactly.
pub fn splits(hedge_size: Decimal, split_pct: &[u32], hyperliquid_lists: bool) -> Vec<Split> {
    let at = |pct: u32| {
        let on_derive = hedge_size * Decimal::from(pct) / Decimal::from(100);
        Split {
            derive_pct: pct,
            on_derive,
            on_hyperliquid: hedge_size - on_derive,
        }
    };
    if !hyperliquid_lists {
        return vec![at(100)];
    }
    let mut out = vec![at(100)];
    out.extend(split_pct.iter().rev().map(|p| at(*p)));
    out.push(at(0));
    out
}

/// The largest regular spot shock Derive's portfolio margin applies to each
/// currency, in percent: scenario 1 of `portfolio-margin-parameters`, read
/// directly on 2026-11-27. The buffer for a hedge held apart from its option
/// is this move on the hedge's notional (design D7).
pub fn stress_pct(underlying: &str) -> Option<Decimal> {
    let pct = match underlying {
        "BTC" => "14",
        "ETH" => "16",
        "SOL" | "XRP" => "24",
        "HYPE" | "ZEC" | "LIT" | "PUMP" => "27",
        "XAUT" => "10",
        "ADA" | "CC" | "VVV" => "33",
        _ => return None,
    };
    Decimal::from_str(pct).ok()
}

/// The buffer (task 6.3): the notional held on Hyperliquid times the stress
/// move. Zero when nothing is held there.
pub fn buffer(hyperliquid_notional: Decimal, stress_pct: Decimal) -> Decimal {
    hyperliquid_notional.abs() * stress_pct / Decimal::from(100)
}

/// A perpetual's book, as a venue sent it when advice was asked for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Book {
    /// Bids, best first: price and size.
    pub bids: Vec<(Decimal, Decimal)>,
    /// Asks, best first.
    pub asks: Vec<(Decimal, Decimal)>,
    /// When the venue stated it.
    pub at_micros: i64,
}

/// What executing `size` against the book costs against its mid (task 6.4):
/// a sale walks the bids, a purchase the asks, level by level. A book too
/// shallow for the size gives no figure.
pub fn execution_cost(book: &Book, size: Decimal) -> Result<Decimal, String> {
    if size.is_zero() {
        return Ok(Decimal::ZERO);
    }
    let (Some(bid), Some(ask)) = (book.bids.first(), book.asks.first()) else {
        return Err("the book holds no bid or no ask".into());
    };
    let mid = (bid.0 + ask.0) / Decimal::from(2);
    let (levels, selling) = if size < Decimal::ZERO {
        (&book.bids, true)
    } else {
        (&book.asks, false)
    };
    let mut left = size.abs();
    let mut cost = Decimal::ZERO;
    for (price, available) in levels {
        if left.is_zero() {
            break;
        }
        let taken = left.min(*available);
        cost += taken * if selling { mid - price } else { price - mid };
        left -= taken;
    }
    if !left.is_zero() {
        let held: Decimal = levels.iter().map(|(_, s)| *s).sum();
        return Err(format!(
            "the book holds {held} of the {} the hedge needs",
            size.abs()
        ));
    }
    Ok(cost)
}

/// A perpetual's current funding, as a venue stated it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Funding {
    /// The hourly rate.
    pub hourly_rate: Decimal,
    /// The mark it applies to.
    pub mark: Decimal,
    /// When the venue stated it.
    pub at_micros: i64,
}

/// What a position pays (positive) or receives over `hours` at the current
/// rate (task 6.5): size × mark × rate × hours. A long pays a positive rate,
/// a short receives it.
pub fn funding_cost(size: Decimal, funding: &Funding, hours: u32) -> Decimal {
    size * funding.mark * funding.hourly_rate * Decimal::from(hours)
}

/// The netting benefit (task 6.6): margin with the hedge apart from its
/// option less margin with it beside, and a reason where standard margin
/// makes it zero.
pub fn netting(
    apart: &Term,
    together: &Term,
    derive_margin_type: Option<&str>,
) -> (Term, Option<String>) {
    let reason = (derive_margin_type == Some("SM")).then(|| {
        "the Derive subaccount is on standard margin (SM), which margins the option and its hedge apart wherever the hedge is held".to_string()
    });
    let term = match (apart, together) {
        (Term::Figure(a), Term::Figure(t)) => Term::Figure(Figure {
            value: if reason.is_some() {
                Decimal::ZERO
            } else {
                a.value - t.value
            },
            origin: if a.origin == Origin::Venue && t.origin == Origin::Venue {
                Origin::Venue
            } else {
                Origin::Computed
            },
            estimate: a.estimate.clone().or_else(|| t.estimate.clone()),
            at_micros: a.at_micros.min(t.at_micros),
        }),
        (Term::Missing { reason }, _) | (_, Term::Missing { reason }) => Term::Missing {
            reason: reason.clone(),
        },
    };
    (term, reason)
}

/// The terms of a placement as figures, or `None` when one is missing.
fn terms(p: &Placement) -> Vec<&Term> {
    vec![
        &p.derive_margin,
        &p.hyperliquid_margin,
        &p.buffer,
        &p.derive_execution,
        &p.hyperliquid_execution,
        &p.funding,
    ]
}

/// The sum of a placement's terms, or `None` when one is missing (task 6.7).
pub fn total(p: &Placement) -> Option<Decimal> {
    terms(p)
        .into_iter()
        .map(|t| match t {
            Term::Figure(f) => Some(f.value),
            Term::Missing { .. } => None,
        })
        .sum()
}

/// Whether each venue's share of the hedge is at or under the cap.
pub fn inside_cap(derive_pct: u32, cap_pct: u32) -> bool {
    derive_pct <= cap_pct && 100 - derive_pct <= cap_pct
}

/// The estimate label a placement rests on, if any of its figures carries one.
fn label_of(p: &Placement) -> Option<EstimateLabel> {
    terms(p).into_iter().find_map(|t| match t {
        Term::Figure(f) => f.estimate.clone(),
        Term::Missing { .. } => None,
    })
}

/// The proposal (task 6.7): the cheapest complete placement inside the cap,
/// and what the cap cost when it excluded a cheaper complete one.
pub fn propose(placements: &[Placement]) -> (Proposal, Option<Decimal>) {
    let complete: Vec<&Placement> = placements.iter().filter(|p| p.total.is_some()).collect();
    if complete.is_empty() {
        return (
            Proposal::None {
                reason: "no placement is complete; each states what it is missing".into(),
            },
            None,
        );
    }
    let cheapest = complete
        .iter()
        .min_by_key(|p| p.total)
        .expect("one complete");
    let Some(chosen) = complete
        .iter()
        .filter(|p| p.inside_cap)
        .min_by_key(|p| p.total)
    else {
        return (
            Proposal::None {
                reason:
                    "no complete placement keeps each venue's share of the hedge inside the cap"
                        .into(),
            },
            None,
        );
    };
    let cap_cost = (chosen.derive_pct != cheapest.derive_pct)
        .then(|| chosen.total.unwrap_or_default() - cheapest.total.unwrap_or_default());
    let label = label_of(chosen);
    (
        Proposal::Placement {
            derive_pct: chosen.derive_pct,
            rests_on_estimate: label.is_some(),
            estimate: label,
        },
        cap_cost,
    )
}

/// What a figure becomes as a term.
fn figure(value: Decimal, origin: Origin, estimate: Option<EstimateLabel>, at_micros: i64) -> Term {
    Term::Figure(Figure {
        value,
        origin,
        estimate,
        at_micros,
    })
}

fn missing(reason: impl Into<String>) -> Term {
    Term::Missing {
        reason: reason.into(),
    }
}

/// One placement's venue estimates, gathered by the caller: Derive's
/// simulation of the option and the hedge's part there, and Hyperliquid's
/// estimate with its part added.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Gathered {
    /// The split.
    pub split: Split,
    /// Derive's estimate.
    pub derive: VenueEstimate,
    /// Hyperliquid's estimate.
    pub hyperliquid: VenueEstimate,
}

/// What each venue's market said when advice was asked for: its book and
/// its funding, or why it did not answer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Market {
    /// Derive's perpetual's book.
    pub derive_book: Result<Book, String>,
    /// Hyperliquid's.
    pub hyperliquid_book: Result<Book, String>,
    /// Derive's perpetual's funding.
    pub derive_funding: Result<Funding, String>,
    /// Hyperliquid's.
    pub hyperliquid_funding: Result<Funding, String>,
}

/// The margin a placement uses on a venue: Derive's fall in net initial
/// margin, or Hyperliquid's rise in its requirement. A Hyperliquid part of
/// zero adds nothing, whatever its account's state.
fn margin_used(estimate: &VenueEstimate, net: bool, part: Decimal) -> Term {
    match estimate {
        VenueEstimate::Estimate(Estimate { before, after, .. }) => {
            let used = if net {
                before.initial.value - after.initial.value
            } else {
                after.initial.value - before.initial.value
            };
            figure(
                used,
                after.initial.origin,
                after.initial.estimate.clone(),
                after.initial.at_micros,
            )
        }
        VenueEstimate::Missing { .. } if !net && part.is_zero() => {
            figure(Decimal::ZERO, Origin::Computed, None, 0)
        }
        VenueEstimate::Missing { venue, reason, .. } => missing(format!("{venue:?}: {reason}")),
    }
}

/// The execution term on one venue for one part of the hedge.
fn execution(book: &Result<Book, String>, part: Decimal, venue: &str) -> Term {
    if part.is_zero() {
        return figure(Decimal::ZERO, Origin::Computed, None, 0);
    }
    match book {
        Ok(book) => match execution_cost(book, part) {
            Ok(cost) => figure(cost, Origin::Computed, None, book.at_micros),
            Err(reason) => missing(format!("{venue}'s book: {reason}")),
        },
        Err(reason) => missing(format!("{venue}'s book: {reason}")),
    }
}

/// The advice (tasks 6.6 to 6.8): each placement's terms, the netting
/// benefit, and the proposal, every figure carrying its origin, its label
/// and its time.
pub fn advise(
    request: &Request,
    gathered: Vec<Gathered>,
    market: &Market,
    funding_hours: u32,
    cap_pct: u32,
    derive_margin_type: Option<&str>,
    mut notes: Vec<String>,
) -> Advice {
    let stress = stress_pct(&request.underlying);
    if stress.is_none() {
        notes.push(format!(
            "Derive states no stress move for {}, so no buffer is computed",
            request.underlying
        ));
    }
    let mut placements = Vec::new();
    for g in &gathered {
        let s = g.split;
        let derive_margin = margin_used(&g.derive, true, s.on_derive);
        let hyperliquid_margin = margin_used(&g.hyperliquid, false, s.on_hyperliquid);
        let buffer_term = if s.on_hyperliquid.is_zero() {
            figure(Decimal::ZERO, Origin::Computed, None, 0)
        } else {
            match (&market.hyperliquid_funding, stress) {
                (Ok(f), Some(pct)) => figure(
                    buffer(s.on_hyperliquid * f.mark, pct),
                    Origin::Computed,
                    None,
                    f.at_micros,
                ),
                (Err(reason), _) => missing(format!("Hyperliquid's mark: {reason}")),
                (_, None) => missing(format!(
                    "no stress move is stated for {}",
                    request.underlying
                )),
            }
        };
        let funding_term = {
            let mut total = Decimal::ZERO;
            let mut at = i64::MAX;
            let mut gap = None;
            for (part, read, venue) in [
                (s.on_derive, &market.derive_funding, "Derive"),
                (s.on_hyperliquid, &market.hyperliquid_funding, "Hyperliquid"),
            ] {
                if part.is_zero() {
                    continue;
                }
                match read {
                    Ok(f) => {
                        total += funding_cost(part, f, funding_hours);
                        at = at.min(f.at_micros);
                    }
                    Err(reason) => gap = Some(format!("{venue}'s funding: {reason}")),
                }
            }
            match gap {
                Some(reason) => missing(reason),
                None => figure(
                    total,
                    Origin::Computed,
                    None,
                    if at == i64::MAX { 0 } else { at },
                ),
            }
        };
        let mut placement = Placement {
            derive_pct: s.derive_pct,
            hedge_on_derive: s.on_derive,
            hedge_on_hyperliquid: s.on_hyperliquid,
            derive_margin,
            hyperliquid_margin,
            buffer: buffer_term,
            derive_execution: execution(&market.derive_book, s.on_derive, "Derive"),
            hyperliquid_execution: execution(
                &market.hyperliquid_book,
                s.on_hyperliquid,
                "Hyperliquid",
            ),
            funding: funding_term,
            total: None,
            inside_cap: inside_cap(s.derive_pct, cap_pct),
        };
        placement.total = total(&placement);
        placements.push(placement);
    }

    // Apart: the hedge all on Hyperliquid. Together: all beside its option.
    let margin = |pct: u32| -> Term {
        match placements.iter().find(|p| p.derive_pct == pct) {
            None => missing("that placement was not compared"),
            Some(p) => match (&p.derive_margin, &p.hyperliquid_margin) {
                (Term::Figure(d), Term::Figure(h)) => figure(
                    d.value + h.value,
                    if h.value.is_zero() {
                        d.origin
                    } else {
                        Origin::Computed
                    },
                    h.estimate.clone(),
                    d.at_micros.max(h.at_micros),
                ),
                (Term::Missing { reason }, _) | (_, Term::Missing { reason }) => {
                    missing(reason.clone())
                }
            },
        }
    };
    let (netting_benefit, netting_reason) = netting(&margin(0), &margin(100), derive_margin_type);
    let (proposal, cap_cost) = propose(&placements);
    let estimates = [0, 100]
        .iter()
        .filter_map(|pct| gathered.iter().find(|g| g.split.derive_pct == *pct))
        .map(|g| {
            if g.split.derive_pct == 100 {
                g.derive.clone()
            } else {
                g.hyperliquid.clone()
            }
        })
        .collect();
    Advice {
        estimates,
        placements,
        netting_benefit,
        netting_reason,
        proposal,
        cap_cost,
        notes,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::advice::{Margins, Venue};

    fn d(s: &str) -> Decimal {
        Decimal::from_str(s).unwrap()
    }

    fn accounts() -> Accounts {
        Accounts {
            derive: vec!["owner_s3".into()],
            hyperliquid: vec!["main".into()],
        }
    }

    fn raw() -> RawRequest {
        RawRequest {
            option: "BTC-20261225-60000-P".into(),
            option_size: d("-1"),
            option_price: d("2100"),
            hedge: None,
            hedge_size: d("-0.47"),
            derive_account: "owner_s3".into(),
            hyperliquid_account: "main".into(),
        }
    }

    #[test]
    fn a_well_formed_request_is_taken() {
        let r = validate(&raw(), &accounts()).unwrap();
        assert_eq!(r.underlying, "BTC");
        assert_eq!(r.derive_perp(), "BTC-PERP");
        let named = RawRequest {
            hedge: Some("BTC-PERP".into()),
            ..raw()
        };
        assert!(validate(&named, &accounts()).is_ok());
    }

    #[test]
    fn a_hedge_on_another_underlying_is_refused_saying_so() {
        let eth = RawRequest {
            hedge: Some("ETH-PERP".into()),
            ..raw()
        };
        let err = validate(&eth, &accounts()).unwrap_err();
        assert!(
            err.contains("perpetual of the option's underlying") && err.contains("BTC-PERP"),
            "{err}"
        );
    }

    #[test]
    fn a_malformed_request_is_refused_saying_what_is_wrong() {
        for (bad, says) in [
            (
                RawRequest {
                    option: "BTC-PERP".into(),
                    ..raw()
                },
                "not a Derive option",
            ),
            (
                RawRequest {
                    option_size: d("0"),
                    ..raw()
                },
                "size is zero",
            ),
            (
                RawRequest {
                    option_price: d("0"),
                    ..raw()
                },
                "price",
            ),
            (
                RawRequest {
                    hedge_size: d("0"),
                    ..raw()
                },
                "hedge's size is zero",
            ),
            (
                RawRequest {
                    derive_account: "owner_s9".into(),
                    ..raw()
                },
                "owner_s9",
            ),
            (
                RawRequest {
                    hyperliquid_account: "other".into(),
                    ..raw()
                },
                "other",
            ),
        ] {
            let err = validate(&bad, &accounts()).unwrap_err();
            assert!(err.contains(says), "{says}: {err}");
        }
    }

    #[test]
    fn an_underlying_hyperliquid_does_not_list_compares_only_the_hedge_on_derive() {
        let only = splits(d("-0.47"), &[25, 50, 75], false);
        assert_eq!(only.len(), 1);
        assert_eq!(only[0].derive_pct, 100);
        assert_eq!(only[0].on_hyperliquid, d("0"));
    }

    #[test]
    fn a_quarter_a_half_and_three_quarters_make_five_placements_that_sum_to_the_hedge() {
        let all = splits(d("-0.47"), &[25, 50, 75], true);
        assert_eq!(
            all.iter().map(|s| s.derive_pct).collect::<Vec<_>>(),
            [100, 75, 50, 25, 0]
        );
        for s in &all {
            assert_eq!(s.on_derive + s.on_hyperliquid, d("-0.47"), "{s:?}");
        }
        assert_eq!(all[3].on_derive, d("-0.1175"));
    }

    #[test]
    fn a_buffer_is_the_notional_held_apart_times_the_stress_move() {
        let b = buffer(d("28181"), stress_pct("BTC").unwrap());
        assert_eq!(b, d("3945.34"));
        assert_eq!(b.round(), d("3945"));
        assert_eq!(buffer(d("0"), d("14")), d("0"));
        assert_eq!(stress_pct("ETH"), Some(d("16")));
        assert_eq!(stress_pct("DOGE"), None);
    }

    fn book(levels: &[(&str, &str)], asks: &[(&str, &str)]) -> Book {
        Book {
            bids: levels.iter().map(|(p, s)| (d(p), d(s))).collect(),
            asks: asks.iter().map(|(p, s)| (d(p), d(s))).collect(),
            at_micros: 7,
        }
    }

    #[test]
    fn execution_inside_the_best_size_costs_half_the_spread() {
        let b = book(&[("100", "5")], &[("102", "5")]);
        // Selling 2 at 100 against a mid of 101.
        assert_eq!(execution_cost(&b, d("-2")).unwrap(), d("2"));
        assert_eq!(execution_cost(&b, d("2")).unwrap(), d("2"));
    }

    #[test]
    fn execution_past_the_best_size_walks_the_depth() {
        let b = book(&[("100", "1"), ("99", "1"), ("97", "5")], &[("102", "1")]);
        // 1 at 100 (1 below mid), 1 at 99 (2 below), 1 at 97 (4 below).
        assert_eq!(execution_cost(&b, d("-3")).unwrap(), d("7"));
    }

    #[test]
    fn a_book_too_shallow_or_unanswered_marks_the_term_missing() {
        let b = book(&[("100", "1")], &[("102", "1")]);
        assert!(
            execution_cost(&b, d("-3"))
                .unwrap_err()
                .contains("holds 1 of the 3")
        );
        let term = execution(&Err("Derive did not answer".into()), d("-1"), "Derive");
        assert!(
            matches!(term, Term::Missing { ref reason } if reason.contains("did not answer")),
            "{term:?}"
        );
    }

    #[test]
    fn the_recorded_books_price_a_hedge_inside_and_past_the_best_size() {
        let derive: serde_json::Value = serde_json::from_str(include_str!(
            "../tests/fixtures/derive_orderbook_BTC-PERP.json"
        ))
        .unwrap();
        let data = &derive["params"]["data"];
        let side = |k: &str| -> Vec<(Decimal, Decimal)> {
            data[k]
                .as_array()
                .unwrap()
                .iter()
                .map(|l| (d(l[0].as_str().unwrap()), d(l[1].as_str().unwrap())))
                .collect()
        };
        let b = Book {
            bids: side("bids"),
            asks: side("asks"),
            at_micros: data["timestamp"].as_i64().unwrap() * 1000,
        };
        // Inside the best bid's 1.7058: half the 0.1 spread on each unit.
        assert_eq!(execution_cost(&b, d("-1")).unwrap(), d("0.05"));
        // Past it: the depth it walks costs more a unit.
        let deep = execution_cost(&b, d("-2")).unwrap();
        assert!(deep / d("2") > d("0.05"), "{deep}");
        // Past the ten levels' 2.2009: no figure.
        assert!(execution_cost(&b, d("-3")).is_err());
    }

    #[test]
    fn funding_is_size_times_mark_times_rate_times_hours_from_each_venues_answer() {
        let derive: serde_json::Value = serde_json::from_str(include_str!(
            "../tests/fixtures/derive_ticker_BTC-PERP.json"
        ))
        .unwrap();
        let t = &derive["result"];
        let df = Funding {
            hourly_rate: d(t["f"].as_str().unwrap()),
            mark: d(t["M"].as_str().unwrap()),
            at_micros: 1,
        };
        let hl: serde_json::Value = serde_json::from_str(include_str!(
            "../tests/fixtures/hyperliquid_metaAndAssetCtxs_BTC.json"
        ))
        .unwrap();
        let c = &hl[1][0];
        let hf = Funding {
            hourly_rate: d(c["funding"].as_str().unwrap()),
            mark: d(c["markPx"].as_str().unwrap()),
            at_micros: 2,
        };
        // A short receives a positive rate: a negative cost.
        let short_hl = funding_cost(d("-0.47"), &hf, 168);
        assert_eq!(
            short_hl,
            d("-0.47") * d(c["markPx"].as_str().unwrap()) * d("0.0000125") * d("168")
        );
        assert!(short_hl < Decimal::ZERO);
        assert!(funding_cost(d("-0.47"), &df, 168) < Decimal::ZERO);
        let market = Market {
            derive_book: Err("x".into()),
            hyperliquid_book: Err("x".into()),
            derive_funding: Ok(df),
            hyperliquid_funding: Err("Hyperliquid did not answer".into()),
        };
        // An absent rate marks the term missing for a placement that needs it.
        let r = validate(&raw(), &accounts()).unwrap();
        let g = gathered_for(&r, &[0], d("9819"), d("6365"));
        let advice = advise(&r, g, &market, 168, 100, Some("PM2"), vec![]);
        assert!(
            matches!(advice.placements[0].funding, Term::Missing { ref reason } if reason.contains("Hyperliquid's funding")),
            "{:?}",
            advice.placements[0].funding
        );
    }

    fn margins(i: &str, origin: Origin, estimate: Option<EstimateLabel>) -> Margins {
        let f = Figure {
            value: d(i),
            origin,
            estimate,
            at_micros: 5,
        };
        Margins {
            initial: f.clone(),
            maintenance: f,
        }
    }

    /// Placements whose Derive figures fall by `apart` with the option alone
    /// and by `together` with the whole hedge beside it, linearly between;
    /// Hyperliquid's part adds a tenth of its notional at 56,000.
    fn gathered_for(r: &Request, pcts: &[u32], apart: Decimal, together: Decimal) -> Vec<Gathered> {
        splits(r.hedge_size, &[25, 50, 75], true)
            .into_iter()
            .filter(|s| pcts.contains(&s.derive_pct))
            .map(|s| {
                let share = Decimal::from(s.derive_pct) / Decimal::from(100);
                let used = apart + (together - apart) * share;
                let hl_added = (s.on_hyperliquid * d("56000")).abs() / d("10");
                Gathered {
                    split: s,
                    derive: VenueEstimate::Estimate(Estimate {
                        venue: Venue::Derive,
                        account: "owner_s3".into(),
                        mode: "PM2".into(),
                        before: margins("100000", Origin::Venue, None),
                        after: margins(&(d("100000") - used).to_string(), Origin::Venue, None),
                        snapshot_micros: 5,
                    }),
                    hyperliquid: VenueEstimate::Estimate(Estimate {
                        venue: Venue::Hyperliquid,
                        account: "main".into(),
                        mode: "portfolio_margin".into(),
                        before: margins("0", Origin::Computed, None),
                        after: margins(&hl_added.to_string(), Origin::Computed, None),
                        snapshot_micros: 5,
                    }),
                }
            })
            .collect()
    }

    fn calm_market() -> Market {
        let book = Book {
            bids: vec![(d("55999"), d("100"))],
            asks: vec![(d("56001"), d("100"))],
            at_micros: 9,
        };
        let funding = Funding {
            hourly_rate: d("0"),
            mark: d("56000"),
            at_micros: 9,
        };
        Market {
            derive_book: Ok(book.clone()),
            hyperliquid_book: Ok(book),
            derive_funding: Ok(funding),
            hyperliquid_funding: Ok(funding),
        }
    }

    #[test]
    fn the_netting_benefit_reproduces_the_measured_trade() {
        let at = |v: &str| {
            Term::Figure(Figure {
                value: d(v),
                origin: Origin::Venue,
                estimate: None,
                at_micros: 1,
            })
        };
        let (benefit, reason) = netting(&at("14187"), &at("6365"), Some("PM2"));
        assert!(
            matches!(benefit, Term::Figure(ref f) if f.value == d("7822")),
            "{benefit:?}"
        );
        assert_eq!(reason, None);
    }

    #[test]
    fn standard_margin_nets_nothing_and_names_the_margin_type() {
        let at = |v: &str| {
            Term::Figure(Figure {
                value: d(v),
                origin: Origin::Venue,
                estimate: None,
                at_micros: 1,
            })
        };
        let (benefit, reason) = netting(&at("14117"), &at("14117"), Some("SM"));
        assert!(
            matches!(benefit, Term::Figure(ref f) if f.value.is_zero()),
            "{benefit:?}"
        );
        assert!(reason.unwrap().contains("standard margin (SM)"));
    }

    #[test]
    fn the_cheapest_complete_placement_inside_the_cap_is_proposed() {
        let r = validate(&raw(), &accounts()).unwrap();
        // Hedging beside the option saves the most: all on Derive is cheapest.
        let advice = advise(
            &r,
            gathered_for(&r, &[100, 75, 50, 25, 0], d("14187"), d("6365")),
            &calm_market(),
            168,
            100,
            Some("PM2"),
            vec![],
        );
        assert_eq!(advice.placements.len(), 5);
        assert!(
            matches!(
                advice.proposal,
                Proposal::Placement {
                    derive_pct: 100,
                    rests_on_estimate: false,
                    ..
                }
            ),
            "{:?}",
            advice.proposal
        );
        assert_eq!(advice.cap_cost, None);
        // The benefit: apart (14,187 + Hyperliquid's 2,632) less together.
        assert!(
            matches!(advice.netting_benefit, Term::Figure(ref f) if f.value == d("14187") + d("2632") - d("6365")),
            "{:?}",
            advice.netting_benefit
        );
    }

    #[test]
    fn a_cap_that_excludes_the_cheapest_proposes_the_next_and_states_what_it_cost() {
        let r = validate(&raw(), &accounts()).unwrap();
        let advice = advise(
            &r,
            gathered_for(&r, &[100, 75, 50, 25, 0], d("14187"), d("6365")),
            &calm_market(),
            168,
            75,
            Some("PM2"),
            vec![],
        );
        assert!(
            matches!(advice.proposal, Proposal::Placement { derive_pct: 75, .. }),
            "{:?}",
            advice.proposal
        );
        let all_derive = advice
            .placements
            .iter()
            .find(|p| p.derive_pct == 100)
            .unwrap();
        assert!(!all_derive.inside_cap, "the excluded placement is shown");
        let at75 = advice
            .placements
            .iter()
            .find(|p| p.derive_pct == 75)
            .unwrap();
        assert_eq!(
            advice.cap_cost,
            Some(at75.total.unwrap() - all_derive.total.unwrap())
        );
        assert!(advice.cap_cost.unwrap() > Decimal::ZERO);
    }

    #[test]
    fn a_stale_hyperliquid_leaves_only_the_hedge_beside_its_option_complete() {
        let r = validate(&raw(), &accounts()).unwrap();
        let mut g = gathered_for(&r, &[100, 75, 50, 25, 0], d("14187"), d("6365"));
        for p in &mut g {
            p.hyperliquid = VenueEstimate::Missing {
                venue: Venue::Hyperliquid,
                account: "main".into(),
                reason: "its snapshot is 1200 s old".into(),
            };
        }
        let advice = advise(&r, g, &calm_market(), 168, 100, Some("PM2"), vec![]);
        for p in &advice.placements {
            assert_eq!(p.total.is_some(), p.derive_pct == 100, "{p:?}");
        }
        assert!(matches!(
            advice.proposal,
            Proposal::Placement {
                derive_pct: 100,
                ..
            }
        ));
        let partial = advice
            .placements
            .iter()
            .find(|p| p.derive_pct == 50)
            .unwrap();
        assert!(
            matches!(partial.hyperliquid_margin, Term::Missing { ref reason } if reason.contains("1200 s old"))
        );
    }

    #[test]
    fn no_complete_placement_proposes_none_and_each_says_what_it_misses() {
        let r = validate(&raw(), &accounts()).unwrap();
        let mut market = calm_market();
        market.derive_book = Err("Derive did not answer".into());
        market.hyperliquid_book = Err("Hyperliquid did not answer".into());
        let advice = advise(
            &r,
            gathered_for(&r, &[100, 75, 50, 25, 0], d("14187"), d("6365")),
            &market,
            168,
            100,
            Some("PM2"),
            vec![],
        );
        assert!(
            matches!(advice.proposal, Proposal::None { ref reason } if reason.contains("no placement is complete")),
            "{:?}",
            advice.proposal
        );
        assert!(advice.placements.iter().all(|p| p.total.is_none()));
    }

    #[test]
    fn a_proposal_resting_on_an_estimate_is_marked_with_its_disagreement() {
        let r = validate(&raw(), &accounts()).unwrap();
        let label = EstimateLabel {
            disagreement: Some(d("-0.0001")),
            measured_at_micros: Some(4),
        };
        // Hedging on Hyperliquid is cheapest here: beside its option the hedge
        // costs Derive 11,000 more, against 2,632 of margin and a 3,685 buffer
        // on Hyperliquid.
        let mut g = gathered_for(&r, &[100, 75, 50, 25, 0], d("9000"), d("20000"));
        for p in &mut g {
            if let VenueEstimate::Estimate(e) = &mut p.hyperliquid {
                e.after.initial.estimate = Some(label.clone());
            }
        }
        let advice = advise(&r, g, &calm_market(), 168, 100, Some("PM2"), vec![]);
        let Proposal::Placement {
            derive_pct,
            rests_on_estimate,
            estimate,
        } = &advice.proposal
        else {
            panic!("{:?}", advice.proposal)
        };
        assert_eq!(*derive_pct, 0);
        assert!(rests_on_estimate);
        assert_eq!(estimate.as_ref().unwrap().disagreement, Some(d("-0.0001")));
        // The label is on the Hyperliquid figure the advice shows, with its time.
        let p = advice
            .placements
            .iter()
            .find(|p| p.derive_pct == 0)
            .unwrap();
        assert!(
            matches!(p.hyperliquid_margin, Term::Figure(ref f) if f.estimate.is_some() && f.at_micros == 5 && f.origin == Origin::Computed)
        );
        assert!(matches!(p.derive_margin, Term::Figure(ref f) if f.origin == Origin::Venue));
    }
}
