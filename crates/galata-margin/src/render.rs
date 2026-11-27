//! **What the command prints** (tasks 7.2 and 7.3): the comparison for a
//! person, and the same figures for a machine.
//!
//! The printed form shows each placement with every cost term and its total,
//! the netting benefit, the proposal, and for each figure where it came from
//! (`v` the venue, `c` computed, `c*` computed and labelled an estimate) and
//! how old it is. Amounts are printed to the cent. The machine-readable form
//! holds every figure exactly, as a decimal string.

use std::fmt::Write as _;

use rust_decimal::Decimal;
use serde::Serialize;

use crate::advice::{Advice, Figure, Origin, Placement, Proposal, Term, VenueEstimate};
use crate::placement::Request;

/// A figure to the cent, with thousands separated.
fn cents(value: Decimal) -> String {
    let rounded = value.round_dp(2);
    let negative = rounded < Decimal::ZERO;
    let text = format!("{:.2}", rounded.abs());
    let (whole, fraction) = text.split_once('.').unwrap_or((&text, "00"));
    let mut grouped = String::new();
    for (i, c) in whole.chars().enumerate() {
        if i > 0 && (whole.len() - i) % 3 == 0 {
            grouped.push(',');
        }
        grouped.push(c);
    }
    format!("{}{grouped}.{fraction}", if negative { "-" } else { "" })
}

/// Where a figure came from, in one or two characters.
fn origin(f: &Figure) -> &'static str {
    match (f.origin, &f.estimate) {
        (Origin::Venue, _) => "v",
        (Origin::Computed, None) => "c",
        (Origin::Computed, Some(_)) => "c*",
    }
}

fn term(t: &Term) -> String {
    match t {
        Term::Figure(f) => format!("{} {}", cents(f.value), origin(f)),
        Term::Missing { .. } => "missing".into(),
    }
}

/// How long before `now` a time was, to the second.
fn age(at: i64, now: i64) -> String {
    if at <= 0 {
        return "no read needed".into();
    }
    let secs = (now - at).max(0) / 1_000_000;
    if secs < 120 {
        format!("{secs} s old")
    } else {
        format!("{} min old", secs / 60)
    }
}

fn placement_name(p: &Placement) -> String {
    match p.derive_pct {
        100 => "all on Derive".into(),
        0 => "all on Hyperliquid".into(),
        pct => format!("{pct}% on Derive"),
    }
}

/// The comparison for a person.
pub fn human(request: &Request, advice: &Advice, now_micros: i64) -> String {
    let mut out = String::new();
    let side = if request.option_size < Decimal::ZERO {
        "sell"
    } else {
        "buy"
    };
    let _ = writeln!(
        out,
        "galata-margin advise: {side} {} {} at {}, hedged with {} {}-PERP",
        request.option_size.abs(),
        request.option,
        request.option_price,
        request.hedge_size,
        request.underlying
    );
    let _ = writeln!(
        out,
        "asked at {}; Derive `{}`, Hyperliquid `{}`",
        galata_datawatch::calendar::time_of(now_micros),
        request.derive_account,
        request.hyperliquid_account
    );
    let _ = writeln!(
        out,
        "advice only: nothing is ordered and no funds are moved\n"
    );

    let header = [
        "placement",
        "hedge D / H",
        "Derive margin",
        "HL margin",
        "buffer",
        "exec Derive",
        "exec HL",
        "funding",
        "total",
        "cap",
    ];
    let mut rows = vec![header.map(String::from).to_vec()];
    for p in &advice.placements {
        rows.push(vec![
            placement_name(p),
            format!("{} / {}", p.hedge_on_derive, p.hedge_on_hyperliquid),
            term(&p.derive_margin),
            term(&p.hyperliquid_margin),
            term(&p.buffer),
            term(&p.derive_execution),
            term(&p.hyperliquid_execution),
            term(&p.funding),
            p.total.map_or_else(|| "incomplete".into(), cents),
            if p.inside_cap { "inside" } else { "outside" }.into(),
        ]);
    }
    let widths: Vec<usize> = (0..header.len())
        .map(|c| rows.iter().map(|r| r[c].chars().count()).max().unwrap_or(0))
        .collect();
    for row in &rows {
        let line: Vec<String> = row
            .iter()
            .enumerate()
            .map(|(c, cell)| {
                if c < 2 {
                    format!("{cell:<w$}", w = widths[c])
                } else {
                    format!("{cell:>w$}", w = widths[c])
                }
            })
            .collect();
        let _ = writeln!(out, "{}", line.join("  ").trim_end());
    }
    let _ = writeln!(
        out,
        "  v the venue's figure, c computed by galata, c* computed and labelled an estimate"
    );

    for p in &advice.placements {
        for (name, t) in [
            ("Derive margin", &p.derive_margin),
            ("Hyperliquid margin", &p.hyperliquid_margin),
            ("buffer", &p.buffer),
            ("execution on Derive", &p.derive_execution),
            ("execution on Hyperliquid", &p.hyperliquid_execution),
            ("funding", &p.funding),
        ] {
            if let Term::Missing { reason } = t {
                let _ = writeln!(out, "  {}: {name} missing: {reason}", placement_name(p));
            }
        }
    }

    let _ = writeln!(out);
    let _ = writeln!(out, "netting benefit: {}", term(&advice.netting_benefit));
    if let Some(reason) = &advice.netting_reason {
        let _ = writeln!(out, "  {reason}");
    }
    match &advice.proposal {
        Proposal::Placement {
            derive_pct,
            rests_on_estimate,
            estimate,
        } => {
            let chosen = advice
                .placements
                .iter()
                .find(|p| p.derive_pct == *derive_pct);
            let _ = writeln!(
                out,
                "proposal: {}, total {}",
                chosen.map(placement_name).unwrap_or_default(),
                chosen.and_then(|p| p.total).map_or_else(String::new, cents)
            );
            if *rests_on_estimate {
                let said = estimate.as_ref().and_then(|l| l.disagreement).map_or_else(
                    || "never compared".into(),
                    |d| format!("disagreement {d} of the ratio"),
                );
                let _ = writeln!(
                    out,
                    "  rests on a Hyperliquid figure labelled an estimate: {said}"
                );
            }
        }
        Proposal::None { reason } => {
            let _ = writeln!(out, "proposal: none. {reason}");
        }
    }
    if let Some(cost) = advice.cap_cost {
        let _ = writeln!(
            out,
            "  the cap excluded a cheaper placement; it costs {}",
            cents(cost)
        );
    }

    let _ = writeln!(out, "\nestimates");
    for e in &advice.estimates {
        match e {
            VenueEstimate::Estimate(e) => {
                let _ = writeln!(
                    out,
                    "  {:?} `{}` ({}): {}initial {} then {}, {}maintenance {} then {}; {}, account snapshot {}",
                    e.venue,
                    e.account,
                    e.mode,
                    if e.after.initial.origin == Origin::Venue {
                        "net "
                    } else {
                        ""
                    },
                    cents(e.before.initial.value),
                    cents(e.after.initial.value),
                    if e.after.initial.origin == Origin::Venue {
                        "net "
                    } else {
                        ""
                    },
                    cents(e.before.maintenance.value),
                    cents(e.after.maintenance.value),
                    match e.after.initial.origin {
                        Origin::Venue => format!(
                            "the venue's simulation, {}",
                            age(e.after.initial.at_micros, now_micros)
                        ),
                        Origin::Computed => "computed by galata".into(),
                    },
                    age(e.snapshot_micros, now_micros)
                );
            }
            VenueEstimate::Missing {
                venue,
                account,
                reason,
            } => {
                let _ = writeln!(out, "  {venue:?} `{account}`: no figure: {reason}");
            }
        }
    }
    for note in &advice.notes {
        let _ = writeln!(out, "note: {note}");
    }
    out
}

/// The machine-readable form: the same figures, each exactly, as a decimal
/// string.
#[derive(Debug, Serialize)]
pub struct Machine<'a> {
    /// When advice was asked for.
    pub asked_at_micros: i64,
    /// The option.
    pub option: &'a str,
    /// Its signed size.
    pub option_size: Decimal,
    /// Its price.
    pub option_price: Decimal,
    /// The hedge's signed size.
    pub hedge_size: Decimal,
    /// The advice.
    pub advice: &'a Advice,
}

/// The machine-readable form, as JSON.
pub fn machine(request: &Request, advice: &Advice, now_micros: i64) -> String {
    serde_json::to_string_pretty(&Machine {
        asked_at_micros: now_micros,
        option: &request.option,
        option_size: request.option_size,
        option_price: request.option_price,
        hedge_size: request.hedge_size,
        advice,
    })
    .unwrap_or_else(|e| format!("{{\"error\": \"{e}\"}}"))
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;
    use crate::advice::{Estimate, EstimateLabel, Margins, Venue};

    fn d(s: &str) -> Decimal {
        Decimal::from_str(s).unwrap()
    }

    const NOW: i64 = 1_795_800_000_000_000;

    fn f(v: &str, origin: Origin, estimate: Option<EstimateLabel>, at: i64) -> Term {
        Term::Figure(Figure {
            value: d(v),
            origin,
            estimate,
            at_micros: at,
        })
    }

    /// The measured trade (design.md): one short BTC-20261225-60000-P and a
    /// short 0.47 BTC hedge; $6,365 together on Derive, $9,819 for the put
    /// alone, Hyperliquid's part at 40x on a 59,970 mark.
    fn measured() -> (Request, Advice) {
        let request = Request {
            option: "BTC-20261225-60000-P".into(),
            underlying: "BTC".into(),
            option_size: d("-1"),
            option_price: d("2100"),
            hedge_size: d("-0.47"),
            derive_account: "owner_s3".into(),
            hyperliquid_account: "main".into(),
        };
        let label = EstimateLabel {
            disagreement: Some(d("-0.00006")),
            measured_at_micros: Some(NOW - 240_000_000),
        };
        let at = NOW - 3_000_000;
        let placement = |pct: u32,
                         on_d: &str,
                         on_h: &str,
                         dm: &str,
                         hm: &str,
                         buf: &str,
                         ed: &str,
                         eh: &str,
                         fund: &str,
                         total: &str,
                         inside: bool| Placement {
            derive_pct: pct,
            hedge_on_derive: d(on_d),
            hedge_on_hyperliquid: d(on_h),
            derive_margin: f(dm, Origin::Venue, None, at),
            hyperliquid_margin: f(
                hm,
                Origin::Computed,
                if hm == "0" { None } else { Some(label.clone()) },
                NOW - 200_000_000,
            ),
            buffer: f(buf, Origin::Computed, None, NOW - 1_000_000),
            derive_execution: f(ed, Origin::Computed, None, NOW - 1_000_000),
            hyperliquid_execution: f(eh, Origin::Computed, None, NOW - 1_000_000),
            funding: f(fund, Origin::Computed, None, NOW - 1_000_000),
            total: Some(d(total)),
            inside_cap: inside,
        };
        let margins = |i: &str, m: &str, o, l: Option<EstimateLabel>, at| Margins {
            initial: Figure {
                value: d(i),
                origin: o,
                estimate: l.clone(),
                at_micros: at,
            },
            maintenance: Figure {
                value: d(m),
                origin: o,
                estimate: l,
                at_micros: at,
            },
        };
        let advice = Advice {
            estimates: vec![
                VenueEstimate::Estimate(Estimate {
                    venue: Venue::Derive,
                    account: "owner_s3".into(),
                    mode: "PM2".into(),
                    before: margins("100000", "120000", Origin::Venue, None, at),
                    after: margins("93635.175426635943", "115501.32", Origin::Venue, None, at),
                    snapshot_micros: NOW - 150_000_000,
                }),
                VenueEstimate::Estimate(Estimate {
                    venue: Venue::Hyperliquid,
                    account: "main".into(),
                    mode: "portfolio_margin".into(),
                    before: margins("0", "0", Origin::Computed, Some(label.clone()), NOW - 200_000_000),
                    after: margins("704.6475", "352.32375", Origin::Computed, Some(label.clone()), NOW - 200_000_000),
                    snapshot_micros: NOW - 200_000_000,
                }),
            ],
            placements: vec![
                placement(100, "-0.47", "0", "6364.824573364057", "0", "0", "0.0235", "0", "-12.53", "6352.318073364057", true),
                placement(50, "-0.235", "-0.235", "8091.5", "352.32375", "1973.0", "0.01175", "0.1175", "-12.31", "10405.14300", true),
                placement(0, "0", "-0.47", "9819", "704.6475", "3946.0", "0", "0.235", "-11.9", "14457.9825", true),
            ],
            netting_benefit: f("4158.823", Origin::Computed, Some(label.clone()), at),
            netting_reason: None,
            proposal: Proposal::Placement { derive_pct: 100, rests_on_estimate: false, estimate: None },
            cap_cost: None,
            notes: vec!["`main` holds no BTC on Hyperliquid, so its hedge is taken at the market's maximum leverage, 40x".into()],
        };
        (request, advice)
    }

    #[test]
    fn the_printed_comparison_of_the_measured_trade() {
        let (request, advice) = measured();
        let printed = human(&request, &advice, NOW);
        let path = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/snapshots/advise_measured_trade.txt"
        );
        if std::env::var_os("UPDATE_SNAPSHOTS").is_some() {
            std::fs::write(path, &printed).unwrap();
        }
        let expected = std::fs::read_to_string(path).unwrap_or_default();
        assert_eq!(printed, expected, "\n--- printed ---\n{printed}");
    }

    #[test]
    fn every_printed_figure_is_in_the_machine_form_as_a_decimal_string() {
        let (request, advice) = measured();
        let printed = human(&request, &advice, NOW);
        let json: serde_json::Value =
            serde_json::from_str(&machine(&request, &advice, NOW)).unwrap();
        // Every decimal string the machine form holds, to the cent.
        fn strings(v: &serde_json::Value, out: &mut Vec<Decimal>) {
            match v {
                serde_json::Value::String(s) => {
                    if let Ok(x) = Decimal::from_str(s) {
                        out.push(x.round_dp(2));
                    }
                }
                serde_json::Value::Array(a) => a.iter().for_each(|v| strings(v, out)),
                serde_json::Value::Object(o) => o.values().for_each(|v| strings(v, out)),
                _ => {}
            }
        }
        let mut held = Vec::new();
        strings(&json, &mut held);
        // Every amount printed to the cent.
        let amount = regex_lite_amounts(&printed);
        assert!(amount.len() > 20, "{amount:?}");
        for printed in amount {
            assert!(
                held.contains(&printed),
                "{printed} is printed and not in the machine form"
            );
        }
        // And no figure in it is a JSON number but times and shares.
        assert!(json["advice"]["placements"][0]["total"].is_string());
    }

    /// The amounts a printed comparison holds: numbers with two decimals.
    fn regex_lite_amounts(text: &str) -> Vec<Decimal> {
        text.split(char::is_whitespace)
            .filter_map(|w| {
                let w = w.trim_end_matches([';', ':']);
                let plain: String = w.chars().filter(|c| *c != ',').collect();
                let (_, frac) = plain.split_once('.')?;
                (frac.len() == 2
                    && plain
                        .chars()
                        .all(|c| c.is_ascii_digit() || c == '.' || c == '-'))
                .then(|| Decimal::from_str(&plain).ok())
                .flatten()
            })
            .collect()
    }
}
