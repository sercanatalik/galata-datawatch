//! A correlation regime flag: the constant correlation the fitted horizons
//! declare, rejected in two consecutive windows.
//!
//! `py/signals` writes `constancy` at each fitted horizon's close: Engle and
//! Sheppard's (2001) test that the correlation stayed constant over the last
//! 30 days, against the declared R. A test repeated every half hour on
//! overlapping windows alarms far more often than its nominal level (Chu,
//! Stinchcombe and White 1996). Consecutive windows share almost all their
//! data, so a false rejection persists rather than blinks: by a Rice
//! crossing-rate estimate, about one false episode a month per horizon at 5%
//! against one in years at 0.1%.
//!
//! **So the finding is deliberately narrow.** The two newest windows of a
//! horizon, a completed bar apart, must both fall below the operator's bound
//! (`[watch] max_constancy_p`, no default; 0.001 suggested). The second window
//! removes a single-bar blip; the bound does the rest. The watch keeps no
//! state, so a flag is reported every hour it holds, as every finding is.
//!
//! **And the sequential monitor's alarm** (`monitor`, Wied and Galeano 2013),
//! when `[watch] monitor_alarms` is true: the newest `wied_galeano_alarm` of a
//! horizon, raised. The monitor bounds its own false alarms per calendar epoch,
//! so nothing is repeated or thresholded here; its α is declared where it is
//! computed. The finding names the pair and the bar the change is dated to.

use std::collections::BTreeMap;
use std::path::Path;

use arrow::array::{Array, Float64Array, Int64Array, RecordBatch, StringArray};

use crate::watch::Finding;

/// The signal and measure the flag reads.
const SIGNAL: &str = "constancy";
const MEASURE: &str = "engle_sheppard_p";
const LOOKBACK_DAYS: i64 = 14;
const DAY_MICROS: i64 = 86_400_000_000;

/// One stored p-value: horizon, asof, computed, value.
type Stored = (String, i64, i64, Option<f64>);

/// Each horizon's two newest asofs whose p-values are both below `bound`.
pub fn flags(tape: &Path, bound: f64, now_micros: i64) -> Vec<Finding> {
    let at = tape.join(format!("kind={}", galata_wire::Kind::Signals));
    judge(&stored(tape, now_micros), bound, &at)
}

fn judge(rows: &[Stored], bound: f64, at: &Path) -> Vec<Finding> {
    // Per horizon and asof, the newest computation.
    let mut by: BTreeMap<&str, BTreeMap<i64, (i64, Option<f64>)>> = BTreeMap::new();
    for (horizon, asof, computed, value) in rows {
        let slot = by.entry(horizon.as_str()).or_default();
        match slot.get(asof) {
            Some((held, _)) if held >= computed => {}
            _ => {
                slot.insert(*asof, (*computed, *value));
            }
        }
    }
    let mut findings = Vec::new();
    for (horizon, asofs) in by {
        let newest: Vec<_> = asofs.iter().rev().take(2).collect();
        let [(_, (_, Some(last))), (_, (_, Some(before)))] = newest[..] else {
            continue;
        };
        if *last < bound && *before < bound {
            findings.push(Finding {
                observed: format!(
                    "{horizon}: constant correlation rejected in two consecutive windows \
                     (Engle–Sheppard p {last:.1e}, then {before:.1e} the bar before): \
                     a correlation regime flag"
                ),
                expected: format!("p at least {bound} in one of them (max_constancy_p)"),
                at: at.to_path_buf(),
            });
        }
    }
    findings
}

fn stored(tape: &Path, now_micros: i64) -> Vec<Stored> {
    let root = tape.join(format!("kind={}", galata_wire::Kind::Signals));
    let oldest = crate::calendar::date_of(now_micros - LOOKBACK_DAYS * DAY_MICROS);
    let Ok(entries) = std::fs::read_dir(&root) else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for entry in entries.filter_map(|e| e.ok()) {
        let Some(date) = entry
            .file_name()
            .to_str()
            .and_then(|n| n.strip_prefix("date="))
            .map(str::to_string)
        else {
            continue;
        };
        if date < oldest {
            continue;
        }
        for (_, path) in galata_segments::list_segments(&entry.path()) {
            // A segment that will not read is the layout check's to report.
            let Ok(batches) = galata_segments::read_segment(&path) else {
                continue;
            };
            out.extend(batches.iter().flat_map(rows));
        }
    }
    out
}

fn rows(batch: &RecordBatch) -> Vec<Stored> {
    let text = |name: &str| {
        batch
            .column_by_name(name)
            .and_then(|c| c.as_any().downcast_ref::<StringArray>().cloned())
    };
    let int = |name: &str| {
        batch
            .column_by_name(name)
            .and_then(|c| c.as_any().downcast_ref::<Int64Array>().cloned())
    };
    let value = batch
        .column_by_name("value")
        .and_then(|c| c.as_any().downcast_ref::<Float64Array>().cloned());
    let (Some(signal), Some(measure), Some(horizon), Some(value), Some(asof), Some(computed)) = (
        text("signal"),
        text("measure"),
        text("horizon"),
        value,
        int("asof_micros"),
        int("computed_micros"),
    ) else {
        return Vec::new();
    };
    (0..batch.num_rows())
        .filter(|&i| signal.value(i) == SIGNAL && measure.value(i) == MEASURE)
        .map(|i| {
            (
                horizon.value(i).to_string(),
                asof.value(i),
                computed.value(i),
                (!value.is_null(i)).then(|| value.value(i)),
            )
        })
        .collect()
}

/// Each horizon whose newest `monitor` alarm is raised.
pub fn alarms(tape: &Path, now_micros: i64) -> Vec<Finding> {
    let at = tape.join(format!("kind={}", galata_wire::Kind::Signals));
    judge_alarms(&stored_alarms(tape, now_micros), &at)
}

/// One stored alarm: horizon, asof, computed, value, params.
type Alarm = (String, i64, i64, Option<f64>, String);

fn judge_alarms(rows: &[Alarm], at: &Path) -> Vec<Finding> {
    let mut newest: BTreeMap<&str, &Alarm> = BTreeMap::new();
    for row in rows {
        let key = (row.1, row.2);
        match newest.get(row.0.as_str()) {
            Some(held) if (held.1, held.2) >= key => {}
            _ => {
                newest.insert(row.0.as_str(), row);
            }
        }
    }
    let mut findings = Vec::new();
    for (horizon, (_, _, _, value, params)) in newest {
        if *value != Some(1.0) {
            continue;
        }
        let p: serde_json::Value = serde_json::from_str(params).unwrap_or_default();
        let pair = p["pair"]
            .as_array()
            .map(|a| {
                a.iter()
                    .filter_map(|t| t.as_str())
                    .collect::<Vec<_>>()
                    .join("|")
            })
            .unwrap_or_else(|| "?".into());
        let since = p["change_at"].as_str().unwrap_or("an undated bar");
        let epoch = p["epoch_start"].as_str().unwrap_or("?");
        findings.push(Finding {
            observed: format!(
                "{horizon}: the sequential monitor alarmed on {pair}, its correlation changed \
                 around {since} (Wied–Galeano, epoch from {epoch}, α {})",
                p["alpha"]
            ),
            expected: "no alarm in the epoch (monitor_alarms)".into(),
            at: at.to_path_buf(),
        });
    }
    findings
}

fn stored_alarms(tape: &Path, now_micros: i64) -> Vec<Alarm> {
    let root = tape.join(format!("kind={}", galata_wire::Kind::Signals));
    let oldest = crate::calendar::date_of(now_micros - LOOKBACK_DAYS * DAY_MICROS);
    let Ok(entries) = std::fs::read_dir(&root) else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for entry in entries.filter_map(|e| e.ok()) {
        let Some(date) = entry
            .file_name()
            .to_str()
            .and_then(|n| n.strip_prefix("date="))
            .map(str::to_string)
        else {
            continue;
        };
        if date < oldest {
            continue;
        }
        for (_, path) in galata_segments::list_segments(&entry.path()) {
            let Ok(batches) = galata_segments::read_segment(&path) else {
                continue;
            };
            for batch in &batches {
                let text = |name: &str| {
                    batch
                        .column_by_name(name)
                        .and_then(|c| c.as_any().downcast_ref::<StringArray>().cloned())
                };
                let int = |name: &str| {
                    batch
                        .column_by_name(name)
                        .and_then(|c| c.as_any().downcast_ref::<Int64Array>().cloned())
                };
                let value = batch
                    .column_by_name("value")
                    .and_then(|c| c.as_any().downcast_ref::<Float64Array>().cloned());
                let (
                    Some(signal),
                    Some(measure),
                    Some(ti),
                    Some(horizon),
                    Some(value),
                    Some(asof),
                    Some(computed),
                    Some(params),
                ) = (
                    text("signal"),
                    text("measure"),
                    text("ticker_i"),
                    text("horizon"),
                    value,
                    int("asof_micros"),
                    int("computed_micros"),
                    text("params"),
                )
                else {
                    continue;
                };
                for i in 0..batch.num_rows() {
                    if signal.value(i) == "monitor"
                        && measure.value(i) == "wied_galeano_alarm"
                        && ti.value(i) == "*"
                    {
                        out.push((
                            horizon.value(i).to_string(),
                            asof.value(i),
                            computed.value(i),
                            (!value.is_null(i)).then(|| value.value(i)),
                            params.value(i).to_string(),
                        ));
                    }
                }
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    const H: i64 = 3_600_000_000;

    fn p(horizon: &str, asof: i64, value: Option<f64>) -> Stored {
        (horizon.to_string(), asof * H, asof * H + 900_000_000, value)
    }

    #[test]
    fn two_consecutive_rejections_are_a_flag() {
        let rows = [
            p("4h", 1, Some(0.5)),
            p("4h", 2, Some(0.0004)),
            p("4h", 3, Some(0.0002)),
        ];
        let found = judge(&rows, 0.001, Path::new("t"));
        assert_eq!(found.len(), 1);
        let said = found[0].to_string();
        for part in ["4h", "2.0e-4", "4.0e-4", "regime flag", "at least 0.001"] {
            assert!(said.contains(part), "{part} missing from: {said}");
        }
    }

    #[test]
    fn one_rejection_is_not() {
        let rows = [p("1h", 1, Some(0.3)), p("1h", 2, Some(0.0001))];
        assert!(judge(&rows, 0.001, Path::new("t")).is_empty());
    }

    #[test]
    fn a_recomputed_window_counts_once_at_its_newest() {
        // The same asof computed twice: the later computation stands.
        let mut again = p("4h", 2, Some(0.4));
        again.2 += 1;
        let rows = [p("4h", 1, Some(0.0001)), p("4h", 2, Some(0.0001)), again];
        assert!(judge(&rows, 0.001, Path::new("t")).is_empty());
    }

    fn alarm(horizon: &str, asof: i64, value: f64) -> Alarm {
        let params = r#"{"alpha":0.05,"change_at":"2026-09-26T14:05:00+00:00","epoch_start":"2026-09-24T00:00:00+00:00","pair":["BTC","GOLD"]}"#;
        (
            horizon.to_string(),
            asof * H,
            asof * H + 1,
            Some(value),
            params.to_string(),
        )
    }

    #[test]
    fn a_raised_alarm_names_its_pair_and_its_date() {
        let found = judge_alarms(
            &[
                alarm("5m", 1, 0.0),
                alarm("5m", 2, 1.0),
                alarm("4h", 2, 0.0),
            ],
            Path::new("t"),
        );
        assert_eq!(found.len(), 1);
        let said = found[0].to_string();
        for part in [
            "5m",
            "BTC|GOLD",
            "2026-09-26T14:05",
            "epoch from 2026-09-24",
            "α 0.05",
        ] {
            assert!(said.contains(part), "{part} missing from: {said}");
        }
    }

    #[test]
    fn only_the_newest_alarm_counts() {
        // Raised in an old run, clear in the newest: a new epoch started.
        assert!(
            judge_alarms(&[alarm("1h", 1, 1.0), alarm("1h", 2, 0.0)], Path::new("t")).is_empty()
        );
    }

    #[test]
    fn an_absent_test_is_no_flag() {
        let rows = [p("5m", 1, None), p("5m", 2, Some(0.0))];
        assert!(judge(&rows, 0.001, Path::new("t")).is_empty());
    }
}
