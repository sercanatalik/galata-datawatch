//! The `[margin]` block of the datawatch configuration (design D10).
//!
//! **Every setting is the owner's.** None has a default, because each one
//! decides a proposal: which accounts are compared, how old a snapshot may
//! be, how the hedge is split, how long funding is held for, and how much of
//! the trade one venue may hold. A missing key, a value outside its bound and
//! a key this block does not know are each refused, by name, before anything
//! is asked of a venue.
//!
//! galata-datawatch holds the block as written (`Config::margin`) and knows
//! nothing of it, so its rules live here, in the crate that does not publish.
//!
//! ```toml
//! [margin]
//! session_key_var = "GALATA_LEDGER_DERIVE_SESSION_KEY"
//! agreement_tolerance = "0.005"   # of the portfolio margin ratio, a decimal string
//! snapshot_age_secs = 900
//! split_pct = [25, 50, 75]
//! funding_hours = 168
//! venue_cap_pct = 100
//!
//! [margin.accounts]
//! derive-xyz = ["owner_s3"]
//! hyperliquid = ["main"]
//! ```

use std::str::FromStr;

use galata_datawatch::config::Config;
use rust_decimal::Decimal;
use serde::Deserialize;

/// The block, read and bounded.
#[derive(Debug, Clone, PartialEq)]
pub struct MarginConfig {
    /// The accounts a placement may land in, per venue, by the ledger's
    /// aliases: a Derive subaccount (`owner_s3`) or a Hyperliquid account
    /// (`main`). A request names one of each (design D6).
    pub accounts: Accounts,
    /// The name of the variable holding the Derive session key: the ledger's,
    /// since one key serves both (design D11).
    pub session_key_var: String,
    /// How far galata's portfolio margin ratio may differ from the venue's
    /// before every Hyperliquid figure is labelled an estimate (design D4).
    pub agreement_tolerance: Decimal,
    /// The oldest account snapshot a figure may rest on (design D8).
    pub snapshot_age_secs: u64,
    /// The hedge's splits, in percent placed on Derive, strictly increasing.
    /// Placements are these plus all on Derive and all on Hyperliquid.
    pub split_pct: Vec<u32>,
    /// The holding period the funding difference is counted over, in hours.
    pub funding_hours: u32,
    /// The largest share of the trade, in percent, one venue may hold.
    pub venue_cap_pct: u32,
}

/// The accounts per venue.
#[derive(Debug, Clone, PartialEq)]
pub struct Accounts {
    /// Derive subaccounts, by alias.
    pub derive: Vec<String>,
    /// Hyperliquid accounts, by alias.
    pub hyperliquid: Vec<String>,
}

/// Why the block was refused: every problem found, each naming its key.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
#[error("[margin] is refused:\n  {}", .problems.join("\n  "))]
pub struct ConfigError {
    /// One line per problem.
    pub problems: Vec<String>,
}

impl ConfigError {
    fn one(problem: impl Into<String>) -> ConfigError {
        ConfigError {
            problems: vec![problem.into()],
        }
    }
}

/// The block as written: every key required, no other key taken.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Raw {
    accounts: RawAccounts,
    session_key_var: String,
    /// A decimal string, so the tolerance is exactly what was written.
    agreement_tolerance: String,
    snapshot_age_secs: u64,
    split_pct: Vec<u32>,
    funding_hours: u32,
    venue_cap_pct: u32,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawAccounts {
    #[serde(rename = "derive-xyz")]
    derive: Vec<String>,
    hyperliquid: Vec<String>,
}

/// The tolerance's upper bound: a tenth of the ratio, past which galata's
/// figure would not be worth labelling at all.
const MAX_TOLERANCE: &str = "0.1";
/// The snapshot age's bounds, in seconds.
const SNAPSHOT_AGE_SECS: std::ops::RangeInclusive<u64> = 60..=3_600;
/// At most five splits: seven placements, seven simulations, inside Derive's
/// 25 reads per 5 s window, which the ledger shares (`design/measured.md`).
const MAX_SPLITS: usize = 5;
/// The funding holding period's bounds, in hours: up to thirty days.
const FUNDING_HOURS: std::ops::RangeInclusive<u32> = 1..=720;

impl MarginConfig {
    /// The block of a loaded configuration. Absent is refused: the optimiser
    /// has nothing it could decide without it.
    pub fn from_config(config: &Config) -> Result<MarginConfig, ConfigError> {
        let table = config.margin.as_ref().ok_or_else(|| {
            ConfigError::one("the configuration has no [margin] block; the optimiser's settings are the owner's to declare")
        })?;
        let ledger_snapshot_secs = config.ledger.as_ref().map(|l| l.snapshot_secs);
        MarginConfig::from_table(table, ledger_snapshot_secs)
    }

    /// The block as a table, checked against the ledger's snapshot cadence
    /// when there is one: a bound shorter than the cadence would refuse every
    /// figure.
    pub fn from_table(
        table: &toml::Table,
        ledger_snapshot_secs: Option<u64>,
    ) -> Result<MarginConfig, ConfigError> {
        let raw: Raw = toml::Value::Table(table.clone())
            .try_into()
            .map_err(|e: toml::de::Error| ConfigError::one(e.message().to_string()))?;
        let mut problems = Vec::new();

        let accounts = Accounts {
            derive: aliases("accounts.derive-xyz", raw.accounts.derive, &mut problems),
            hyperliquid: aliases(
                "accounts.hyperliquid",
                raw.accounts.hyperliquid,
                &mut problems,
            ),
        };

        let var = &raw.session_key_var;
        if var.is_empty()
            || !var
                .chars()
                .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || c == '_')
            || var.starts_with(|c: char| c.is_ascii_digit())
        {
            problems.push(format!(
                "`session_key_var` must name a variable, A-Z, 0-9 and _: {var:?}"
            ));
        }

        let max = Decimal::from_str(MAX_TOLERANCE).expect("a decimal");
        let agreement_tolerance = match Decimal::from_str(&raw.agreement_tolerance) {
            Ok(t) if t > Decimal::ZERO && t <= max => t,
            Ok(t) => {
                problems.push(format!(
                    "`agreement_tolerance` must be above 0 and at most {MAX_TOLERANCE} of the ratio: {t}"
                ));
                Decimal::ZERO
            }
            Err(_) => {
                problems.push(format!(
                    "`agreement_tolerance` must be a decimal string, such as \"0.005\": {:?}",
                    raw.agreement_tolerance
                ));
                Decimal::ZERO
            }
        };

        if !SNAPSHOT_AGE_SECS.contains(&raw.snapshot_age_secs) {
            problems.push(format!(
                "`snapshot_age_secs` must be from {} to {}: {}",
                SNAPSHOT_AGE_SECS.start(),
                SNAPSHOT_AGE_SECS.end(),
                raw.snapshot_age_secs
            ));
        } else if let Some(cadence) = ledger_snapshot_secs
            && raw.snapshot_age_secs < cadence
        {
            problems.push(format!(
                "`snapshot_age_secs` ({}) is shorter than the ledger's snapshot cadence ({cadence} s), so every snapshot would be refused as stale",
                raw.snapshot_age_secs
            ));
        }

        let splits = &raw.split_pct;
        if splits.is_empty() || splits.len() > MAX_SPLITS {
            problems.push(format!(
                "`split_pct` must hold from 1 to {MAX_SPLITS} splits: {}",
                splits.len()
            ));
        }
        if splits.iter().any(|p| !(1..=99).contains(p)) {
            problems.push(format!(
                "`split_pct` must each be from 1 to 99 percent on Derive: {splits:?}"
            ));
        }
        if splits.windows(2).any(|w| w[0] >= w[1]) {
            problems.push(format!(
                "`split_pct` must be strictly increasing: {splits:?}"
            ));
        }

        if !FUNDING_HOURS.contains(&raw.funding_hours) {
            problems.push(format!(
                "`funding_hours` must be from {} to {}: {}",
                FUNDING_HOURS.start(),
                FUNDING_HOURS.end(),
                raw.funding_hours
            ));
        }
        if !(1..=100).contains(&raw.venue_cap_pct) {
            problems.push(format!(
                "`venue_cap_pct` must be from 1 to 100: {}",
                raw.venue_cap_pct
            ));
        }

        if !problems.is_empty() {
            return Err(ConfigError { problems });
        }
        Ok(MarginConfig {
            accounts,
            session_key_var: raw.session_key_var,
            agreement_tolerance,
            snapshot_age_secs: raw.snapshot_age_secs,
            split_pct: raw.split_pct,
            funding_hours: raw.funding_hours,
            venue_cap_pct: raw.venue_cap_pct,
        })
    }
}

/// At least one alias, each a ledger alias's shape, none twice.
fn aliases(key: &str, given: Vec<String>, problems: &mut Vec<String>) -> Vec<String> {
    if given.is_empty() {
        problems.push(format!("`{key}` must name at least one account"));
    }
    for alias in &given {
        if alias.is_empty()
            || !alias
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
        {
            problems.push(format!(
                "`{key}`: {alias:?} is not an alias (letters, digits, _ and -)"
            ));
        }
    }
    let mut seen = std::collections::BTreeSet::new();
    for alias in &given {
        if !seen.insert(alias) {
            problems.push(format!("`{key}` names {alias:?} twice"));
        }
    }
    given
}

#[cfg(test)]
mod tests {
    use super::*;

    const VALID: &str = r#"
session_key_var = "GALATA_LEDGER_DERIVE_SESSION_KEY"
agreement_tolerance = "0.005"
snapshot_age_secs = 900
split_pct = [25, 50, 75]
funding_hours = 168
venue_cap_pct = 100

[accounts]
derive-xyz = ["owner_s3"]
hyperliquid = ["main"]
"#;

    fn table(text: &str) -> toml::Table {
        text.parse().expect("a table")
    }

    fn refused(text: &str, cadence: Option<u64>) -> Vec<String> {
        MarginConfig::from_table(&table(text), cadence)
            .expect_err("refused")
            .problems
    }

    #[test]
    fn a_valid_block_is_read_as_written() {
        let config = MarginConfig::from_table(&table(VALID), Some(300)).unwrap();
        assert_eq!(config.accounts.derive, ["owner_s3"]);
        assert_eq!(config.accounts.hyperliquid, ["main"]);
        assert_eq!(config.session_key_var, "GALATA_LEDGER_DERIVE_SESSION_KEY");
        assert_eq!(config.agreement_tolerance.to_string(), "0.005");
        assert_eq!(config.snapshot_age_secs, 900);
        assert_eq!(config.split_pct, [25, 50, 75]);
        assert_eq!(config.funding_hours, 168);
        assert_eq!(config.venue_cap_pct, 100);
    }

    #[test]
    fn each_missing_key_is_refused_by_name() {
        let top = table(VALID);
        for key in top.keys() {
            let mut without = top.clone();
            without.remove(key);
            let problems = MarginConfig::from_table(&without, None)
                .unwrap_err()
                .problems;
            assert!(
                problems.iter().any(|p| p.contains(key.as_str())),
                "{key}: {problems:?}"
            );
        }
        for venue in ["derive-xyz", "hyperliquid"] {
            let mut without = top.clone();
            without["accounts"].as_table_mut().unwrap().remove(venue);
            let problems = MarginConfig::from_table(&without, None)
                .unwrap_err()
                .problems;
            assert!(
                problems.iter().any(|p| p.contains(venue)),
                "{venue}: {problems:?}"
            );
        }
    }

    #[test]
    fn an_unknown_key_is_refused_by_name() {
        let problems = refused(&format!("{VALID}\n[extra]\nx = 1\n"), None);
        assert!(problems.iter().any(|p| p.contains("extra")), "{problems:?}");
        let problems = refused(
            &VALID.replace(
                "hyperliquid = [\"main\"]",
                "hyperliquid = [\"main\"]\nbinance = [\"x\"]",
            ),
            None,
        );
        assert!(
            problems.iter().any(|p| p.contains("binance")),
            "{problems:?}"
        );
    }

    #[test]
    fn a_value_out_of_its_bound_is_refused_by_name() {
        for (from, to, key) in [
            ("\"0.005\"", "\"0\"", "agreement_tolerance"),
            ("\"0.005\"", "\"0.2\"", "agreement_tolerance"),
            ("\"0.005\"", "\"half\"", "agreement_tolerance"),
            ("= 900", "= 30", "snapshot_age_secs"),
            ("= 900", "= 7200", "snapshot_age_secs"),
            ("[25, 50, 75]", "[]", "split_pct"),
            ("[25, 50, 75]", "[0, 50]", "split_pct"),
            ("[25, 50, 75]", "[50, 25]", "split_pct"),
            ("[25, 50, 75]", "[10, 20, 30, 40, 50, 60]", "split_pct"),
            ("= 168", "= 0", "funding_hours"),
            ("= 168", "= 721", "funding_hours"),
            (
                "venue_cap_pct = 100",
                "venue_cap_pct = 101",
                "venue_cap_pct",
            ),
            ("[\"owner_s3\"]", "[]", "accounts.derive-xyz"),
            ("[\"main\"]", "[\"main\", \"main\"]", "accounts.hyperliquid"),
            ("[\"main\"]", "[\"0xabc def\"]", "accounts.hyperliquid"),
            (
                "\"GALATA_LEDGER_DERIVE_SESSION_KEY\"",
                "\"a key\"",
                "session_key_var",
            ),
        ] {
            assert_eq!(
                VALID.matches(from).count(),
                1,
                "{from} is not unique in the block"
            );
            let problems = refused(&VALID.replacen(from, to, 1), None);
            assert!(
                problems.iter().any(|p| p.contains(key)),
                "{from} -> {to}: {problems:?}"
            );
        }
    }

    #[test]
    fn a_bound_shorter_than_the_ledgers_cadence_is_refused() {
        let text = VALID.replace("= 900", "= 120");
        assert!(MarginConfig::from_table(&table(&text), None).is_ok());
        let problems = refused(&text, Some(300));
        assert!(
            problems
                .iter()
                .any(|p| p.contains("ledger's snapshot cadence")),
            "{problems:?}"
        );
    }
}
