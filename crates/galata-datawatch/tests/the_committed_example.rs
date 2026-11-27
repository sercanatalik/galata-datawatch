//! The committed example configuration loads, and so does every key it
//! documents commented out for the operator to enable.

use std::path::PathBuf;

use galata_datawatch::adapters;
use galata_datawatch::config::{Adapters, Config, Origin};

/// The adapter rules the binaries use.
struct Resolver;

impl Adapters for Resolver {
    fn supplies(&self, venue: &str, series: galata_wire::Series) -> bool {
        adapters::supplies(venue, series)
    }
    fn known(&self, venue: &str) -> bool {
        adapters::known().contains(&venue)
    }
    fn known_names(&self) -> Vec<&'static str> {
        adapters::known()
    }
    fn max_instruments(&self, venue: &str) -> Option<u32> {
        adapters::max_instruments(venue)
    }
    fn instrument_problem(&self, venue: &str, ticker: &str) -> Option<String> {
        adapters::instrument_problem(venue, ticker)
    }
}

fn example() -> (PathBuf, String) {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../config/datawatch.toml");
    let text = std::fs::read_to_string(&path).unwrap();
    (path, text)
}

#[test]
fn the_committed_example_loads() {
    let (path, _) = example();
    Config::load_from(&path, &Resolver).unwrap();
}

/// A line `# <key> = <value>`: a key the example tells the operator to enable.
fn documented_key(line: &str) -> Option<&str> {
    let rest = line.strip_prefix("# ")?;
    let (key, _) = rest.split_once(" = ")?;
    key.chars()
        .all(|c| c.is_ascii_lowercase() || c == '_')
        .then_some(key)
}

#[test]
fn every_documented_key_loads_uncommented() {
    let (path, text) = example();
    let lines: Vec<&str> = text.lines().collect();
    let mut found = Vec::new();
    for (i, line) in lines.iter().enumerate() {
        let Some(key) = documented_key(line) else {
            continue;
        };
        found.push(key.to_string());
        let mut enabled = lines.clone();
        let uncommented = line.strip_prefix("# ").unwrap().to_string();
        enabled[i] = &uncommented;
        let candidate = enabled.join("\n");
        if let Err(error) = Config::load_from_str(&candidate, Origin::File(path.clone()), &Resolver)
        {
            panic!("the example documents `{line}`, and uncommented it is refused: {error}");
        }
    }
    assert!(
        !found.is_empty(),
        "no documented key found: the test would pass on nothing"
    );
    eprintln!("documented keys checked: {found:?}");
}

/// The ledger binary's rules: the costs it declares for every venue it keeps.
struct LedgerResolver {
    /// A venue this build is told to lack, as a build without its feature does.
    without: Option<&'static str>,
}

impl Adapters for LedgerResolver {
    fn supplies(&self, venue: &str, series: galata_wire::Series) -> bool {
        adapters::supplies(venue, series)
    }
    fn known(&self, venue: &str) -> bool {
        adapters::known().contains(&venue) && Some(venue) != self.without
    }
    fn known_names(&self) -> Vec<&'static str> {
        adapters::known()
            .into_iter()
            .filter(|v| Some(*v) != self.without)
            .collect()
    }
    fn max_instruments(&self, venue: &str) -> Option<u32> {
        adapters::max_instruments(venue)
    }
    fn instrument_problem(&self, venue: &str, ticker: &str) -> Option<String> {
        adapters::instrument_problem(venue, ticker)
    }
    fn ledger_cost(&self, venue: &str) -> Option<galata_datawatch::config::LedgerCost> {
        (Some(venue) != self.without)
            .then(|| adapters::ledger_cost(venue))
            .flatten()
    }
    fn keeps_ledgers(&self) -> bool {
        true
    }
}

/// The example's commented `[ledger]` block, enabled, with a Derive wallet
/// beside the Hyperliquid account, at the given cadences.
fn with_a_derive_ledger(snapshot_secs: u32, discover_secs: u32) -> String {
    let (_, text) = example();
    format!(
        "{text}
[ledger]
root = \"var/ledger\"
snapshot_secs = {snapshot_secs}
discover_secs = {discover_secs}
events_secs = 300
fold_position_tolerance = 0.0
fold_relative_tolerance = 0.00002
ledger_share = 0.25
fingerprint_key_var = \"GALATA_LEDGER_FINGERPRINT_KEY\"

[ledger.account.main]
venue = \"hyperliquid\"
address_var = \"GALATA_LEDGER_HL_MAIN\"
dexes = [\"\", \"xyz\"]

[ledger.account.owner]
venue = \"derive-xyz\"
address_var = \"GALATA_LEDGER_DERIVE_WALLET\"
session_key_var = \"GALATA_LEDGER_DERIVE_SESSION_KEY\"
session_key_warn_days = 14
market = \"mainnet\"
"
    )
}

#[test]
fn the_committed_example_loads_for_the_ledger() {
    let (path, _) = example();
    Config::load_from(&path, &LedgerResolver { without: None }).unwrap();
}

#[cfg(feature = "derive-xyz")]
#[test]
fn a_derive_wallet_loads_at_the_deployments_cadences() {
    let (path, _) = example();
    Config::load_from_str(
        &with_a_derive_ledger(300, 600),
        Origin::File(path),
        &LedgerResolver { without: None },
    )
    .unwrap();
    // One request a snapshot and two a discovery: 0.4 a minute, against 75.
    let cost = adapters::ledger_cost("derive-xyz").unwrap();
    assert_eq!((cost.snapshot, cost.discovery), (1.0, 2.0));
    assert!(cost.reads_with_key && !cost.has_dexes);
}

#[cfg(feature = "derive-xyz")]
#[test]
fn a_derive_cadence_over_the_ledgers_share_is_refused_naming_the_budget() {
    let (path, _) = example();
    // Every second, and discovery every second: 60 + 120 a minute, over the
    // 75 that a quarter of 300 allows.
    let refused = Config::load_from_str(
        &with_a_derive_ledger(1, 1),
        Origin::File(path),
        &LedgerResolver { without: None },
    )
    .unwrap_err()
    .to_string();
    assert!(refused.contains("derive-xyz"), "{refused}");
    assert!(
        refused.contains("75"),
        "the share's allowance is named: {refused}"
    );
}

#[test]
fn a_build_without_derive_says_so_naming_what_it_keeps() {
    let (path, _) = example();
    let refused = Config::load_from_str(
        &with_a_derive_ledger(300, 600),
        Origin::File(path),
        &LedgerResolver {
            without: Some("derive-xyz"),
        },
    )
    .unwrap_err()
    .to_string();
    assert!(refused.contains("derive-xyz"), "{refused}");
    assert!(
        refused.contains("hyperliquid"),
        "what it keeps is named: {refused}"
    );
}
