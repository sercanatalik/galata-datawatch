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
