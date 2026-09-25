//! The ledger binary: a file on disk, and the wiring the library holds.
//!
//! Like `galata-datawatch`, it chooses only where the configuration comes
//! from. Every rule — the root's mode, the fingerprints, the refusals — lives
//! in the library, and `galata-datawatch-vault`'s ledger entry calls the same
//! function with a vault document instead.

use galata_datawatch::adapters;
use galata_datawatch::boot;
use galata_datawatch::config::{Adapters, EnvSecrets, FileSource, LedgerCost};

/// What the loader asks an adapter, answered without this file naming a venue.
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
    fn ledger_cost(&self, venue: &str) -> Option<LedgerCost> {
        adapters::ledger_cost(venue)
    }
}

fn main() -> std::process::ExitCode {
    match boot::boot_ledger(
        &match FileSource::from_env("config/datawatch.toml") {
            Ok(source) => source,
            Err(error) => {
                eprintln!("{error}");
                return std::process::ExitCode::FAILURE;
            }
        },
        &EnvSecrets,
        &Resolver,
    ) {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(error) => {
            // The whole chain, as capture prints it.
            eprint!("{error}");
            let mut source = error.source();
            while let Some(cause) = source {
                eprint!(": {cause}");
                source = cause.source();
            }
            eprintln!();
            std::process::ExitCode::FAILURE
        }
    }
}
