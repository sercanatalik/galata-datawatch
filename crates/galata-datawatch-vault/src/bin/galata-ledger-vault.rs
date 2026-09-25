//! The ledger, configured from a vault document.
//!
//! **The same process as `galata-ledger`, differing in one value**, as
//! `galata-datawatch-vault` is to `galata-datawatch`: both call
//! `galata_datawatch::boot::boot_ledger`, and this one hands it a
//! [`VaultConfig`] and the vault's secrets.
//!
//! **The addresses and the fingerprint key are read once, at boot**, before
//! anything is asked of the venue, and the loop holds none of the vault. A
//! discovered sub-account's address comes from the venue's own listing, so the
//! loop never needs to reach back. Its token should read only this venue's
//! ledger variables and the fingerprint key (`scripts/mint-service-tokens.sh`).

use galata_datawatch::adapters;
use galata_datawatch::boot;
use galata_datawatch::config::source::document_from_env;
use galata_datawatch::config::{Adapters, LedgerCost};
use galata_datawatch_vault::{VaultConfig, VaultSecrets};
use galata_vault::Vault;

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
    fn keeps_ledgers(&self) -> bool {
        true
    }
}

fn main() -> std::process::ExitCode {
    match run() {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(error) => {
            eprint!("{error}");
            let mut source = std::error::Error::source(&*error);
            while let Some(cause) = source {
                eprint!(": {cause}");
                source = cause.source();
            }
            eprintln!();
            std::process::ExitCode::FAILURE
        }
    }
}

fn run() -> Result<(), Box<dyn std::error::Error>> {
    let document = document_from_env()?;
    let vault = Vault::from_env()?;
    let config = VaultConfig::fetch(&vault, &document)?;
    boot::boot_ledger(&config, &VaultSecrets::new(&vault), &Resolver)
}
