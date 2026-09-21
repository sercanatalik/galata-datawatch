//! Capture, configured from a vault document.
//!
//! **The same process as `galata-datawatch`, differing in one value.** Both
//! call `galata_datawatch::boot::boot`; this one hands it a [`VaultConfig`]
//! where the other hands it a `FileSource`. Everything after that line — the
//! crypto provider, the subscriber, the argv venue rule, the universe
//! pre-check, the capture loop — is the library's, and is therefore the same
//! by construction rather than because two copies agree.
//!
//! **Nothing here names how a vault client authenticates.** That is the
//! vault's own rule, stated once in its own documentation; a copy here would
//! disagree with it rather than fail, and `check-secret-reach.sh` holds the
//! line. `Vault::from_env` reads whatever it reads.

use galata_datawatch::adapters;
use galata_datawatch::boot;
use galata_datawatch::config::Adapters;
use galata_datawatch::config::source::document_from_env;
use galata_datawatch_vault::VaultConfig;
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
}

fn main() -> std::process::ExitCode {
    match run() {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(error) => {
            // The whole chain, not just the top: a vault refusal's reason is
            // under the message rather than in it.
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
    // **Which document — asked of the library, not decided here.** Naming both
    // a file and a document is a refusal, and it is the same refusal the file
    // binary gives, because it is the same function.
    let document = document_from_env()?;

    // Fetched once, before anything connects to a venue. Nothing below holds
    // the vault, so the capture loop cannot reach it even by accident.
    let vault = Vault::from_env()?;
    let config = VaultConfig::fetch(&vault, &document)?;

    boot::boot(&config, &Resolver)
}
