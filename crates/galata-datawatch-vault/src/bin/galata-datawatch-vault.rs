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
//!
//! **The password comes from the vault too**, which until 2026-09-23 it did
//! not: the document was fetched from the vault and the broker password was
//! then read from the process environment, because `boot` named `EnvSecrets`
//! itself. A deployment that moved its configuration into a vault to stop
//! holding it on disk went on holding its password where `ps e` and every
//! child process can read it.
//!
//! A `config`-scoped token cannot serve both — the vault gives that scope's
//! bundle no field for the vault key, so it cannot decrypt a secret at all —
//! and the refusal says so in the vault's own words. A config-only deployment
//! therefore uses `config`; a broker-backed deployment uses one `read` token,
//! preferably with a secret allow-list. A child vault remains the answer when
//! credentials need cryptographic isolation. One process opens one vault; two
//! role-specific tokens are not a supported mode.

use galata_datawatch::adapters;
use galata_datawatch::boot;
use galata_datawatch::config::Adapters;
use galata_datawatch::config::source::document_from_env;
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

    // Fetched once, before anything connects to a venue.
    let vault = Vault::from_env()?;
    let config = VaultConfig::fetch(&vault, &document)?;

    // **The vault is boot's argument, and still not the loop's value.** It
    // used to say "nothing below holds the vault"; that was true of a call
    // taking only a document, and is now too strong — `boot` borrows this for
    // as long as it runs. What has not changed is the part that matters:
    // `boot` reads the password once, before a socket is opened, and hands the
    // loop a `Sink`. No value the capture loop holds carries a vault, so the
    // rule *fetch at boot, never on the capture path* is still structural
    // rather than remembered.
    boot::boot(&config, &VaultSecrets::new(&vault), &Resolver)
}
