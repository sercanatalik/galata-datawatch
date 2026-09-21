//! The capture binary: a file on disk, and the wiring the library holds.
//!
//! **Every rule this depends on lives in the library**, including the wiring
//! itself. What is here is the choice of configuration source — a file — and
//! the exit code.

use galata_datawatch::adapters;
use galata_datawatch::boot;
use galata_datawatch::config::{Adapters, FileSource};

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

fn run() -> Result<(), Box<dyn std::error::Error>> {
    // The one thing this binary decides. Naming two sources at once is a
    // refusal rather than a precedence rule, and `FileSource::from_env` is
    // where that refusal lives.
    boot::boot(&FileSource::from_env("config/datawatch.toml")?, &Resolver)
}

fn main() -> std::process::ExitCode {
    match run() {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(error) => {
            // **The whole chain, not just the top.** `main` returning a
            // `Result` prints the error's `Debug` and nothing under it, which
            // was tolerable while the top line carried a URL and stopped being
            // so when redaction took it out: `error sending request` alone does
            // not distinguish DNS from TLS from refused. The cause is in the
            // source chain, and this is what prints it.
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
