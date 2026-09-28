//! Commit one run of computed signals to the tape.
//!
//! ```text
//!   galata-signals-commit <file.arrow>
//! ```
//!
//! **The tape's one writer of `kind=signals`.** `py/signals` computes — the
//! arithmetic is galata-research's — and hands its rows over as an Arrow IPC
//! file; this checks them against [`signals::schema`] and writes them through
//! [`signals::write`], so the dataset's contract (segment names, labels, the
//! rename that commits) is written once, in Rust, and no language
//! reimplements it.
//!
//! **Refused, and nothing written**, when the fields are not the schema's
//! (name, type and nullability: a LargeUtf8 from polars is a refusal, not a
//! cast), or when the rows do not share one `computed_micros`, `run_id` and
//! `code` — one file is one run. On success the input is removed: it was a
//! hand-off, and the tape now holds it.
//!
//! ```text
//!   0   committed       1   broken
//!   2   bad argument    3   nothing to do (the file holds no rows)
//! ```

use galata_datawatch::adapters;
use galata_datawatch::config::{Adapters, Config, FileSource};
use galata_datawatch::signals;

const DONE: u8 = 0;
const BROKEN: u8 = 1;
const BAD_ARGUMENT: u8 = 2;
const NOTHING: u8 = 3;

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
    if let Some(code) = galata_datawatch::config::check_requested(&Resolver) {
        return code;
    }
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();
    match run() {
        Ok(code) => std::process::ExitCode::from(code),
        Err(error) => {
            tracing::error!("{error}");
            std::process::ExitCode::from(BROKEN)
        }
    }
}

fn run() -> Result<u8, Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let [file] = args.as_slice() else {
        eprintln!("usage: galata-signals-commit <file.arrow>");
        return Ok(BAD_ARGUMENT);
    };
    let config = Config::load(&FileSource::from_env("config/datawatch.toml")?, &Resolver)?;
    // **A root that cannot be read is a refusal**, as for every tool that
    // reads a declared store: a mistyped tape would otherwise be created
    // afresh beside the real one, and every signal written into it.
    galata_segments::scannable(&config.paths.tape)?;
    match signals::commit(&config.paths.tape, std::path::Path::new(file)) {
        Ok(done) => {
            tracing::info!(
                rows = done.rows,
                segments = done.segments.len(),
                run_id = done.run_id,
                "committed"
            );
            Ok(DONE)
        }
        Err(signals::SignalError::Empty { path }) => {
            tracing::info!("{} holds no rows", path.display());
            Ok(NOTHING)
        }
        Err(signals::SignalError::Segment(error)) => Err(error.into()),
        Err(refusal) => {
            tracing::error!("{refusal}; nothing was written");
            Ok(BAD_ARGUMENT)
        }
    }
}
