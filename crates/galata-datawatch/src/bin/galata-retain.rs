//! What a declared horizon has expired — and, only if you say so, its removal.
//!
//! ```text
//!   galata-retain              report what would be removed
//!   galata-retain --delete     remove it
//! ```
//!
//! **There is no `--dry-run`; there is `--delete`.** The difference is which
//! way round the mistake goes: forgetting a flag that *protects* is a deletion,
//! forgetting one that *destroys* is a report. Only one of those is
//! recoverable.
//!
//! ```text
//!   0   done            1   broken
//!   2   bad argument    3   nothing to do
//! ```

use galata_datawatch::adapters;
use galata_datawatch::config::{Adapters, Config, FileSource};
use galata_datawatch::retain;

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
    let mut delete = false;
    for arg in &args {
        match arg.as_str() {
            "--delete" => delete = true,
            other => {
                eprintln!(
                    "unknown argument {other:?}\nusage: galata-retain [--delete]\n\nWithout \
                     --delete it reports and removes nothing."
                );
                return Ok(BAD_ARGUMENT);
            }
        }
    }

    // Through a source, so the two configuration variables are reconciled in
    // one place — and both set is a refusal rather than a precedence rule
    // somebody has to know.
    let config = Config::load(&FileSource::from_env("config/datawatch.toml")?, &Resolver)?;
    let policy = config.retention.policy();

    if policy.is_empty() {
        // Not an error, and not a success either. Nobody declared a horizon,
        // so there is nothing to enact.
        tracing::info!(
            "no retention is declared, so nothing expires. Numbers are the measurer's and \
             horizons are the operator's — add a [retention] block to declare one"
        );
        return Ok(NOTHING);
    }

    let now = galata_datawatch::capture::SystemClock.now_micros();
    // **A root that cannot be read is a refusal, not an empty sweep.**
    //
    // Every listing answers an unreadable directory with nothing, which is
    // right for a subtree and wrong for a declared root: no partitions means
    // no candidates, which this reports as "nothing to do" and exits 3. A
    // mistyped path then looks exactly like a tidy store.
    galata_segments::scannable(&config.paths.archive)?;
    galata_segments::scannable(&config.paths.tape)?;

    // **Deleting holds both stores, before it lists what to delete.** It
    // removes whole partitions, so a rebuild or a compaction beside it would
    // read a directory that vanishes under it. Refused rather than waited
    // for: a deletion is not urgent, and a refusal names who was in the way.
    // Archive before tape, the order every holder of both takes. The report
    // removes nothing and takes nothing.
    let _held = if delete {
        Some((
            galata_segments::hold(&config.paths.archive)?,
            galata_segments::hold(&config.paths.tape)?,
        ))
    } else {
        None
    };

    let swept = retain::sweep(&config.paths.archive, &config.paths.tape, &policy, now);

    for path in &swept.unclassified {
        // Reported, never selected. Somebody put this here.
        tracing::warn!(path = %path.display(), "unclassified, and left alone");
    }
    for candidate in &swept.candidates {
        tracing::info!(
            family = candidate.family.as_str(),
            date = candidate.date,
            mb = format!("{:.1}", candidate.bytes as f64 / 1_048_576.0),
            path = %candidate.path.display(),
            "expired"
        );
    }

    if swept.is_empty() {
        tracing::info!("{}", swept.report());
        return Ok(NOTHING);
    }

    if !delete {
        tracing::info!(
            "{} — nothing was removed. Pass --delete to remove it",
            swept.report()
        );
        return Ok(DONE);
    }

    let mut removed = 0usize;
    let mut failed = 0usize;
    for candidate in &swept.candidates {
        match std::fs::remove_dir_all(&candidate.path) {
            Ok(()) => removed += 1,
            Err(error) => {
                // One partition that will not go is not a reason to stop: the
                // rest are still expired, and stopping would leave the decision
                // half-enacted with no record of where.
                tracing::error!(path = %candidate.path.display(), %error, "could not remove");
                failed += 1;
            }
        }
    }
    tracing::info!(removed, failed, "{}", swept.report());
    Ok(if failed > 0 { BROKEN } else { DONE })
}

use galata_datawatch::capture::Clock;
