//! Rebuild a day of the tape from the archive.
//!
//! ```text
//!   galata-tape-rebuild <venue> <date>          one day
//!   galata-tape-rebuild <venue> <from> <to>     a half-open range of dates
//! ```
//!
//! **The exit code is the interface**, because this runs from a scheduler that
//! reads nothing else:
//!
//! ```text
//!   0   the tape was rebuilt
//!   1   something is broken
//!   2   the arguments are wrong
//!   3   there was nothing to do
//! ```
//!
//! `3` is separate from `0` on purpose. *Rebuilt nothing because the range is
//! empty* and *rebuilt a day* are different facts, and a scheduler that cannot
//! tell them apart cannot alert on the first.

use galata_datawatch::adapters::{self, AdapterConfig};
use galata_datawatch::calendar::midnight_of;
use galata_datawatch::config::{Adapters, Config, EnvSecrets, FileSource};
use galata_datawatch::tape;
use galata_segments::{Hold, Mode};

/// Done.
const DONE: i32 = 0;
/// Broken.
const BROKEN: i32 = 1;
/// The arguments are wrong.
const BAD_ARGUMENT: i32 = 2;
/// There was nothing to do.
const NOTHING: i32 = 3;

const MICROS_PER_DAY: i64 = 86_400_000_000;

/// How long to wait for a compaction or a deletion to release a root.
///
/// The longest hold measured is a compaction of the 4-hour soak's archive,
/// 64,587 segments in 9.44 s; a full day extrapolates to about a minute. Ten
/// minutes is ten times that, so a wait this long means a holder is stuck,
/// and the refusal after it names the root.
const PATIENCE: std::time::Duration = std::time::Duration::from_secs(600);

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
        Ok(code) => std::process::ExitCode::from(code as u8),
        Err(error) => {
            tracing::error!("{error}");
            std::process::ExitCode::from(BROKEN as u8)
        }
    }
}

fn run() -> Result<i32, Box<dyn std::error::Error>> {
    let mut args: Vec<String> = std::env::args().skip(1).collect();
    // **Explicit, like `galata-retain --delete`.** Removing what a previous run
    // wrote is the kind of thing that should require saying so.
    let replace = args.iter().any(|a| a == "--replace");
    args.retain(|a| a != "--replace");
    let (venue_name, from_date, to_date) = match args.as_slice() {
        [venue, date] => (venue.clone(), date.clone(), None),
        [venue, from, to] => (venue.clone(), from.clone(), Some(to.clone())),
        _ => {
            eprintln!(
                "usage: galata-tape-rebuild [--replace] <venue> <date>\n       \
                 galata-tape-rebuild [--replace] <venue> <from-date> <to-date>   (half-open)\n\n\
                 --replace removes this venue's segments from the partitions this run will write, \
                 BEFORE writing them, so this venue's rows there are absent for the duration of the \
                 rebuild. Other venues' segments in the same partitions are left alone. The tape is a cache and the \
                 archive is untouched, so the remedy for a crash in that window is to run it \
                 again.\n\n\
                 Without it, a re-run after the archive has grown leaves both copies and \
                 check_layout reports the overlap — which a scheduled retry will hit."
            );
            return Ok(BAD_ARGUMENT);
        }
    };

    let Some(from_micros) = midnight_of(&from_date) else {
        eprintln!("{from_date:?} is not a YYYY-MM-DD date");
        return Ok(BAD_ARGUMENT);
    };
    let to_micros = match &to_date {
        None => from_micros + MICROS_PER_DAY,
        Some(date) => match midnight_of(date) {
            Some(micros) => micros,
            None => {
                eprintln!("{date:?} is not a YYYY-MM-DD date");
                return Ok(BAD_ARGUMENT);
            }
        },
    };
    if to_micros <= from_micros {
        eprintln!("the range ends before it begins");
        return Ok(BAD_ARGUMENT);
    }

    // Through a source, so the two configuration variables are reconciled in
    // one place — and both set is a refusal rather than a precedence rule
    // somebody has to know.
    let config = Config::load(&FileSource::from_env("config/datawatch.toml")?, &Resolver)?;
    let Some(venue) = config.venue.get(&venue_name) else {
        eprintln!("{venue_name} is not a venue this configuration declares");
        return Ok(BAD_ARGUMENT);
    };

    // **No network.** The adapter is built for its `normalise`, which is a pure
    // function of the bytes — the whole reason that seam has no async on it.
    let adapter = adapters::build(AdapterConfig::from_declared(
        &venue_name,
        venue,
        &EnvSecrets,
    )?)?;
    let scope = format!("venue={venue_name}");

    // **A root that cannot be read is a refusal, not an empty result.** No
    // partitions means nothing to rebuild, which is indistinguishable from a
    // tidy store — so a mistyped path would look healthy for as long as
    // nobody checked.
    galata_segments::scannable(&config.paths.archive)?;

    // **Read shared, write exclusive, archive before tape — for the whole run.**
    // Shared on the archive so a compaction or a deletion cannot remove a
    // segment between the listing and the read, while other readers carry on;
    // exclusive on the tape because a replacement removes what another
    // rebuild may be writing beside. Waiting rather than refusing: what this
    // waits for is bounded, and a rebuild late is better than none.
    let _reading = hold_or_wait(&config.paths.archive, Mode::Shared)?;
    let _writing = hold_or_wait(&config.paths.tape, Mode::Exclusive)?;

    let report = tape::rebuild_with(
        &config.paths.archive,
        &config.paths.tape,
        adapter.as_ref(),
        Some(&[&scope]),
        from_micros,
        to_micros,
        if replace {
            tape::Replace::Partitions
        } else {
            tape::Replace::Never
        },
    )?;

    if report.is_empty() {
        tracing::info!(venue = venue_name, from = from_date, "nothing to rebuild");
        return Ok(NOTHING);
    }
    tracing::info!(venue = venue_name, from = from_date, "{}", report.report());

    // **Checked after writing, and it decides the exit code.** A rebuild that
    // wrote a tree `check_layout` complains about has not succeeded, whatever
    // the row count says — and the commonest complaint here is exactly the one
    // a partial rebuild causes: two segments claiming the same sequence range.
    let problems = tape::check_layout(&config.paths.tape);
    if !problems.is_empty() {
        for problem in &problems {
            tracing::error!("{problem}");
        }
        return Ok(BROKEN);
    }
    Ok(DONE)
}

/// Take a root's hold, saying so if another holder makes this wait.
fn hold_or_wait(root: &std::path::Path, mode: Mode) -> Result<Hold, galata_segments::SegmentError> {
    match galata_segments::wait(root, mode, std::time::Duration::ZERO) {
        Err(galata_segments::SegmentError::Held { .. }) => {
            tracing::info!(root = %root.display(), ?mode, "another holder has this root; waiting");
            galata_segments::wait(root, mode, PATIENCE)
        }
        taken => taken,
    }
}
