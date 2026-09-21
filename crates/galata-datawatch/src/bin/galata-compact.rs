//! Merge a partition's many small segments into one.
//!
//! ```text
//!   galata-compact              compact every closed partition
//!   galata-compact --report     say which are overdue, and compact nothing
//! ```
//!
//! **Never today.** A partition still being written to is not closed, and
//! compacting one would race the writer. **One at a time**: the run takes an
//! exclusive hold, so two compactions cannot each rewrite what the other is
//! reading.
//!
//! ```text
//!   0   done            1   broken
//!   2   bad argument    3   nothing to do
//! ```

use galata_datawatch::adapters;
use galata_datawatch::config::{Adapters, Config};
use galata_segments::{Codec, compact_closed, hold, overdue_closed};

const DONE: u8 = 0;
const BROKEN: u8 = 1;
const BAD_ARGUMENT: u8 = 2;
const NOTHING: u8 = 3;

/// How many segments a closed partition may hold before it is worth merging.
///
/// Not a tuning knob so much as a threshold below which the work costs more
/// than it saves: three hours of one venue is 21,439 segments across a handful
/// of partitions, so anything left in the tens is already compacted.
const OVERDUE_ABOVE: usize = 64;

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
    let mut report_only = false;
    for arg in &args {
        match arg.as_str() {
            "--report" => report_only = true,
            other => {
                eprintln!("unknown argument {other:?}\nusage: galata-compact [--report]");
                return Ok(BAD_ARGUMENT);
            }
        }
    }

    let path =
        std::env::var("GALATA_CONFIG").unwrap_or_else(|_| "config/datawatch.toml".to_string());
    let config = Config::load_from(std::path::Path::new(&path), &Resolver)?;

    // Today by OUR clock, and the store's own calendar — the same pair the
    // partition names were written with, so "closed" means the same thing to
    // both.
    let today =
        galata_datawatch::calendar::date_of(galata_datawatch::capture::SystemClock.now_micros());

    if report_only {
        let overdue = overdue_closed(&config.paths.archive, &today, OVERDUE_ABOVE);
        if overdue.is_empty() {
            tracing::info!(above = OVERDUE_ABOVE, "no closed partition is overdue");
            return Ok(NOTHING);
        }
        for (path, segments) in &overdue {
            tracing::info!(segments, path = %path.display(), "overdue");
        }
        tracing::info!(partitions = overdue.len(), "nothing was compacted");
        return Ok(DONE);
    }

    // **The hold, before anything is read.** Two compactions over one tree
    // would each rewrite what the other is reading.
    let _held = hold(&config.paths.archive)?;

    let compacted = compact_closed(&config.paths.archive, &today, Codec::Zstd)?;
    if compacted.segments_before == 0 {
        tracing::info!(today, "no closed partition needed compacting");
        return Ok(NOTHING);
    }
    tracing::info!(
        segments_before = compacted.segments_before,
        segments_after = compacted.segments_after,
        rows = compacted.rows,
        "compacted"
    );
    Ok(DONE)
}

use galata_datawatch::capture::Clock;
