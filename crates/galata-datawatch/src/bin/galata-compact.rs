//! Merge a partition's many small segments into one.
//!
//! ```text
//!   galata-compact                  compact every closed partition
//!   galata-compact --closed-hours   and today's closed hours
//!   galata-compact --report         say which are overdue, and compact nothing
//! ```
//!
//! **Never the open hour.** Without `--closed-hours`, never today: a partition
//! still being written to is not closed. With it, today's hours that ended at
//! least [`GRACE_MICROS`] ago are, because capture only ever writes new
//! segments at its own receipt clock's now (`compact-closed-hours`). **One at a time**: the run takes an
//! exclusive hold, so two compactions cannot each rewrite what the other is
//! reading.
//!
//! ```text
//!   0   done            1   broken
//!   2   bad argument    3   nothing to do
//! ```

use galata_datawatch::adapters;
use galata_datawatch::config::{Adapters, Config, FileSource};
use galata_segments::{
    Codec, HOUR_MICROS, compact_closed, compact_closed_hours, hold, overdue_closed,
};

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

/// How long after an hour ends before it counts as closed: 150 two-second
/// flushes, so the writer's last flush of the hour is on disk long before.
const GRACE_MICROS: i64 = 5 * 60 * 1_000_000;

/// The start of the latest hour that ended at least [`GRACE_MICROS`] before
/// `now`: every segment ending before it is in a closed hour.
fn closed_hours_cutoff(now_micros: i64) -> i64 {
    (now_micros - GRACE_MICROS).div_euclid(HOUR_MICROS) * HOUR_MICROS
}

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
    // Before anything else: `--check-config` judges a document and exits.
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
    let mut report_only = false;
    let mut closed_hours = false;
    for arg in &args {
        match arg.as_str() {
            "--report" => report_only = true,
            "--closed-hours" => closed_hours = true,
            other => {
                eprintln!(
                    "unknown argument {other:?}\nusage: galata-compact [--closed-hours] [--report]"
                );
                return Ok(BAD_ARGUMENT);
            }
        }
    }

    // Through a source, so the two configuration variables are reconciled in
    // one place — and both set is a refusal rather than a precedence rule
    // somebody has to know.
    let config = Config::load(&FileSource::from_env("config/datawatch.toml")?, &Resolver)?;

    // Today by OUR clock, and the store's own calendar — the same pair the
    // partition names were written with, so "closed" means the same thing to
    // both.
    let now = galata_datawatch::capture::SystemClock.now_micros();
    let today = galata_datawatch::calendar::date_of(now);

    // **A root that cannot be read is a refusal, and this must come before
    // `hold`.**
    //
    // Every listing answers an unreadable directory with nothing, which is
    // right for a subtree and wrong for a declared root: no partitions means
    // nothing overdue, reported as "nothing to do" with exit 3.
    //
    // Worse here than elsewhere: `hold` creates the directory it locks, so a
    // mistyped path was CREATED — after which it is a real, empty, perfectly
    // scannable store, and every later run agrees there is nothing to do.
    galata_segments::scannable(&config.paths.archive)?;

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

    let mut compacted = compact_closed(&config.paths.archive, &today, Codec::Zstd)?;
    if closed_hours {
        let cutoff = closed_hours_cutoff(now);
        let hours = compact_closed_hours(&config.paths.archive, &today, cutoff, Codec::Zstd)?;
        tracing::info!(
            before = cutoff,
            segments_before = hours.segments_before,
            segments_after = hours.segments_after,
            "today's closed hours"
        );
        compacted.segments_before += hours.segments_before;
        compacted.segments_after += hours.segments_after;
        compacted.rows += hours.rows;
    }
    if compacted.segments_before == 0 {
        tracing::info!(today, closed_hours, "nothing needed compacting");
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

#[cfg(test)]
mod tests {
    use super::*;

    const H: i64 = HOUR_MICROS;

    #[test]
    fn the_cutoff_is_the_latest_hour_ended_five_minutes_ago() {
        // At 13:20 the hour 12:00–13:00 ended twenty minutes ago: closed.
        assert_eq!(closed_hours_cutoff(13 * H + 20 * 60_000_000), 13 * H);
        // At 13:04 it ended four minutes ago: not yet.
        assert_eq!(closed_hours_cutoff(13 * H + 4 * 60_000_000), 12 * H);
        // At exactly 13:05 it has.
        assert_eq!(closed_hours_cutoff(13 * H + GRACE_MICROS), 13 * H);
    }
}
