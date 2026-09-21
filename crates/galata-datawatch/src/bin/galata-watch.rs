//! Judge the record.
//!
//! ```text
//!   galata-watch
//! ```
//!
//! **Watches the record, not the scheduler.** A missed run shows up as a
//! partition that did not get compacted, which is a fact on disk; watching the
//! scheduler as well would be two witnesses to one event and an argument about
//! which to believe.
//!
//! **Reads and reports. Stops nothing, restarts nothing, deletes nothing.**
//!
//! ```text
//!   0   nothing to report      1   findings
//!   2   bad argument           3   nothing to check
//! ```
//!
//! `3` is separate from `0` for the reason it is in `galata-retain`: *checked
//! and found nothing* and *had nothing to check* are different facts, and the
//! second is what an empty archive looks like when capture has silently
//! stopped.

use galata_datawatch::adapters;
use galata_datawatch::config::{Adapters, Config, FileSource};
use galata_datawatch::watch::{self, Thresholds};

const CLEAN: u8 = 0;
const FINDINGS: u8 = 1;
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
            std::process::ExitCode::from(FINDINGS)
        }
    }
}

fn run() -> Result<u8, Box<dyn std::error::Error>> {
    if std::env::args().count() > 1 {
        eprintln!("usage: galata-watch\n\nIt reads and reports. It stops nothing.");
        return Ok(BAD_ARGUMENT);
    }

    let config = Config::load(&FileSource::from_env("config/datawatch.toml")?, &Resolver)?;
    let thresholds = Thresholds {
        max_segments_in_closed_partition: config.watch.max_segments_in_closed_partition,
        max_record_age_secs: config.watch.max_record_age_secs,
    };

    if thresholds.is_empty() {
        tracing::info!(
            "no [watch] thresholds are declared, so only structural problems are checked. \
             Numbers are the measurer's and bounds are the operator's"
        );
    }

    // Today by OUR clock and the store's own calendar — the same pair the
    // partitions were named with, so "closed" means the same thing to both.
    let now = galata_datawatch::capture::SystemClock.now_micros();
    let today = galata_datawatch::calendar::date_of(now);

    // **A root that cannot be read is a refusal, not an empty result.** No
    // partitions means nothing to report, which is indistinguishable from a
    // tidy store — so a mistyped path would look healthy for as long as
    // nobody checked.
    galata_segments::scannable(&config.paths.archive)?;
    galata_segments::scannable(&config.paths.tape)?;

    let report = watch::watch(
        &config.paths.archive,
        &config.paths.tape,
        &today,
        &thresholds,
        now,
    );

    for finding in &report.findings {
        // A finding, not an alert: the number, the bound and the path.
        tracing::warn!("{finding}");
    }

    if report.had_nothing() {
        tracing::info!(
            root = %config.paths.archive.display(),
            "nothing to check — the archive holds no partitions. An empty archive is what \
             capture having silently stopped looks like"
        );
        return Ok(NOTHING);
    }
    if report.is_clean() {
        tracing::info!(partitions = report.checked, "nothing to report");
        return Ok(CLEAN);
    }
    tracing::warn!(
        findings = report.findings.len(),
        partitions = report.checked,
        "findings"
    );
    Ok(FINDINGS)
}

use galata_datawatch::capture::Clock;
