//! Statistics derived from the tape, printed as JSON: the inspection tool for
//! `galata_datawatch::derive`, and the same numbers anything else would get
//! through the library.
//!
//! ```text
//!   galata-derive <venue> <horizon> --from <time> --to <time>
//!       --min-observations <n> --z <k> [--reference <ticker>]
//! ```
//!
//! `<horizon>` is spelled as a bar width (`30m`, `1h`); times are UTC,
//! `YYYY-MM-DD` or `YYYY-MM-DDTHH:MM`. **The floor and `--z` have no
//! defaults**: a statistic's floor is the reader's choice, and a default would
//! be the builder answering it for them.

use galata_datawatch::adapters;
use galata_datawatch::config::{Adapters, Config, FileSource};
use galata_datawatch::derive::Horizon;
use galata_datawatch::derive::tape::{from_tape, width_micros};

const PRINTED: u8 = 0;
const REFUSED: u8 = 1;
const BAD_ARGUMENT: u8 = 2;

const USAGE: &str = "usage: galata-derive <venue> <horizon> --from <time> --to <time> \
                     --min-observations <n> --z <k> [--reference <ticker>]\n\
                     times are UTC: YYYY-MM-DD or YYYY-MM-DDTHH:MM";

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

/// `YYYY-MM-DD` or `YYYY-MM-DDTHH:MM`, UTC, to microseconds.
fn time(spelled: &str) -> Option<i64> {
    let (date, clock) = spelled.split_once('T').unwrap_or((spelled, "00:00"));
    let (h, m) = clock.split_once(':')?;
    let (h, m): (i64, i64) = (h.parse().ok()?, m.parse().ok()?);
    if !(0..24).contains(&h) || !(0..60).contains(&m) {
        return None;
    }
    Some(galata_datawatch::calendar::midnight_of(date)? + (h * 3_600 + m * 60) * 1_000_000)
}

fn main() -> std::process::ExitCode {
    match run() {
        Ok(code) => std::process::ExitCode::from(code),
        Err(error) => {
            eprintln!("galata-derive: {error}");
            std::process::ExitCode::from(REFUSED)
        }
    }
}

fn run() -> Result<u8, Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let flag = |name: &str| {
        args.iter()
            .position(|a| a == name)
            .and_then(|i| args.get(i + 1))
            .cloned()
    };
    let (Some(venue), Some(spelled)) = (args.first(), args.get(1)) else {
        eprintln!("{USAGE}");
        return Ok(BAD_ARGUMENT);
    };
    let parsed = (
        width_micros(spelled),
        flag("--from").as_deref().and_then(time),
        flag("--to").as_deref().and_then(time),
        flag("--min-observations").and_then(|v| v.parse::<usize>().ok()),
        flag("--z").and_then(|v| v.parse::<f64>().ok()),
    );
    let (Some(width), Some(from), Some(to), Some(floor), Some(z)) = parsed else {
        eprintln!("{USAGE}");
        return Ok(BAD_ARGUMENT);
    };
    if to <= from {
        eprintln!("--to must be after --from\n{USAGE}");
        return Ok(BAD_ARGUMENT);
    }

    let config = Config::load(&FileSource::from_env("config/datawatch.toml")?, &Resolver)?;
    galata_segments::scannable(&config.paths.tape)?;
    let derived = from_tape(
        &config.paths.tape,
        venue,
        &Horizon {
            bucket_secs: width / 1_000_000,
            from_micros: from,
            to_micros: to,
            min_observations: floor,
            z,
            reference: flag("--reference"),
        },
    )?;
    println!("{}", serde_json::to_string_pretty(&derived)?);
    Ok(PRINTED)
}
