//! `galata-quant-signals varcov --var <root> --config <signals.toml> [--now <ISO 8601>]`
//!
//! Computes `varcov`'s EWMA horizons from the tape under `<root>/tape` and
//! commits them there (`add-galata-quant`). The one place this crate reads
//! the clock; `--now` sets it.
//!
//! ```text
//!   0   wrote rows       1   broken
//!   2   bad argument     3   nothing to do: every horizon nothing new or skipped
//! ```

use std::path::PathBuf;
use std::process::ExitCode;

use galata_signals::{CODE, bars, declared, store, varcov};

const DONE: u8 = 0;
const BROKEN: u8 = 1;
const BAD_ARGUMENT: u8 = 2;
const NOTHING: u8 = 3;

fn usage() -> ExitCode {
    eprintln!(
        "usage: galata-quant-signals varcov --var <root> --config <signals.toml> [--now <ISO 8601 with a zone>]"
    );
    ExitCode::from(BAD_ARGUMENT)
}

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.first().map(String::as_str) != Some("varcov") {
        return usage();
    }
    let (mut var, mut config, mut now) = (None, None, None);
    let mut it = args[1..].iter();
    while let Some(flag) = it.next() {
        let value = it.next();
        match (flag.as_str(), value) {
            ("--var", Some(v)) => var = Some(PathBuf::from(v)),
            ("--config", Some(v)) => config = Some(PathBuf::from(v)),
            ("--now", Some(v)) => now = Some(v.clone()),
            _ => return usage(),
        }
    }
    let (Some(var), Some(config)) = (var, config) else {
        return usage();
    };
    let computed_micros = match now {
        Some(text) => match text.parse::<jiff::Timestamp>() {
            Ok(t) => t.as_microsecond(),
            Err(e) => {
                eprintln!("--now {text}: {e} (give it a zone)");
                return ExitCode::from(BAD_ARGUMENT);
            }
        },
        // The clock, read once, here.
        None => jiff::Timestamp::now().as_microsecond(),
    };
    let tape = var.join("tape");
    if !tape.is_dir() {
        eprintln!("{} holds no tape/", var.display());
        return ExitCode::from(BAD_ARGUMENT);
    }
    let text = match std::fs::read_to_string(&config) {
        Ok(t) => t,
        Err(e) => {
            eprintln!("{}: {e}", config.display());
            return ExitCode::from(BAD_ARGUMENT);
        }
    };
    let declared = match declared::read(&text) {
        Ok(d) => d,
        Err(e) => {
            eprintln!("{e}");
            return ExitCode::from(BAD_ARGUMENT);
        }
    };
    match run(&tape, &declared, computed_micros) {
        Ok(true) => ExitCode::from(DONE),
        Ok(false) => ExitCode::from(NOTHING),
        Err(e) => {
            eprintln!("galata-quant-signals varcov: {e}");
            ExitCode::from(BROKEN)
        }
    }
}

fn run(
    tape: &std::path::Path,
    declared: &declared::Declared,
    computed_micros: i64,
) -> Result<bool, String> {
    for name in &declared.skipped {
        println!("{name}: skipped, a fitted horizon (slice 2 of galata-quant)");
    }
    let run = varcov::Run {
        computed_micros,
        code: CODE.to_string(),
    };
    let mut rows = Vec::new();
    {
        // Held shared while reading, so a projection in progress is waited out.
        let _hold = galata_segments::wait(tape, galata_segments::Mode::Shared, store::PATIENCE)
            .map_err(|e| format!("the tape is held: {e}"))?;
        let stored = store::stored_asof(tape)?;
        let mut sources: std::collections::BTreeMap<
            String,
            std::collections::BTreeMap<String, Vec<bars::Bar>>,
        > = Default::default();
        for hz in &declared.ewma {
            if !sources.contains_key(&hz.bars) {
                let width = declared::width(&hz.bars).expect("declared::read checked it");
                sources.insert(hz.bars.clone(), bars::candles(tape, &hz.bars, width)?);
            }
            let fine = &sources[&hz.bars];
            let (fw, cw) = (
                declared::width(&hz.bars).expect("checked"),
                declared::width(&hz.name).expect("checked"),
            );
            let built = if fw == cw {
                fine.clone()
            } else {
                fine.iter()
                    .map(|(t, b)| (t.clone(), bars::build(b, fw, cw)))
                    .filter(|(_, b)| !b.is_empty())
                    .collect()
            };
            let outcome = varcov::compute(
                hz,
                &bars::log_returns(&built),
                stored.get(&hz.name).copied(),
                &run,
            );
            println!("{}", outcome.said);
            rows.extend(outcome.rows);
        }
    }
    if rows.is_empty() {
        return Ok(false);
    }
    match store::commit(
        tape,
        rows,
        computed_micros,
        &run.run_id(),
        &run.code,
        store::PATIENCE,
    )? {
        store::Committed::Written { rows, segments } => {
            println!("committed {rows} rows in {} segment(s)", segments.len());
            Ok(true)
        }
        store::Committed::Nothing => {
            println!("nothing to commit: another run stored these horizons meanwhile");
            Ok(false)
        }
    }
}
