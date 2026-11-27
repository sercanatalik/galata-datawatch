//! `galata-margin advise`: where to place an option on Derive and its
//! perpetual hedge, and what each placement costs against the owner's book
//! (`add-cross-venue-margin-optimiser`). It advises and does nothing else.
//!
//! **This binary is galata-margin's clock**: the library reads none, and is
//! handed this one (`scripts/check-clock-discipline.sh`).
//!
//! Exit 0 when advice was produced, a proposal or none; 2 when the request is
//! refused or the command cannot start, with the reason on standard error.

use std::process::ExitCode;
use std::str::FromStr;

use galata_datawatch::adapters;
use galata_datawatch::config::{Adapters, Config, EnvSecrets, FileSource};
use galata_margin::market::MarketReader;
use galata_margin::placement::RawRequest;
use rust_decimal::Decimal;

const USAGE: &str = "usage: galata-margin advise --option <BTC-20261225-60000-P> --size <signed> --price <p> \
--hedge-size <signed> --derive <subaccount alias> --hyperliquid <account alias> [--hedge <BTC-PERP>] [--json]";

/// The configuration's venues, judged by the adapters this build carries.
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
    fn max_instruments(&self, venue: &str) -> Option<u32> {
        adapters::max_instruments(venue)
    }
    fn instrument_problem(&self, venue: &str, ticker: &str) -> Option<String> {
        adapters::instrument_problem(venue, ticker)
    }
}

/// The request and the output form, from the arguments after `advise`.
fn parse(args: &[String]) -> Result<(RawRequest, bool), String> {
    let flag = |name: &str| -> Option<String> {
        args.iter()
            .position(|a| a == name)
            .and_then(|i| args.get(i + 1))
            .cloned()
    };
    let need =
        |value: Option<String>, name: &str| value.ok_or_else(|| format!("{name} is required"));
    let decimal = |value: String, name: &str| {
        Decimal::from_str(&value).map_err(|_| format!("{name} must be a decimal: {value:?}"))
    };
    let known = [
        "--option",
        "--size",
        "--price",
        "--hedge-size",
        "--derive",
        "--hyperliquid",
        "--hedge",
        "--json",
    ];
    let mut i = 0;
    while i < args.len() {
        let a = &args[i];
        if !known.contains(&a.as_str()) {
            return Err(format!("unknown argument: {a}"));
        }
        i += if a == "--json" { 1 } else { 2 };
    }
    let request = RawRequest {
        option: need(flag("--option"), "--option")?,
        option_size: decimal(need(flag("--size"), "--size")?, "--size")?,
        option_price: decimal(need(flag("--price"), "--price")?, "--price")?,
        hedge: flag("--hedge"),
        hedge_size: decimal(need(flag("--hedge-size"), "--hedge-size")?, "--hedge-size")?,
        derive_account: need(flag("--derive"), "--derive")?,
        hyperliquid_account: need(flag("--hyperliquid"), "--hyperliquid")?,
    };
    Ok((request, args.iter().any(|a| a == "--json")))
}

fn now_micros() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_micros() as i64)
        .unwrap_or_default()
}

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let (request, json) = match args.split_first() {
        Some((command, rest)) if command == "advise" => match parse(rest) {
            Ok(parsed) => parsed,
            Err(reason) => {
                eprintln!("galata-margin: {reason}\n{USAGE}");
                return ExitCode::from(2);
            }
        },
        Some((flag, _)) if flag == "--help" || flag == "-h" => {
            println!("{USAGE}");
            return ExitCode::SUCCESS;
        }
        _ => {
            eprintln!("{USAGE}");
            return ExitCode::from(2);
        }
    };
    let config = match FileSource::from_env("config/datawatch.toml")
        .and_then(|s| Config::load(&s, &Resolver))
    {
        Ok(config) => config,
        Err(e) => {
            eprintln!("galata-margin: the configuration could not be loaded: {e}");
            return ExitCode::from(2);
        }
    };
    let runtime = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(e) => {
            eprintln!("galata-margin: no runtime: {e}");
            return ExitCode::from(2);
        }
    };
    let reader = MarketReader::mainnet();
    let advised = runtime.block_on(galata_margin::command::advise(
        &config,
        &request,
        &EnvSecrets,
        &now_micros,
        &reader,
        None,
    ));
    match advised {
        Ok(a) => {
            if json {
                println!(
                    "{}",
                    galata_margin::render::machine(&a.request, &a.advice, a.asked_at_micros)
                );
            } else {
                print!(
                    "{}",
                    galata_margin::render::human(&a.request, &a.advice, a.asked_at_micros)
                );
            }
            ExitCode::SUCCESS
        }
        Err(reason) => {
            eprintln!("galata-margin: refused: {reason}");
            ExitCode::from(2)
        }
    }
}
