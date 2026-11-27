//! `galata-margin advise` refuses before it asks anything it should not
//! (task 7.1): a malformed request and a configuration with no `[margin]`
//! block on the binary itself; a session key that can move funds on the
//! library, against a local Derive.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpListener;
use std::process::Command;
use std::sync::{Arc, Mutex};

use galata_datawatch::adapters;
use galata_datawatch::adapters::derive_xyz::sign::SessionKey;
use galata_datawatch::config::{Adapters, Config, ConfigError, Secret, SecretSource};
use galata_margin::market::MarketReader;
use galata_margin::placement::RawRequest;
use rust_decimal::Decimal;

const SHIPPED: &str = include_str!("../../../config/datawatch.toml");

const MARGIN: &str = r#"
[margin]
session_key_var = "GALATA_LEDGER_DERIVE_SESSION_KEY"
agreement_tolerance = "0.001"
snapshot_age_secs = 900
split_pct = [25, 50, 75]
funding_hours = 168
venue_cap_pct = 100

[margin.accounts]
derive-xyz = ["owner_s3"]
hyperliquid = ["main"]
"#;

/// A test key, never anyone's.
const KEY: &str = "0x4c0883a69102937d6231471b5dbb6204fe5129617082792ae468d01a3f362318";

fn ledger(root: &std::path::Path) -> String {
    format!(
        r#"
[ledger]
root = "{root}/ledger"
tape = "{root}/ledger-tape"
snapshot_secs = 300
discover_secs = 600
events_secs = 300
fold_position_tolerance = 0.0
fold_relative_tolerance = 0.00002
ledger_share = 0.25
fingerprint_key_var = "GALATA_LEDGER_FINGERPRINT_KEY"

[ledger.account.main]
venue = "hyperliquid"
address_var = "GALATA_LEDGER_HL_MAIN"
dexes = ["", "xyz"]

[ledger.account.owner]
venue = "derive-xyz"
address_var = "GALATA_LEDGER_DERIVE_WALLET"
session_key_var = "GALATA_LEDGER_DERIVE_SESSION_KEY"
market = "mainnet"
"#,
        root = root.display()
    )
}

fn config_file(dir: &tempfile::TempDir, blocks: &[&str]) -> std::path::PathBuf {
    let path = dir.path().join("datawatch.toml");
    std::fs::write(&path, format!("{SHIPPED}\n{}", blocks.join("\n"))).unwrap();
    path
}

fn advise(config: &std::path::Path, extra: &[&str]) -> (i32, String) {
    let out = Command::new(env!("CARGO_BIN_EXE_galata-margin"))
        .args([
            "advise",
            "--option",
            "BTC-20261225-60000-P",
            "--size",
            "-1",
            "--price",
            "2100",
            "--hedge-size",
            "-0.47",
            "--derive",
            "owner_s3",
            "--hyperliquid",
            "main",
        ])
        .args(extra)
        .env_clear()
        .env("GALATA_CONFIG", config)
        .output()
        .unwrap();
    (
        out.status.code().unwrap(),
        String::from_utf8(out.stderr).unwrap(),
    )
}

#[test]
fn a_configuration_with_no_margin_block_is_refused_and_exits_non_zero() {
    let dir = tempfile::tempdir().unwrap();
    let (code, said) = advise(&config_file(&dir, &[]), &[]);
    assert_eq!(code, 2, "{said}");
    assert!(said.contains("[margin]"), "{said}");
}

#[test]
fn a_refused_request_exits_non_zero_saying_why() {
    let dir = tempfile::tempdir().unwrap();
    let config = config_file(&dir, &[MARGIN]);
    let (code, said) = advise(&config, &["--hedge", "ETH-PERP"]);
    assert_eq!(code, 2, "{said}");
    assert!(
        said.contains("perpetual of the option's underlying"),
        "{said}"
    );
    let (code, said) = advise(&config, &["--frobnicate", "1"]);
    assert_eq!(code, 2, "{said}");
    assert!(said.contains("unknown argument"), "{said}");
}

/// The configuration's venues, as the binary judges them.
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

/// Every secret the command reads, as test values.
struct Secrets;

impl SecretSource for Secrets {
    fn secret(&self, name: &str) -> Result<Secret, ConfigError> {
        Ok(Secret::new(match name {
            "GALATA_LEDGER_DERIVE_SESSION_KEY" => KEY,
            "GALATA_LEDGER_DERIVE_WALLET" => "0x00000000000000000000000000000000000000aa",
            _ => "0123456789abcdef0123456789abcdef",
        }))
    }
}

#[test]
fn a_session_key_that_can_move_funds_stops_advice_before_any_account_is_read() {
    let address = SessionKey::from_secret(&Secret::new(KEY))
        .unwrap()
        .address();
    let keys = serde_json::json!({"id": 1, "result": {"public_session_keys": [{
        "public_session_key": address, "protocol_scopes": ["trade:all", "transfer:all"],
        "offchain_scopes": ["account_info"], "label": "t", "ip_whitelist": [],
        "expiry_sec": 1_900_000_000_i64, "registered_sec": 1_700_000_000_i64, "subaccount_ids": []
    }]}})
    .to_string();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let asked = Arc::new(Mutex::new(Vec::new()));
    let log = asked.clone();
    std::thread::spawn(move || {
        for stream in listener.incoming().flatten() {
            let mut reader = BufReader::new(stream.try_clone().unwrap());
            let mut line = String::new();
            reader.read_line(&mut line).unwrap();
            log.lock().unwrap().push(
                line.split_whitespace()
                    .nth(1)
                    .unwrap_or_default()
                    .to_string(),
            );
            let mut length = 0;
            loop {
                let mut h = String::new();
                reader.read_line(&mut h).unwrap();
                if h == "\r\n" || h.is_empty() {
                    break;
                }
                if let Some(v) = h.to_ascii_lowercase().strip_prefix("content-length:") {
                    length = v.trim().parse().unwrap();
                }
            }
            let mut body = vec![0; length];
            reader.read_exact(&mut body).unwrap();
            let mut stream = stream;
            let _ = stream.write_all(format!("HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{keys}", keys.len()).as_bytes());
        }
    });

    let dir = tempfile::tempdir().unwrap();
    let config = Config::load_from(
        &config_file(&dir, &[&ledger(dir.path()), MARGIN]),
        &Resolver,
    )
    .unwrap();
    let request = RawRequest {
        option: "BTC-20261225-60000-P".into(),
        option_size: Decimal::from(-1),
        option_price: Decimal::from(2100),
        hedge: None,
        hedge_size: "-0.47".parse().unwrap(),
        derive_account: "owner_s3".into(),
        hyperliquid_account: "main".into(),
    };
    let clock = || 1_795_800_000_000_000_i64;
    let refused = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(galata_margin::command::advise(
            &config,
            &request,
            &Secrets,
            &clock,
            &MarketReader::at(
                "http://127.0.0.1:1/info",
                "http://127.0.0.1:1",
                "ws://127.0.0.1:1",
            ),
            Some(&url),
        ))
        .unwrap_err();
    assert!(refused.contains("transfer:all"), "{refused}");
    assert!(!refused.contains(&address), "{refused}");
    assert_eq!(
        *asked.lock().unwrap(),
        ["/private/session_keys"],
        "nothing but the key's scopes was asked"
    );
}
