//! `account-data-access`, against a real `nats-server` (the broker's own test
//! harness, shared by path), through galata-markets' own router
//! (`add-markets-accounts`, tasks 3.2 to 3.4).
//!
//! The ledger is played by the harness's `test-ledger`, which may publish
//! under `accounts.`; galata-markets reads as itself, on its own inbox.

#[path = "../../galata-broker/tests/support/mod.rs"]
mod support;

use std::collections::BTreeMap;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use futures_util::StreamExt;
use galata_broker::{BUS_ADMIN, BUS_ADMIN_INBOX, GALATA_MARKETS, GALATA_MARKETS_INBOX, streams};
use galata_datawatch::config::{ConfigError, Secret, SecretSource};
use galata_datawatch::ledger::project;
use galata_markets::access::token_var;
use galata_markets::{Markets, app, routes};
use galata_wire::{
    Account, AccountMode, Envelope, Event, Fill, Kind, Margin, Num, Side, StateSet, Venue,
};
use serde_json::Value;
use support::{Broker, TEST_LEDGER, connect, connect_with};
use tower::ServiceExt;

/// Stand-in tokens, made for the test and printed nowhere.
struct Stand;

impl SecretSource for Stand {
    fn secret(&self, name: &str) -> Result<Secret, ConfigError> {
        Ok(Secret::new(format!("stand-in-{name}")))
    }
}

/// The watcher's token: the first client granted account data.
fn bearer() -> String {
    format!("Bearer stand-in-{}", token_var("watcher"))
}

fn n(s: &str) -> Num {
    Num::from_str(s).unwrap()
}

fn on(venue: &str, alias: &str, recv: i64, event: Event) -> Envelope {
    Envelope::for_account(
        Venue::new(venue).unwrap(),
        Account::new(alias).unwrap(),
        Some(recv),
        recv,
        event,
    )
}

fn fill(tid: u64, recv: i64) -> Envelope {
    let mut envelope = on(
        "hyperliquid",
        "main",
        recv,
        Event::Fill(Fill {
            dex: None,
            market: "BTC".into(),
            side: Side::Bid,
            price: n("81213.5"),
            size: n("0.01"),
            start_position: None,
            direction: Some("Open Long".into()),
            closed_pnl: None,
            fee: Some(n("0.3")),
            fee_token: Some("USDC".into()),
            builder_fee: None,
            crossed: Some(true),
            order_id: (1000 + tid).to_string(),
            trade_id: tid.to_string(),
            twap_id: None,
            liquidated: None,
        }),
    );
    // The venue's time: fills in trade-id order.
    envelope.at_micros = Some(1_000 + tid as i64);
    envelope
}

fn margin(value: &str) -> Event {
    Event::Margin(Margin {
        dex: None,
        mode: AccountMode::PortfolioMargin,
        equity_not_held: None,
        account_value: Some(n(value)),
        total_notional: Some(n("0")),
        total_raw_usd: None,
        margin_used: Some(n("0")),
        maintenance_margin_used: Some(n("0")),
        withdrawable: None,
    })
}

fn set(venue: &str, alias: &str, recv: i64, kind: Kind, rows: Vec<Event>) -> Envelope {
    on(
        venue,
        alias,
        recv,
        Event::StateSet(StateSet::of(kind, rows).unwrap()),
    )
}

struct Harness {
    _broker: Broker,
    _ledger_tape: tempfile::TempDir,
    _tape: tempfile::TempDir,
    admin: async_nats::Client,
    ledger: async_nats::jetstream::Context,
    root: std::path::PathBuf,
    markets: Arc<Markets>,
}

impl Harness {
    async fn start() -> Harness {
        let broker = Broker::start();
        let admin = connect_with(
            &broker,
            BUS_ADMIN,
            async_nats::ConnectOptions::new().custom_inbox_prefix(BUS_ADMIN_INBOX),
        )
        .await;
        streams::apply(&admin).await;
        let ledger = async_nats::jetstream::new(connect(&broker, TEST_LEDGER).await);
        let bus = connect_with(
            &broker,
            GALATA_MARKETS,
            async_nats::ConnectOptions::new().custom_inbox_prefix(GALATA_MARKETS_INBOX),
        )
        .await;
        // The ledger's projection: owner-only, as the ledger holds it.
        let ledger_tape = tempfile::tempdir().unwrap();
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(ledger_tape.path(), std::fs::Permissions::from_mode(0o700))
                .unwrap();
        }
        let tape = tempfile::tempdir().unwrap();
        let mut markets = Markets::over(tape.path().to_path_buf());
        markets.bus = Some(bus);
        markets.ledger_tape = Some(ledger_tape.path().to_path_buf());
        Harness {
            root: ledger_tape.path().to_path_buf(),
            _broker: broker,
            _ledger_tape: ledger_tape,
            _tape: tape,
            admin,
            ledger,
            markets: Arc::new(markets),
        }
    }

    /// The ledger publishing one envelope, acknowledged: stored.
    async fn publish(&self, envelope: &Envelope) {
        let alias = envelope.account().unwrap().as_str().to_string();
        let venue = match &envelope.address {
            galata_wire::Address::Account { venue, .. } => venue.as_str().to_string(),
            _ => unreachable!(),
        };
        let subject = format!("accounts.{venue}.{alias}.{}", envelope.kind().as_str());
        self.ledger
            .publish(subject, galata_broker::encode(envelope).unwrap().into())
            .await
            .unwrap()
            .await
            .expect("stored");
    }

    async fn get(&self, path: &str, last: Option<u64>) -> (StatusCode, Body) {
        let app = app(
            galata_markets::access::Tokens::from_source(&Stand).unwrap(),
            routes(self.markets.clone()),
        );
        let mut request = Request::get(path).header("Authorization", bearer());
        if let Some(last) = last {
            request = request.header("Last-Event-ID", last.to_string());
        }
        let response = app
            .oneshot(request.body(Body::empty()).unwrap())
            .await
            .unwrap();
        (response.status(), response.into_body())
    }

    async fn json(&self, path: &str) -> Value {
        let (status, body) = self.get(path, None).await;
        let bytes = axum::body::to_bytes(body, 1 << 24).await.unwrap();
        let text = String::from_utf8(bytes.to_vec()).unwrap();
        assert_eq!(status, StatusCode::OK, "{path}: {text}");
        serde_json::from_str(&text).unwrap()
    }
}

/// One event as a client reads it: its id and the fill's trade id.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Read {
    id: u64,
    trade: String,
}

/// The next `n` events, within five seconds.
async fn read(body: &mut Body, n: usize, carried: &mut String) -> Vec<Read> {
    let mut out = Vec::new();
    let mut frames = std::mem::take(body).into_data_stream();
    while out.len() < n {
        while let Some(end) = carried.find("\n\n") {
            let block: String = carried.drain(..end + 2).collect();
            let fields: BTreeMap<&str, &str> = block
                .lines()
                .filter_map(|l| l.split_once(':'))
                .map(|(k, v)| (k, v.trim_start()))
                .collect();
            let Some(data) = fields.get("data") else {
                continue;
            };
            let envelope: Value = serde_json::from_str(data).unwrap();
            out.push(Read {
                id: fields["id"].parse().unwrap(),
                trade: envelope["event"]["Fill"]["trade_id"]
                    .as_str()
                    .unwrap_or_default()
                    .to_string(),
            });
            if out.len() == n {
                break;
            }
        }
        if out.len() == n {
            break;
        }
        let frame = tokio::time::timeout(Duration::from_secs(5), frames.next())
            .await
            .expect("an event within 5 s")
            .expect("the stream is open")
            .unwrap();
        carried.push_str(std::str::from_utf8(&frame).unwrap());
    }
    *body = Body::from_stream(frames);
    out
}

fn trades(rows: &Value) -> Vec<String> {
    rows.as_array()
        .unwrap()
        .iter()
        .map(|r| r["trade_id"].as_str().unwrap().to_string())
        .collect()
}

#[tokio::test]
async fn now_is_each_kinds_newest_set_from_the_stream_else_the_projection() {
    let h = Harness::start().await;
    // The projection: margin read at 100, and positions held then.
    project::write(
        &h.root,
        "hyperliquid",
        "main",
        &[on("hyperliquid", "main", 100, margin("10"))],
    )
    .unwrap();
    // The stream: an older positions set, then the newest, which holds none —
    // the account is flat.
    h.publish(&set(
        "hyperliquid",
        "main",
        150,
        Kind::Positions,
        Vec::new(),
    ))
    .await;
    h.publish(&set(
        "hyperliquid",
        "main",
        200,
        Kind::Positions,
        Vec::new(),
    ))
    .await;

    let now = h.json("/v1/accounts/hyperliquid/main/now").await;
    assert_eq!(now["stream_reached"], true);
    let positions = &now["kinds"]["positions"];
    assert_eq!(positions["source"], "stream", "{now}");
    assert_eq!(positions["read_micros"], 200, "the newest set");
    assert_eq!(
        positions["rows"],
        serde_json::json!([]),
        "an empty set at its time"
    );
    assert_eq!(positions["position"], 2);
    let margin = &now["kinds"]["margin"];
    assert_eq!(margin["source"], "projection", "{now}");
    assert_eq!(margin["read_micros"], 100);
    assert_eq!(margin["rows"][0]["account_value"], "10");
    // Held in neither: absent, not empty.
    let absent: Vec<&str> = now["absent"]
        .as_array()
        .unwrap()
        .iter()
        .map(|k| k.as_str().unwrap())
        .collect();
    assert!(absent.contains(&"orders"), "{now}");
    assert!(now["kinds"].get("orders").is_none());
}

#[tokio::test]
async fn the_accounts_listed_join_the_projection_and_the_stream() {
    let h = Harness::start().await;
    project::write(&h.root, "hyperliquid", "main", &[fill(1, 100)]).unwrap();
    h.publish(&set(
        "derive-xyz",
        "owner_s3",
        300,
        Kind::PositionState,
        Vec::new(),
    ))
    .await;
    h.publish(&fill(2, 400)).await;
    let listed = h.json("/v1/accounts").await;
    assert_eq!(listed["stream_reached"], true);
    let accounts = listed["accounts"].as_array().unwrap();
    let named: Vec<(String, String)> = accounts
        .iter()
        .map(|a| {
            (
                a["venue"].as_str().unwrap().into(),
                a["alias"].as_str().unwrap().into(),
            )
        })
        .collect();
    assert_eq!(
        named,
        [
            ("derive-xyz".to_string(), "owner_s3".to_string()),
            ("hyperliquid".to_string(), "main".to_string()),
        ]
    );
    assert_eq!(accounts[0]["kinds"]["position_state"], 300);
    // Each kind's newest, from whichever source holds the later.
    assert_eq!(accounts[1]["kinds"]["fills"], 400);
}

#[tokio::test]
async fn history_then_live_gives_each_fill_once() {
    let h = Harness::start().await;
    // The projection holds fills 1 and 2, received by 100.
    project::write(&h.root, "hyperliquid", "main", &[fill(1, 90), fill(2, 100)]).unwrap();
    // The stream holds fill 2 too, and fill 3, received after the bound.
    h.publish(&fill(2, 100)).await;
    h.publish(&fill(3, 300)).await;

    let history = h
        .json("/v1/accounts/hyperliquid/main/fills?from=0&to=100000")
        .await;
    assert_eq!(trades(&history["rows"]), ["1", "2", "3"], "{history}");
    assert_eq!(history["bound_micros"], 100);
    let position = history["position"].as_u64().unwrap();
    assert_eq!(position, 2, "the stream position the answer read to");

    // Fill 4 arrives between the history answer and the stream opening.
    h.publish(&fill(4, 400)).await;
    let (status, mut body) = h
        .get(
            &format!("/v1/accounts/live?accounts=hyperliquid/main&kinds=fills&after={position}"),
            None,
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    let mut carried = String::new();
    let first = read(&mut body, 1, &mut carried).await;
    assert_eq!(
        first,
        [Read {
            id: 3,
            trade: "4".into()
        }]
    );
    h.publish(&fill(5, 500)).await;
    let next = read(&mut body, 1, &mut carried).await;
    assert_eq!(
        next,
        [Read {
            id: 4,
            trade: "5".into()
        }]
    );

    let mut every: Vec<String> = trades(&history["rows"]);
    every.extend(first.into_iter().chain(next).map(|r| r.trade));
    assert_eq!(
        every,
        ["1", "2", "3", "4", "5"],
        "each fill once, across the two"
    );
}

#[tokio::test]
async fn a_reconnection_resumes_with_nothing_missed_or_repeated() {
    let h = Harness::start().await;
    for tid in 1..=3 {
        h.publish(&fill(tid, 100 * tid as i64)).await;
    }
    // A state set on the same account: not asked for, so not sent.
    h.publish(&set(
        "hyperliquid",
        "main",
        150,
        Kind::Positions,
        Vec::new(),
    ))
    .await;
    let path = "/v1/accounts/live?accounts=hyperliquid/main&kinds=fills&after=0";
    let (status, mut body) = h.get(path, None).await;
    assert_eq!(status, StatusCode::OK);
    let mut carried = String::new();
    let seen = read(&mut body, 3, &mut carried).await;
    assert_eq!(
        seen.iter().map(|r| r.trade.as_str()).collect::<Vec<_>>(),
        ["1", "2", "3"]
    );
    let last = seen.last().unwrap().id;
    drop(body);

    // Missed while away.
    for tid in 4..=5 {
        h.publish(&fill(tid, 100 * tid as i64)).await;
    }
    let (status, mut body) = h
        .get(
            "/v1/accounts/live?accounts=hyperliquid/main&kinds=fills",
            Some(last),
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    let mut carried = String::new();
    let resumed = read(&mut body, 2, &mut carried).await;
    assert_eq!(
        resumed.iter().map(|r| r.trade.as_str()).collect::<Vec<_>>(),
        ["4", "5"],
        "nothing missed, nothing repeated"
    );
    assert!(resumed.iter().all(|r| r.id > last));
    // And then live.
    h.publish(&fill(6, 600)).await;
    assert_eq!(read(&mut body, 1, &mut carried).await[0].trade, "6");
}

#[tokio::test]
async fn a_position_older_than_the_stream_holds_is_refused_naming_the_oldest() {
    let h = Harness::start().await;
    // The account stream kept to five messages, as its age would keep it.
    let js = async_nats::jetstream::new(h.admin.clone());
    let mut config = js
        .get_stream("ACCOUNTS")
        .await
        .unwrap()
        .info()
        .await
        .unwrap()
        .config
        .clone();
    config.max_messages = 5;
    js.update_stream(&config).await.unwrap();
    for tid in 1..=12 {
        h.publish(&fill(tid, 100 * tid as i64)).await;
    }
    for (path, last) in [
        ("/v1/accounts/live?accounts=hyperliquid/main", Some(2)),
        ("/v1/accounts/live?accounts=hyperliquid/main&after=2", None),
    ] {
        let (status, body) = h.get(path, last).await;
        assert_eq!(status, StatusCode::GONE, "{path}");
        let text =
            String::from_utf8(axum::body::to_bytes(body, 1 << 16).await.unwrap().to_vec()).unwrap();
        assert!(text.contains("the oldest it holds is 8"), "{text}");
    }
    // Still held: served.
    let (status, mut body) = h
        .get("/v1/accounts/live?accounts=hyperliquid/main", Some(8))
        .await;
    assert_eq!(status, StatusCode::OK);
    let mut carried = String::new();
    assert_eq!(read(&mut body, 1, &mut carried).await[0].id, 9);
}
