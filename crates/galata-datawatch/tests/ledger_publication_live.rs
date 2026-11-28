//! The ledger's publication against a real broker (`add-realtime-account-ledger`,
//! tasks 3.2 and 3.3): `galata-bus` running `nats-server` on the generated
//! table, the stream table applied at its start. What the record holds
//! reaches the account stream acknowledged, **each event once and in the
//! order read**, across a broker stopped and started mid-run.
//!
//! The stream's own acknowledgements are the evidence: each carries the
//! position the message was stored at and whether the stream already held it.
//! (A reader of the stream would need a client this crate does not link, which
//! `check-no-transport.sh` holds.)
#![cfg(all(feature = "bin", feature = "hyperliquid", feature = "ledger"))]

use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::{Mutex, mpsc};
use std::time::{Duration, Instant};

use galata_broker::{
    AccountPublisher, BrokerIdentity, JetStreamPublisher, Subject, grants, ledger_identity,
    ledger_inbox, password_var,
};
use galata_datawatch::adapters::hyperliquid::ledger::{self as hl, LedgerNormaliser};
use galata_datawatch::ledger::publish::{AccountBus, Publication};
use galata_datawatch::record::{AccountAddress, Archive, Payload, PayloadAddress};
use galata_datawatch::sink::NullSink;
use galata_wire::{Envelope, Origin, Venue};

const FLAT: &[u8] = br#"{"marginSummary":{"accountValue":"0.0","totalNtlPos":"0.0","totalRawUsd":"0.0","totalMarginUsed":"0.0"},"crossMaintenanceMarginUsed":"0.0","withdrawable":"0.0","assetPositions":[],"time":1790332213000}"#;

fn password(identity: &str) -> String {
    format!("test-password-{identity}")
}

fn nats_server() -> PathBuf {
    std::env::var_os("PATH")
        .and_then(|paths| {
            std::env::split_paths(&paths)
                .map(|dir| dir.join("nats-server"))
                .find(|c| c.is_file())
        })
        .expect("no nats-server on PATH: install nats-server 2.15.3")
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn table() -> grants::Grants {
    grants::table(&["hyperliquid"], &["hyperliquid"])
}

/// The generated table, JetStream's store in `dir`.
fn config(dir: &Path, port: u16) -> PathBuf {
    let path = dir.join("nats-server.conf");
    std::fs::write(
        &path,
        format!(
            "listen: 127.0.0.1:{port}\njetstream {{\n  store_dir: \"{}\"\n  max_memory_store: 0\n  max_file_store: 4GB\n}}\n{}",
            dir.join("jetstream").display(),
            grants::to_nats_config(&table())
        ),
    )
    .unwrap();
    path
}

/// `galata-bus` serving `nats-server` on `port` with its store in `dir`, the
/// stream table applied: started again on the same two, the same broker
/// restarted.
fn start(dir: &Path, port: u16) -> Child {
    let mut command = Command::new(env!("CARGO_BIN_EXE_galata-bus"));
    command
        .args(["--url", &format!("nats://127.0.0.1:{port}"), "--"])
        .arg(nats_server())
        .arg("-c")
        .arg(config(dir, port))
        .env("NO_COLOR", "1")
        .stdout(Stdio::null())
        .stderr(Stdio::piped());
    for grant in table().iter() {
        command.env(password_var(&grant.identity), password(&grant.identity));
    }
    let mut child = command.spawn().expect("galata-bus starts");
    let stderr = child.stderr.take().unwrap();
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        for line in BufReader::new(stderr).lines().map_while(Result::ok) {
            if tx.send(line).is_err() {
                break;
            }
        }
    });
    let deadline = Instant::now() + Duration::from_secs(20);
    let mut applied = 0;
    while applied < 3 {
        let left = deadline.saturating_duration_since(Instant::now());
        match rx.recv_timeout(left) {
            Ok(line) if line.contains("stream applied") => applied += 1,
            Ok(_) => {}
            Err(_) => panic!("the broker did not apply its streams"),
        }
    }
    child
}

fn stop(child: &mut Child) {
    let _ = Command::new("kill")
        .args(["-TERM", &child.id().to_string()])
        .status();
    let deadline = Instant::now() + Duration::from_secs(20);
    while child.try_wait().unwrap().is_none() {
        assert!(Instant::now() < deadline, "the broker did not stop");
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// One acknowledgement: the id, the subject, the stream position, and whether
/// the stream already held it.
type Ack = (String, String, u64, bool);

/// The account stream as the ledger reaches it — each envelope on
/// `Subject::of`, as `boot::LedgerBus` puts it — keeping each acknowledgement.
struct Acked {
    publisher: JetStreamPublisher,
    seen: Mutex<Vec<Ack>>,
}

impl AccountBus for Acked {
    fn publish_acked<'a>(
        &'a self,
        id: &'a str,
        envelope: &'a Envelope,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), String>> + Send + 'a>> {
        Box::pin(async move {
            let subject = Subject::of(envelope).ok_or("no account subject")?;
            let ack = self
                .publisher
                .publish_acked(&subject, id, envelope)
                .await
                .map_err(|e| e.to_string())?;
            self.seen.lock().unwrap().push((
                id.to_string(),
                subject.as_str().to_string(),
                ack.stream_seq,
                ack.duplicate,
            ));
            Ok(())
        })
    }
}

/// Record one Hyperliquid answer under `main`, as the ledger records it.
fn record(archive: &mut Archive, channel: &str, kind: &str, recv: i64, bytes: Vec<u8>) {
    let payload = Payload {
        seq: 0,
        recv_micros: recv,
        address: PayloadAddress::Account(AccountAddress {
            venue: "hyperliquid".into(),
            account: "main".into(),
            fingerprint: "0123456789abcdef".into(),
        }),
        channel: channel.into(),
        kind: kind.into(),
        symbol: None,
        origin: Origin::Fetched,
        payload: bytes,
    };
    galata_datawatch::ingest::ingest(
        archive,
        &LedgerNormaliser::new().unwrap(),
        &NullSink,
        payload,
    )
    .unwrap();
}

fn fills(tids: std::ops::Range<u64>) -> Vec<u8> {
    let rows: Vec<String> = tids
        .map(|t| format!(r#"{{"coin":"BTC","px":"81213.5","sz":"0.01","side":"B","time":{},"startPosition":"0.0","dir":"Open Long","closedPnl":"0.0","hash":"0x0","oid":{},"crossed":true,"fee":"0.3","builderFee":"0.0","tid":{t},"feeToken":"USDC","twapId":null}}"#, 1790332213000 + t, 1000 + t))
        .collect();
    format!("[{}]", rows.join(",")).into_bytes()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn what_the_record_holds_reaches_the_stream_once_in_order_across_a_broker_restart() {
    let dir = tempfile::tempdir().unwrap();
    let port = free_port();
    let url = format!("nats://127.0.0.1:{port}");
    let mut broker = start(dir.path(), port);

    let identity = BrokerIdentity::new(
        ledger_identity("hyperliquid"),
        password(&ledger_identity("hyperliquid")),
        password_var(&ledger_identity("hyperliquid")),
    );
    let bus = Acked {
        publisher: JetStreamPublisher::connect(&url, &identity, &ledger_inbox("hyperliquid"))
            .await
            .expect("the ledger connects as itself"),
        seen: Mutex::new(Vec::new()),
    };
    let root = dir.path().join("ledger");
    let mut archive = Archive::open(&root).from_seq(1);
    let normaliser = LedgerNormaliser::new().unwrap();
    let mut publication = Publication::new(
        Venue::new("hyperliquid").unwrap(),
        Publication::cursor_path(&root, "hyperliquid"),
    );
    let states = hl::states;
    let mut now = 1_790_332_213_000_000;

    // The stream begins: nothing recorded yet.
    publication
        .publish(&root, &normaliser, &states, &bus, now)
        .await
        .unwrap();

    // A snapshot and three fills, published and acknowledged.
    record(
        &mut archive,
        hl::SNAPSHOT_CHANNEL,
        "margin",
        now,
        hl::snapshot_bytes("", None, FLAT),
    );
    record(
        &mut archive,
        "userFillsByTime",
        "fills",
        now + 1,
        fills(1..4),
    );
    now += 2_000_000;
    let out = publication
        .publish(&root, &normaliser, &states, &bus, now)
        .await
        .unwrap();
    assert_eq!((out.events, out.stopped), (3, false));
    assert!(out.sets >= 2, "margin and positions: {out:?}");

    // The broker stops; two more fills are recorded meanwhile.
    stop(&mut broker);
    record(
        &mut archive,
        "userFillsByTime",
        "fills",
        now + 1,
        fills(4..6),
    );
    now += 2_000_000;
    let out = publication
        .publish(&root, &normaliser, &states, &bus, now)
        .await
        .unwrap();
    assert!(out.stopped, "nothing acknowledged while the broker is away");

    // It starts again, the same broker on the same store: everything read
    // meanwhile arrives.
    let mut broker = start(dir.path(), port);
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        now += 2_000_000;
        let out = publication
            .publish(&root, &normaliser, &states, &bus, now)
            .await
            .unwrap();
        if !out.stopped && out.held == 0 {
            break;
        }
        assert!(Instant::now() < deadline, "never caught up: {out:?}");
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    let seen = bus.seen.lock().unwrap().clone();
    let fills: Vec<&Ack> = seen
        .iter()
        .filter(|(id, ..)| id.contains(":fill:"))
        .collect();
    let expected: Vec<String> = (1..6)
        .map(|t| format!("hyperliquid:main:fill:{t}:{}", 1000 + t))
        .collect();
    assert_eq!(
        fills.iter().map(|(id, ..)| id).collect::<Vec<_>>(),
        expected.iter().collect::<Vec<_>>(),
        "each fill once, in the order read"
    );
    // A fill whose first send went out as the broker stopped is stored once,
    // when it comes back: the retry, carrying the same id, is answered as a
    // duplicate of the copy the stream already holds (design D4). Each fill's
    // position is its own.
    let positions: std::collections::BTreeSet<u64> = fills.iter().map(|f| f.2).collect();
    assert_eq!(
        positions.len(),
        fills.len(),
        "a fill stored twice: {fills:?}"
    );
    assert!(
        fills
            .iter()
            .filter(|(.., duplicate)| *duplicate)
            .all(|(id, ..)| id.contains(":fill:4:") || id.contains(":fill:5:")),
        "only a fill sent across the outage is answered as already held: {fills:?}"
    );
    assert!(
        fills.windows(2).all(|w| w[0].2 < w[1].2),
        "stored in the order read, across the restart"
    );
    assert!(
        seen.iter()
            .all(|(_, subject, ..)| subject.starts_with("accounts.hyperliquid.main.")),
        "{seen:?}"
    );
    assert!(
        seen.iter()
            .any(|(_, subject, ..)| subject == "accounts.hyperliquid.main.positions")
    );
    stop(&mut broker);
}
