//! **`galata-bus` against a real `nats-server`**: it applies the stream table,
//! then is the server — and a table it cannot apply costs the streams, not the
//! bus.
//!
//! Needs `nats-server` on PATH, as the broker crate's live tests do; CI
//! installs a pinned one.

#![cfg(feature = "bin")]

use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::mpsc;
use std::time::{Duration, Instant};

use galata_broker::{BUS_ADMIN, BrokerIdentity, NatsPublisher, grants, password_var, streams};

const VENUES: [&str; 1] = ["hyperliquid"];
const LEDGERS: [&str; 1] = ["hyperliquid"];

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

/// A server config in `dir` on `port`: JetStream on, the generated table,
/// and — if `admin_password` is given — that literal for `bus-admin` in place
/// of its variable, so the server and `galata-bus` can be made to disagree.
fn write_config(dir: &Path, port: u16, admin_password: Option<&str>) -> PathBuf {
    let mut table = grants::to_nats_config(&grants::table(&VENUES, &LEDGERS));
    if let Some(literal) = admin_password {
        table = table.replace(
            &format!("${}", password_var(BUS_ADMIN)),
            &format!("\"{literal}\""),
        );
    }
    let path = dir.join("nats-server.conf");
    std::fs::write(
        &path,
        format!(
            "listen: 127.0.0.1:{port}\njetstream {{\n  store_dir: \"{}\"\n  max_memory_store: 0\n  max_file_store: 4GB\n}}\n{table}",
            dir.join("jetstream").display()
        ),
    )
    .unwrap();
    path
}

/// `galata-bus --url … -- nats-server -c <config>`, every identity's password
/// in its environment, its log read line by line into the returned channel.
fn start(port: u16, config: &Path) -> (Child, mpsc::Receiver<String>) {
    let mut command = Command::new(env!("CARGO_BIN_EXE_galata-bus"));
    command
        .args(["--url", &format!("nats://127.0.0.1:{port}"), "--"])
        .arg(nats_server())
        .arg("-c")
        .arg(config)
        .env("NO_COLOR", "1")
        .stdout(Stdio::null())
        .stderr(Stdio::piped());
    for grant in grants::table(&VENUES, &LEDGERS).iter() {
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
    (child, rx)
}

/// Lines from the log until `count` of them hold `needle`, or the time is up.
fn wait_for(
    log: &mpsc::Receiver<String>,
    needle: &str,
    count: usize,
    limit: Duration,
) -> Vec<String> {
    let deadline = Instant::now() + limit;
    let mut found = Vec::new();
    while found.len() < count {
        let left = deadline.saturating_duration_since(Instant::now());
        match log.recv_timeout(left) {
            Ok(line) if line.contains(needle) => found.push(line),
            Ok(_) => {}
            Err(_) => break,
        }
    }
    found
}

fn sigterm(child: &Child) {
    let status = Command::new("kill")
        .args(["-TERM", &child.id().to_string()])
        .status()
        .unwrap();
    assert!(status.success());
}

fn wait_exit(child: &mut Child, limit: Duration) -> Option<std::process::ExitStatus> {
    let deadline = Instant::now() + limit;
    while Instant::now() < deadline {
        if let Some(status) = child.try_wait().unwrap() {
            return Some(status);
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    None
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}

#[test]
fn it_applies_the_streams_and_then_is_the_server() {
    let dir = tempfile::tempdir().unwrap();
    let port = free_port();
    let config = write_config(dir.path(), port, None);
    let (mut child, log) = start(port, &config);

    let applied = wait_for(&log, "stream applied", 3, Duration::from_secs(20));
    assert_eq!(
        applied.len(),
        3,
        "the helper applied the three streams: {applied:?}"
    );

    // The process launchd would signal IS the server now: exec, not a parent.
    let comm = Command::new("ps")
        .args(["-o", "comm=", "-p", &child.id().to_string()])
        .output()
        .unwrap();
    let comm = String::from_utf8_lossy(&comm.stdout);
    assert!(
        comm.trim().ends_with("nats-server"),
        "the process is {comm:?}"
    );

    let streams_seen = runtime().block_on(async {
        let client =
            streams::connect_admin(&format!("nats://127.0.0.1:{port}"), &password(BUS_ADMIN))
                .await
                .unwrap();
        admin_view::names(&client).await
    });
    assert_eq!(streams_seen, vec!["ACCOUNTS", "MARKETS", "STATUS"]);

    sigterm(&child);
    let status =
        wait_exit(&mut child, Duration::from_secs(10)).expect("the server stops on one SIGTERM");
    assert!(status.success(), "a clean stop: {status:?}");
}

#[test]
fn a_table_it_cannot_apply_costs_the_streams_and_not_the_bus() {
    let dir = tempfile::tempdir().unwrap();
    let port = free_port();
    // The server holds one password for bus-admin, galata-bus another.
    let config = write_config(
        dir.path(),
        port,
        Some("a-password-galata-bus-does-not-have"),
    );
    let (mut child, log) = start(port, &config);

    let refused = wait_for(&log, "streams are not applied", 1, Duration::from_secs(20));
    assert_eq!(refused.len(), 1, "the failure is said by name");
    assert!(refused[0].contains(BUS_ADMIN), "{}", refused[0]);

    // Publishers and subscribers are served as before.
    let served = runtime().block_on(async {
        let capture = BrokerIdentity::new(
            "datawatch-hyperliquid",
            password("datawatch-hyperliquid"),
            password_var("datawatch-hyperliquid"),
        );
        match NatsPublisher::connect(&format!("nats://127.0.0.1:{port}"), &capture).await {
            Ok(publisher) => publisher.flush().await.is_ok(),
            Err(_) => false,
        }
    });
    assert!(served, "capture is served though no stream was applied");

    sigterm(&child);
    assert!(wait_exit(&mut child, Duration::from_secs(10)).is_some());
}

/// The stream names the broker holds, sorted — through the admin's own grants.
mod admin_view {
    pub async fn names(client: &galata_broker::streams::AdminClient) -> Vec<String> {
        let mut names = Vec::new();
        for stream in galata_broker::streams::table() {
            if galata_broker::streams::held(client, stream.name)
                .await
                .is_some()
            {
                names.push(stream.name.to_string());
            }
        }
        names.sort();
        names
    }
}
