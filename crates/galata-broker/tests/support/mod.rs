//! **A real broker for a test**: `nats-server`, on a free port of its own,
//! with a scratch store, JetStream on, and the permissions the code generates.
//!
//! A permission set that has never been loaded into a running server is a
//! decoration (`grants::rule`, on the `allow: []` inversion that review, unit
//! tests and `nats-server -t` all called valid). The same holds for streams: a
//! stream table never applied to a broker says nothing about what one keeps.
//!
//! A missing `nats-server` fails the test, by name — a test that skipped itself
//! would pass on every machine that cannot run it. CI installs a pinned one.

#![allow(dead_code)]

use std::io::{BufRead, BufReader};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use galata_broker::grants;

/// The venues the test broker's permissions are generated for.
pub const VENUES: [&str; 1] = ["hyperliquid"];

/// **Test-only identities**, added to the generated table for the test broker
/// alone and never to a deployment's: this change grants no consumer and no
/// account publisher, and the stream tests need both.
///
/// - `test-consumer` may use the JetStream API, to replay a stream;
/// - `test-ledger` may publish account data and hear the broker's
///   acknowledgements.
pub const TEST_CONSUMER: &str = "test-consumer";
/// See [`TEST_CONSUMER`].
pub const TEST_LEDGER: &str = "test-ledger";

fn with_test_identities(generated: &str) -> String {
    let close = "  ]\n}\n";
    assert!(
        generated.ends_with(close),
        "the generated table's shape changed"
    );
    let users = format!(
        "    {{\n      user: {TEST_CONSUMER}\n      password: \"{}\"\n      permissions: {{\n        \
         publish: {{ allow: [\"$JS.API.>\"] }}\n        subscribe: {{ allow: [\"_INBOX.>\"] }}\n      }}\n    }},\n\
         \x20   {{\n      user: {TEST_LEDGER}\n      password: \"{}\"\n      permissions: {{\n        \
         publish: {{ allow: [\"accounts.>\"] }}\n        subscribe: {{ allow: [\"_INBOX.>\"] }}\n      }}\n    }},\n",
        password(TEST_CONSUMER),
        password(TEST_LEDGER)
    );
    format!(
        "{}{users}{close}",
        &generated[..generated.len() - close.len()]
    )
}

/// A running test broker. Stopped, and its process reaped, when dropped.
pub struct Broker {
    child: Option<Child>,
    /// Its port on 127.0.0.1.
    pub port: u16,
    /// The folder holding its config and its store.
    pub dir: PathBuf,
    _scratch: Option<tempfile::TempDir>,
}

/// The stand-in password each identity is started with: test values, never a
/// deployment's.
pub fn password(identity: &str) -> String {
    format!("test-password-{identity}")
}

/// The first `nats-server` on PATH.
fn nats_server() -> PathBuf {
    let found = std::env::var_os("PATH").and_then(|paths| {
        std::env::split_paths(&paths)
            .map(|dir| dir.join("nats-server"))
            .find(|candidate| candidate.is_file())
    });
    found.unwrap_or_else(|| {
        panic!(
            "no nats-server on PATH: these tests run a real broker. Install nats-server 2.15.3 \
             (macOS: brew install nats-server; CI installs it in its dependency step)"
        )
    })
}

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .and_then(|l| l.local_addr())
        .map(|a| a.port())
        .expect("a free local port")
}

impl Broker {
    /// A broker on a fresh scratch store.
    pub fn start() -> Broker {
        let scratch = tempfile::tempdir().expect("a scratch folder");
        let dir = scratch.path().to_path_buf();
        let mut broker = Broker::start_in(&dir);
        broker._scratch = Some(scratch);
        broker
    }

    /// A broker whose config and store are in `dir`, which may already hold a
    /// store from an earlier broker: what a restart is.
    pub fn start_in(dir: &Path) -> Broker {
        let port = free_port();
        let store = dir.join("jetstream");
        let config = format!(
            "listen: 127.0.0.1:{port}\n\
             jetstream {{\n  store_dir: \"{}\"\n  max_memory_store: 0\n  max_file_store: 4GB\n}}\n{}",
            store.display(),
            with_test_identities(&grants::to_nats_config(&grants::table(&VENUES)))
        );
        let config_path = dir.join("nats-server.conf");
        std::fs::write(&config_path, config).expect("the test broker's config");

        let mut command = Command::new(nats_server());
        command
            .arg("-c")
            .arg(&config_path)
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        for grant in grants::table(&VENUES).iter() {
            command.env(
                grants::password_var(&grant.identity),
                password(&grant.identity),
            );
        }
        let child = command.spawn().expect("nats-server starts");
        let broker = Broker {
            child: Some(child),
            port,
            dir: dir.to_path_buf(),
            _scratch: None,
        };
        broker.wait_ready();
        broker
    }

    /// Where a client connects.
    pub fn url(&self) -> String {
        format!("nats://127.0.0.1:{}", self.port)
    }

    /// Until the server greets a connection, and says JetStream is on.
    fn wait_ready(&self) {
        let deadline = Instant::now() + Duration::from_secs(15);
        while Instant::now() < deadline {
            if let Ok(stream) = TcpStream::connect(("127.0.0.1", self.port)) {
                let _ = stream.set_read_timeout(Some(Duration::from_secs(2)));
                let mut line = String::new();
                if BufReader::new(stream).read_line(&mut line).is_ok() && line.starts_with("INFO ")
                {
                    assert!(
                        line.contains("\"jetstream\":true"),
                        "the test broker answered without JetStream: {line}"
                    );
                    return;
                }
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        panic!(
            "the test broker did not answer on port {} within 15 s",
            self.port
        );
    }

    /// Stop it and wait for it to exit; its store stays in `dir`.
    pub fn stop(mut self) {
        self.halt();
    }

    fn halt(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

impl Drop for Broker {
    fn drop(&mut self) {
        self.halt();
    }
}

/// A client connected as one of the table's identities.
pub async fn connect(broker: &Broker, identity: &str) -> async_nats::Client {
    connect_with(broker, identity, async_nats::ConnectOptions::new()).await
}

/// The same, with options of the caller's — an inbox prefix, say.
pub async fn connect_with(
    broker: &Broker,
    identity: &str,
    options: async_nats::ConnectOptions,
) -> async_nats::Client {
    options
        .user_and_password(identity.to_string(), password(identity))
        .connect(broker.url())
        .await
        .unwrap_or_else(|e| panic!("{identity} could not connect to the test broker: {e}"))
}
