//! **The broker's process**: apply the stream table, and be `nats-server`.
//!
//! ```text
//!   galata-bus [--url URL] -- nats-server -c /etc/galata/nats-server.conf
//! ```
//!
//! starts a helper — itself, as `galata-bus apply --url URL` — and then
//! **replaces itself with the server** (`exec`). The helper waits until the
//! server answers, applies `galata_broker::streams::table()` as `bus-admin`,
//! logs what each stream holds, and exits.
//!
//! **`exec`, not a supervisor.** The server is then the container's process,
//! and launchd's SIGTERM reaches it with nothing in between, as it always has
//! (`containerise-host-services` D9). A table that cannot be applied is said by
//! name, and the server, which never depended on it, serves on.
//!
//! **One identity, and the right to nothing else.** `bus-admin` may create,
//! update and read the declared streams, and hears replies on an inbox of its
//! own; its password arrives in the environment as every broker password does.

use std::os::unix::process::CommandExt;
use std::process::{Command, ExitCode, Stdio};
use std::time::Duration;

use galata_broker::streams::{self, Applied};
use galata_broker::{BUS_ADMIN, ConnectRefusal, password_var};
use galata_datawatch::config::{EnvSecrets, SecretSource};

/// Where the helper finds the server, from inside the broker's container.
const DEFAULT_URL: &str = "nats://127.0.0.1:4222";

/// How long the helper waits for the server to answer before saying so.
const WAIT_FOR_SERVER: Duration = Duration::from_secs(60);

const USAGE: &str = "usage: galata-bus [--url URL] -- <nats-server> [args ...]";

fn usage() -> ExitCode {
    eprintln!("{USAGE}");
    ExitCode::from(2)
}

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match args.first().map(String::as_str) {
        Some("apply") => return apply_mode(&args[1..]),
        Some("--help" | "-h") => {
            println!(
                "{USAGE}\n\nApplies the broker's stream table as {BUS_ADMIN} once the server at URL \
                 answers (default {DEFAULT_URL}), and becomes the server meanwhile.\nThe password \
                 is read from {}.",
                password_var(BUS_ADMIN)
            );
            return ExitCode::SUCCESS;
        }
        _ => {}
    }

    let mut url = DEFAULT_URL.to_string();
    let mut rest = args.as_slice();
    if rest.first().map(String::as_str) == Some("--url") {
        match rest.get(1) {
            Some(given) => url = given.clone(),
            None => return usage(),
        }
        rest = &rest[2..];
    }
    let Some(("--", server)) = rest.split_first().map(|(head, tail)| (head.as_str(), tail)) else {
        return usage();
    };
    let Some((program, server_args)) = server.split_first() else {
        return usage();
    };

    // The helper, detached from our standard input and sharing our log.
    // Failing to start it costs the streams, not the bus: say so and serve.
    let helper = std::env::current_exe().and_then(|me| {
        Command::new(me)
            .args(["apply", "--url", &url])
            .stdin(Stdio::null())
            .spawn()
    });
    if let Err(error) = helper {
        eprintln!(
            "galata-bus: could not start the helper that applies the streams: {error}; serving without them"
        );
    }

    // Becomes the server: returns only if it could not.
    let error = Command::new(program).args(server_args).exec();
    eprintln!("galata-bus: could not start {program}: {error}");
    ExitCode::from(127)
}

/// `galata-bus apply [--url URL]`: wait for the server, apply the table, exit.
fn apply_mode(args: &[String]) -> ExitCode {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .with_writer(std::io::stderr)
        .init();
    let url = match args {
        [] => DEFAULT_URL.to_string(),
        [flag, given] if flag == "--url" => given.clone(),
        _ => return usage(),
    };
    // Through the one door every secret is read by (check-secret-reach).
    let password = match EnvSecrets.secret(&password_var(BUS_ADMIN)) {
        Ok(password) => password,
        Err(absent) => {
            tracing::error!(
                "{absent}, so the streams are not applied; the broker serves without them"
            );
            return ExitCode::FAILURE;
        }
    };
    let runtime = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => {
            tracing::error!("no runtime for applying the streams: {error}");
            return ExitCode::FAILURE;
        }
    };
    runtime.block_on(apply(&url, password.expose()))
}

async fn apply(url: &str, password: &str) -> ExitCode {
    // **The runtime's timer, not a clock read here** (check-clock-discipline):
    // a server not up yet is asked again until the wait is over; one that
    // refuses the identity will not change its mind.
    let mut last: Option<String> = None;
    let reached = tokio::time::timeout(WAIT_FOR_SERVER, async {
        loop {
            match streams::connect_admin(url, password).await {
                Ok(client) => return Ok(client),
                Err(refusal @ ConnectRefusal::Rejected { .. }) => return Err(refusal),
                Err(refusal) => {
                    last = Some(refusal.to_string());
                    tokio::time::sleep(Duration::from_millis(250)).await;
                }
            }
        }
    })
    .await;
    let client = match reached {
        Ok(Ok(client)) => client,
        Ok(Err(refusal)) => {
            tracing::error!(
                "the streams are not applied, and the broker serves without them: {refusal}"
            );
            return ExitCode::FAILURE;
        }
        Err(_) => {
            tracing::error!(
                "the streams are not applied, and the broker serves without them: the server did \
                 not answer within {} s{}",
                WAIT_FOR_SERVER.as_secs(),
                last.map(|said| format!(" ({said})")).unwrap_or_default()
            );
            return ExitCode::FAILURE;
        }
    };

    let mut failed = 0;
    for outcome in streams::apply(&client).await {
        match outcome {
            Applied::Held {
                name,
                messages,
                bytes,
            } => {
                tracing::info!(stream = name, messages, bytes, "stream applied");
            }
            Applied::Failed { name, reason } => {
                failed += 1;
                tracing::error!(
                    stream = name,
                    "stream not applied: {reason}; the broker serves without it"
                );
            }
        }
    }
    if failed == 0 {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}
