//! **The service's process** (`add-galata-markets`): the deployment's
//! configuration for where the tape, the archive and the broker are; its
//! clients' tokens and its bus password from the environment, through the one
//! door secrets are read by; then serve until SIGTERM.
//!
//! ```text
//!   galata-markets [--listen ADDR]          ADDR defaults to 0.0.0.0:8780
//! ```
//!
//! **A broker that is away is not a reason not to serve.** History and
//! metadata answer from the tape; the bus is asked for in the background,
//! and the live stream says it is not yet reachable until it answers.

use std::net::SocketAddr;
use std::process::ExitCode;
use std::sync::Arc;

use galata_broker::{GALATA_MARKETS, GALATA_MARKETS_INBOX, password_var};
use galata_datawatch::adapters;
use galata_datawatch::config::{Adapters, Config, EnvSecrets, FileSource, SecretSource};
use galata_markets::access::Tokens;
use galata_markets::fill::Normalisers;
use galata_markets::{Markets, service, status};

const USAGE: &str = "usage: galata-markets [--listen ADDR]";

/// Where the service listens inside its container; the launcher publishes it
/// on the Mac's tailnet address.
const DEFAULT_LISTEN: &str = "0.0.0.0:8780";

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

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let listen = match args.as_slice() {
        [] => DEFAULT_LISTEN.to_string(),
        [flag, addr] if flag == "--listen" => addr.clone(),
        [flag] if flag == "--help" || flag == "-h" => {
            println!(
                "{USAGE}\n\nServes market data from the tape, the archive and the bus. Reads the \
                 configuration named by GALATA_CONFIG, each client's token from \
                 GALATA_MARKETS_TOKEN_<CLIENT>, and its bus password from {}.",
                password_var(GALATA_MARKETS)
            );
            return ExitCode::SUCCESS;
        }
        _ => {
            eprintln!("{USAGE}");
            return ExitCode::from(2);
        }
    };
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .with_writer(std::io::stderr)
        .init();
    let runtime = match tokio::runtime::Runtime::new() {
        Ok(runtime) => runtime,
        Err(error) => {
            tracing::error!("no runtime: {error}");
            return ExitCode::FAILURE;
        }
    };
    match runtime.block_on(serve(&listen)) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            tracing::error!("{error}");
            ExitCode::FAILURE
        }
    }
}

async fn serve(listen: &str) -> Result<(), String> {
    let addr: SocketAddr = listen
        .parse()
        .map_err(|e| format!("--listen {listen}: {e}"))?;
    let config = FileSource::from_env("config/datawatch.toml")
        .and_then(|source| Config::load(&source, &Resolver))
        .map_err(|e| e.to_string())?;
    let tokens = Tokens::from_source(&EnvSecrets).map_err(|e| e.to_string())?;
    let normalisers = Normalisers::from_config(&config)?;

    let mut markets = Markets::over(config.paths.tape.clone());
    markets.archive = config.paths.archive.clone();
    markets.normalisers = normalisers;
    markets.health = galata_markets::health::Sources::from_config(&config);

    // The bus, asked for in the background: a broker that is away delays the
    // live stream, never the rest.
    match &config.broker {
        Some(broker) => {
            let password = EnvSecrets
                .secret(&password_var(GALATA_MARKETS))
                .map_err(|e| e.to_string())?;
            let client = async_nats::ConnectOptions::with_user_and_password(
                GALATA_MARKETS.to_string(),
                password.expose().to_string(),
            )
            .custom_inbox_prefix(GALATA_MARKETS_INBOX)
            .retry_on_initial_connect()
            .connect(broker.url.as_str())
            .await
            .map_err(|e| format!("the bus could not be asked for: {e}"))?;
            tokio::spawn(status::follow(client.clone(), markets.statuses.clone()));
            markets.bus = Some(client);
        }
        None => tracing::warn!(
            "the configuration names no broker: history and metadata are served, the live stream is not"
        ),
    }

    let app = service(tokens, Arc::new(markets));
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .map_err(|e| format!("could not listen on {addr}: {e}"))?;
    tracing::info!(%addr, "galata-markets serving");
    axum::serve(listener, app)
        .with_graceful_shutdown(stopped())
        .await
        .map_err(|e| e.to_string())?;
    tracing::info!("asked to stop; stopped cleanly");
    Ok(())
}

/// SIGTERM, as launchd stops a service and `container` passes it on, or
/// SIGINT, by hand.
async fn stopped() {
    let mut term = match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
        Ok(term) => term,
        Err(_) => {
            let _ = tokio::signal::ctrl_c().await;
            return;
        }
    };
    tokio::select! {
        _ = term.recv() => {}
        _ = tokio::signal::ctrl_c() => {}
    }
}
