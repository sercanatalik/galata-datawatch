//! The capture binary: load, assemble, run, exit.
//!
//! **The rules live in the library.** This file wires them together and does
//! nothing else — every invariant it depends on is asserted somewhere it can be
//! tested without a process.

use std::sync::Arc;

use galata_datawatch::adapters::{self, AdapterConfig};
use galata_datawatch::capture::{Capture, Clock, SystemClock, Wiring};
use galata_datawatch::config::{Adapters, Config};
use galata_datawatch::sink::NullSink;
use galata_datawatch::venue::Subscription;
use galata_wire::{Clipped, Ticker};

/// What the loader asks an adapter, answered without this file naming a venue.
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

fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    // One process per venue, and the venue comes from argv rather than a
    // second configuration field — so the process's identity cannot drift from
    // the venue it is capturing.
    let venue_name = std::env::args()
        .nth(1)
        .ok_or("usage: galata-datawatch <venue>. One process per venue.")?;

    let path =
        std::env::var("GALATA_CONFIG").unwrap_or_else(|_| "config/datawatch.toml".to_string());

    // One file, one type, one load. Anything absent, unparseable, unknown or
    // out of bounds refuses here and the process exits non-zero.
    let config = Config::load_from(std::path::Path::new(&path), &Resolver)?;
    let config_hash = config.hash();

    let venue = config
        .venue
        .get(&venue_name)
        .ok_or_else(|| format!("{venue_name} is not a venue this configuration declares"))?;

    let adapter = adapters::build(AdapterConfig::from_declared(&venue_name, venue)?)?;

    let declared: Vec<Subscription> = venue
        .instruments
        .iter()
        .flat_map(|instrument| {
            venue.series.iter().map(move |series| {
                Ticker::new(instrument.ticker.clone()).map(|ticker| Subscription {
                    ticker,
                    series: *series,
                })
            })
        })
        .collect::<Result<_, _>>()?;

    let runtime = tokio::runtime::Runtime::new()?;
    runtime.block_on(async move {
        let clock: Arc<dyn Clock> = Arc::new(SystemClock);
        let mut capture = Capture::new(Wiring {
            adapter,
            // No broker yet. The record does not depend on one, so running
            // without it is a supported state rather than a degraded one.
            sink: Arc::new(NullSink),
            clock,
            archive_root: config.paths.archive.clone(),
            status_dir: config.paths.status.clone(),
            flush_secs: config.capture.flush_secs,
            status_secs: config.capture.status_secs,
            declared,
            // This venue never closes. A venue with a calendar declares one,
            // and until it does an unknown overstates the loss rather than
            // erasing it.
            clipped: Clipped::Continuous,
            config_hash,
        });

        // What was not covered while this process was not running, published
        // before anything else — so the record never claims coverage it does
        // not have.
        let gaps = capture.report_restart_gap();
        if gaps > 0 {
            tracing::info!(gaps, "published the window this process was not covering");
        }

        let shutdown = tokio_util::sync::CancellationToken::new();
        let signal = shutdown.clone();
        tokio::spawn(async move {
            let _ = tokio::signal::ctrl_c().await;
            signal.cancel();
        });

        capture.run(shutdown).await
    })?;

    Ok(())
}
