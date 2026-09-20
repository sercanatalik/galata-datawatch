//! The capture binary: load, assemble, run, exit.
//!
//! **The rules live in the library.** This file wires them together and does
//! nothing else — every invariant it depends on is asserted somewhere it can be
//! tested without a process.

use std::sync::Arc;

use galata_datawatch::adapters::{self, AdapterConfig, History};
use galata_datawatch::capture::{Capture, Clock, SystemClock, WalkInterval, WalkRequest, Wiring};
use galata_datawatch::config::{Adapters, Config};
use galata_datawatch::sink::NullSink;
use galata_datawatch::venue::Subscription;
use galata_wire::{Clipped, Series, Ticker};

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
    // **One crypto provider, installed explicitly, before anything can build a
    // client.**
    //
    // rustls 0.23 is provider-agnostic and refuses to guess. The websocket path
    // gets away without this because exactly one provider feature is enabled in
    // the graph and rustls can infer it; `reqwest` with `rustls-no-provider`
    // cannot, and says so by panicking when a `Client` is built — at runtime,
    // on the first request, not at compile time.
    //
    // `ring` rather than aws-lc: aws-lc wants cmake and NASM at build time,
    // which a published crate should not require of its consumers, and two
    // providers in one process is the failure this line exists to prevent.
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("no other crypto provider may already be installed");

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

    let adapter_config = AdapterConfig::from_declared(&venue_name, venue)?;

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

    let declared_series: Vec<Series> = venue.series.clone();
    let walk_config = adapter_config.clone();

    let runtime = tokio::runtime::Runtime::new()?;
    runtime.block_on(async move {
        // BEFORE anything connects. An unlisted coin is answered by a hang-up
        // rather than a refusal, and it takes every other subscription with
        // it — seventeen resets in eighteen seconds, measured, with a log that
        // reads like a network fault. One request per dex removes it.
        adapters::check_universe(&adapter_config).await?;
        let adapter = adapters::build(adapter_config)?;

        // Which history to walk, taken from the venue's own declaration: the
        // series it hands back on request, at the bar width it PUSHES — which
        // is the one width that may resume from the record, because live
        // capture keeps the record's receipt clock within seconds of it. A
        // width the stream does not push would have to state its own need. The
        // width is carried for every item and used where the series has one;
        // funding pages forward and has none.
        let live_interval = adapter.live_interval_micros().ok_or_else(|| {
            "this venue pushes no bar width, so no walk may resume from the record".to_string()
        })?;
        let walk_items: Vec<(Series, WalkInterval)> = declared_series
            .iter()
            .filter(|series| adapter.declaration().serves_historically(**series))
            .map(|series| (*series, WalkInterval::live(live_interval)))
            .collect();

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

        // The history, before the live loop and after the restart gap — so a
        // backfill is never mistaken for coverage the record already had, and
        // so the walk's own requests are paced against a venue nothing else is
        // yet talking to.
        //
        // Every page crosses the one path. The walk publishes its status on the
        // usual timer while it runs, because a backfill of several hundred
        // requests is minutes of work and a silent process is indistinguishable
        // from a stuck one.
        let history = History::for_config(&walk_config)?;
        let request = WalkRequest {
            items: walk_items,
            share: config.capture.walk_share,
            cold_start_days: config.capture.cold_start_days,
            cap: config.capture.walk_cap,
        };
        let outcomes = capture
            .walk(&request, |fetch| {
                let history = history.clone();
                // The loop owns the clock, and it is read here because the
                // payload's receipt time is a fact about when bytes arrived.
                let at = SystemClock.now_micros();
                async move { history.fetch(fetch, at).await }
            })
            .await?;

        // **A run that covered less than it was asked for is not a green one.**
        // The venue's own reach never reaches here — that is a stated fact and
        // exits zero. Our cap does, because our cap is ours to raise.
        let capped: Vec<&galata_datawatch::capture::WalkOutcome> =
            outcomes.iter().filter(|o| o.capped).collect();
        if !capped.is_empty() {
            for outcome in &capped {
                tracing::error!("{}", outcome.report());
            }
            return Err(format!(
                "{} of {} walks were truncated by capture.walk_cap = {}; raise it or accept the \
                 shorter history explicitly",
                capped.len(),
                outcomes.len(),
                config.capture.walk_cap
            )
            .into());
        }

        let shutdown = tokio_util::sync::CancellationToken::new();
        let signal = shutdown.clone();
        tokio::spawn(async move {
            let _ = tokio::signal::ctrl_c().await;
            signal.cancel();
        });

        capture
            .run(shutdown)
            .await
            .map_err(Box::<dyn std::error::Error>::from)
    })?;

    Ok(())
}
