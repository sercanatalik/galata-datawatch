//! Boot a capture process: load, assemble, run, exit.
//!
//! **The rules live in the modules below this one.** This is the wiring, and
//! it is here rather than in a binary because it does not depend on where the
//! configuration came from. Of its 253 lines exactly one named a source; every
//! other line reads `config`. A second binary that wanted a different source
//! could only have reached them by copying all 253, which would have put the
//! crypto provider, the subscriber, the argv venue rule, the universe
//! pre-check and the capture wiring in two places to vary one.
//!
//! So the source is an argument. [`boot`] is what both binaries call, and they
//! differ only in the [`crate::config::ConfigSource`] they hand it.

use std::sync::Arc;

use galata_broker::{BrokerIdentity, NatsPublisher, Publisher, Subject};
use galata_wire::{Clipped, Series, Ticker};

use crate::adapters::{self, AdapterConfig, History};
use crate::capture::{Capture, Clock, SystemClock, WalkInterval, WalkRequest, Wiring};
use crate::config::{Adapters, Config, ConfigSource, SecretSource};
use crate::sink::{NatsSink, NullSink, Outbound, Sink};
use crate::venue::Subscription;

/// Load, assemble, run, exit — from whatever source the caller hands it.
///
/// One process per venue, named by `argv[1]` rather than by a configuration
/// field, so the process's identity cannot drift from the venue it captures.
///
/// Returns when capture stops. Every refusal below — an unknown venue, a
/// configuration out of bounds, a universe the venue will not serve — happens
/// before anything connects.
pub fn boot(
    source: &dyn ConfigSource,
    secrets: &dyn SecretSource,
    adapters: &dyn Adapters,
) -> Result<(), Box<dyn std::error::Error>> {
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

    // **`--import <dir>`: a rescue, taken once, inside this boot.** Verified in
    // full here, before a runtime exists or anything is written: a bad page
    // refuses the whole directory, and the boot never publishes a gap it then
    // abandons. The pages are taken after the restart gap and before the walk.
    let rest: Vec<String> = std::env::args().skip(2).collect();
    let import_dir: Option<std::path::PathBuf> = match rest.as_slice() {
        [] => None,
        [flag, dir] if flag == "--import" => Some(std::path::PathBuf::from(dir)),
        _ => {
            return Err(format!(
                "usage: galata-datawatch <venue> [--import <dir>]; not understood: {}",
                rest.join(" ")
            )
            .into());
        }
    };
    let imported: Vec<crate::capture::ImportedPage> = match &import_dir {
        Some(dir) => crate::capture::verified_pages(dir, &venue_name)?,
        None => Vec::new(),
    };

    // One source, one type, one load. Anything absent, unparseable, unknown or
    // out of bounds refuses here and the process exits non-zero — and naming
    // two sources at once is itself a refusal, rather than a precedence rule
    // somebody has to know.
    let config = Config::load(source, adapters)?;
    let config_hash = config.hash();

    let venue = config
        .venue
        .get(&venue_name)
        .ok_or_else(|| format!("{venue_name} is not a venue this configuration declares"))?;

    // **Through the secret source this process was handed**, not the
    // environment's by name. Until `poll-a-venue` this read `&EnvSecrets`, so
    // under the vault binary a chain provider's URL — and now a signing
    // venue's keys — came from the process environment rather than the vault,
    // which is exactly where the vault exists to keep them from.
    let adapter_config = AdapterConfig::from_declared(&venue_name, venue, secrets)?;

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
    // Each declared instrument once — what a poll's gap is attributed to.
    let about: Vec<Ticker> = venue
        .instruments
        .iter()
        .map(|instrument| Ticker::new(instrument.ticker.clone()))
        .collect::<Result<_, _>>()?;
    let walk_config = adapter_config.clone();
    let walk_candles: Vec<String> = venue.walk_candles.clone();

    let runtime = tokio::runtime::Runtime::new()?;
    runtime.block_on(async move {
        // BEFORE anything connects. An unlisted coin is answered by a hang-up
        // rather than a refusal, and it takes every other subscription with
        // it — seventeen resets in eighteen seconds, measured, with a log that
        // reads like a network fault. One request per dex removes it.
        // **Built first, and asked nothing.** Construction is local, so the
        // declared walk widths are checked against the venue's own names below
        // before a single request is made.
        let adapter = adapters::build(adapter_config.clone())?;
        // Which history to walk, taken from the venue's own declaration: the
        // series it hands back on request, at the bar width it PUSHES — which
        // is the one width that may resume from the record, because live
        // capture keeps the record's receipt clock within seconds of it. A
        // width the stream does not push would have to state its own need. The
        // width is carried for every item and used where the series has one;
        // funding pages forward and has none.
        let streams = adapter.transport().is_stream();
        if !streams && !imported.is_empty() {
            return Err(format!(
                "{venue_name} is not a stream venue, and an import is taken beside a stream's walk"
            )
            .into());
        }
        // **Only a streaming venue has a bar width to resume from.** A chain
        // has none, and asking it for one before noticing that is how a
        // cursor venue got refused for not being a stream.
        let live_interval = if streams {
            adapter.live_interval_micros().ok_or_else(|| {
                "this venue pushes no bar width, so no walk may resume from the record".to_string()
            })?
        } else {
            // Unused: a cursor venue takes the branch below before any walk is
            // planned.
            0
        };
        // And the widths the walk FETCHES beside it (`walk_candles`), each for
        // the venue's whole reach. Refused here, before the universe check or
        // any socket, if the venue cannot name one: a walk for a width it
        // does not serve would spend the budget on refusals.
        let walk_items: Vec<(Series, WalkInterval)> = if streams {
            crate::capture::walk_items(
                adapter.declaration(),
                |label| adapter.interval_micros(label),
                &declared_series,
                live_interval,
                &walk_candles,
            )
            .map_err(|e| format!("[venue.{venue_name}] {e}"))?
        } else {
            Vec::new()
        };

        adapters::check_universe(&adapter_config).await?;

        // **The boot asymmetry.** A broker that is ABSENT is an outage the
        // record survives, so capture runs on a NullSink and says so. A broker
        // that REJECTS THE IDENTITY is a misconfiguration that will never fix
        // itself — and the status surface that would report it is a publish
        // too, so running on would mean archiving everything, publishing
        // nothing, and being unable to say so.
        let sink: Arc<dyn Sink> = match &config.broker {
            None => {
                tracing::info!(
                    "no broker is configured; events are recorded and not published. The record \
                     does not depend on one"
                );
                Arc::new(NullSink)
            }
            Some(broker) => {
                // **Through the one door.** `check-secret-reach.sh` refuses an
                // `env::var` for a secret anywhere but `config/source.rs`: one
                // call site is one place to get the logging wrong, and the
                // third would be added by somebody who did not read this.
                // **The caller's, not this function's.** Until 2026-09-23
                // this line named `EnvSecrets`, so a binary that fetched its
                // configuration FROM A VAULT still took its broker password
                // from the process environment — where `ps e`, a crash dump
                // and every child process can read it — and no caller could
                // say otherwise. `password_var` names where the password is;
                // what that name MEANS is the source's to decide.
                let password = secrets.secret(&broker.password_var)?;
                let identity = BrokerIdentity::new(
                    broker.user.clone(),
                    password.expose(),
                    broker.password_var.clone(),
                );
                match NatsPublisher::connect(&broker.url, &identity).await {
                    Ok(publisher) => {
                        let (tx, rx) = tokio::sync::mpsc::channel::<Outbound>(broker.queue);
                        tokio::spawn(publish_loop(publisher, rx));
                        tracing::info!(url = broker.url, user = broker.user, "publishing");
                        Arc::new(NatsSink::new(tx))
                    }
                    // Fatal: it will never fix itself.
                    Err(refusal) if refusal.is_fatal() => return Err(Box::from(refusal)),
                    Err(refusal) => {
                        tracing::warn!(
                            "{refusal}\n  Capture continues and the record is unaffected; \
                             events are not published until this is fixed"
                        );
                        Arc::new(NullSink)
                    }
                }
            }
        };

        let clock: Arc<dyn Clock> = Arc::new(SystemClock);
        let mut capture = Capture::new(Wiring {
            adapter,
            sink,
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
        let adapter_transport = capture.venue_transport();

        let gaps = capture.report_restart_gap();
        if gaps > 0 {
            tracing::info!(gaps, "published the window this process was not covering");
        }

        // **One arm per transport, matched.** This used to be "not a stream,
        // so a cursor" — and a poll venue would have been captured as a chain.
        // A match makes the next transport a compile error here rather than a
        // fall-through into one of these.
        match adapter_transport {
            crate::venue::Transport::Stream { .. } => {}
            // **A cursor venue is a different program.** No walk, no session,
            // no rotation: the cursor loop IS the backfill, because asking for
            // old blocks and asking for new ones is the same call at a
            // different position.
            crate::venue::Transport::Cursor { .. } => {
                // **The cursor loop is behind `rh-chain`**, because
                // `capture::cursor` is. Ungated, this line made `cargo build -p
                // galata-datawatch` fail on the crate's OWN default features,
                // and nothing noticed: the feature-matrix guard builds `--lib`
                // only, so the one target that could not compile was the one
                // target it never compiled.
                #[cfg(feature = "rh-chain")]
                {
                    let shutdown = tokio_util::sync::CancellationToken::new();
                    let signal = shutdown.clone();
                    tokio::spawn(async move {
                        shutdown_signal().await;
                        signal.cancel();
                    });
                    return capture
                        .run_cursor(
                            shutdown,
                            config.capture.cold_start_days as u64 * BLOCKS_PER_DAY,
                        )
                        .await
                        .map_err(Box::<dyn std::error::Error>::from);
                }
                // Refused by name rather than by a link error or, worse, by
                // falling through into the stream path and capturing a chain as
                // though it were a socket.
                #[cfg(not(feature = "rh-chain"))]
                {
                    return Err(format!(
                        "{venue_name} is a cursor venue and this build has no cursor \
                         loop. Rebuild with --features rh-chain."
                    )
                    .into());
                }
            }
            // **A poll venue asks on a timer, and serves no history** — so no
            // walk: the restart gap above is the whole of what it can say about
            // the time this process was not running.
            crate::venue::Transport::Poll { .. } => {
                let shutdown = tokio_util::sync::CancellationToken::new();
                let signal = shutdown.clone();
                tokio::spawn(async move {
                    shutdown_signal().await;
                    signal.cancel();
                });
                let polls = capture.run_polled(shutdown, &about).await?;
                tracing::info!("{}", polls.report());
                return Ok(());
            }
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

        // **The rescue, through the one path, after the restart gap and before
        // the walk.** Each page's receipt time is the clock as it is taken:
        // the moment this record received it. Its original fetch time stays in
        // the manifest; carrying it here would put an older receipt under a
        // newer sequence, and the tape's bound and replay assume the two agree.
        if let Some(dir) = &import_dir {
            let pages = imported.len();
            for page in imported {
                let at = SystemClock.now_micros();
                capture.take(history.candle_page(&page.symbol, page.bytes, at))?;
                tracing::info!(
                    file = page.file,
                    interval = page.interval,
                    "imported a rescued page"
                );
            }
            tracing::info!(pages, dir = %dir.display(), "imported a rescue through the one path");
        }

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
        let capped: Vec<&crate::capture::WalkOutcome> =
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

        // **The same fetch, paced and capped by the same request, for the gaps
        // this process publishes while running.** The walk above resumed from
        // the record's latest receipt and never looks behind it, so without
        // this a session lost mid-run keeps candles and funding the venue
        // would hand back missing until nobody remembers why.
        capture.fill_with(request, move |fetch| {
            let history = history.clone();
            let at = SystemClock.now_micros();
            async move { history.fetch(fetch, at).await }
        });
        // And the bars closed while running, where the operator asked: the
        // stream never sends one final, so without this nothing closes them.
        if let Some(secs) = config.capture.settle_secs {
            capture.settle_every(secs);
            tracing::info!(settle_secs = secs, "settling the bars closed while running");
        }

        let shutdown = tokio_util::sync::CancellationToken::new();
        let signal = shutdown.clone();
        tokio::spawn(async move {
            shutdown_signal().await;
            signal.cancel();
        });

        capture
            .run(shutdown)
            .await
            .map_err(Box::<dyn std::error::Error>::from)
    })?;

    Ok(())
}

/// Load, check, run the **ledger** for one venue — from whatever source the
/// caller hands it, as [`boot`] does for capture.
///
/// One process per venue, named by `argv[1]`. Every refusal — no `[ledger]`
/// block, a root others can read, an alias whose address changed, a history
/// that cannot be verified — happens before anything is asked of the venue.
///
/// **A separate process from capture on purpose** (D5): a ledger that fails
/// cannot stop the capture it runs beside, and capture is the P0.
#[cfg(feature = "ledger")]
pub fn boot_ledger(
    source: &dyn ConfigSource,
    secrets: &dyn SecretSource,
    adapters: &dyn Adapters,
) -> Result<(), Box<dyn std::error::Error>> {
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("no other crypto provider may already be installed");
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let venue_name = std::env::args()
        .nth(1)
        .ok_or("usage: galata-ledger <venue>. One process per venue.")?;
    let config = Config::load(source, adapters)?;
    let ledger = config
        .ledger
        .as_ref()
        .ok_or("the configuration declares no [ledger] block")?;
    // The network comes from the venue's own declaration, so the ledger and
    // capture cannot disagree about which one the accounts are on.
    let market = config
        .venue
        .get(&venue_name)
        .map(|v| v.market.clone())
        .ok_or_else(|| {
            format!(
                "[venue.{venue_name}] is not declared, and it names the network the ledger reads"
            )
        })?;

    crate::ledger::check_root(&ledger.root)?;
    let (key, masters) = crate::ledger::resolve(ledger, &venue_name, secrets)?;
    if masters.is_empty() {
        return Err(format!("no [ledger.account.*] names venue {venue_name}").into());
    }
    crate::ledger::check_fingerprints(&ledger.root, &masters)?;
    let bindings = crate::ledger::Bindings::read(&ledger.root, &venue_name)?;
    tracing::info!(
        venue = %venue_name,
        accounts = masters.len(),
        config = %config.hash(),
        "ledger starting"
    );

    // Declared, or `Config::validate` refused the ledger. Said again here so
    // no default can stand in for a tolerance nobody chose.
    let tolerance = |value: Option<f64>, key: &str| -> Result<galata_wire::Num, String> {
        let value = value.ok_or(format!("[ledger] {key} is not declared"))?;
        galata_wire::Num::try_from(value).map_err(|e| format!("[ledger] {key}: {e}"))
    };
    let fold_tolerances = crate::ledger::fold::Tolerances {
        position: tolerance(ledger.fold_position_tolerance, "fold_position_tolerance")?,
        relative: tolerance(ledger.fold_relative_tolerance, "fold_relative_tolerance")?,
    };
    // `Config::validate` already refused a ledger without it; said again here
    // so no default can stand in for it.
    let events_secs = ledger
        .events_secs
        .ok_or("[ledger] events_secs is not declared; the ledger invents no default")?;
    // A full page of fills or funding weighs its request plus one per 20
    // rows: 20 + 2,000 / 20 = 120 at worst. Paused between pages so a
    // catch-up walk spends no more than the ledger's share of the budget.
    let page_pause = match adapters::ledger_cost(&venue_name) {
        Some(cost) => {
            let allowed = (ledger.ledger_share * cost.budget_per_minute).max(1.0);
            ((cost.events + 100.0) / allowed * 60_000_000.0) as i64
        }
        None => 0,
    };
    let parts = adapters::LedgerParts {
        // Seeded by the clock, as capture's is: sequences must not collide
        // with the ones a previous run left on disk.
        archive: crate::record::Archive::open(&ledger.root)
            .from_seq(SystemClock.now_micros().max(0) as u64),
        // The ledger publishes nothing in this change; its record and its
        // status file are the surface.
        sink: Box::new(NullSink),
        key,
        cadences: crate::ledger::run::Cadences {
            snapshot_micros: ledger.snapshot_secs as i64 * 1_000_000,
            discover_micros: ledger.discover_secs as i64 * 1_000_000,
            events_micros: events_secs as i64 * 1_000_000,
            page_pause_micros: page_pause,
        },
        masters,
        bindings,
        status: crate::capture::StatusFile::named(
            &config.paths.status,
            &format!("ledger-{venue_name}"),
        ),
        fold: (
            crate::capture::StatusFile::named(
                &config.paths.status,
                &format!("ledger-fold-{venue_name}"),
            ),
            fold_tolerances,
        ),
    };

    let runtime = tokio::runtime::Runtime::new()?;
    runtime.block_on(async move {
        let shutdown = tokio_util::sync::CancellationToken::new();
        let signal = shutdown.clone();
        tokio::spawn(async move {
            shutdown_signal().await;
            signal.cancel();
        });
        adapters::run_ledger(&venue_name, &market, parts, shutdown).await
    })
}

/// Wait for a request to stop: an interrupt, **or a termination**.
///
/// SIGTERM is how a service manager stops a job — launchd (then SIGKILL after
/// 20 s), systemd, Docker. Waiting on SIGINT alone left SIGTERM's default
/// action in place, which ends the process at once: the buffer not yet
/// committed was lost, no clean-shutdown marker was written, and the next
/// start dated a planned stop as a kill. Reproduced under a scratch capture
/// before this was written.
async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        match signal(SignalKind::terminate()) {
            Ok(mut terminate) => {
                tokio::select! {
                    _ = tokio::signal::ctrl_c() => {}
                    _ = terminate.recv() => {
                        tracing::info!("asked to terminate; stopping cleanly");
                    }
                }
            }
            // Without a SIGTERM handler, an interrupt is still a clean stop.
            Err(error) => {
                tracing::warn!(%error, "no SIGTERM handler; only an interrupt stops cleanly");
                let _ = tokio::signal::ctrl_c().await;
            }
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

/// Drain the queue onto the bus.
///
/// **The only place that awaits a publish.** `NatsSink::emit` is synchronous
/// and hands over through a bounded channel, so a broker that is slow costs
/// dropped events and a counter — never a stalled capture loop.
async fn publish_loop(publisher: NatsPublisher, mut rx: tokio::sync::mpsc::Receiver<Outbound>) {
    let mut reachable = true;
    while let Some(outbound) = rx.recv().await {
        let sent = match &outbound {
            Outbound::Event(envelope) => {
                let Some(subject) = Subject::of(envelope) else {
                    // Addressed to a market rather than a venue. Nothing
                    // capture produces is, today; publishing it to an invented
                    // subject would be worse than declining to.
                    continue;
                };
                publisher.publish(&subject, envelope).await
            }
            // **Its own subject root**, so a market-data subscriber does not
            // receive snapshots and a dashboard takes `status.>` without also
            // taking the firehose.
            Outbound::Status { venue, json } => {
                publisher
                    .publish_status(&Subject::status(venue), json)
                    .await
            }
        };
        // No catch-all above. `Outbound` is `#[non_exhaustive]`, so a binary in
        // another crate needed one; inside the defining crate the compiler
        // requires exhaustiveness instead, and will name that match the day a
        // third variant appears — which is the better of the two.
        match sent {
            Ok(()) => {
                if !reachable {
                    tracing::info!("the broker is taking events again");
                    reachable = true;
                }
            }
            Err(error) => {
                // **On the edge, not per message.** A warning per event is what
                // buries a log, and the count is on the status surface.
                if reachable {
                    tracing::warn!(%error, "the broker refused; the record is unaffected");
                    reachable = false;
                }
            }
        }
    }
    // The channel closed, which means capture is shutting down. Push what the
    // client still holds: `publish` buffers, so a publish that returned is not
    // yet a publish that arrived.
    if let Err(error) = publisher.flush().await {
        tracing::warn!(%error, "the broker did not take the last of the buffer");
    }
}

/// Blocks a day holds on a chain making about nine a second.
///
/// **Measured, not documented**, and used only to turn a declared
/// `cold_start_days` into a block count — the one place this system converts
/// between the two units, and it says so.
#[cfg(feature = "rh-chain")]
const BLOCKS_PER_DAY: u64 = 9 * 60 * 60 * 24;
