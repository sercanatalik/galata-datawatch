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
    let walk_funding_days = venue.walk_funding_days;

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
        // **What follows from the set, beside it.** A venue of options
        // captures its underlyings' indexes, which nobody declares; they join
        // the declared subscriptions here, once, so everything after this
        // holds, counts and gaps them like any other.
        let mut declared = declared;
        declared.extend(adapter.implied_subscriptions());
        // A venue that serves no history has no walk, no bar width to resume
        // one from, and nothing to fill a gap with. It still streams.
        let serves_history = !adapter.declaration().historical.is_empty();
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
        let live_interval = if streams && serves_history {
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
        let walk_items: Vec<(Series, WalkInterval)> = if streams && serves_history {
            crate::capture::walk_items(
                adapter.declaration(),
                |label| adapter.interval_micros(label),
                &declared_series,
                live_interval,
                &walk_candles,
                walk_funding_days,
            )
            .map_err(|e| format!("[venue.{venue_name}] {e}"))?
        } else {
            Vec::new()
        };

        // The reference answers the check read, where checking the venue meant
        // reading its reference data. Taken through the one path below, so
        // the record holds what this boot was decided on.
        let universe = adapters::check_universe(&adapter_config, SystemClock.now_micros()).await?;

        // **One stop for the whole run**, made before the sink: a broker that
        // answers late and refuses the identity stops capture through it, as
        // SIGTERM does. The signal is attached just before each loop runs, as
        // it always was.
        let shutdown = tokio_util::sync::CancellationToken::new();
        let refused: Refused = Arc::default();

        // **The boot asymmetry.** A broker that is ABSENT is an outage the
        // record survives, so capture runs, says so, and keeps asking for it
        // in the background (`reach_broker`). A broker that REJECTS THE
        // IDENTITY is a misconfiguration that will never fix itself — and the
        // status surface that would report it is a publish too, so running on
        // would mean archiving everything, publishing nothing, and being
        // unable to say so.
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
                        let sink = NatsSink::new(tx);
                        let declarer =
                            Declarer::for_capture(adapter.venue(), &declared, sink.unpublished());
                        tokio::spawn(publish_loop(publisher, rx, declarer));
                        tracing::info!(url = broker.url, user = broker.user, "publishing");
                        Arc::new(sink)
                    }
                    // Fatal: it will never fix itself.
                    Err(refusal) if refusal.is_fatal() => return Err(Box::from(refusal)),
                    // **Asked again, not given up on.** Until 2026-11-25 this
                    // was a sink that published nothing until a restart.
                    Err(refusal) => {
                        tracing::warn!(
                            "{refusal}\n  Capture continues and the record is unaffected; events \
                             are not published until the broker answers, and it is asked again \
                             in the background"
                        );
                        let (tx, rx) = tokio::sync::mpsc::channel::<Outbound>(broker.queue);
                        let sink = NatsSink::new(tx);
                        let declarer =
                            Declarer::for_capture(adapter.venue(), &declared, sink.unpublished());
                        let url = broker.url.clone();
                        let user = broker.user.clone();
                        let connect_url = url.clone();
                        tokio::spawn(reach_broker(
                            move || {
                                let url = connect_url.clone();
                                let identity = identity.clone();
                                async move { NatsPublisher::connect(&url, &identity).await }
                            },
                            move |publisher, rx| async move {
                                tracing::info!(url, user, "publishing: the broker answered");
                                publish_loop(publisher, rx, declarer).await;
                            },
                            rx,
                            broker_backoff(),
                            shutdown.clone(),
                            refused.clone(),
                        ));
                        Arc::new(sink)
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

        // **Before the subscription**, as the walk is: an instrument the
        // venue's reference data says has expired leaves the set here, so the
        // first subscribe never names it.
        for page in universe.pages {
            capture.take(page)?;
        }
        // And what follows from the set that the venue does not have: never
        // subscribed, and listed on the status surface with why.
        for (ticker, reason) in &universe.absent {
            capture.retire(ticker, reason, capture.now());
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
                    let signal = shutdown.clone();
                    tokio::spawn(async move {
                        shutdown_signal().await;
                        signal.cancel();
                    });
                    let ran = capture
                        .run_cursor(
                            shutdown,
                            config.capture.cold_start_days as u64 * BLOCKS_PER_DAY,
                        )
                        .await
                        .map_err(Box::<dyn std::error::Error>::from);
                    broker_verdict(&refused)?;
                    return ran;
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
                let signal = shutdown.clone();
                tokio::spawn(async move {
                    shutdown_signal().await;
                    signal.cancel();
                });
                let polls = capture.run_polled(shutdown, &about).await;
                broker_verdict(&refused)?;
                tracing::info!("{}", polls?.report());
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
        // **The reference refresh, where the venue has one.** The boot's
        // answers were taken above; this asks again on the venue's own
        // interval, as its own task, so a delisting is learned while running.
        if let (Some(reference), Some(source)) = (
            capture.venue_reference(),
            adapters::ReferenceSource::for_config(&walk_config)?,
        ) {
            // The loop owns the clock; this is the binary's reading of it,
            // taken as each answer arrives.
            let now: Arc<dyn Fn() -> i64 + Send + Sync> = Arc::new(|| SystemClock.now_micros());
            capture.refresh_reference_with(reference.interval_micros, move |symbols| {
                let source = source.clone();
                let now = now.clone();
                async move { source.fetch(symbols, now).await }
            });
            tracing::info!(
                every_secs = reference.interval_micros / 1_000_000,
                "asking the venue for its reference data again while running"
            );
        }

        // **A venue with no history streams and nothing else.** No walk, no
        // fill and no settle: there is nothing the venue would hand back.
        if !serves_history {
            if import_dir.is_some() {
                return Err(format!(
                    "{venue_name} serves no history, and an import is taken beside a walk"
                )
                .into());
            }
            let shutdown = tokio_util::sync::CancellationToken::new();
            let signal = shutdown.clone();
            tokio::spawn(async move {
                shutdown_signal().await;
                signal.cancel();
            });
            return capture
                .run(shutdown)
                .await
                .map_err(Box::<dyn std::error::Error>::from);
        }

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

        // **Only the live width before the subscription.** It resumes from
        // the record, so it must precede the stream: a backfill must never be
        // mistaken for coverage the record already had. The widths that ask
        // for a stated need (`walk_candles`, a declared funding depth) never
        // resume from the record, and at the venue's weight they are 6 to 36
        // minutes of no quotes and no trades after every restart. They are
        // asked as history fills once the live loop runs
        // (`pace-by-the-venue-weight`).
        let (live_items, history_items): (Vec<_>, Vec<_>) = walk_items
            .into_iter()
            .partition(|(_, interval)| interval.is_live());
        let request = WalkRequest {
            items: live_items,
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
        // The fills know every width, so a settle and a gap on a declared
        // width are filled too, as before.
        let mut fill_request = request;
        fill_request.items.extend(history_items.iter().copied());
        capture.fill_with(fill_request, move |fetch| {
            let history = history.clone();
            let at = SystemClock.now_micros();
            async move { history.fetch(fetch, at).await }
        });
        capture.fill_history(&history_items);
        // And the bars closed while running, where the operator asked: the
        // stream never sends one final, so without this nothing closes them.
        if let Some(secs) = config.capture.settle_secs {
            capture.settle_every(secs);
            tracing::info!(settle_secs = secs, "settling the bars closed while running");
        }

        let signal = shutdown.clone();
        tokio::spawn(async move {
            shutdown_signal().await;
            signal.cancel();
        });

        let ran = capture
            .run(shutdown)
            .await
            .map_err(Box::<dyn std::error::Error>::from);
        // A broker that answered late and refused the identity stopped capture
        // through `shutdown`; its words are why the process exits non-zero.
        broker_verdict(&refused)?;
        ran
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
    // From here every refusal is the ledger's to state in its status before
    // it exits (`add-service-health-monitoring`, design D6): the venue and
    // the status folder are known.
    let status =
        crate::capture::StatusFile::named(&config.paths.status, &format!("ledger-{venue_name}"));
    let ran = run_ledger_for(&config, &venue_name, secrets);
    if let Err(error) = &ran {
        crate::ledger::run::record_stop(
            &status,
            &venue_name,
            &error.to_string(),
            SystemClock.now_micros(),
        );
    }
    ran
}

/// The ledger for one venue, from a loaded configuration, until it stops.
#[cfg(feature = "ledger")]
fn run_ledger_for(
    config: &Config,
    venue_name: &str,
    secrets: &dyn SecretSource,
) -> Result<(), Box<dyn std::error::Error>> {
    let venue_name = venue_name.to_string();
    let ledger = config
        .ledger
        .as_ref()
        .ok_or("the configuration declares no [ledger] block")?;
    // The network: the venue's own, where it is declared, so the ledger and
    // capture cannot disagree about which one the accounts are on; otherwise
    // the one its accounts name, which the load has checked is one and
    // present (design D7 of `add-derive-account-ledger`).
    let market = match config.venue.get(&venue_name) {
        Some(venue) => venue.market.clone(),
        None => ledger
            .account
            .values()
            .filter(|a| a.venue == venue_name)
            .find_map(|a| a.market.clone())
            .ok_or_else(|| {
                format!(
                    "[venue.{venue_name}] is not declared, and no [ledger.account.*] of it names \
                     its network (`market`)"
                )
            })?,
    };

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
        // Nothing inline: the ledger publishes from its record, past a cursor
        // the account stream acknowledged (`ledger::publish`).
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
        projection: ledger.tape.clone(),
        bus: None,
    };
    let mut parts = parts;
    // The ledger's own identity on the bus: it publishes its venue's accounts
    // and nothing else (`ledger-<venue>`, design D6), with a password of its
    // own, read through the one door as capture's is.
    let ledger_identity = match &config.broker {
        None => {
            tracing::info!(
                "no broker is configured; the ledger records its accounts and publishes none"
            );
            None
        }
        Some(broker) => {
            let user = galata_broker::ledger_identity(&venue_name);
            let var = galata_broker::password_var(&user);
            let password = secrets.secret(&var)?;
            Some((
                broker.url.clone(),
                BrokerIdentity::new(user, password.expose(), var),
            ))
        }
    };

    let runtime = tokio::runtime::Runtime::new()?;
    runtime.block_on(async move {
        if let Some((url, identity)) = ledger_identity {
            // A broker that refuses the identity is a misconfiguration that
            // will never fix itself: fatal, as capture's is. One that does not
            // answer is an outage the record survives: published once it does.
            let publisher = galata_broker::JetStreamPublisher::connect(
                &url,
                &identity,
                &galata_broker::ledger_inbox(&venue_name),
            )
            .await?;
            tracing::info!(url, user = identity.user, "publishing the accounts");
            parts.bus = Some(Box::new(LedgerBus(publisher)));
        }
        let shutdown = tokio_util::sync::CancellationToken::new();
        let signal = shutdown.clone();
        tokio::spawn(async move {
            shutdown_signal().await;
            signal.cancel();
        });
        adapters::run_ledger(&venue_name, &market, parts, shutdown).await
    })
}

/// The account stream, for a ledger: galata-broker's acknowledged publisher,
/// each envelope on its account's subject (`Subject::of`, a set under its
/// kind).
#[cfg(feature = "ledger")]
pub struct LedgerBus(pub galata_broker::JetStreamPublisher);

#[cfg(feature = "ledger")]
impl crate::ledger::publish::AccountBus for LedgerBus {
    fn publish_acked<'a>(
        &'a self,
        id: &'a str,
        envelope: &'a galata_wire::Envelope,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), String>> + Send + 'a>> {
        Box::pin(async move {
            use galata_broker::AccountPublisher as _;
            let subject =
                Subject::of(envelope).ok_or_else(|| "a row with no account subject".to_string())?;
            self.0
                .publish_acked(&subject, id, envelope)
                .await
                .map(|_| ())
                .map_err(|e| e.to_string())
        })
    }
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

/// The broker retry's waits: a second, doubling, and never more than a minute.
///
/// **No jitter**, unlike a venue's reconnect: jitter spreads a host's
/// processes across a venue's per-IP connection budget, and the broker is this
/// deployment's own, asked by one capture per venue. A jittered wait could also
/// pass the minute a broker that comes back may be left unasked.
fn broker_backoff() -> crate::source::Backoff {
    crate::source::Backoff::new(
        std::time::Duration::from_secs(1),
        std::time::Duration::from_secs(60),
        0.0,
    )
}

/// A broker refusal found after capture started, for boot to return once
/// capture has stopped.
type Refused = Arc<std::sync::Mutex<Option<galata_broker::ConnectRefusal>>>;

/// **Keep asking for a broker that did not answer at the start**, then publish.
///
/// Until 2026-11-25 a broker that did not answer at the start meant a sink
/// that published nothing for the rest of the process's life. Since the broker
/// is addressed by the host's tailnet name, that is every capture started while
/// the tailnet is down, and it stayed so after the tailnet came back.
///
/// Archiving never waits for this: the events capture produces meanwhile wait
/// in the bounded channel `rx` drains, and are dropped and counted when it is
/// full (`NatsSink`). Each attempt is classified as the start's was:
///
/// - **unreachable**: wait on `backoff` and ask again, silently — the start
///   said it once;
/// - **connected**: hand the publisher and the channel to `publish`;
/// - **refused the identity**: a misconfiguration that will never fix itself,
///   exactly as at the start. The refusal goes in `refused` and capture is
///   stopped through `stop`, so the process exits non-zero with the start's
///   words whether or not the tailnet was up when it began.
///
/// The connect and publish steps are arguments so that a test can be the
/// broker.
async fn reach_broker<P, C, CF, R, RF>(
    mut connect: C,
    publish: R,
    rx: tokio::sync::mpsc::Receiver<Outbound>,
    mut backoff: crate::source::Backoff,
    stop: tokio_util::sync::CancellationToken,
    refused: Refused,
) where
    C: FnMut() -> CF,
    CF: std::future::Future<Output = Result<P, galata_broker::ConnectRefusal>>,
    R: FnOnce(P, tokio::sync::mpsc::Receiver<Outbound>) -> RF,
    RF: std::future::Future<Output = ()>,
{
    loop {
        tokio::select! {
            // Capture is stopping for its own reasons: nothing left to publish.
            _ = stop.cancelled() => return,
            _ = tokio::time::sleep(backoff.next_wait()) => {}
        }
        match connect().await {
            Ok(publisher) => {
                publish(publisher, rx).await;
                return;
            }
            Err(refusal) if refusal.is_fatal() => {
                tracing::error!("{refusal}\n  Capture stops: it will not fix itself");
                if let Ok(mut slot) = refused.lock() {
                    *slot = Some(refusal);
                }
                stop.cancel();
                return;
            }
            Err(_) => {}
        }
    }
}

/// The refusal that stopped capture, if a broker answered late and refused.
fn broker_verdict(refused: &Refused) -> Result<(), Box<dyn std::error::Error>> {
    match refused.lock().ok().and_then(|mut slot| slot.take()) {
        Some(refusal) => Err(Box::from(refusal)),
        None => Ok(()),
    }
}

/// Who capture declares its unpublished spans for, and where they wait.
///
/// One declaration per pair it covers, over the whole span: a consumer of one
/// instrument meets the hole on that instrument's own subject.
struct Declarer {
    venue: galata_wire::Venue,
    pairs: Vec<(Ticker, Series)>,
    clipped: Clipped,
    unpublished: crate::sink::Unpublished,
}

impl Declarer {
    fn for_capture(
        venue: &galata_wire::Venue,
        declared: &[Subscription],
        unpublished: crate::sink::Unpublished,
    ) -> Declarer {
        Declarer {
            venue: venue.clone(),
            pairs: declared
                .iter()
                .map(|s| (s.ticker.clone(), s.series))
                .collect(),
            clipped: Clipped::Continuous,
            unpublished,
        }
    }
}

/// **Declare what the bus missed**, if anything: for every pair, a gap with
/// the cause `unpublished` over the pending span, straight to the bus and
/// never through the archive — the record holds the data; only the bus missed
/// it. A declaration that does not go out leaves the span pending, merged with
/// the next, so none is ever lost to make room for data.
async fn declare<P: Publisher + ?Sized>(publisher: &P, declarer: &Declarer) {
    let Some((from, to)) = declarer.unpublished.take() else {
        return;
    };
    for (ticker, series) in &declarer.pairs {
        let envelope = galata_wire::Envelope::new(
            declarer.venue.clone(),
            ticker.clone(),
            Some(from),
            to,
            galata_wire::Event::Gap(galata_wire::Gap {
                series: *series,
                from_micros: from,
                to_micros: to,
                cause: galata_wire::GapCause::Unpublished,
                clipped: declarer.clipped,
                dex: None,
            }),
        );
        let Some(subject) = Subject::of(&envelope) else {
            continue;
        };
        if publisher.publish(&subject, &envelope).await.is_err() {
            declarer.unpublished.merge((from, to));
            return;
        }
    }
    tracing::info!(
        from_micros = from,
        to_micros = to,
        pairs = declarer.pairs.len(),
        "declared on the bus what it did not carry; the record holds it"
    );
}

/// Drain the queue onto the bus, declaring what could not be published.
///
/// **The only place that awaits a publish.** `NatsSink::emit` is synchronous
/// and hands over through a bounded channel, so a broker that is slow costs
/// dropped events and a counter — never a stalled capture loop.
///
/// **What it drops, and what a publish refuses, is declared where the hole
/// is**: just before the first event received after the span, or, if none
/// comes, once a publish succeeds with the queue empty. The queue's backlog
/// was received before the span, so it is published before the declaration,
/// and a consumer reading in order meets the declaration between the last
/// event before the hole and the first after it. Declared as soon as one
/// publish succeeded, it was stored ahead of that backlog (`design/measured.md`,
/// "The broker stopped for four minutes, capture publishing").
async fn drain<P: Publisher + ?Sized>(
    publisher: &P,
    rx: &mut tokio::sync::mpsc::Receiver<Outbound>,
    declarer: &Declarer,
) {
    let mut reachable = true;
    while let Some(outbound) = rx.recv().await {
        // Tried even while the broker refuses: a declaration that does not go
        // out stays pending, and one that does is never behind an event newer
        // than its span.
        if let Outbound::Event(envelope) = &outbound
            && declarer.unpublished.ends_before(envelope.recv_micros)
        {
            declare(publisher, declarer).await;
        }
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
                // Nothing queued behind it: nothing received before the span
                // is still to come, and no later event may arrive soon.
                if rx.is_empty() {
                    declare(publisher, declarer).await;
                }
            }
            Err(error) => {
                if let Outbound::Event(envelope) = &outbound {
                    declarer.unpublished.note(envelope.recv_micros);
                }
                // **On the edge, not per message.** A warning per event is what
                // buries a log, and the count is on the status surface.
                if reachable {
                    tracing::warn!(%error, "the broker refused; the record is unaffected");
                    reachable = false;
                }
            }
        }
    }
}

/// [`drain`] onto the real broker, then push what the client still holds.
async fn publish_loop(
    publisher: NatsPublisher,
    mut rx: tokio::sync::mpsc::Receiver<Outbound>,
    declarer: Declarer,
) {
    drain(&publisher, &mut rx, &declarer).await;
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

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use galata_broker::ConnectRefusal;

    use super::*;

    fn unreachable() -> ConnectRefusal {
        ConnectRefusal::Unreachable {
            addr: "nats://mac.example.ts.net:4222".into(),
            reason: "DNS error: failed to lookup address information".into(),
        }
    }

    fn rejected() -> ConnectRefusal {
        ConnectRefusal::Rejected {
            addr: "nats://mac.example.ts.net:4222".into(),
            identity: "datawatch-hyperliquid".into(),
            var: "GALATA_BROKER_PASSWORD_DATAWATCH_HYPERLIQUID".into(),
            reason: "Authorization Violation".into(),
        }
    }

    /// Milliseconds rather than the deployment's seconds: the same doubling.
    fn quick() -> crate::source::Backoff {
        crate::source::Backoff::new(Duration::from_millis(1), Duration::from_millis(4), 0.0)
    }

    #[tokio::test]
    async fn a_broker_that_answers_late_is_handed_what_was_queued_meanwhile() {
        // Capture archived and emitted while the broker was away; the bounded
        // queue held it, and the broker gets it once it answers.
        let (tx, rx) = tokio::sync::mpsc::channel::<Outbound>(8);
        let venue = galata_wire::Venue::new("hyperliquid").unwrap();
        for n in 0..3u8 {
            tx.try_send(Outbound::Status {
                venue: venue.clone(),
                json: vec![n],
            })
            .unwrap();
        }
        drop(tx);

        let attempts = Arc::new(AtomicUsize::new(0));
        let published = Arc::new(std::sync::Mutex::new(Vec::new()));
        let stop = tokio_util::sync::CancellationToken::new();
        let refused: Refused = Arc::default();
        let (counted, into) = (attempts.clone(), published.clone());
        reach_broker(
            move || {
                let attempt = counted.fetch_add(1, Ordering::SeqCst) + 1;
                async move {
                    if attempt < 3 {
                        Err(unreachable())
                    } else {
                        Ok(())
                    }
                }
            },
            move |(), mut rx: tokio::sync::mpsc::Receiver<Outbound>| async move {
                while let Some(outbound) = rx.recv().await {
                    into.lock().unwrap().push(outbound);
                }
            },
            rx,
            quick(),
            stop.clone(),
            refused.clone(),
        )
        .await;

        assert_eq!(
            attempts.load(Ordering::SeqCst),
            3,
            "twice unreachable, then connected"
        );
        assert_eq!(
            published.lock().unwrap().len(),
            3,
            "everything queued while it was away"
        );
        assert!(
            !stop.is_cancelled(),
            "a broker that was only away does not stop capture"
        );
        assert!(broker_verdict(&refused).is_ok());
    }

    #[tokio::test]
    async fn a_broker_that_answers_late_and_refuses_stops_capture_with_the_starts_words() {
        // The same refusal at the start would have stopped capture starting;
        // whether the tailnet was up at that moment no longer decides it.
        let (_tx, rx) = tokio::sync::mpsc::channel::<Outbound>(8);
        let attempts = Arc::new(AtomicUsize::new(0));
        let stop = tokio_util::sync::CancellationToken::new();
        let refused: Refused = Arc::default();
        let counted = attempts.clone();
        reach_broker(
            move || {
                let attempt = counted.fetch_add(1, Ordering::SeqCst) + 1;
                async move {
                    if attempt < 2 {
                        Err::<(), _>(unreachable())
                    } else {
                        Err(rejected())
                    }
                }
            },
            |(), _rx| async move { panic!("a broker that refused is never published to") },
            rx,
            quick(),
            stop.clone(),
            refused.clone(),
        )
        .await;

        assert!(stop.is_cancelled(), "capture is stopped");
        let said = broker_verdict(&refused).unwrap_err().to_string();
        assert_eq!(
            said,
            rejected().to_string(),
            "the words a refusal at the start uses"
        );
        assert!(said.contains("datawatch-hyperliquid"));
        assert!(said.contains("GALATA_BROKER_PASSWORD_DATAWATCH_HYPERLIQUID"));
    }

    #[tokio::test]
    async fn a_capture_that_stops_stops_asking() {
        let (_tx, rx) = tokio::sync::mpsc::channel::<Outbound>(8);
        let stop = tokio_util::sync::CancellationToken::new();
        stop.cancel();
        let attempts = Arc::new(AtomicUsize::new(0));
        let counted = attempts.clone();
        reach_broker(
            move || {
                counted.fetch_add(1, Ordering::SeqCst);
                async { Err::<(), _>(unreachable()) }
            },
            |(), _rx| async {},
            rx,
            crate::source::Backoff::new(
                Duration::from_secs(3_600),
                Duration::from_secs(3_600),
                0.0,
            ),
            stop,
            Arc::default(),
        )
        .await;
        assert_eq!(attempts.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn the_broker_is_asked_again_within_a_minute_at_most() {
        let mut backoff = broker_backoff();
        let waits: Vec<u64> = (0..12).map(|_| backoff.next_wait().as_secs()).collect();
        assert_eq!(waits, [1, 2, 4, 8, 16, 32, 60, 60, 60, 60, 60, 60]);
    }

    // ---- capture declares what it did not publish ------------------------------

    use std::pin::Pin;
    use std::sync::Mutex;

    use galata_broker::PublishError;
    use galata_wire::{Envelope, Event, GapCause, Quote};

    use crate::sink::{NatsSink, Sink};

    /// A broker that records what it is sent, and refuses on request.
    #[derive(Default)]
    struct Recording {
        sent: Mutex<Vec<(String, Envelope)>>,
        refuse_events: Mutex<bool>,
        refuse_gaps: Mutex<bool>,
    }

    impl Recording {
        fn gaps(&self) -> Vec<(String, galata_wire::Gap)> {
            self.sent
                .lock()
                .unwrap()
                .iter()
                .filter_map(|(subject, e)| match &e.event {
                    Event::Gap(g) => Some((subject.clone(), g.clone())),
                    _ => None,
                })
                .collect()
        }
        fn events(&self) -> usize {
            self.sent
                .lock()
                .unwrap()
                .iter()
                .filter(|(_, e)| !matches!(e.event, Event::Gap(_)))
                .count()
        }
        /// What was sent, in order: an event by its receipt time, a
        /// declaration as `None`.
        fn sequence(&self) -> Vec<Option<i64>> {
            self.sent
                .lock()
                .unwrap()
                .iter()
                .map(|(_, e)| match e.event {
                    Event::Gap(_) => None,
                    _ => Some(e.recv_micros),
                })
                .collect()
        }
        /// A broker that refuses refuses everything, declarations included.
        fn refuse(&self, refused: bool) {
            *self.refuse_events.lock().unwrap() = refused;
            *self.refuse_gaps.lock().unwrap() = refused;
        }
    }

    impl Publisher for Recording {
        fn publish<'a>(
            &'a self,
            subject: &'a Subject,
            envelope: &'a Envelope,
        ) -> Pin<Box<dyn std::future::Future<Output = Result<(), PublishError>> + Send + 'a>>
        {
            Box::pin(async move {
                let is_gap = matches!(envelope.event, Event::Gap(_));
                let refused = if is_gap {
                    *self.refuse_gaps.lock().unwrap()
                } else {
                    *self.refuse_events.lock().unwrap()
                };
                if refused {
                    return Err(PublishError::Failed {
                        subject: subject.as_str().to_string(),
                        reason: "refused by the test".into(),
                    });
                }
                self.sent
                    .lock()
                    .unwrap()
                    .push((subject.as_str().to_string(), envelope.clone()));
                Ok(())
            })
        }
        fn publish_status<'a>(
            &'a self,
            _subject: &'a Subject,
            _body: &'a [u8],
        ) -> Pin<Box<dyn std::future::Future<Output = Result<(), PublishError>> + Send + 'a>>
        {
            Box::pin(async { Ok(()) })
        }
    }

    fn venue() -> galata_wire::Venue {
        galata_wire::Venue::new("hyperliquid").unwrap()
    }

    fn quote(recv_micros: i64) -> Envelope {
        Envelope::new(
            venue(),
            galata_wire::Ticker::new("BTC").unwrap(),
            Some(recv_micros),
            recv_micros,
            Event::Quote(Quote {
                bid_px: None,
                ask_px: None,
                bid_sz: None,
                ask_sz: None,
                bid_spread: None,
                ask_spread: None,
            }),
        )
    }

    fn declared() -> Vec<Subscription> {
        ["BTC", "ETH"]
            .into_iter()
            .map(|t| Subscription {
                ticker: galata_wire::Ticker::new(t).unwrap(),
                series: Series::Quotes,
            })
            .collect()
    }

    #[tokio::test]
    async fn an_overflow_is_declared_for_every_pair_over_exactly_the_dropped_span() {
        let (tx, mut rx) = tokio::sync::mpsc::channel::<Outbound>(2);
        let sink = NatsSink::new(tx);
        let declarer = Declarer::for_capture(&venue(), &declared(), sink.unpublished());
        // The broker is away: the queue holds two, and 3, 4 and 5 are dropped.
        for recv in 1..=5 {
            let _ = sink.emit(&quote(recv));
        }
        drop(sink);
        let broker = Recording::default();
        drain(&broker, &mut rx, &declarer).await;

        assert_eq!(broker.events(), 2, "the two the queue held");
        let gaps = broker.gaps();
        assert_eq!(gaps.len(), 2, "one declaration per pair: {gaps:?}");
        for (subject, gap) in &gaps {
            assert!(
                subject.ends_with(".quotes") || subject.ends_with(".gaps"),
                "{subject}"
            );
            assert_eq!(gap.cause, GapCause::Unpublished);
            assert_eq!(
                (gap.from_micros, gap.to_micros),
                (3, 5),
                "exactly the dropped receipt times"
            );
        }
        let subjects: Vec<&str> = gaps.iter().map(|(s, _)| s.as_str()).collect();
        assert_eq!(
            subjects,
            [
                "markets.hyperliquid.BTC.gaps",
                "markets.hyperliquid.ETH.gaps"
            ]
        );
    }

    #[tokio::test]
    async fn the_record_holds_every_event_and_no_declaration() {
        // Capture's own path: durable in the archive, then offered to a queue
        // of one while the broker is away. The declaration goes to the bus
        // past the full queue, and never into the record.
        let root = tempfile::tempdir().unwrap();
        let mut archive = crate::record::Archive::open(root.path());
        let (tx, mut rx) = tokio::sync::mpsc::channel::<Outbound>(1);
        let sink = NatsSink::new(tx);
        let declarer = Declarer::for_capture(&venue(), &declared(), sink.unpublished());
        for recv in 1..=4 {
            crate::ingest::record_generated(&mut archive, &sink, "hyperliquid", quote(recv))
                .unwrap();
        }
        archive.flush().unwrap();
        drop(sink);
        let broker = Recording::default();
        drain(&broker, &mut rx, &declarer).await;

        let spans: Vec<(i64, i64)> = broker
            .gaps()
            .iter()
            .map(|(_, g)| (g.from_micros, g.to_micros))
            .collect();
        assert_eq!(
            spans,
            [(2, 4), (2, 4)],
            "declared, though the queue was full"
        );
        let record = crate::replay::read_all(root.path()).unwrap();
        assert_eq!(record.len(), 4, "every event is in the record");
        for replayed in &record {
            let body = String::from_utf8_lossy(&replayed.payload().payload);
            assert!(
                !body.contains("unpublished"),
                "a declaration in the record: {body}"
            );
        }
    }

    #[tokio::test]
    async fn nothing_dropped_is_nothing_declared() {
        let (tx, mut rx) = tokio::sync::mpsc::channel::<Outbound>(16);
        let sink = NatsSink::new(tx);
        let declarer = Declarer::for_capture(&venue(), &declared(), sink.unpublished());
        for recv in 1..=10 {
            sink.emit(&quote(recv)).unwrap();
        }
        drop(sink);
        let broker = Recording::default();
        drain(&broker, &mut rx, &declarer).await;
        assert_eq!(broker.events(), 10);
        assert!(broker.gaps().is_empty());
    }

    #[tokio::test]
    async fn two_outages_are_two_spans_each_its_own() {
        let unpublished = crate::sink::Unpublished::default();
        let declarer = Declarer::for_capture(&venue(), &declared(), unpublished.clone());
        let broker = Recording::default();
        unpublished.note(3);
        unpublished.note(5);
        declare(&broker, &declarer).await;
        unpublished.note(10);
        unpublished.note(12);
        declare(&broker, &declarer).await;
        let spans: Vec<(i64, i64)> = broker
            .gaps()
            .iter()
            .map(|(_, g)| (g.from_micros, g.to_micros))
            .collect();
        assert_eq!(
            spans,
            [(3, 5), (3, 5), (10, 12), (10, 12)],
            "per pair, per outage"
        );
    }

    #[tokio::test]
    async fn the_declaration_is_stored_between_the_backlog_and_what_came_after() {
        // What stopping the broker for four minutes showed: when it answers
        // again, the queue still holds what was received before the span. The
        // declaration goes after that backlog, and before the first event
        // received after the span.
        let (tx, mut rx) = tokio::sync::mpsc::channel::<Outbound>(16);
        let sink = NatsSink::new(tx);
        let unpublished = sink.unpublished();
        let declarer = Declarer::for_capture(&venue(), &declared(), unpublished.clone());
        for recv in [1, 2] {
            sink.emit(&quote(recv)).unwrap();
        }
        unpublished.note(3);
        unpublished.note(5);
        for recv in [6, 7] {
            sink.emit(&quote(recv)).unwrap();
        }
        drop(sink);
        let broker = Recording::default();
        drain(&broker, &mut rx, &declarer).await;

        assert_eq!(
            broker.sequence(),
            [Some(1), Some(2), None, None, Some(6), Some(7)],
            "the backlog, the declaration for each pair, then what came after"
        );
        let spans: Vec<(i64, i64)> = broker
            .gaps()
            .iter()
            .map(|(_, g)| (g.from_micros, g.to_micros))
            .collect();
        assert_eq!(spans, [(3, 5), (3, 5)]);
    }

    #[tokio::test]
    async fn a_span_with_nothing_after_it_is_declared_once_the_queue_is_empty() {
        let (tx, mut rx) = tokio::sync::mpsc::channel::<Outbound>(16);
        let sink = NatsSink::new(tx);
        let unpublished = sink.unpublished();
        let declarer = Declarer::for_capture(&venue(), &declared(), unpublished.clone());
        for recv in [1, 2] {
            sink.emit(&quote(recv)).unwrap();
        }
        unpublished.note(3);
        unpublished.note(4);
        drop(sink);
        let broker = Recording::default();
        drain(&broker, &mut rx, &declarer).await;
        assert_eq!(
            broker.sequence(),
            [Some(1), Some(2), None, None],
            "declared after the backlog, with no later event to wait for"
        );
    }

    #[tokio::test]
    async fn a_refused_publish_is_declared_before_the_first_event_taken_again() {
        let (tx, mut rx) = tokio::sync::mpsc::channel::<Outbound>(16);
        let sink = NatsSink::new(tx);
        let declarer = Declarer::for_capture(&venue(), &declared(), sink.unpublished());
        let broker = Recording::default();
        broker.refuse(true);
        for recv in [7, 8] {
            sink.emit(&quote(recv)).unwrap();
        }
        // Refused while the broker says no; then it takes events again.
        let first = rx.recv().await.unwrap();
        let second = rx.recv().await.unwrap();
        let (tx2, mut rx2) = tokio::sync::mpsc::channel::<Outbound>(16);
        tx2.send(first).await.unwrap();
        tx2.send(second).await.unwrap();
        drop(tx2);
        drain(&broker, &mut rx2, &declarer).await;
        assert!(
            broker.sequence().is_empty(),
            "nothing went out, so nothing is declared yet"
        );
        broker.refuse(false);
        sink.emit(&quote(9)).unwrap();
        drop(sink);
        drain(&broker, &mut rx, &declarer).await;
        assert_eq!(
            broker.sequence(),
            [None, None, Some(9)],
            "declared ahead of the first event taken again"
        );
        let spans: Vec<(i64, i64)> = broker
            .gaps()
            .iter()
            .map(|(_, g)| (g.from_micros, g.to_micros))
            .collect();
        assert_eq!(spans, [(7, 8), (7, 8)]);
    }

    #[tokio::test]
    async fn a_declaration_that_does_not_go_out_is_kept_and_merged() {
        let unpublished = crate::sink::Unpublished::default();
        let declarer = Declarer::for_capture(&venue(), &declared(), unpublished.clone());
        let broker = Recording::default();
        unpublished.note(3);
        unpublished.note(5);
        *broker.refuse_gaps.lock().unwrap() = true;
        declare(&broker, &declarer).await;
        assert!(broker.gaps().is_empty());
        unpublished.note(8);
        *broker.refuse_gaps.lock().unwrap() = false;
        declare(&broker, &declarer).await;
        let spans: Vec<(i64, i64)> = broker
            .gaps()
            .iter()
            .map(|(_, g)| (g.from_micros, g.to_micros))
            .collect();
        assert_eq!(
            spans,
            [(3, 8), (3, 8)],
            "never dropped: merged with what came after"
        );
    }
}
