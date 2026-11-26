//! **A declared hole, filled from the record** (`market-live-stream`, design
//! D6).
//!
//! When the bus missed a span, capture declares it on each pair's `.gaps`
//! subject, stored where the hole is. The fill for that span is
//!
//! ```text
//!   the archive's payloads received in [from, to], normalised as capture
//!   normalised them (same sequence, same receipt time)
//!   less what the window holds for the same receipt span, by
//!   (sequence, ticker, dataset)
//! ```
//!
//! — a function of the record and the window, not of what one connection
//! happened to send, so a client resuming from before the declaration receives
//! the same fill. It is computed once per venue and span and shared.
//!
//! **It waits for the record**: segments are written every few seconds, so a
//! fill waits up to [`FILL_WAIT`] for the archive to hold something received
//! after the span. **It is capped** at [`FILL_CAP`] events: a span the record
//! cannot fill in time, or one too large, passes the declaration through, and
//! the client reads the span from history once it is projected.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_nats::jetstream::{self, consumer::DeliverPolicy, consumer::pull::OrderedConfig};
use futures_util::StreamExt;
use galata_broker::streams::MARKET_STREAM;
use galata_datawatch::adapters::{self, AdapterConfig};
use galata_datawatch::config::Config;
use galata_datawatch::ingest::normalise_replayed;
use galata_datawatch::normalise::Normalise;
use galata_datawatch::replay;
use galata_datawatch::sink::{Sink, SinkError};
use galata_datawatch::venue::Adapter;
use galata_wire::{Envelope, Event, Kind};

/// How long a fill waits for the record to reach the span's end.
pub const FILL_WAIT: Duration = Duration::from_secs(10);

/// The most events one fill holds (design D6, proposed until measured).
pub const FILL_CAP: usize = 500_000;

/// How far before a span the window is read for what it holds: the broker's
/// clock and capture's differ by a fraction of a second.
const SKEW_MARGIN_MICROS: i64 = 5_000_000;

/// Each venue's normaliser, built for replay: no network and no secret.
#[derive(Clone, Default)]
pub struct Normalisers(Arc<BTreeMap<String, Arc<dyn Adapter>>>);

impl std::fmt::Debug for Normalisers {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("Normalisers")
            .field(&self.0.keys().collect::<Vec<_>>())
            .finish()
    }
}

impl Normalisers {
    /// Every venue a configuration declares, as the tape rebuild builds them.
    pub fn from_config(config: &Config) -> Result<Normalisers, String> {
        let mut built = BTreeMap::new();
        for (name, venue) in &config.venue {
            let adapter = AdapterConfig::for_replay(name, venue)
                .and_then(adapters::build)
                .map_err(|e| format!("{name}: {e}"))?;
            built.insert(name.clone(), Arc::from(adapter));
        }
        Ok(Normalisers(Arc::new(built)))
    }

    /// Normalisers already built, by venue.
    pub fn of(adapters: Vec<(String, Box<dyn Adapter>)>) -> Normalisers {
        Normalisers(Arc::new(
            adapters
                .into_iter()
                .map(|(v, a)| (v, Arc::from(a)))
                .collect(),
        ))
    }

    fn get(&self, venue: &str) -> Option<Arc<dyn Adapter>> {
        self.0.get(venue).cloned()
    }
}

/// One event of a fill.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FillEvent {
    /// The archive sequence it came from.
    pub seq: u64,
    /// Its ticker.
    pub ticker: String,
    /// Its dataset.
    pub kind: Kind,
    /// When capture received it.
    pub recv_micros: i64,
    /// The envelope, as capture would have published it.
    pub data: String,
}

/// A fill, or why there is none.
pub type Filled = Arc<Result<Vec<FillEvent>, String>>;

/// Fills by venue and span, shared by every connection.
#[derive(Debug, Clone, Default)]
pub struct FillCache(Arc<tokio::sync::Mutex<FillMap>>);

/// Fills by (venue, from, to).
type FillMap = HashMap<(String, i64, i64), Filled>;

/// The settings a fill is made under.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FillLimits {
    /// How long to wait for the record.
    pub wait: Duration,
    /// The most events.
    pub cap: usize,
}

impl Default for FillLimits {
    fn default() -> FillLimits {
        FillLimits {
            wait: FILL_WAIT,
            cap: FILL_CAP,
        }
    }
}

/// A sink that keeps what it is given.
#[derive(Default)]
struct Collect(Mutex<Vec<Envelope>>);

impl Sink for Collect {
    fn emit(&self, envelope: &Envelope) -> Result<(), SinkError> {
        if let Ok(mut held) = self.0.lock() {
            held.push(envelope.clone());
        }
        Ok(())
    }
}

/// Whether the record already holds a payload received after `to`.
pub fn reaches(archive: &Path, venue: &str, to: i64) -> bool {
    let scope = format!("venue={venue}");
    replay::stream_range(archive, Some(&[scope.as_str()]), to + 1, to + 600_000_000)
        .next()
        .is_some_and(|r| r.is_ok())
}

/// **The record's market events for a venue over a receipt span**, normalised
/// as capture normalised them.
pub fn from_record(
    archive: &Path,
    normaliser: &dyn Normalise,
    venue: &str,
    from: i64,
    to: i64,
    cap: usize,
) -> Result<Vec<Envelope>, String> {
    let scope = format!("venue={venue}");
    let sink = Collect::default();
    for replayed in replay::stream_range(archive, Some(&[scope.as_str()]), from, to + 1) {
        let replayed = replayed.map_err(|e| e.to_string())?;
        normalise_replayed(normaliser, &sink, replayed).map_err(|e| e.to_string())?;
        let held = sink.0.lock().map(|h| h.len()).unwrap_or(0);
        if held > cap {
            return Err(format!(
                "the span holds more than {cap} events, too many to fill in place"
            ));
        }
    }
    let held = sink.0.into_inner().map_err(|e| e.to_string())?;
    Ok(held
        .into_iter()
        .filter(|e| (from..=to).contains(&e.recv_micros))
        .filter(|e| !matches!(e.event, Event::Gap(_) | Event::Unparsed(_)))
        .collect())
}

/// What the window holds for a venue over a receipt span, by (sequence,
/// ticker, dataset): everything published before the declaration at
/// `until` that was received in the span.
async fn window_keys(
    js: &jetstream::Context,
    venue: &str,
    from: i64,
    to: i64,
    until: u64,
) -> Result<HashSet<(u64, String, Kind)>, String> {
    let start = time::OffsetDateTime::from_unix_timestamp_nanos(
        i128::from(from - SKEW_MARGIN_MICROS) * 1_000,
    )
    .map_err(|e| e.to_string())?;
    let mut messages = js
        .get_stream(MARKET_STREAM)
        .await
        .map_err(|e| e.to_string())?
        .create_consumer(OrderedConfig {
            filter_subject: format!("markets.{venue}.>"),
            deliver_policy: DeliverPolicy::ByStartTime { start_time: start },
            ..Default::default()
        })
        .await
        .map_err(|e| e.to_string())?
        .messages()
        .await
        .map_err(|e| e.to_string())?;
    let mut keys = HashSet::new();
    loop {
        let next = tokio::time::timeout(Duration::from_secs(2), messages.next()).await;
        let Ok(Some(Ok(message))) = next else { break };
        let Ok(info) = message.info() else { continue };
        if info.stream_sequence >= until {
            break;
        }
        if let Ok(envelope) = galata_broker::decode(&message.payload)
            && (from..=to).contains(&envelope.recv_micros)
            && let Some(ticker) = envelope.ticker()
        {
            keys.insert((envelope.seq, ticker.as_str().to_string(), envelope.kind()));
        }
        if info.pending == 0 {
            break;
        }
    }
    Ok(keys)
}

impl FillCache {
    /// **The fill for a venue's span**, computed once and shared.
    #[allow(clippy::too_many_arguments)]
    pub async fn fill(
        &self,
        js: &jetstream::Context,
        normalisers: &Normalisers,
        archive: &Path,
        venue: &str,
        from: i64,
        to: i64,
        until: u64,
        limits: FillLimits,
    ) -> Filled {
        let mut held = self.0.lock().await;
        let key = (venue.to_string(), from, to);
        if let Some(filled) = held.get(&key) {
            return filled.clone();
        }
        let filled =
            Arc::new(compute(js, normalisers, archive, venue, from, to, until, limits).await);
        held.insert(key, filled.clone());
        filled
    }
}

#[allow(clippy::too_many_arguments)]
async fn compute(
    js: &jetstream::Context,
    normalisers: &Normalisers,
    archive: &Path,
    venue: &str,
    from: i64,
    to: i64,
    until: u64,
    limits: FillLimits,
) -> Result<Vec<FillEvent>, String> {
    let normaliser = normalisers
        .get(venue)
        .ok_or_else(|| format!("no normaliser is built for {venue}"))?;
    // The record reaches the span's end within a few seconds of the hole's
    // end; a declaration arrives just after it.
    let deadline = tokio::time::Instant::now() + limits.wait;
    loop {
        let (root, v) = (archive.to_path_buf(), venue.to_string());
        let reached = tokio::task::spawn_blocking(move || reaches(&root, &v, to))
            .await
            .unwrap_or(false);
        if reached {
            break;
        }
        if tokio::time::Instant::now() >= deadline {
            return Err("the record does not yet reach the span's end".into());
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    let (root, v, cap) = (archive.to_path_buf(), venue.to_string(), limits.cap);
    let record = tokio::task::spawn_blocking(move || {
        from_record(&root, normaliser.as_ref(), &v, from, to, cap)
    })
    .await
    .map_err(|e| e.to_string())??;
    let window = window_keys(js, venue, from, to, until).await?;
    let mut fill: Vec<FillEvent> = record
        .into_iter()
        .filter_map(|e| {
            let ticker = e.ticker()?.as_str().to_string();
            let kind = e.kind();
            if window.contains(&(e.seq, ticker.clone(), kind)) {
                return None;
            }
            let data = String::from_utf8(galata_broker::encode(&e).ok()?).ok()?;
            Some(FillEvent {
                seq: e.seq,
                ticker,
                kind,
                recv_micros: e.recv_micros,
                data,
            })
        })
        .collect();
    fill.sort_by_key(|e| (e.recv_micros, e.seq));
    Ok(fill)
}
