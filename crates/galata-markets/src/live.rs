//! **The live stream** (`market-live-stream`, design D5–D7).
//!
//! `GET /v1/live?venue&tickers&kinds[&period][&after|&from]`, as server-sent
//! events. Each connection is one ordered consumer of the market window,
//! filtered to `markets.<venue>.<ticker>.>` for its tickers — the `.gaps`
//! subject included, so the bus's declarations reach it — and each event is
//! the bus's own envelope, its decimals already strings. **Its id is its
//! position in the market window**, so a client that reconnects with
//! `Last-Event-ID` resumes there with nothing missed and nothing repeated.
//!
//! Where it starts:
//!
//! ```text
//!   Last-Event-ID: n    after n, where n is still in the window; older is
//!                       refused, naming the oldest position the window holds
//!   after=<position>    after a history answer's tape bound: every window
//!                       message whose envelope sequence is past it, refused
//!                       where the window begins after the tape ends
//!   from=<micros>       from a moment in the window
//!   (none)              from now
//! ```
//!
//! An `unpublished` declaration is sent as `event: declaration`; a record
//! gap, where `gaps` is asked for, as an ordinary event.

use std::collections::BTreeSet;
use std::convert::Infallible;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use async_nats::jetstream::{self, consumer::DeliverPolicy, consumer::pull::OrderedConfig};
use axum::extract::{Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::sse::{Event as SseEvent, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use futures_util::{Stream, StreamExt};
use galata_broker::streams::MARKET_STREAM;
use galata_datawatch::tape::bars::{Bar, Forming, Period, SourceBar, interval_micros};
use galata_wire::{Envelope, Event, GapCause, Kind, Num};
use serde::Deserialize;

use crate::Shared;

/// The datasets the live stream carries.
pub const KINDS: [Kind; 7] = [
    Kind::Quotes,
    Kind::Candles,
    Kind::Funding,
    Kind::Marks,
    Kind::Trades,
    Kind::Gaps,
    Kind::Instruments,
];

/// The most tickers one connection follows: the instrument set's cap.
pub const MAX_TICKERS: usize = 50;

/// How many events wait for a client before it is called behind (design
/// D7, proposed until measured).
pub const QUEUE: usize = 10_000;

/// What a client asks for.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct LiveQuery {
    /// The venue.
    pub venue: String,
    /// Tickers, comma-separated.
    pub tickers: String,
    /// Datasets, comma-separated; every one of [`KINDS`] where absent.
    pub kinds: Option<String>,
    /// For candles, a period to stream forming bars at.
    pub period: Option<String>,
    /// A history answer's tape bound, to continue after.
    pub after: Option<u64>,
    /// A moment in the window, micros, to start from.
    pub from: Option<i64>,
}

/// Where a connection starts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Start {
    /// From now.
    Now,
    /// After a window position.
    Resume(u64),
    /// After a tape position, by envelope sequence.
    After(u64),
    /// From a moment.
    From(i64),
}

/// Why a stream was not opened.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
#[non_exhaustive]
pub enum Refused {
    /// Something asked for is not offered.
    #[error("{0}")]
    Asked(String),
    /// A resume point the window no longer holds.
    #[error(
        "position {asked} has left the window; the oldest it holds is {oldest}: read the rest from history"
    )]
    Aged {
        /// What was asked.
        asked: u64,
        /// The oldest position held.
        oldest: u64,
    },
    /// A tape bound older than the window's start.
    #[error(
        "the window begins after the tape ends (after {after}, the window's oldest is {oldest}): read the rest from history once it is projected"
    )]
    BeforeWindow {
        /// The tape position asked to continue after.
        after: u64,
        /// The oldest envelope sequence the window holds for the venue.
        oldest: u64,
    },
    /// The bus could not be asked.
    #[error("the bus is not reachable: {0}")]
    Bus(String),
}

impl IntoResponse for Refused {
    fn into_response(self) -> Response {
        let status = match self {
            Refused::Asked(_) => StatusCode::BAD_REQUEST,
            Refused::Aged { .. } | Refused::BeforeWindow { .. } => StatusCode::GONE,
            Refused::Bus(_) => StatusCode::SERVICE_UNAVAILABLE,
        };
        (status, format!("{self}\n")).into_response()
    }
}

/// A connection's subscription, checked.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Subscription {
    /// The venue.
    pub venue: String,
    /// Its tickers.
    pub tickers: BTreeSet<String>,
    /// Its datasets.
    pub kinds: BTreeSet<Kind>,
    /// Where it starts.
    pub start: Start,
    /// A period: the stream then carries candles as bars, and nothing else.
    pub period: Option<Period>,
}

impl Subscription {
    /// A query and the `Last-Event-ID` it came with, checked.
    pub fn of(query: &LiveQuery, last_event_id: Option<&str>) -> Result<Subscription, Refused> {
        let tickers: BTreeSet<String> = query
            .tickers
            .split(',')
            .map(str::trim)
            .filter(|t| !t.is_empty())
            .map(str::to_string)
            .collect();
        if tickers.is_empty() {
            return Err(Refused::Asked("name at least one ticker".into()));
        }
        if tickers.len() > MAX_TICKERS {
            return Err(Refused::Asked(format!(
                "{} tickers asked; one stream follows at most {MAX_TICKERS}",
                tickers.len()
            )));
        }
        let kinds =
            match &query.kinds {
                None => KINDS.into_iter().collect(),
                Some(asked) => asked
                    .split(',')
                    .map(str::trim)
                    .filter(|k| !k.is_empty())
                    .map(|k| {
                        Kind::from_str(k).ok().filter(|kind| KINDS.contains(kind)).ok_or_else(|| {
                        Refused::Asked(format!(
                            "{k:?} is not a dataset the live stream carries; ask for any of {}",
                            KINDS.map(|k| k.as_str()).join(", ")
                        ))
                    })
                    })
                    .collect::<Result<_, _>>()?,
            };
        let period = match &query.period {
            None => None,
            Some(label) => Some(Period::parse(label).map_err(|e| Refused::Asked(e.to_string()))?),
        };
        let kinds = match period {
            Some(_)
                if query
                    .kinds
                    .as_deref()
                    .is_some_and(|k| k.trim() != "candles") =>
            {
                return Err(Refused::Asked(
                    "a period streams candles as bars, alone; open another stream for other datasets".into(),
                ));
            }
            Some(_) => [Kind::Candles].into_iter().collect(),
            None => kinds,
        };
        let start =
            match (last_event_id, query.after, query.from) {
                (Some(id), _, _) => Start::Resume(id.trim().parse().map_err(|_| {
                    Refused::Asked(format!("Last-Event-ID {id:?} is not a position"))
                })?),
                (None, Some(_), Some(_)) => {
                    return Err(Refused::Asked(
                        "ask after a tape position or from a moment, not both".into(),
                    ));
                }
                (None, Some(after), None) => Start::After(after),
                (None, None, Some(from)) => Start::From(from),
                (None, None, None) => Start::Now,
            };
        Ok(Subscription {
            venue: query.venue.clone(),
            tickers,
            kinds,
            start,
            period,
        })
    }

    fn subjects(&self) -> Vec<String> {
        self.tickers
            .iter()
            .map(|t| format!("markets.{}.{t}.>", self.venue))
            .collect()
    }
}

/// One thing a connection sends.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Sent {
    /// A market event: its window position and the bus's envelope.
    Event {
        /// Its position in the window.
        id: u64,
        /// The envelope, as the bus carried it.
        data: String,
    },
    /// A declaration passed through: the bus missed a span.
    Declaration {
        /// Its position in the window.
        id: u64,
        /// The envelope.
        data: String,
    },
    /// A market event the bus missed, filled from the record in the place of
    /// its declaration; the last of a fill carries the declaration's id.
    Fill {
        /// The declaration's position, on the fill's last event only.
        id: Option<u64>,
        /// The envelope, as capture would have published it.
        data: String,
    },
    /// A bar at the stream's period, as it now stands.
    Bar {
        /// The window position of the update that moved it.
        id: u64,
        /// The bar, as JSON.
        data: String,
    },
}

impl Sent {
    fn into_sse(self) -> SseEvent {
        match self {
            Sent::Event { id, data } => SseEvent::default().id(id.to_string()).data(data),
            Sent::Declaration { id, data } => SseEvent::default()
                .id(id.to_string())
                .event("declaration")
                .data(data),
            Sent::Bar { id, data } => SseEvent::default()
                .id(id.to_string())
                .event("bar")
                .data(data),
            Sent::Fill { id, data } => {
                let event = SseEvent::default().event("fill").data(data);
                match id {
                    Some(id) => event.id(id.to_string()),
                    None => event,
                }
            }
        }
    }
}

/// The deliver policy a start means, against the window as it stands.
async fn policy(js: &jetstream::Context, sub: &Subscription) -> Result<DeliverPolicy, Refused> {
    match sub.start {
        Start::Now => Ok(DeliverPolicy::New),
        Start::From(micros) => Ok(DeliverPolicy::ByStartTime {
            start_time: time::OffsetDateTime::from_unix_timestamp_nanos(i128::from(micros) * 1_000)
                .map_err(|e| Refused::Asked(format!("from {micros}: {e}")))?,
        }),
        Start::Resume(id) => {
            let mut stream = js.get_stream(MARKET_STREAM).await.map_err(bus)?;
            let oldest = stream.info().await.map_err(bus)?.state.first_sequence;
            if id + 1 < oldest {
                return Err(Refused::Aged { asked: id, oldest });
            }
            Ok(DeliverPolicy::ByStartSequence {
                start_sequence: id + 1,
            })
        }
        Start::After(after) => {
            if let Some(oldest) = oldest_seq(js, &sub.venue).await?
                && oldest > after.saturating_add(1)
            {
                return Err(Refused::BeforeWindow { after, oldest });
            }
            Ok(DeliverPolicy::All)
        }
    }
}

/// The envelope sequence of the oldest message the window holds for a
/// venue, read through a consumer of its own.
async fn oldest_seq(js: &jetstream::Context, venue: &str) -> Result<Option<u64>, Refused> {
    let stream = js.get_stream(MARKET_STREAM).await.map_err(bus)?;
    let mut first = stream
        .create_consumer(OrderedConfig {
            filter_subject: format!("markets.{venue}.>"),
            deliver_policy: DeliverPolicy::All,
            ..Default::default()
        })
        .await
        .map_err(bus)?
        .messages()
        .await
        .map_err(bus)?;
    match tokio::time::timeout(Duration::from_secs(2), first.next()).await {
        Ok(Some(Ok(message))) => Ok(galata_broker::decode(&message.payload).ok().map(|e| e.seq)),
        _ => Ok(None),
    }
}

fn bus(error: impl std::fmt::Display) -> Refused {
    Refused::Bus(error.to_string())
}

/// What a connection does with one window message: send it, send it as a
/// declaration, or skip it.
pub fn judge(sub: &Subscription, envelope: &Envelope, id: u64, raw: &[u8]) -> Option<Sent> {
    // A declaration first: it is the bus's alone and carries no record
    // sequence (capture publishes it with 0), so the tape bound says nothing
    // about it. What it stands for is judged by the bound once filled.
    if let Event::Gap(gap) = &envelope.event
        && gap.cause == GapCause::Unpublished
    {
        let data = String::from_utf8_lossy(raw).into_owned();
        return Some(Sent::Declaration { id, data });
    }
    if !after_the_bound(sub, envelope.seq) {
        return None;
    }
    let data = String::from_utf8_lossy(raw).into_owned();
    sub.kinds
        .contains(&envelope.kind())
        .then_some(Sent::Event { id, data })
}

/// Whether a record sequence is one a client started after a tape bound has
/// not already read from history; every one is, for any other start.
fn after_the_bound(sub: &Subscription, seq: u64) -> bool {
    match sub.start {
        Start::After(after) => seq > after,
        _ => true,
    }
}

/// **Pump the window into a connection's queue** until the client leaves or
/// falls behind.
async fn pump(
    markets: Shared,
    js: jetstream::Context,
    sub: Subscription,
    deliver: DeliverPolicy,
    queue: tokio::sync::mpsc::Sender<Sent>,
    behind: Arc<AtomicBool>,
) {
    let opened = async {
        js.get_stream(MARKET_STREAM)
            .await
            .map_err(bus)?
            .create_consumer(OrderedConfig {
                filter_subjects: sub.subjects(),
                deliver_policy: deliver,
                ..Default::default()
            })
            .await
            .map_err(bus)?
            .messages()
            .await
            .map_err(bus)
    };
    let mut messages = match opened.await {
        Ok(messages) => messages,
        Err(error) => {
            tracing::warn!(%error, "a live stream could not open its consumer");
            return;
        }
    };
    let mut filled_spans: std::collections::HashSet<(i64, i64)> = std::collections::HashSet::new();
    while let Some(message) = messages.next().await {
        let Ok(message) = message else { break };
        let Ok(info) = message.info() else { continue };
        let id = info.stream_sequence;
        let Ok(envelope) = galata_broker::decode(&message.payload) else {
            continue;
        };
        let Some(sent) = judge(&sub, &envelope, id, &message.payload) else {
            continue;
        };
        let outgoing = match (&sent, &envelope.event) {
            (Sent::Declaration { .. }, Event::Gap(gap)) => {
                let span = (gap.from_micros, gap.to_micros);
                match fill_in_place(&markets, &js, &sub, &mut filled_spans, span, id).await {
                    Some(fill) => {
                        let mut events: Vec<Sent> = fill
                            .iter()
                            .filter(|e| {
                                sub.tickers.contains(&e.ticker)
                                    && sub.kinds.contains(&e.kind)
                                    && after_the_bound(&sub, e.seq)
                            })
                            .map(|e| Sent::Fill {
                                id: None,
                                data: e.data.clone(),
                            })
                            .collect();
                        // The last carries the declaration's position, so a
                        // client resuming after it does not take it again.
                        if let Some(Sent::Fill { id: last, .. }) = events.last_mut() {
                            *last = Some(id);
                        }
                        events
                    }
                    None if filled_spans.contains(&span) => Vec::new(),
                    None => vec![sent],
                }
            }
            _ => vec![sent],
        };
        for sent in outgoing {
            match queue.try_send(sent) {
                Ok(()) => {}
                Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                    behind.store(true, Ordering::SeqCst);
                    return;
                }
                Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => return,
            }
        }
    }
}

/// **The fill for a declared span, once per connection.** `None` where it was
/// already sent — on this connection, or, for a client that resumed, before
/// its resume point — or where the record cannot fill it, which the caller
/// tells apart by `filled` and passes the declaration through.
async fn fill_in_place(
    markets: &Shared,
    js: &jetstream::Context,
    sub: &Subscription,
    filled: &mut std::collections::HashSet<(i64, i64)>,
    span: (i64, i64),
    id: u64,
) -> Option<Vec<crate::fill::FillEvent>> {
    if filled.contains(&span) {
        return None;
    }
    if let Start::Resume(resumed) = sub.start
        && declared_before(js, sub, span, resumed).await
    {
        filled.insert(span);
        return None;
    }
    let made = markets
        .fills
        .fill(
            js,
            &markets.normalisers,
            &markets.archive,
            &sub.venue,
            span.0,
            span.1,
            id,
            markets.fill_limits,
        )
        .await;
    match made.as_ref() {
        Ok(events) => {
            filled.insert(span);
            Some(events.clone())
        }
        Err(reason) => {
            tracing::info!(%reason, "a declared span passes through unfilled");
            None
        }
    }
}

/// Whether a declaration of `span` for these tickers lies at or before a
/// resume point: the client that resumed then had its fill already.
/// Declarations of one span are published together, so the window just
/// before the resume point holds them.
async fn declared_before(
    js: &jetstream::Context,
    sub: &Subscription,
    span: (i64, i64),
    resumed: u64,
) -> bool {
    let subjects: Vec<String> = sub
        .tickers
        .iter()
        .map(|t| format!("markets.{}.{t}.gaps", sub.venue))
        .collect();
    let opened = async {
        js.get_stream(MARKET_STREAM)
            .await
            .ok()?
            .create_consumer(OrderedConfig {
                filter_subjects: subjects,
                deliver_policy: DeliverPolicy::ByStartSequence {
                    start_sequence: resumed.saturating_sub(256).max(1),
                },
                ..Default::default()
            })
            .await
            .ok()?
            .messages()
            .await
            .ok()
    };
    let Some(mut messages) = opened.await else {
        return false;
    };
    loop {
        let next = tokio::time::timeout(Duration::from_secs(2), messages.next()).await;
        let Ok(Some(Ok(message))) = next else {
            return false;
        };
        let Ok(info) = message.info() else {
            return false;
        };
        if info.stream_sequence > resumed {
            return false;
        }
        if let Ok(envelope) = galata_broker::decode(&message.payload)
            && let Event::Gap(gap) = &envelope.event
            && gap.cause == GapCause::Unpublished
            && (gap.from_micros, gap.to_micros) == span
        {
            return true;
        }
        if info.pending == 0 {
            return false;
        }
    }
}

/// A decimal in the tape's raw units, at [`galata_datawatch::tape::bars::SCALE`].
fn raw(value: Num) -> i128 {
    let mut value = value;
    value.rescale(u32::from(
        galata_datawatch::tape::bars::SCALE.unsigned_abs(),
    ));
    value.mantissa()
}

/// A bar as a client reads it: every decimal a string, as the bus's are.
pub fn bar_json(venue: &str, ticker: &str, period: Period, bar: &Bar) -> String {
    let text = |v: i128| Num::from_i128_with_scale(v, 18).normalize().to_string();
    serde_json::json!({
        "venue": venue,
        "ticker": ticker,
        "start_micros": bar.start_micros,
        "period": period.label(),
        "source_interval": crate::periods::interval_label(bar.source_interval_micros),
        "open": text(bar.open),
        "high": text(bar.high),
        "low": text(bar.low),
        "close": text(bar.close),
        "volume": text(bar.volume),
        "trade_count": bar.trade_count,
        "sources": bar.sources,
        "sources_expected": bar.sources_expected,
        "complete": bar.is_complete(),
        "gap": bar.gap,
        "is_final": bar.is_final,
    })
    .to_string()
}

/// **Bars at a period**: the current period seeded from the tape, the
/// window's candle updates folded in from its start, and a bar sent each
/// time one changes — after the client's resume point, or, for a new client,
/// once it has caught up with the window and then on every change.
async fn bars(
    markets: Shared,
    js: jetstream::Context,
    sub: Subscription,
    tape: std::path::PathBuf,
    queue: tokio::sync::mpsc::Sender<Sent>,
    behind: Arc<AtomicBool>,
) {
    let Some(period) = sub.period else { return };
    let now = crate::now_micros();
    let current = period.start_of(now);
    // What has been sent before is what a resuming client already has.
    let threshold = match sub.start {
        Start::Resume(id) => Some(id),
        _ => match js.get_stream(MARKET_STREAM).await {
            Ok(mut stream) => stream.info().await.ok().map(|i| i.state.last_sequence),
            Err(_) => None,
        },
    };
    let from_time = match sub.start {
        Start::From(micros) => Some(micros),
        _ => None,
    };
    let mut forming = std::collections::BTreeMap::new();
    let mut gaps = std::collections::BTreeMap::new();
    for ticker in &sub.tickers {
        let (root, venue, t) = (tape.clone(), sub.venue.clone(), ticker.clone());
        let seeded = tokio::task::spawn_blocking(move || {
            let (_, sources) =
                crate::periods::sources(&root, &venue, &t, current, current + period.micros())?;
            let gaps = crate::periods::candle_gaps(
                &root,
                &venue,
                &t,
                current - 2 * 3_600_000_000,
                now + period.micros(),
            )?;
            Ok::<_, crate::history::Refusal>((sources, gaps))
        })
        .await;
        let (sources, held_gaps) = match seeded {
            Ok(Ok(seeded)) => seeded,
            _ => Default::default(),
        };
        forming.insert(ticker.clone(), Forming::new(period, sources));
        gaps.insert(ticker.clone(), held_gaps);
    }
    let subjects: Vec<String> = sub
        .tickers
        .iter()
        .flat_map(|t| {
            [
                format!("markets.{}.{t}.candles", sub.venue),
                format!("markets.{}.{t}.gaps", sub.venue),
            ]
        })
        .collect();
    let opened = async {
        js.get_stream(MARKET_STREAM)
            .await
            .map_err(bus)?
            .create_consumer(OrderedConfig {
                filter_subjects: subjects,
                deliver_policy: DeliverPolicy::All,
                ..Default::default()
            })
            .await
            .map_err(bus)?
            .messages()
            .await
            .map_err(bus)
    };
    let mut messages = match opened.await {
        Ok(messages) => messages,
        Err(error) => {
            tracing::warn!(%error, "a bar stream could not open its consumer");
            return;
        }
    };
    let mut last: std::collections::BTreeMap<String, (u64, Bar)> =
        std::collections::BTreeMap::new();
    let mut caught_up = false;
    let mut filled_spans: std::collections::HashSet<(i64, i64)> = std::collections::HashSet::new();
    let send = |sent: Sent| match queue.try_send(sent) {
        Ok(()) => true,
        Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
            behind.store(true, Ordering::SeqCst);
            false
        }
        Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => false,
    };
    while let Some(message) = messages.next().await {
        let Ok(message) = message else { break };
        let Ok(info) = message.info() else { continue };
        let (id, pending) = (info.stream_sequence, info.pending);
        let after_start = match (from_time, threshold) {
            (Some(from), _) => info.published.unix_timestamp_nanos() / 1_000 >= i128::from(from),
            (None, Some(threshold)) => id > threshold,
            (None, None) => true,
        };
        if let Ok(envelope) = galata_broker::decode(&message.payload) {
            let ticker = envelope
                .ticker()
                .map(|t| t.as_str().to_string())
                .unwrap_or_default();
            match &envelope.event {
                Event::Gap(gap) if gap.cause == GapCause::Unpublished => {
                    let span = (gap.from_micros, gap.to_micros);
                    match fill_in_place(&markets, &js, &sub, &mut filled_spans, span, id).await {
                        // The fill's candle updates folded in, and each bar they
                        // moved sent once, at the declaration's position.
                        Some(fill) => {
                            let mut moved: std::collections::BTreeMap<String, Bar> =
                                std::collections::BTreeMap::new();
                            for event in fill.iter().filter(|e| e.kind == Kind::Candles) {
                                let (Some(state), Ok(filled)) = (
                                    forming.get_mut(&event.ticker),
                                    galata_broker::decode(event.data.as_bytes()),
                                ) else {
                                    continue;
                                };
                                if let (Event::Candle(candle), Some(at)) =
                                    (&filled.event, filled.at_micros)
                                {
                                    let Some(width) = interval_micros(&candle.interval) else {
                                        continue;
                                    };
                                    let source = SourceBar {
                                        at_micros: at,
                                        recv_micros: filled.recv_micros,
                                        open: raw(candle.open),
                                        high: raw(candle.high),
                                        low: raw(candle.low),
                                        close: raw(candle.close),
                                        volume: raw(candle.volume),
                                        trade_count: candle.trade_count,
                                        is_final: candle.is_final,
                                    };
                                    let held_gaps =
                                        gaps.get(&event.ticker).cloned().unwrap_or_default();
                                    if let Some(bar) = state.take(width, source, &held_gaps) {
                                        moved.insert(event.ticker.clone(), bar);
                                    }
                                }
                            }
                            for (ticker, bar) in moved {
                                if after_start {
                                    if !send(Sent::Bar {
                                        id,
                                        data: bar_json(&sub.venue, &ticker, period, &bar),
                                    }) {
                                        return;
                                    }
                                } else {
                                    last.insert(ticker, (id, bar));
                                }
                            }
                        }
                        None if filled_spans.contains(&span) => {}
                        None => {
                            let data = String::from_utf8_lossy(&message.payload).into_owned();
                            if after_start && !send(Sent::Declaration { id, data }) {
                                return;
                            }
                        }
                    }
                }
                Event::Candle(candle) => {
                    let (Some(width), Some(at), Some(state)) = (
                        interval_micros(&candle.interval),
                        envelope.at_micros,
                        forming.get_mut(&ticker),
                    ) else {
                        continue;
                    };
                    let source = SourceBar {
                        at_micros: at,
                        recv_micros: envelope.recv_micros,
                        open: raw(candle.open),
                        high: raw(candle.high),
                        low: raw(candle.low),
                        close: raw(candle.close),
                        volume: raw(candle.volume),
                        trade_count: candle.trade_count,
                        is_final: candle.is_final,
                    };
                    let held_gaps = gaps.get(&ticker).cloned().unwrap_or_default();
                    if let Some(bar) = state.take(width, source, &held_gaps) {
                        if after_start {
                            let data = bar_json(&sub.venue, &ticker, period, &bar);
                            if !send(Sent::Bar { id, data }) {
                                return;
                            }
                        } else {
                            last.insert(ticker.clone(), (id, bar));
                        }
                    }
                }
                _ => {}
            }
        }
        // Caught up with the window: a new client has the current bars now.
        if pending == 0 && !caught_up {
            caught_up = true;
            for (ticker, (id, bar)) in std::mem::take(&mut last) {
                if !send(Sent::Bar {
                    id,
                    data: bar_json(&sub.venue, &ticker, period, &bar),
                }) {
                    return;
                }
            }
        }
    }
}

/// The events a connection sends, ending with a comment where it fell
/// behind. The account stream's too (`crate::accounts`).
pub(crate) fn events(
    queue: tokio::sync::mpsc::Receiver<Sent>,
    behind: Arc<AtomicBool>,
) -> impl Stream<Item = Result<SseEvent, Infallible>> {
    futures_util::stream::unfold(
        (queue, behind, false),
        |(mut queue, behind, ended)| async move {
            if ended {
                return None;
            }
            match queue.recv().await {
                Some(sent) => Some((Ok(sent.into_sse()), (queue, behind, false))),
                None if behind.load(Ordering::SeqCst) => Some((
                    Ok(SseEvent::default().comment(
                        "behind: this client fell behind the stream; reconnect with Last-Event-ID",
                    )),
                    (queue, behind, true),
                )),
                None => None,
            }
        },
    )
}

/// `GET /v1/live`.
pub async fn live(
    State(markets): State<Shared>,
    headers: HeaderMap,
    Query(query): Query<LiveQuery>,
) -> Response {
    let last = headers.get("last-event-id").and_then(|v| v.to_str().ok());
    let sub = match Subscription::of(&query, last) {
        Ok(sub) => sub,
        Err(refused) => return refused.into_response(),
    };
    let Some(client) = markets.bus.clone() else {
        return Refused::Bus("galata-markets is not connected to the bus".into()).into_response();
    };
    let js = jetstream::new(client);
    let deliver = match policy(&js, &sub).await {
        Ok(deliver) => deliver,
        Err(refused) => return refused.into_response(),
    };
    let (queue, receiver) = tokio::sync::mpsc::channel(markets.queue);
    let behind = Arc::new(AtomicBool::new(false));
    if sub.period.is_some() {
        tokio::spawn(bars(
            markets.clone(),
            js,
            sub,
            markets.tape.clone(),
            queue,
            behind.clone(),
        ));
    } else {
        tokio::spawn(pump(
            markets.clone(),
            js,
            sub,
            deliver,
            queue,
            behind.clone(),
        ));
    }
    Sse::new(events(receiver, behind))
        .keep_alive(KeepAlive::new().interval(Duration::from_secs(15)))
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn query(tickers: &str) -> LiveQuery {
        LiveQuery {
            venue: "hyperliquid".into(),
            tickers: tickers.into(),
            ..LiveQuery::default()
        }
    }

    #[test]
    fn a_subscription_names_its_tickers_kinds_and_start() {
        let mut q = query("BTC, ETH");
        q.kinds = Some("quotes".into());
        let sub = Subscription::of(&q, None).unwrap();
        assert_eq!(
            sub.subjects(),
            ["markets.hyperliquid.BTC.>", "markets.hyperliquid.ETH.>"]
        );
        assert_eq!(sub.kinds, [Kind::Quotes].into_iter().collect());
        assert_eq!(sub.start, Start::Now);
        assert_eq!(
            Subscription::of(&q, Some("41")).unwrap().start,
            Start::Resume(41)
        );
    }

    #[test]
    fn what_is_not_offered_is_refused_by_name() {
        assert!(Subscription::of(&query(""), None).is_err());
        let mut q = query("BTC");
        q.kinds = Some("book".into());
        assert!(
            Subscription::of(&q, None)
                .unwrap_err()
                .to_string()
                .contains("\"book\"")
        );
        let many: Vec<String> = (0..51).map(|n| format!("T{n}")).collect();
        assert!(
            Subscription::of(&query(&many.join(",")), None)
                .unwrap_err()
                .to_string()
                .contains("at most 50")
        );
        let mut both = query("BTC");
        both.after = Some(1);
        both.from = Some(1);
        assert!(Subscription::of(&both, None).is_err());
        assert!(Subscription::of(&query("BTC"), Some("not-a-number")).is_err());
    }
}
