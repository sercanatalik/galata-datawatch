//! **Account data, served** (`add-markets-accounts`).
//!
//! ```text
//!   GET /v1/accounts                                   the accounts held, by venue and alias
//!   GET /v1/accounts/{venue}/{alias}/now               each state kind's newest set, with its read time
//!   GET /v1/accounts/{venue}/{alias}/{kind}?from&to    one kind over a window: the projection, then the stream
//!   GET /v1/accounts/live?accounts&kinds[&after|&from] server-sent events, resumable by Last-Event-ID
//! ```
//!
//! **Every route is a read**, behind a client's token and then the grant of
//! account data ([`crate::access::ACCOUNT_CLIENTS`]). An account is named by
//! its venue and alias, the account stream's own subject tokens. No answer
//! carries an address, a wallet or a subaccount id, because neither source
//! holds one: the ledger's projection is written by alias, and so are the
//! stream's subjects and rows. The ledger's record, which holds addresses, is
//! not mounted here.
//!
//! **Rows answer in the projection's columns**: `venue`, `account`, `dex`,
//! `ticker`, `at_micros`, `recv_micros`, then the kind's own, decimals as exact
//! strings. A row from the stream is put into those columns by the
//! projection's own builder (`ledger::project::batch`), so a row reads the
//! same whichever source held it. The live stream sends the bus's envelopes,
//! as `/v1/live` does.
//!
//! **History joins at a time bound** (design D4). The projection answers up to
//! the newest receipt it holds for the account and kind, and the stream after
//! it. A row both hold appears once, by the kind's identity as the ledger
//! reads it (`ledger::events::identity`). The answer states the bound, and the
//! stream position it read to, which `live?after=` continues after.

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use arrow::array::{Array, Int64Array};
use arrow::record_batch::RecordBatch;
use async_nats::jetstream::{self, consumer::DeliverPolicy, consumer::pull::OrderedConfig};
use axum::Json;
use axum::Router;
use axum::extract::{Path as UrlPath, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::sse::{KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use futures_util::StreamExt;
use galata_broker::streams::ACCOUNT_STREAM;
use galata_datawatch::ledger::project;
use galata_wire::{Envelope, Event, Kind};
use serde::Deserialize;
use serde_json::{Map, Value, json};

use crate::Shared;
use crate::access::{AccountGrant, require_account_grant};
use crate::live::Sent;

pub use crate::rows::{Row, decimal, rows_json};

const DAY: i64 = 86_400_000_000;

/// How much one answer may hold, and how long the stream is waited for.
/// **Proposed until measured** on the deployment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limits {
    /// The longest history window, as `/v1/tape`'s for every dataset but
    /// quotes: thirty days of fills, with a day to spare.
    pub window_micros: i64,
    /// The most rows one history answer holds.
    pub rows: usize,
    /// The most subjects one live stream follows: each account and kind asked
    /// for is one.
    pub subjects: usize,
    /// How long a history answer waits for the account stream before it is
    /// refused, never cut short.
    pub read: Duration,
}

impl Default for Limits {
    fn default() -> Limits {
        Limits {
            window_micros: 31 * DAY,
            rows: 200_000,
            subjects: 256,
            read: Duration::from_secs(30),
        }
    }
}

/// Why an account route did not answer.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
#[non_exhaustive]
pub enum Refused {
    /// Something asked for is not offered, or not a name.
    #[error("{0}")]
    Asked(String),
    /// An account neither source holds anything of.
    #[error("no account {venue}/{alias} is held")]
    NotHeld {
        /// The venue asked for.
        venue: String,
        /// The alias asked for.
        alias: String,
    },
    /// A window longer than the limit.
    #[error("the window is longer than the {limit_days}-day limit; ask again in parts")]
    WindowTooLong {
        /// The limit, in days.
        limit_days: i64,
    },
    /// An answer holding more rows than the limit.
    #[error(
        "the answer holds {rows} rows, more than the limit of {limit}; ask for a shorter window"
    )]
    TooManyRows {
        /// What it held.
        rows: usize,
        /// The limit.
        limit: usize,
    },
    /// A resume point the account stream no longer holds.
    #[error(
        "position {asked} has left the account stream; the oldest it holds is {oldest}: read the rest from history"
    )]
    Aged {
        /// What was asked.
        asked: u64,
        /// The oldest position held.
        oldest: u64,
    },
    /// The bus could not be asked, or did not answer in time.
    #[error("the bus is not reachable: {0}")]
    Bus(String),
    /// The projection would not read.
    #[error("the ledger's projection could not be read: {0}")]
    Unreadable(String),
}

impl IntoResponse for Refused {
    fn into_response(self) -> Response {
        let status = match self {
            Refused::Asked(_) | Refused::WindowTooLong { .. } | Refused::TooManyRows { .. } => {
                StatusCode::BAD_REQUEST
            }
            Refused::NotHeld { .. } => StatusCode::NOT_FOUND,
            Refused::Aged { .. } => StatusCode::GONE,
            Refused::Bus(_) => StatusCode::SERVICE_UNAVAILABLE,
            Refused::Unreadable(_) => StatusCode::INTERNAL_SERVER_ERROR,
        };
        (status, format!("{self}\n")).into_response()
    }
}

fn bus(error: impl std::fmt::Display) -> Refused {
    Refused::Bus(error.to_string())
}

fn unreadable(error: impl std::fmt::Display) -> Refused {
    Refused::Unreadable(error.to_string())
}

/// **The account routes**, behind the grant of account data. The token is
/// checked outside them, by [`crate::app`].
pub fn routes(markets: Shared, grant: AccountGrant) -> Router {
    Router::new()
        .route("/v1/accounts", get(list_route))
        .route("/v1/accounts/live", get(live_route))
        .route("/v1/accounts/{venue}/{alias}/now", get(now_route))
        .route("/v1/accounts/{venue}/{alias}/{kind}", get(history_route))
        .layer(axum::middleware::from_fn_with_state(
            grant,
            require_account_grant,
        ))
        .with_state(markets)
}

/// A venue or an alias, as a subject token and a folder name: letters,
/// digits, `-` and `_`. Anything else would reach past one account.
fn name(what: &str, value: &str) -> Result<(), Refused> {
    let ok = !value.is_empty()
        && value.len() <= 64
        && value
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_');
    if ok {
        Ok(())
    } else {
        Err(Refused::Asked(format!(
            "{what} {value:?} is not a name: letters, digits, '-' and '_'"
        )))
    }
}

/// **The kinds an account of a venue is projected and published as**: a
/// venue read by wallet (Derive) states its subaccounts' own, every other the
/// projection's.
pub fn kinds_of(venue: &str) -> &'static [Kind] {
    if venue == galata_datawatch::adapters::derive_xyz::VENUE {
        &project::WALLET_PROJECTED
    } else {
        &project::PROJECTED
    }
}

/// Every kind an account route offers, on any venue.
fn every_kind() -> BTreeSet<Kind> {
    project::PROJECTED
        .iter()
        .chain(project::WALLET_PROJECTED.iter())
        .copied()
        .collect()
}

/// A kind by the name a client used, where `offered` holds it.
fn kind_named(name: &str, offered: &BTreeSet<Kind>) -> Result<Kind, Refused> {
    Kind::from_str(name)
        .ok()
        .filter(|kind| offered.contains(kind))
        .ok_or_else(|| {
            Refused::Asked(format!(
                "{name:?} is not an account kind offered here; ask for one of {}",
                offered
                    .iter()
                    .map(|k| k.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            ))
        })
}

/// The subject one account's kind is published on.
pub fn subject(venue: &str, alias: &str, kind: Kind) -> String {
    format!("accounts.{venue}.{alias}.{}", kind.as_str())
}

// ---- rows ------------------------------------------------------------------

fn int(row: &Row, column: &str) -> Option<i64> {
    row.get(column).and_then(Value::as_i64)
}

/// A row's time: the venue's, where it stated one, else when it was read.
fn time_of(row: &Row) -> i64 {
    int(row, "at_micros")
        .or_else(|| int(row, "recv_micros"))
        .unwrap_or_default()
}

/// **A row's identity, in the projection's columns**, as the ledger reads one
/// (`ledger::events::identity`): equal for two holdings of one event, and
/// only for those.
pub fn identity(kind: Kind, row: &Row) -> String {
    let field = |name: &str| row.get(name).map(Value::to_string).unwrap_or_default();
    match kind {
        Kind::Fills => format!("{}|{}", field("trade_id"), field("order_id")),
        Kind::OrderUpdates => format!(
            "{}|{}|{}|{}",
            field("account"),
            field("order_id"),
            field("status"),
            field("at_micros")
        ),
        Kind::PnlHistory => format!(
            "{}|{}|{}",
            field("account"),
            field("window"),
            field("at_micros")
        ),
        _ => {
            // A state is a statement told apart by when it was read; any
            // other event by its time and its whole content.
            let mut whole = row.clone();
            whole.remove("schema_version");
            if !kind.is_state() {
                whole.remove("recv_micros");
            }
            Value::Object(whole).to_string()
        }
    }
}

/// An envelope's rows, each in an envelope of its own: a state set's rows,
/// stamped with the set's times, or the event itself.
fn each_row(envelope: &Envelope) -> Vec<Envelope> {
    match &envelope.event {
        Event::StateSet(set) => set
            .rows
            .iter()
            .map(|row| Envelope {
                address: envelope.address.clone(),
                seq: envelope.seq,
                at_micros: envelope.at_micros,
                recv_micros: envelope.recv_micros,
                event: row.clone(),
            })
            .collect(),
        _ => vec![envelope.clone()],
    }
}

/// Stream envelopes as rows in the projection's columns.
fn stream_rows(
    kind: Kind,
    venue: &str,
    alias: &str,
    envelopes: &[Envelope],
) -> Result<Vec<Row>, Refused> {
    let each: Vec<Envelope> = envelopes.iter().flat_map(each_row).collect();
    let batch = project::batch(kind, venue, alias, &each).map_err(unreadable)?;
    Ok(rows_json(&batch))
}

// ---- history ---------------------------------------------------------------

/// A history answer, before it is sent.
#[derive(Debug, Clone, PartialEq)]
pub struct History {
    /// The newest receipt the projection held: rows to it came from there.
    pub bound: Option<i64>,
    /// The account stream position the answer read to.
    pub position: u64,
    /// The rows, in time order.
    pub rows: Vec<Row>,
}

/// **The join** (design D4): the projection's rows, then the stream's received
/// past the projection's bound, inside `[from, to)`, each identity once, in
/// time order. The projection's copy is the one kept.
#[allow(clippy::too_many_arguments)]
pub fn join(
    kind: Kind,
    venue: &str,
    alias: &str,
    projection: Option<&RecordBatch>,
    stream: &[Envelope],
    position: u64,
    window: (i64, i64),
    limit: usize,
) -> Result<History, Refused> {
    let projected = projection.map(rows_json).unwrap_or_default();
    let bound = projected.iter().filter_map(|r| int(r, "recv_micros")).max();
    let past: Vec<Envelope> = stream
        .iter()
        .filter(|e| bound.is_none_or(|b| e.recv_micros > b))
        .cloned()
        .collect();
    let streamed = stream_rows(kind, venue, alias, &past)?;
    let inside = |row: &Row| {
        let t = time_of(row);
        window.0 <= t && t < window.1
    };
    let mut seen = HashSet::new();
    let mut rows: Vec<Row> = projected
        .into_iter()
        .chain(streamed)
        .filter(|row| inside(row) && seen.insert(identity(kind, row)))
        .collect();
    if rows.len() > limit {
        return Err(Refused::TooManyRows {
            rows: rows.len(),
            limit,
        });
    }
    rows.sort_by_key(|row| (time_of(row), int(row, "recv_micros").unwrap_or_default()));
    Ok(History {
        bound,
        position,
        rows,
    })
}

/// A moment, as async-nats takes one.
fn moment(micros: i64) -> Result<time::OffsetDateTime, Refused> {
    time::OffsetDateTime::from_unix_timestamp_nanos(i128::from(micros) * 1_000)
        .map_err(|e| Refused::Asked(format!("{micros}: {e}")))
}

/// How far before a receipt time the stream is read from. A message is
/// stored after it was received, so none received past the bound was stored
/// before it; a minute covers the ledger's clock and the broker's disagreeing.
const SLACK_MICROS: i64 = 60_000_000;

/// **One subject's messages received after a time**, and the stream position
/// they were read to: every matching message the stream held when asked.
/// Refused, never cut short, where the stream does not answer in `wait`.
pub async fn read_after(
    js: &jetstream::Context,
    subject: String,
    after_recv: Option<i64>,
    wait: Duration,
) -> Result<(Vec<Envelope>, u64), Refused> {
    let read = async {
        let mut stream = js.get_stream(ACCOUNT_STREAM).await.map_err(bus)?;
        let last = stream.info().await.map_err(bus)?.state.last_sequence;
        let deliver_policy = match after_recv {
            Some(recv) => DeliverPolicy::ByStartTime {
                start_time: moment(recv - SLACK_MICROS)?,
            },
            None => DeliverPolicy::All,
        };
        let consumer = stream
            .create_consumer(OrderedConfig {
                filter_subject: subject,
                deliver_policy,
                ..Default::default()
            })
            .await
            .map_err(bus)?;
        let mut position = last;
        let mut read = Vec::new();
        if consumer.cached_info().num_pending > 0 {
            let mut messages = consumer.messages().await.map_err(bus)?;
            while let Some(message) = messages.next().await {
                let message = message.map_err(bus)?;
                let info = message.info().map_err(bus)?;
                position = position.max(info.stream_sequence);
                let pending = info.pending;
                read.push(galata_broker::decode(&message.payload).map_err(bus)?);
                if pending == 0 {
                    break;
                }
            }
        }
        Ok((read, position))
    };
    tokio::time::timeout(wait, read)
        .await
        .map_err(|_| Refused::Bus("the account stream did not answer in time".into()))?
}

/// What a history request asks: a window, venue micros, `[from, to)`.
#[derive(Debug, Clone, Copy, Deserialize)]
pub struct Window {
    /// Start, inclusive.
    pub from: i64,
    /// End, exclusive.
    pub to: i64,
}

async fn history(
    markets: &Shared,
    venue: &str,
    alias: &str,
    kind: &str,
    window: Window,
) -> Result<Value, Refused> {
    name("venue", venue)?;
    name("alias", alias)?;
    let kind = kind_named(kind, &kinds_of(venue).iter().copied().collect())?;
    if window.to <= window.from {
        return Err(Refused::Asked(format!(
            "the window [{}, {}) runs backwards or is empty",
            window.from, window.to
        )));
    }
    let limits = markets.account_limits;
    if window.to - window.from > limits.window_micros {
        return Err(Refused::WindowTooLong {
            limit_days: limits.window_micros / DAY,
        });
    }
    let client = markets
        .bus
        .clone()
        .ok_or_else(|| Refused::Bus("galata-markets is not connected to the bus".into()))?;
    let projection = match markets.ledger_tape.clone() {
        Some(root) => {
            let (v, a) = (venue.to_string(), alias.to_string());
            tokio::task::spawn_blocking(move || project::read(&root, &v, &a, kind))
                .await
                .map_err(unreadable)?
                .map_err(unreadable)?
        }
        None => None,
    };
    let bound = projection.as_ref().and_then(|batch| {
        batch
            .column_by_name("recv_micros")
            .and_then(|c| c.as_any().downcast_ref::<Int64Array>())
            .and_then(arrow::compute::max)
    });
    let (stream, position) = read_after(
        &jetstream::new(client),
        subject(venue, alias, kind),
        bound,
        limits.read,
    )
    .await?;
    let answer = join(
        kind,
        venue,
        alias,
        projection.as_ref(),
        &stream,
        position,
        (window.from, window.to),
        limits.rows,
    )?;
    Ok(json!({
        "venue": venue,
        "alias": alias,
        "kind": kind.as_str(),
        "from": window.from,
        "to": window.to,
        "bound_micros": answer.bound,
        "position": answer.position,
        "rows": answer.rows,
    }))
}

/// `GET /v1/accounts/{venue}/{alias}/{kind}?from&to`.
pub async fn history_route(
    State(markets): State<Shared>,
    UrlPath((venue, alias, kind)): UrlPath<(String, String, String)>,
    Query(window): Query<Window>,
) -> Response {
    match history(&markets, &venue, &alias, &kind, window).await {
        Ok(answer) => Json(answer).into_response(),
        Err(refused) => refused.into_response(),
    }
}

// ---- now -------------------------------------------------------------------

/// The newest message on a subject, and its position: `None` where the
/// stream holds none.
async fn newest_on(
    js: &jetstream::Context,
    subject: &str,
) -> Result<Option<(u64, Envelope)>, Refused> {
    let stream = js.get_stream(ACCOUNT_STREAM).await.map_err(bus)?;
    match stream.get_last_raw_message_by_subject(subject).await {
        Ok(message) => Ok(Some((
            message.sequence,
            galata_broker::decode(&message.payload).map_err(bus)?,
        ))),
        Err(error)
            if error.kind()
                == async_nats::jetstream::stream::LastRawMessageErrorKind::NoMessageFound =>
        {
            Ok(None)
        }
        Err(error) => Err(bus(error)),
    }
}

/// A set as one source holds it: when it was read, where it is known, and its
/// rows.
type HeldSet = (Option<i64>, Vec<Row>);

/// One kind's newest set from the projection: its newest read's rows and that
/// read's time. A file of open orders with no row states a read that found
/// none, at a time it does not keep. `None` where the projection holds no set.
fn projected_newest(
    root: &Path,
    venue: &str,
    alias: &str,
    kind: Kind,
) -> Result<Option<HeldSet>, Refused> {
    match project::read(root, venue, alias, kind).map_err(unreadable)? {
        None => Ok(None),
        Some(batch) if batch.num_rows() == 0 => {
            Ok((kind == Kind::Orders).then(|| (None, Vec::new())))
        }
        Some(_) => Ok(project::newest(root, venue, alias, kind)
            .map_err(unreadable)?
            .map(|(read, batch)| (Some(read), rows_json(&batch)))),
    }
}

async fn now(markets: &Shared, venue: &str, alias: &str) -> Result<Value, Refused> {
    name("venue", venue)?;
    name("alias", alias)?;
    let states: Vec<Kind> = kinds_of(venue)
        .iter()
        .copied()
        .filter(Kind::is_state)
        .collect();
    let js = markets.bus.clone().map(jetstream::new);
    let mut reached = js.is_some();
    let mut kinds = Map::new();
    let mut absent = Vec::new();
    for kind in states {
        let mut found = None;
        if let Some(js) = &js
            && reached
        {
            match newest_on(js, &subject(venue, alias, kind)).await {
                Ok(Some((position, envelope))) if matches!(envelope.event, Event::StateSet(_)) => {
                    found = Some(json!({
                        "read_micros": envelope.recv_micros,
                        "source": "stream",
                        "position": position,
                        "rows": stream_rows(kind, venue, alias, std::slice::from_ref(&envelope))?,
                    }));
                }
                Ok(_) => {}
                Err(error) => {
                    tracing::warn!(%error, "an account's newest set could not be asked of the stream");
                    reached = false;
                }
            }
        }
        if found.is_none()
            && let Some(root) = markets.ledger_tape.clone()
        {
            let (v, a) = (venue.to_string(), alias.to_string());
            let held = tokio::task::spawn_blocking(move || projected_newest(&root, &v, &a, kind))
                .await
                .map_err(unreadable)??;
            found = held.map(|(read, rows)| {
                json!({
                    "read_micros": read,
                    "source": "projection",
                    "rows": rows,
                })
            });
        }
        match found {
            Some(set) => {
                kinds.insert(kind.as_str().to_string(), set);
            }
            None => absent.push(kind.as_str()),
        }
    }
    if kinds.is_empty() {
        return Err(Refused::NotHeld {
            venue: venue.into(),
            alias: alias.into(),
        });
    }
    Ok(json!({
        "venue": venue,
        "alias": alias,
        "stream_reached": reached,
        "kinds": kinds,
        "absent": absent,
    }))
}

/// `GET /v1/accounts/{venue}/{alias}/now`.
pub async fn now_route(
    State(markets): State<Shared>,
    UrlPath((venue, alias)): UrlPath<(String, String)>,
) -> Response {
    match now(&markets, &venue, &alias).await {
        Ok(answer) => Json(answer).into_response(),
        Err(refused) => refused.into_response(),
    }
}

// ---- the accounts held -----------------------------------------------------

/// Each account held, by venue and alias, and each kind's newest time
/// (`None` where a kind is held with no row to time it).
pub type Held = BTreeMap<(String, String), BTreeMap<String, Option<i64>>>;

/// The `name=value` folders under a directory, by value.
fn folders(dir: &Path, name: &str) -> Result<Vec<String>, Refused> {
    let prefix = format!("{name}=");
    let mut out = Vec::new();
    let entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(out),
        Err(e) => return Err(unreadable(format!("{}: {e}", dir.display()))),
    };
    for entry in entries {
        let entry = entry.map_err(unreadable)?;
        if let Some(value) = entry
            .file_name()
            .to_str()
            .and_then(|f| f.strip_prefix(&prefix))
            && entry.path().is_dir()
        {
            out.push(value.to_string());
        }
    }
    out.sort();
    Ok(out)
}

/// **The accounts the projection holds**, and each kind's newest receipt.
pub fn projected(root: &Path) -> Result<Held, Refused> {
    let offered = every_kind();
    let mut held = Held::new();
    for venue in folders(root, "venue")? {
        let venue_dir = root.join(format!("venue={venue}"));
        for alias in folders(&venue_dir, "account")? {
            let account_dir = venue_dir.join(format!("account={alias}"));
            for kind in folders(&account_dir, "kind")? {
                let Some(kind) = Kind::from_str(&kind).ok().filter(|k| offered.contains(k)) else {
                    continue;
                };
                let Some(batch) = project::read(root, &venue, &alias, kind).map_err(unreadable)?
                else {
                    continue;
                };
                let newest = batch
                    .column_by_name("recv_micros")
                    .and_then(|c| c.as_any().downcast_ref::<Int64Array>())
                    .and_then(arrow::compute::max);
                held.entry((venue.clone(), alias.clone()))
                    .or_default()
                    .insert(kind.as_str().to_string(), newest);
            }
        }
    }
    Ok(held)
}

/// **The accounts the stream holds**, and each kind's newest receipt.
async fn streamed(js: &jetstream::Context) -> Result<Held, Refused> {
    let stream = js.get_stream(ACCOUNT_STREAM).await.map_err(bus)?;
    let mut subjects = stream.info_with_subjects("accounts.>").await.map_err(bus)?;
    let mut names = Vec::new();
    while let Some(entry) = subjects.next().await {
        names.push(entry.map_err(bus)?.0);
    }
    let mut held = Held::new();
    for subject in names {
        let [_, venue, alias, kind] = subject.split('.').collect::<Vec<_>>()[..] else {
            continue;
        };
        let newest = newest_on(js, &subject)
            .await?
            .map(|(_, envelope)| envelope.recv_micros);
        held.entry((venue.to_string(), alias.to_string()))
            .or_default()
            .insert(kind.to_string(), newest);
    }
    Ok(held)
}

/// Both sources' accounts as one answer, each kind's newest time the later.
pub fn listing(projected: Held, streamed: Held, stream_reached: bool) -> Value {
    let mut all = projected;
    for (account, kinds) in streamed {
        let held = all.entry(account).or_default();
        for (kind, newest) in kinds {
            let entry = held.entry(kind).or_default();
            *entry = (*entry).max(newest);
        }
    }
    let accounts: Vec<Value> = all
        .into_iter()
        .map(|((venue, alias), kinds)| {
            json!({
                "venue": venue,
                "alias": alias,
                "kinds": kinds,
            })
        })
        .collect();
    json!({
        "accounts": accounts,
        "stream_reached": stream_reached,
    })
}

/// `GET /v1/accounts`.
pub async fn list_route(State(markets): State<Shared>) -> Response {
    let projected = match markets.ledger_tape.clone() {
        Some(root) => match tokio::task::spawn_blocking(move || projected(&root)).await {
            Ok(Ok(held)) => held,
            Ok(Err(refused)) => return refused.into_response(),
            Err(join) => return unreadable(join).into_response(),
        },
        None => Held::new(),
    };
    let (streamed, reached) = match markets.bus.clone() {
        Some(client) => match streamed(&jetstream::new(client)).await {
            Ok(held) => (held, true),
            Err(error) => {
                tracing::warn!(%error, "the accounts the stream holds could not be asked");
                (Held::new(), false)
            }
        },
        None => (Held::new(), false),
    };
    Json(listing(projected, streamed, reached)).into_response()
}

// ---- live ------------------------------------------------------------------

/// What a live client asks for.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct LiveQuery {
    /// Accounts, comma-separated, each `venue/alias`.
    pub accounts: String,
    /// Kinds, comma-separated; every kind where absent.
    pub kinds: Option<String>,
    /// A history answer's position, to continue after.
    pub after: Option<u64>,
    /// A moment in the stream, micros, to start from.
    pub from: Option<i64>,
}

/// Where a live connection starts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Start {
    /// From now.
    Now,
    /// After a stream position: a client's last event, or a history answer's.
    After(u64),
    /// From a moment.
    From(i64),
}

/// A live connection's subscription, checked.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Subscription {
    /// The subjects it follows, none overlapping another.
    pub subjects: Vec<String>,
    /// Where it starts.
    pub start: Start,
}

impl Subscription {
    /// A query and the `Last-Event-ID` it came with, checked.
    pub fn of(
        query: &LiveQuery,
        last_event_id: Option<&str>,
        limits: Limits,
    ) -> Result<Subscription, Refused> {
        let mut accounts = BTreeSet::new();
        for asked in query
            .accounts
            .split(',')
            .map(str::trim)
            .filter(|a| !a.is_empty())
        {
            let Some((venue, alias)) = asked.split_once('/') else {
                return Err(Refused::Asked(format!(
                    "{asked:?} is not an account: name it as venue/alias"
                )));
            };
            name("venue", venue)?;
            name("alias", alias)?;
            accounts.insert((venue.to_string(), alias.to_string()));
        }
        if accounts.is_empty() {
            return Err(Refused::Asked(
                "name at least one account, as venue/alias".into(),
            ));
        }
        let offered = every_kind();
        let kinds: Option<BTreeSet<Kind>> = match &query.kinds {
            None => None,
            Some(asked) => Some(
                asked
                    .split(',')
                    .map(str::trim)
                    .filter(|k| !k.is_empty())
                    .map(|k| kind_named(k, &offered))
                    .collect::<Result<_, _>>()?,
            ),
        };
        let subjects: Vec<String> = accounts
            .iter()
            .flat_map(|(venue, alias)| match &kinds {
                None => vec![format!("accounts.{venue}.{alias}.>")],
                Some(kinds) => kinds.iter().map(|k| subject(venue, alias, *k)).collect(),
            })
            .collect();
        if subjects.is_empty() {
            return Err(Refused::Asked("name at least one kind".into()));
        }
        if subjects.len() > limits.subjects {
            return Err(Refused::Asked(format!(
                "{} accounts and kinds asked; one stream follows at most {}",
                subjects.len(),
                limits.subjects
            )));
        }
        let start =
            match (last_event_id, query.after, query.from) {
                (Some(id), _, _) => Start::After(id.trim().parse().map_err(|_| {
                    Refused::Asked(format!("Last-Event-ID {id:?} is not a position"))
                })?),
                (None, Some(_), Some(_)) => {
                    return Err(Refused::Asked(
                        "ask after a position or from a moment, not both".into(),
                    ));
                }
                (None, Some(after), None) => Start::After(after),
                (None, None, Some(from)) => Start::From(from),
                (None, None, None) => Start::Now,
            };
        Ok(Subscription { subjects, start })
    }
}

/// The deliver policy a start means, against the account stream as it stands.
async fn policy(js: &jetstream::Context, start: Start) -> Result<DeliverPolicy, Refused> {
    match start {
        Start::Now => Ok(DeliverPolicy::New),
        Start::From(micros) => Ok(DeliverPolicy::ByStartTime {
            start_time: moment(micros)?,
        }),
        Start::After(position) => {
            let mut stream = js.get_stream(ACCOUNT_STREAM).await.map_err(bus)?;
            let oldest = stream.info().await.map_err(bus)?.state.first_sequence;
            if position.saturating_add(1) < oldest {
                return Err(Refused::Aged {
                    asked: position,
                    oldest,
                });
            }
            Ok(DeliverPolicy::ByStartSequence {
                start_sequence: position + 1,
            })
        }
    }
}

/// **Pump the account stream into a connection's queue** until the client
/// leaves or falls behind.
async fn pump(
    js: jetstream::Context,
    sub: Subscription,
    deliver_policy: DeliverPolicy,
    queue: tokio::sync::mpsc::Sender<Sent>,
    behind: Arc<AtomicBool>,
) {
    let opened = async {
        js.get_stream(ACCOUNT_STREAM)
            .await
            .map_err(bus)?
            .create_consumer(OrderedConfig {
                filter_subjects: sub.subjects.clone(),
                deliver_policy,
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
            tracing::warn!(%error, "an account stream could not open its consumer");
            return;
        }
    };
    while let Some(message) = messages.next().await {
        let Ok(message) = message else { break };
        let Ok(info) = message.info() else { continue };
        let sent = Sent::Event {
            id: info.stream_sequence,
            data: String::from_utf8_lossy(&message.payload).into_owned(),
        };
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

/// `GET /v1/accounts/live`.
pub async fn live_route(
    State(markets): State<Shared>,
    headers: HeaderMap,
    Query(query): Query<LiveQuery>,
) -> Response {
    let last = headers.get("last-event-id").and_then(|v| v.to_str().ok());
    let sub = match Subscription::of(&query, last, markets.account_limits) {
        Ok(sub) => sub,
        Err(refused) => return refused.into_response(),
    };
    let Some(client) = markets.bus.clone() else {
        return Refused::Bus("galata-markets is not connected to the bus".into()).into_response();
    };
    let js = jetstream::new(client);
    let deliver = match policy(&js, sub.start).await {
        Ok(deliver) => deliver,
        Err(refused) => return refused.into_response(),
    };
    let (queue, receiver) = tokio::sync::mpsc::channel(markets.queue);
    let behind = Arc::new(AtomicBool::new(false));
    tokio::spawn(pump(js, sub, deliver, queue, behind.clone()));
    Sse::new(crate::live::events(receiver, behind))
        .keep_alive(KeepAlive::new().interval(Duration::from_secs(15)))
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::access::AccountGrant;
    use crate::{Markets, app, routes_granting};
    use axum::body::Body;
    use axum::http::Request;
    use galata_datawatch::adapters::hyperliquid::ledger as hl;
    use galata_datawatch::config::Secret;
    use galata_wire::{Account, Fill, Num, Side, StateSet, Venue};
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    /// A ledger projection root, owner-only as the ledger requires.
    fn root() -> tempfile::TempDir {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        dir
    }

    fn n(s: &str) -> Num {
        Num::from_str(s).unwrap()
    }

    fn fill(tid: u64, at: i64, recv: i64) -> Envelope {
        Envelope::for_account(
            Venue::new("hyperliquid").unwrap(),
            Account::new("main").unwrap(),
            Some(at),
            recv,
            Event::Fill(Fill {
                dex: None,
                market: "BTC".into(),
                side: Side::Bid,
                price: n("81213.5"),
                size: n("0.01"),
                start_position: Some(n("0")),
                direction: Some("Open Long".into()),
                closed_pnl: Some(n("0")),
                fee: Some(n("0.3")),
                fee_token: Some("USDC".into()),
                builder_fee: None,
                crossed: Some(true),
                order_id: (1000 + tid).to_string(),
                trade_id: tid.to_string(),
                twap_id: None,
                liquidated: None,
            }),
        )
    }

    fn tids(rows: &[Row]) -> Vec<String> {
        rows.iter()
            .map(|r| r["trade_id"].as_str().unwrap().to_string())
            .collect()
    }

    #[test]
    fn decimals_are_exact_strings() {
        assert_eq!(decimal(81_213_500_000_000_000_000_000, 18), "81213.5");
        assert_eq!(decimal(0, 18), "0");
        assert_eq!(decimal(-500_000_000_000_000_000, 18), "-0.5");
        assert_eq!(decimal(1, 18), "0.000000000000000001");
        assert_eq!(decimal(3, 0), "3");
        assert_eq!(decimal(-12_000_000_000_000_000_000, 18), "-12");
    }

    #[test]
    fn history_joins_at_the_bound_each_row_once_in_time_order() {
        let dir = root();
        // The projection: fills 1 to 3, the newest received at 200.
        project::write(
            dir.path(),
            "hyperliquid",
            "main",
            &[fill(1, 10, 100), fill(2, 20, 100), fill(3, 30, 200)],
        )
        .unwrap();
        let projection = project::read(dir.path(), "hyperliquid", "main", Kind::Fills)
            .unwrap()
            .unwrap();
        // The stream: fill 3, held in both; fill 3 again, a later receipt of
        // the same fill; fill 4, received after the bound but filled before
        // fill 3; and fill 5.
        let stream = [
            fill(3, 30, 200),
            fill(3, 30, 250),
            fill(4, 25, 300),
            fill(5, 50, 400),
        ];
        let all = join(
            Kind::Fills,
            "hyperliquid",
            "main",
            Some(&projection),
            &stream,
            77,
            (0, 1_000),
            10,
        )
        .unwrap();
        assert_eq!(all.bound, Some(200));
        assert_eq!(all.position, 77);
        assert_eq!(
            tids(&all.rows),
            ["1", "2", "4", "3", "5"],
            "each once, by time"
        );
        // Fill 3 is the projection's copy, received at 200.
        assert_eq!(all.rows[3]["recv_micros"], 200);
        assert_eq!(all.rows[0]["price"], "81213.5");
        assert_eq!(all.rows[0]["account"], "main");

        let inside = join(
            Kind::Fills,
            "hyperliquid",
            "main",
            Some(&projection),
            &stream,
            77,
            (20, 31),
            10,
        )
        .unwrap();
        assert_eq!(tids(&inside.rows), ["2", "4", "3"]);

        let refused = join(
            Kind::Fills,
            "hyperliquid",
            "main",
            Some(&projection),
            &stream,
            77,
            (0, 1_000),
            4,
        )
        .unwrap_err();
        assert_eq!(refused, Refused::TooManyRows { rows: 5, limit: 4 });

        // No projection: the stream alone, each fill once.
        let streamed = join(
            Kind::Fills,
            "hyperliquid",
            "main",
            None,
            &stream,
            77,
            (0, 1_000),
            10,
        )
        .unwrap();
        assert_eq!(streamed.bound, None);
        assert_eq!(tids(&streamed.rows), ["4", "3", "5"]);
    }

    #[test]
    fn a_set_from_the_stream_reads_as_the_projection_does() {
        let dir = root();
        let fills = [fill(1, 10, 100), fill(2, 20, 100)];
        project::write(dir.path(), "hyperliquid", "main", &fills).unwrap();
        let projected = rows_json(
            &project::read(dir.path(), "hyperliquid", "main", Kind::Fills)
                .unwrap()
                .unwrap(),
        );
        assert_eq!(
            stream_rows(Kind::Fills, "hyperliquid", "main", &fills).unwrap(),
            projected
        );

        // A state set: each row stamped with the set's times.
        let set = Envelope::for_account(
            Venue::new("hyperliquid").unwrap(),
            Account::new("main").unwrap(),
            Some(500),
            500,
            Event::StateSet(StateSet::of(Kind::Balances, Vec::new()).unwrap()),
        );
        assert!(
            stream_rows(Kind::Balances, "hyperliquid", "main", &[set])
                .unwrap()
                .is_empty(),
            "an empty set is no row"
        );
    }

    #[test]
    fn identity_is_the_ledgers() {
        let one = &stream_rows(Kind::Fills, "hyperliquid", "main", &[fill(7, 10, 100)]).unwrap()[0];
        let again =
            &stream_rows(Kind::Fills, "hyperliquid", "main", &[fill(7, 10, 900)]).unwrap()[0];
        assert_eq!(identity(Kind::Fills, one), identity(Kind::Fills, again));
        let other =
            &stream_rows(Kind::Fills, "hyperliquid", "main", &[fill(8, 10, 100)]).unwrap()[0];
        assert_ne!(identity(Kind::Fills, one), identity(Kind::Fills, other));
    }

    #[test]
    fn a_live_subscription_names_its_subjects_and_start() {
        let limits = Limits::default();
        let query = LiveQuery {
            accounts: "hyperliquid/main, derive-xyz/owner_s3".into(),
            kinds: Some("fills,positions".into()),
            ..LiveQuery::default()
        };
        let sub = Subscription::of(&query, None, limits).unwrap();
        let mut subjects = sub.subjects.clone();
        subjects.sort();
        assert_eq!(
            subjects,
            [
                "accounts.derive-xyz.owner_s3.fills",
                "accounts.derive-xyz.owner_s3.positions",
                "accounts.hyperliquid.main.fills",
                "accounts.hyperliquid.main.positions",
            ]
        );
        assert_eq!(sub.start, Start::Now);
        assert_eq!(
            Subscription::of(&query, Some("41"), limits).unwrap().start,
            Start::After(41)
        );
        let every = LiveQuery {
            accounts: "hyperliquid/main".into(),
            after: Some(9),
            ..LiveQuery::default()
        };
        let sub = Subscription::of(&every, None, limits).unwrap();
        assert_eq!(sub.subjects, ["accounts.hyperliquid.main.>"]);
        assert_eq!(sub.start, Start::After(9));
    }

    #[test]
    fn what_is_not_an_account_or_a_kind_is_refused_by_name() {
        let limits = Limits::default();
        let ask = |accounts: &str, kinds: Option<&str>| {
            Subscription::of(
                &LiveQuery {
                    accounts: accounts.into(),
                    kinds: kinds.map(str::to_string),
                    ..LiveQuery::default()
                },
                None,
                limits,
            )
        };
        assert!(ask("", None).is_err());
        assert!(
            ask("hyperliquid", None)
                .unwrap_err()
                .to_string()
                .contains("venue/alias")
        );
        assert!(ask("hyperliquid/ma.in", None).is_err(), "a subject token");
        assert!(ask("hyperliquid/>", None).is_err(), "a wildcard");
        assert!(
            ask("hyperliquid/main", Some("quotes"))
                .unwrap_err()
                .to_string()
                .contains("\"quotes\"")
        );
        let both = LiveQuery {
            accounts: "hyperliquid/main".into(),
            after: Some(1),
            from: Some(1),
            ..LiveQuery::default()
        };
        assert!(Subscription::of(&both, None, limits).is_err());
    }

    // ---- over the router ----------------------------------------------------

    /// The scrubbed stream frames of 2026-11-28 (`design/measured.md`), which
    /// carry the account's address as the venue sends it.
    fn frame(name: &str) -> Vec<u8> {
        std::fs::read(format!(
            "{}/../galata-datawatch/tests/fixtures/hyperliquid/ledger-ws-{name}.json",
            env!("CARGO_MANIFEST_DIR")
        ))
        .unwrap()
    }

    /// A projection made as the ledger makes one: recorded frames, through
    /// the ledger's own normaliser, written by alias.
    fn recorded_projection() -> (tempfile::TempDir, Vec<String>) {
        let key = galata_datawatch::ledger::FingerprintKey::new(Secret::new("deployment-key"));
        let venue = Venue::new("hyperliquid").unwrap();
        let main = Account::new("main").unwrap();
        let mut rows = Vec::new();
        let mut addresses = Vec::new();
        for (name, channel, recv) in [
            ("clearinghouse-state-main", hl::WS_CLEARINGHOUSE, 100),
            ("open-orders-main", hl::WS_OPEN_ORDERS, 100),
            ("user-fills-snapshot", hl::WS_FILLS, 101),
            ("user-fundings-snapshot", hl::WS_FUNDINGS, 102),
            ("ledger-updates-snapshot", hl::WS_LEDGER_UPDATES, 103),
        ] {
            let bytes = frame(name);
            let text = String::from_utf8(bytes.clone()).unwrap();
            addresses.extend(hex_addresses(&text));
            rows.extend(
                hl::stream_rows(
                    &venue,
                    &main,
                    Some(&key),
                    "0123456789abcdef",
                    channel,
                    recv,
                    &hl::stream_bytes(None, None, &bytes),
                )
                .unwrap(),
            );
        }
        let dir = root();
        project::write_kinds(
            dir.path(),
            "hyperliquid",
            "main",
            &rows,
            &project::PROJECTED,
            Some(100),
        )
        .unwrap();
        addresses.sort();
        addresses.dedup();
        (dir, addresses)
    }

    /// Every `0x` and forty hex digits in a text.
    fn hex_addresses(text: &str) -> Vec<String> {
        let bytes = text.as_bytes();
        let mut out = Vec::new();
        for i in 0..bytes.len().saturating_sub(41) {
            if &bytes[i..i + 2] == b"0x"
                && bytes[i + 2..i + 42].iter().all(u8::is_ascii_hexdigit)
                && bytes.get(i + 42).is_none_or(|b| !b.is_ascii_hexdigit())
            {
                out.push(text[i..i + 42].to_lowercase());
            }
        }
        out
    }

    fn markets_over(ledger_tape: &Path) -> Shared {
        let tape = tempfile::tempdir().unwrap();
        let mut markets = Markets::over(tape.path().to_path_buf());
        markets.ledger_tape = Some(ledger_tape.to_path_buf());
        Arc::new(markets)
    }

    async fn ask(markets: Shared, path: &str, client: Option<&str>) -> (StatusCode, String) {
        // The watcher granted, the operator not: a client without the grant.
        let app = app(
            crate::access::tests::stand_in(),
            routes_granting(markets, AccountGrant(&["watcher"])),
        );
        let mut request = Request::get(path);
        if let Some(client) = client {
            request = request.header(
                "Authorization",
                format!("Bearer stand-in-token-for-{client}"),
            );
        }
        let response = app
            .oneshot(request.body(Body::empty()).unwrap())
            .await
            .unwrap();
        let status = response.status();
        let body = response.into_body().collect().await.unwrap().to_bytes();
        (status, String::from_utf8(body.to_vec()).unwrap())
    }

    #[tokio::test]
    async fn the_accounts_answer_only_a_client_granted_them_and_name_nothing_to_another() {
        let (dir, _) = recorded_projection();
        let markets = markets_over(dir.path());
        for path in [
            "/v1/accounts",
            "/v1/accounts/hyperliquid/main/now",
            "/v1/accounts/hyperliquid/main/fills?from=0&to=1000",
            "/v1/accounts/live?accounts=hyperliquid/main",
        ] {
            let (status, body) = ask(markets.clone(), path, Some("operator")).await;
            assert_eq!(status, StatusCode::FORBIDDEN, "{path}");
            assert!(
                !body.contains("main") && !body.contains("hyperliquid"),
                "{path}: {body}"
            );
            let (status, body) = ask(markets.clone(), path, None).await;
            assert_eq!(status, StatusCode::UNAUTHORIZED, "{path}: {body}");
        }
        let (status, body) = ask(markets.clone(), "/v1/accounts", Some("watcher")).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let listed: Value = serde_json::from_str(&body).unwrap();
        assert_eq!(listed["accounts"][0]["venue"], "hyperliquid");
        assert_eq!(listed["accounts"][0]["alias"], "main");
        assert_eq!(listed["stream_reached"], false);
        assert!(
            listed["accounts"][0]["kinds"]["fills"].as_i64().is_some(),
            "{body}"
        );
        // The market routes are not behind the account grant.
        let (status, _) = ask(markets, "/v1/venues", Some("operator")).await;
        assert_eq!(status, StatusCode::OK);
    }

    #[tokio::test]
    async fn no_answer_carries_an_address() {
        let (dir, addresses) = recorded_projection();
        assert!(
            !addresses.is_empty(),
            "the recorded frames carry the account's address, or this proves nothing"
        );
        let markets = markets_over(dir.path());
        let mut answers = Vec::new();
        for path in ["/v1/accounts", "/v1/accounts/hyperliquid/main/now"] {
            let (status, body) = ask(markets.clone(), path, Some("watcher")).await;
            assert_eq!(status, StatusCode::OK, "{path}: {body}");
            answers.push(body);
        }
        for kind in project::PROJECTED {
            let batch = project::read(dir.path(), "hyperliquid", "main", kind).unwrap();
            let history = join(
                kind,
                "hyperliquid",
                "main",
                batch.as_ref(),
                &[],
                0,
                (0, i64::MAX),
                usize::MAX,
            )
            .unwrap();
            answers.push(serde_json::to_string(&history.rows).unwrap());
        }
        let fills: Vec<Row> = serde_json::from_str(&answers[answers.len() - 13]).unwrap();
        assert!(!fills.is_empty(), "the recorded fills were projected");
        for answer in &answers {
            assert_eq!(hex_addresses(answer), Vec::<String>::new(), "{answer}");
            for address in &addresses {
                assert!(!answer.to_lowercase().contains(&address[2..]), "{address}");
            }
        }
    }

    #[tokio::test]
    async fn now_from_the_projection_states_a_kind_held_in_neither_absent() {
        let (dir, _) = recorded_projection();
        let markets = markets_over(dir.path());
        let (status, body) = ask(
            markets.clone(),
            "/v1/accounts/hyperliquid/main/now",
            Some("watcher"),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let now: Value = serde_json::from_str(&body).unwrap();
        assert_eq!(now["stream_reached"], false);
        let margin = &now["kinds"]["margin"];
        assert_eq!(margin["source"], "projection");
        assert_eq!(margin["read_micros"], 100);
        assert!(!margin["rows"].as_array().unwrap().is_empty());
        // Open orders were read, and none rested: an empty set, not absent.
        assert_eq!(now["kinds"]["orders"]["rows"], json!([]));
        // Never read here: absent, not empty.
        let absent: Vec<&str> = now["absent"]
            .as_array()
            .unwrap()
            .iter()
            .map(|k| k.as_str().unwrap())
            .collect();
        assert!(absent.contains(&"borrow_lend"), "{body}");
        assert!(now["kinds"].get("borrow_lend").is_none());

        let (status, body) = ask(
            markets,
            "/v1/accounts/hyperliquid/nobody/now",
            Some("watcher"),
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
    }

    #[tokio::test]
    async fn history_without_the_bus_is_refused_never_cut_short() {
        let (dir, _) = recorded_projection();
        let markets = markets_over(dir.path());
        let (status, body) = ask(
            markets.clone(),
            "/v1/accounts/hyperliquid/main/fills?from=0&to=1000",
            Some("watcher"),
        )
        .await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{body}");
        let (status, body) = ask(
            markets.clone(),
            &format!("/v1/accounts/hyperliquid/main/fills?from=0&to={}", 32 * DAY),
            Some("watcher"),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(body.contains("31-day limit"), "{body}");
        let (status, body) = ask(
            markets,
            "/v1/accounts/hyperliquid/main/quotes?from=0&to=1000",
            Some("watcher"),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(body.contains("\"quotes\""), "{body}");
    }
}
