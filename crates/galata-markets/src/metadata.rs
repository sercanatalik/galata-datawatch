//! **What exists and what is held** (`market-metadata`, design D8).
//!
//! - `/v1/venues`: each venue, its dexes, how many instruments its capture
//!   follows, and its capture's latest reported health, with the moment of
//!   the report; one older than a minute is not called current.
//! - `/v1/instruments`: each instrument's newest reference row from the tape's
//!   `instruments` dataset — the one format every venue's reference data is in
//!   — with the datasets its capture follows; as of a past moment where asked.
//!   An instrument capture follows with no reference row is listed with
//!   `reference` null.
//! - `/v1/instruments/{venue}/{ticker}`: the same, and what the tape holds for
//!   it, per dataset and, for candles, per source interval — so a client can
//!   tell which periods can be served over which spans before asking.
//!
//! **What is held is found by reading as little as possible**: the reader
//! prunes by ticker, so one instrument's rows of one day are cheap, and only
//! a dataset's first and last days need reading — the newest day for where it
//! ends and, for candles, which intervals there are; then days oldest first
//! until each of those intervals has begun. An answer is kept for
//! [`HELD_FOR`] per instrument (found in implementation: computed on first
//! asking rather than at start, which the design proposed).

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arrow::array::{Array, BooleanArray, Decimal128Array, Int64Array, StringArray, UInt32Array};
use arrow::record_batch::RecordBatch;
use axum::Json;
use axum::extract::{Path as UrlPath, Query, State};
use axum::response::{IntoResponse, Response};
use galata_datawatch::calendar::midnight_of;
use galata_datawatch::tape::bars::interval_micros;
use galata_datawatch::tape::{ReadError, Reader, Window};
use galata_wire::{Instrument, Kind, Num};
use serde::{Deserialize, Serialize};

use crate::Shared;
use crate::history::Refusal;
use crate::periods::interval_label;
use crate::status::VenueStatus;

const DAY: i64 = 86_400_000_000;

/// How far back an instrument's newest reference row is looked for. Rows are
/// written every five minutes, so a week is a capture that stopped long ago.
pub const REFERENCE_LOOKBACK_MICROS: i64 = 7 * DAY;

/// How long what is held for one instrument is kept before it is read again.
pub const HELD_FOR: Duration = Duration::from_secs(15 * 60);

/// A span held, `[from, to)`, venue time where the rows state one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct Span {
    /// The first row's moment.
    pub from_micros: i64,
    /// Just after the last row's moment, or its bar's close.
    pub to_micros: i64,
}

/// What the tape holds for one instrument.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct Held {
    /// By dataset: quotes, funding, marks, candles.
    pub datasets: BTreeMap<String, Span>,
    /// Candles by source interval, `1m`, `1h`, `4h`, `1d`.
    pub candles: BTreeMap<String, Span>,
}

/// A venue, as `/v1/venues` lists it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct VenueView {
    /// The venue.
    pub venue: String,
    /// The dexes its instruments are on; the main dex is `""`, as the venue
    /// spells it.
    pub dexes: Vec<String>,
    /// How many instruments its capture follows.
    pub instruments: usize,
    /// Its capture's latest report, where there is one.
    pub health: Option<Health>,
}

/// A capture's health, as it last reported it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Health {
    /// `connected`, `rotating` or `down`.
    pub connection: String,
    /// Subscriptions declared.
    pub subs_declared: u64,
    /// Subscriptions the venue holds.
    pub subs_held: u64,
    /// The newest event received.
    pub last_event_micros: Option<i64>,
    /// When capture made this report.
    pub reported_at_micros: i64,
    /// Whether the report is recent enough to call current.
    pub current: bool,
}

/// An instrument, as `/v1/instruments` lists it.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct InstrumentView {
    /// The venue.
    pub venue: String,
    /// galata's ticker.
    pub ticker: String,
    /// The datasets its capture follows.
    pub datasets: Vec<String>,
    /// Its newest reference row, in the one format; null where none is held.
    pub reference: Option<Instrument>,
    /// What the tape holds, on the one-instrument route only.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub held: Option<Held>,
}

/// The filters `/v1/instruments` takes.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct InstrumentQuery {
    /// One venue.
    pub venue: Option<String>,
    /// One base asset, across venues.
    pub base: Option<String>,
    /// One contract type.
    pub contract_type: Option<String>,
    /// Active or not.
    pub active: Option<bool>,
    /// As of this moment, micros; now where absent.
    pub at: Option<i64>,
}

/// **Every venue**: those with a status, and those with reference rows.
pub fn venues(
    statuses: &[VenueStatus],
    references: &[(String, String, Instrument)],
    now: i64,
) -> Vec<VenueView> {
    let mut names: BTreeSet<String> = statuses.iter().map(|s| s.venue.clone()).collect();
    names.extend(references.iter().map(|(v, _, _)| v.clone()));
    names
        .into_iter()
        .map(|venue| {
            let status = statuses.iter().find(|s| s.venue == venue);
            let mut tickers: BTreeSet<&str> = references
                .iter()
                .filter(|(v, _, _)| *v == venue)
                .map(|(_, t, _)| t.as_str())
                .collect();
            if let Some(status) = status {
                tickers.extend(status.pairs.iter().map(|(t, _)| t.as_str()));
            }
            let dexes: BTreeSet<String> = references
                .iter()
                .filter(|(v, _, _)| *v == venue)
                .map(|(_, _, i)| i.dex.clone().unwrap_or_default())
                .collect();
            VenueView {
                venue: venue.clone(),
                dexes: dexes.into_iter().collect(),
                instruments: tickers.len(),
                health: status.map(|s| Health {
                    connection: s.connection.clone(),
                    subs_declared: s.subs_declared,
                    subs_held: s.subs_held,
                    last_event_micros: s.last_event_micros,
                    reported_at_micros: s.reported_at_micros,
                    current: s.is_current(now),
                }),
            }
        })
        .collect()
}

/// **Every instrument** a venue's capture follows or the tape has reference
/// data for, filtered.
pub fn instruments(
    statuses: &[VenueStatus],
    references: &[(String, String, Instrument)],
    query: &InstrumentQuery,
) -> Vec<InstrumentView> {
    let mut by: BTreeMap<(String, String), InstrumentView> = BTreeMap::new();
    for status in statuses {
        for (ticker, series) in &status.pairs {
            by.entry((status.venue.clone(), ticker.clone()))
                .or_insert_with(|| InstrumentView {
                    venue: status.venue.clone(),
                    ticker: ticker.clone(),
                    datasets: Vec::new(),
                    reference: None,
                    held: None,
                })
                .datasets
                .push(series.clone());
        }
    }
    for (venue, ticker, instrument) in references {
        by.entry((venue.clone(), ticker.clone()))
            .or_insert_with(|| InstrumentView {
                venue: venue.clone(),
                ticker: ticker.clone(),
                datasets: Vec::new(),
                reference: None,
                held: None,
            })
            .reference = Some(instrument.clone());
    }
    by.into_values()
        .filter(|view| query.venue.as_ref().is_none_or(|v| *v == view.venue))
        .filter(|view| match (&view.reference, &query.base) {
            (_, None) => true,
            (Some(r), Some(base)) => r.base == *base,
            (None, Some(_)) => false,
        })
        .filter(|view| match (&view.reference, &query.contract_type) {
            (_, None) => true,
            (Some(r), Some(kind)) => r.contract_type == *kind,
            (None, Some(_)) => false,
        })
        .filter(|view| match (&view.reference, query.active) {
            (_, None) => true,
            (Some(r), Some(active)) => r.active == active,
            (None, Some(_)) => false,
        })
        .collect()
}

/// **Each instrument's newest reference row as of `at`**, from the tape.
pub fn references(root: &Path, at: i64) -> Result<Vec<(String, String, Instrument)>, Refusal> {
    let batches = match Reader::open(root, &["kind=instruments"]) {
        Ok(reader) => reader
            .view(Window {
                kind: Kind::Instruments,
                from_micros: at - REFERENCE_LOOKBACK_MICROS,
                to_micros: at + 1,
                ticker: None,
            })
            .map_err(|e| Refusal::Unreadable(e.to_string()))?,
        Err(ReadError::NoFrontier { .. }) => return Ok(Vec::new()),
        Err(e) => return Err(Refusal::Unreadable(e.to_string())),
    };
    let mut newest: BTreeMap<(String, String), Instrument> = BTreeMap::new();
    for batch in &batches {
        for (venue, ticker, instrument) in reference_rows(batch)? {
            if instrument.observed_at > at {
                continue;
            }
            let key = (venue, ticker);
            if newest
                .get(&key)
                .is_none_or(|seen| seen.observed_at <= instrument.observed_at)
            {
                newest.insert(key, instrument);
            }
        }
    }
    Ok(newest.into_iter().map(|((v, t), i)| (v, t, i)).collect())
}

/// The tape's instrument rows, back into the one format. A column a file
/// written before it lacks is absent.
fn reference_rows(batch: &RecordBatch) -> Result<Vec<(String, String, Instrument)>, Refusal> {
    let text = |name: &str| {
        batch
            .column_by_name(name)
            .and_then(|c| c.as_any().downcast_ref::<StringArray>().cloned())
    };
    let int = |name: &str| {
        batch
            .column_by_name(name)
            .and_then(|c| c.as_any().downcast_ref::<Int64Array>().cloned())
    };
    let u32s = |name: &str| {
        batch
            .column_by_name(name)
            .and_then(|c| c.as_any().downcast_ref::<UInt32Array>().cloned())
    };
    let dec = |name: &str| {
        batch
            .column_by_name(name)
            .and_then(|c| c.as_any().downcast_ref::<Decimal128Array>().cloned())
    };
    let flag = |name: &str| {
        batch
            .column_by_name(name)
            .and_then(|c| c.as_any().downcast_ref::<BooleanArray>().cloned())
    };
    let need =
        |name: &'static str| Refusal::Unreadable(format!("the instruments dataset has no {name}"));
    let (venue, ticker) = (
        text("venue").ok_or_else(|| need("venue"))?,
        text("ticker").ok_or_else(|| need("ticker"))?,
    );
    let (tick, lot, min) = (
        dec("tick_size").ok_or_else(|| need("tick_size"))?,
        dec("lot_size").ok_or_else(|| need("lot_size"))?,
        dec("min_size").ok_or_else(|| need("min_size"))?,
    );
    let (contract, base, quote, hours) = (
        text("contract_type").ok_or_else(|| need("contract_type"))?,
        text("base").ok_or_else(|| need("base"))?,
        text("quote").ok_or_else(|| need("quote"))?,
        text("hours").ok_or_else(|| need("hours"))?,
    );
    let active = flag("active").ok_or_else(|| need("active"))?;
    let observed = int("observed_at").ok_or_else(|| need("observed_at"))?;
    let (index, multiplier, strike, expiry, right) = (
        u32s("venue_index"),
        dec("ui_multiplier"),
        dec("strike"),
        int("expiry_micros"),
        text("right"),
    );
    let (symbol, dex, figures, notional, leverage, isolated) = (
        text("venue_symbol"),
        text("dex"),
        u32s("price_figures"),
        dec("min_notional"),
        u32s("max_leverage"),
        flag("isolated_only"),
    );
    fn num(v: i128) -> Num {
        Num::from_i128_with_scale(v, 18).normalize()
    }
    fn opt<A: Array, T>(a: &Option<A>, i: usize, f: impl Fn(&A, usize) -> T) -> Option<T> {
        a.as_ref().filter(|a| !a.is_null(i)).map(|a| f(a, i))
    }
    let mut out = Vec::with_capacity(batch.num_rows());
    for i in 0..batch.num_rows() {
        out.push((
            venue.value(i).to_string(),
            ticker.value(i).to_string(),
            Instrument {
                tick_size: num(tick.value(i)),
                lot_size: num(lot.value(i)),
                min_size: num(min.value(i)),
                contract_type: contract.value(i).to_string(),
                base: base.value(i).to_string(),
                quote: quote.value(i).to_string(),
                hours: hours.value(i).to_string(),
                active: active.value(i),
                venue_index: opt(&index, i, |a, i| a.value(i)),
                ui_multiplier: opt(&multiplier, i, |a, i| num(a.value(i))),
                observed_at: observed.value(i),
                strike: opt(&strike, i, |a, i| num(a.value(i))),
                expiry_micros: opt(&expiry, i, |a, i| a.value(i)),
                right: opt(&right, i, |a, i| a.value(i).to_string()),
                venue_symbol: opt(&symbol, i, |a, i| a.value(i).to_string()),
                dex: opt(&dex, i, |a, i| a.value(i).to_string()),
                price_figures: opt(&figures, i, |a, i| a.value(i))
                    .and_then(|f| u8::try_from(f).ok()),
                min_notional: opt(&notional, i, |a, i| num(a.value(i))),
                max_leverage: opt(&leverage, i, |a, i| a.value(i)),
                isolated_only: opt(&isolated, i, |a, i| a.value(i)),
            },
        ));
    }
    Ok(out)
}

/// The days a dataset is partitioned into, oldest first, as micros.
fn days(root: &Path, kind: Kind) -> Vec<i64> {
    let Ok(entries) = std::fs::read_dir(root.join(format!("kind={}", kind.as_str()))) else {
        return Vec::new();
    };
    let mut days: Vec<i64> = entries
        .filter_map(|e| e.ok()?.file_name().into_string().ok())
        .filter_map(|name| midnight_of(name.strip_prefix("date=")?))
        .collect();
    days.sort_unstable();
    days
}

/// One instrument's rows of one dataset on one day: each row's moment (venue
/// time, else receipt), and for candles its interval.
fn day_rows(
    reader: &Reader,
    kind: Kind,
    venue: &str,
    ticker: &str,
    day: i64,
) -> Result<Vec<(i64, Option<i64>)>, Refusal> {
    let batches = reader
        .view(Window {
            kind,
            from_micros: day,
            to_micros: day + DAY,
            ticker: Some(ticker.to_string()),
        })
        .map_err(|e| Refusal::Unreadable(e.to_string()))?;
    let mut out = Vec::new();
    for batch in &batches {
        let venues = batch
            .column_by_name("venue")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>());
        let at = batch
            .column_by_name("at_micros")
            .and_then(|c| c.as_any().downcast_ref::<Int64Array>());
        let recv = batch
            .column_by_name("recv_micros")
            .and_then(|c| c.as_any().downcast_ref::<Int64Array>());
        let interval = batch
            .column_by_name("interval")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>());
        let (Some(venues), Some(at), Some(recv)) = (venues, at, recv) else {
            return Err(Refusal::Unreadable(format!(
                "{kind} rows without venue or times"
            )));
        };
        for i in 0..batch.num_rows() {
            if venues.value(i) != venue {
                continue;
            }
            let moment = if at.is_null(i) {
                recv.value(i)
            } else {
                at.value(i)
            };
            out.push((moment, interval.and_then(|c| interval_micros(c.value(i)))));
        }
    }
    Ok(out)
}

/// **What the tape holds for one instrument**, reading its first and last
/// days of each dataset.
pub fn held(root: &Path, venue: &str, ticker: &str) -> Result<Held, Refusal> {
    let mut held = Held::default();
    for kind in [Kind::Quotes, Kind::Funding, Kind::Marks, Kind::Candles] {
        let scope = format!("kind={}", kind.as_str());
        let reader = match Reader::open(root, &[scope.as_str()]) {
            Ok(reader) => reader,
            Err(ReadError::NoFrontier { .. }) => continue,
            Err(e) => return Err(Refusal::Unreadable(e.to_string())),
        };
        let days = days(root, kind);
        // Where it ends, and, for candles, the intervals there are: the newest
        // two days with rows.
        let mut last: Option<i64> = None;
        let mut ends: BTreeMap<i64, i64> = BTreeMap::new();
        let mut seen_days = 0;
        for &day in days.iter().rev() {
            let rows = day_rows(&reader, kind, venue, ticker, day)?;
            if rows.is_empty() {
                continue;
            }
            for &(moment, interval) in &rows {
                last = Some(last.map_or(moment, |l| l.max(moment)));
                if let Some(width) = interval {
                    let close = moment + width;
                    let end = ends.entry(width).or_insert(close);
                    *end = (*end).max(close);
                }
            }
            seen_days += 1;
            if kind != Kind::Candles || seen_days == 2 {
                break;
            }
        }
        let Some(last) = last else { continue };
        // Where it begins: oldest days first, until each interval has begun.
        let mut first: Option<i64> = None;
        let mut begins: BTreeMap<i64, i64> = BTreeMap::new();
        for &day in &days {
            for (moment, interval) in day_rows(&reader, kind, venue, ticker, day)? {
                first = Some(first.map_or(moment, |f| f.min(moment)));
                if let Some(width) = interval {
                    let begin = begins.entry(width).or_insert(moment);
                    *begin = (*begin).min(moment);
                }
            }
            let done = if kind == Kind::Candles {
                ends.keys().all(|w| begins.contains_key(w))
            } else {
                first.is_some()
            };
            if done {
                break;
            }
        }
        let Some(first) = first else { continue };
        held.datasets.insert(
            kind.as_str().to_string(),
            Span {
                from_micros: first,
                to_micros: ends
                    .values()
                    .copied()
                    .max()
                    .unwrap_or(last + 1)
                    .max(last + 1),
            },
        );
        for (width, to) in ends {
            if let Some(&from) = begins.get(&width) {
                held.candles.insert(
                    interval_label(width),
                    Span {
                        from_micros: from,
                        to_micros: to,
                    },
                );
            }
        }
    }
    Ok(held)
}

/// What is held per instrument, kept for [`HELD_FOR`].
#[derive(Debug, Clone, Default)]
pub struct HeldCache(Arc<Mutex<HeldMap>>);

/// What is held, by (venue, ticker), with when it was found.
type HeldMap = HashMap<(String, String), (tokio::time::Instant, Held)>;

impl HeldCache {
    fn get(&self, key: &(String, String)) -> Option<Held> {
        let held = self.0.lock().ok()?;
        let (at, value) = held.get(key)?;
        (at.elapsed() < HELD_FOR).then(|| value.clone())
    }

    fn put(&self, key: (String, String), value: Held) {
        if let Ok(mut held) = self.0.lock() {
            held.insert(key, (tokio::time::Instant::now(), value));
        }
    }
}

async fn blocking<T: Send + 'static>(
    f: impl FnOnce() -> Result<T, Refusal> + Send + 'static,
) -> Result<T, Refusal> {
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|e| Refusal::Unreadable(e.to_string()))?
}

/// `GET /v1/venues`.
pub async fn venues_route(State(markets): State<Shared>) -> Response {
    let now = crate::now_micros();
    let root = markets.tape.clone();
    match blocking(move || references(&root, now)).await {
        Ok(refs) => Json(venues(&markets.statuses.latest(), &refs, now)).into_response(),
        Err(refusal) => refusal.into_response(),
    }
}

/// `GET /v1/instruments`.
pub async fn instruments_route(
    State(markets): State<Shared>,
    Query(query): Query<InstrumentQuery>,
) -> Response {
    let at = query.at.unwrap_or_else(crate::now_micros);
    let root = markets.tape.clone();
    match blocking(move || references(&root, at)).await {
        Ok(refs) => Json(instruments(&markets.statuses.latest(), &refs, &query)).into_response(),
        Err(refusal) => refusal.into_response(),
    }
}

/// `GET /v1/instruments/{venue}/{ticker}`.
pub async fn instrument_route(
    State(markets): State<Shared>,
    UrlPath((venue, ticker)): UrlPath<(String, String)>,
) -> Response {
    let now = crate::now_micros();
    let root = markets.tape.clone();
    let refs = match blocking(move || references(&root, now)).await {
        Ok(refs) => refs,
        Err(refusal) => return refusal.into_response(),
    };
    let query = InstrumentQuery {
        venue: Some(venue.clone()),
        ..InstrumentQuery::default()
    };
    let Some(mut view) = instruments(&markets.statuses.latest(), &refs, &query)
        .into_iter()
        .find(|v| v.ticker == ticker)
    else {
        return (
            axum::http::StatusCode::NOT_FOUND,
            format!("{venue} has no instrument {ticker}\n"),
        )
            .into_response();
    };
    let key = (venue.clone(), ticker.clone());
    let held = match markets.held.get(&key) {
        Some(held) => held,
        None => {
            let root = markets.tape.clone();
            match blocking(move || held(&root, &venue, &ticker)).await {
                Ok(held) => {
                    markets.held.put(key, held.clone());
                    held
                }
                Err(refusal) => return refusal.into_response(),
            }
        }
    };
    view.held = Some(held);
    Json(view).into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::status::tests::report;
    use galata_datawatch::tape::{Row, Tape};
    use galata_wire::{Candle, Envelope, Event, Funding, Ticker, Venue};
    use std::str::FromStr;

    /// 2026-11-26 00:00 UTC.
    const T0: i64 = 1_795_651_200_000_000;
    const HOUR: i64 = 3_600_000_000;

    fn reference(
        base: &str,
        dex: Option<&str>,
        symbol: &str,
        leverage: u32,
        observed: i64,
    ) -> Instrument {
        Instrument {
            tick_size: Num::from_str("0.1").unwrap(),
            lot_size: Num::from_str("0.00001").unwrap(),
            min_size: Num::from_str("0.00001").unwrap(),
            contract_type: "perp".into(),
            base: base.into(),
            quote: "USDC".into(),
            hours: "continuous".into(),
            active: true,
            venue_index: Some(0),
            ui_multiplier: None,
            observed_at: observed,
            strike: None,
            expiry_micros: None,
            right: None,
            venue_symbol: Some(symbol.into()),
            dex: dex.map(str::to_string),
            price_figures: Some(5),
            min_notional: Some(Num::from(10)),
            max_leverage: Some(leverage),
            isolated_only: Some(false),
        }
    }

    /// A row at a stream position of its venue's own: the tape names a
    /// segment by its positions, and two venues' segments must not share one.
    fn row(venue: &str, ticker: &str, event: Event, recv: i64, at: Option<i64>) -> Row {
        Row {
            stream_seq: if venue == "hyperliquid" { 1_000 } else { 9_000 },
            source_recv_micros: recv,
            envelope: Envelope::new(
                Venue::new(venue).unwrap(),
                Ticker::new(ticker).unwrap(),
                at,
                recv,
                event,
            ),
        }
    }

    /// Reference rows for hyperliquid's BTC (leverage 50, then 40) and GOLD on
    /// `xyz`, and derive's BTC-PERP.
    fn tape() -> tempfile::TempDir {
        let root = tempfile::tempdir().unwrap();
        let mut tape = Tape::open(root.path());
        let at = |h: i64| T0 + h * HOUR;
        tape.take(row(
            "hyperliquid",
            "BTC",
            Event::Instrument(reference("BTC", None, "BTC", 50, at(1))),
            at(1),
            None,
        ));
        tape.take(row(
            "hyperliquid",
            "BTC",
            Event::Instrument(reference("BTC", None, "BTC", 40, at(5))),
            at(5),
            None,
        ));
        tape.take(row(
            "hyperliquid",
            "GOLD",
            Event::Instrument(reference("GOLD", Some("xyz"), "xyz:GOLD", 25, at(5))),
            at(5),
            None,
        ));
        tape.take(row(
            "derive-xyz",
            "BTC-PERP",
            Event::Instrument(reference("BTC", None, "BTC-PERP", 1, at(5))),
            at(5),
            None,
        ));
        tape.commit().unwrap();
        root
    }

    fn statuses(at: i64) -> Vec<VenueStatus> {
        vec![
            VenueStatus::parse(&report(
                "hyperliquid",
                at,
                &[
                    ("BTC", "quotes", at),
                    ("BTC", "candles", at),
                    ("GOLD", "quotes", at),
                    ("ETH", "quotes", at),
                ],
            ))
            .unwrap(),
        ]
    }

    #[test]
    fn venues_are_listed_with_their_dexes_and_their_captures_health() {
        let root = tape();
        let now = T0 + 6 * HOUR;
        let refs = references(root.path(), now).unwrap();
        let listed = venues(&statuses(now - 1_000_000), &refs, now);
        let hl = listed.iter().find(|v| v.venue == "hyperliquid").unwrap();
        assert_eq!(hl.dexes, ["", "xyz"], "the main dex and xyz");
        assert_eq!(
            hl.instruments, 3,
            "BTC, GOLD and ETH, which has no reference row yet"
        );
        let health = hl.health.as_ref().unwrap();
        assert_eq!(
            (health.connection.as_str(), health.current),
            ("connected", true)
        );
        let derive = listed.iter().find(|v| v.venue == "derive-xyz").unwrap();
        assert!(
            derive.health.is_none(),
            "no status, so no health is claimed"
        );
    }

    #[test]
    fn a_venue_whose_capture_has_gone_quiet_is_not_shown_as_current() {
        let root = tape();
        let now = T0 + 6 * HOUR;
        let refs = references(root.path(), now).unwrap();
        let quiet = now - 10 * 60_000_000;
        let listed = venues(&statuses(quiet), &refs, now);
        let health = listed[1].health.as_ref().unwrap();
        assert!(!health.current);
        assert_eq!(
            health.reported_at_micros, quiet,
            "the moment of the last report"
        );
    }

    #[test]
    fn every_btc_instrument_comes_back_in_one_format_across_venues() {
        let root = tape();
        let refs = references(root.path(), T0 + 6 * HOUR).unwrap();
        let query = InstrumentQuery {
            base: Some("BTC".into()),
            ..InstrumentQuery::default()
        };
        let found = instruments(&statuses(T0), &refs, &query);
        let names: Vec<(&str, &str)> = found
            .iter()
            .map(|v| (v.venue.as_str(), v.ticker.as_str()))
            .collect();
        assert_eq!(names, [("derive-xyz", "BTC-PERP"), ("hyperliquid", "BTC")]);
        let btc = &found[1];
        assert_eq!(btc.datasets, ["quotes", "candles"]);
        let r = btc.reference.as_ref().unwrap();
        assert_eq!(r.venue_symbol.as_deref(), Some("BTC"));
        assert_eq!(r.max_leverage, Some(40), "the newest row");
        assert_eq!(r.tick_size, Num::from_str("0.1").unwrap());
    }

    #[test]
    fn as_of_a_past_moment_an_instrument_is_as_it_stood_then() {
        let root = tape();
        let refs = references(root.path(), T0 + 3 * HOUR).unwrap();
        let btc = refs
            .iter()
            .find(|(v, t, _)| v == "hyperliquid" && t == "BTC")
            .unwrap();
        assert_eq!(btc.2.max_leverage, Some(50));
    }

    #[test]
    fn an_instrument_with_no_reference_row_is_listed_with_it_absent() {
        let root = tape();
        let refs = references(root.path(), T0 + 6 * HOUR).unwrap();
        let found = instruments(&statuses(T0), &refs, &InstrumentQuery::default());
        let eth = found.iter().find(|v| v.ticker == "ETH").unwrap();
        assert_eq!(eth.datasets, ["quotes"]);
        assert!(eth.reference.is_none());
        let json = serde_json::to_value(eth).unwrap();
        assert!(json["reference"].is_null());
        assert!(json.get("held").is_none());
    }

    #[test]
    fn what_is_held_for_an_instrument_per_dataset_and_candle_interval() {
        // The venue's hourly bars from 2025-06-01, galata's minutes from
        // 2026-11-25, and funding for two days.
        let root = tempfile::tempdir().unwrap();
        let mut tape = Tape::open(root.path());
        let candle = |interval: &str, at: i64| {
            row(
                "hyperliquid",
                "BTC",
                Event::Candle(Candle {
                    interval: interval.into(),
                    open: Num::from(1),
                    high: Num::from(1),
                    low: Num::from(1),
                    close: Num::from(1),
                    volume: Num::from(1),
                    trade_count: None,
                    is_final: true,
                }),
                at + 1,
                Some(at),
            )
        };
        let old = 1_748_736_000_000_000; // 2025-06-01
        tape.take(candle("1h", old));
        tape.take(candle("1h", T0 + 5 * HOUR));
        let minutes_from = T0 - 24 * HOUR;
        tape.take(candle("1m", minutes_from));
        tape.take(candle("1m", T0 + 5 * HOUR));
        let funding = |at: i64| {
            row(
                "hyperliquid",
                "BTC",
                Event::Funding(Funding {
                    rate: Num::from(0),
                    next_micros: None,
                    premium: None,
                }),
                at + 1,
                Some(at),
            )
        };
        tape.take(funding(T0 - 24 * HOUR));
        tape.take(funding(T0 + 3 * HOUR));
        tape.commit().unwrap();

        let held = held(root.path(), "hyperliquid", "BTC").unwrap();
        assert_eq!(
            held.candles["1h"],
            Span {
                from_micros: old,
                to_micros: T0 + 6 * HOUR
            }
        );
        assert_eq!(
            held.candles["1m"],
            Span {
                from_micros: minutes_from,
                to_micros: T0 + 5 * HOUR + 60_000_000
            }
        );
        assert_eq!(
            held.datasets["funding"],
            Span {
                from_micros: T0 - 24 * HOUR,
                to_micros: T0 + 3 * HOUR + 1
            }
        );
        assert!(!held.datasets.contains_key("quotes"));
    }
}
