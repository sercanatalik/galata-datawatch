//! **History from the tape** (`market-history`, design D3).
//!
//! One dataset for one venue over a venue-time window, as an Arrow IPC stream
//! in the tape's own columns: prices and sizes stay `Decimal128(38, 18)`, so
//! nothing on the way rounds them. Each answer is read through a
//! [`Reader`] opened for it, so it holds
//! nothing past what the tape has durably written, and it says where that is:
//! the schema's metadata carries `galata.bound.<venue>`, the stream position
//! a client continues the live stream after.
//!
//! **Refused, never cut short**: a window or an answer larger than the stated
//! [`Limits`] is refused naming the limit, so a client never mistakes a
//! truncated answer for the whole one. Quotes are served one ticker at a time.
//!
//! A window can span tape files written before a column was added: each
//! batch is conformed to the dataset's current schema, an absent nullable
//! column filled with nulls, so one answer has one schema.

use std::collections::HashMap;
use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, BooleanArray, StringArray, new_null_array};
use arrow::compute::{
    SortColumn, SortOptions, concat_batches, filter_record_batch, lexsort_to_indices,
    take_record_batch,
};
use arrow::datatypes::{Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use axum::extract::{Path as UrlPath, Query, State};
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use galata_datawatch::tape::{ReadError, Reader, Window, schema_for};
use galata_wire::Kind;
use serde::Deserialize;

use crate::Shared;

/// The media type of an Arrow IPC stream.
pub const ARROW_STREAM: &str = "application/vnd.apache.arrow.stream";

/// The datasets served, by the name a client asks with.
pub const DATASETS: [Kind; 6] = [
    Kind::Quotes,
    Kind::Candles,
    Kind::Funding,
    Kind::Marks,
    Kind::Gaps,
    Kind::Instruments,
];

/// The schema metadata key stating where one venue's tape ends.
pub fn bound_key(venue: &str) -> String {
    format!("galata.bound.{venue}")
}

const DAY: i64 = 86_400_000_000;

/// How much one answer may hold. **Proposed until measured** on the
/// deployment (`add-galata-markets`, task 6.4).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limits {
    /// The longest window for quotes, which are served one ticker at a time.
    pub quote_window_micros: i64,
    /// The longest window for every other dataset.
    pub window_micros: i64,
    /// The most rows one answer holds. It also bounds what an answer holds in
    /// memory, since a window is read whole before it is sent.
    pub rows: usize,
    /// The most bars one candle answer holds.
    pub bars: usize,
}

impl Default for Limits {
    fn default() -> Limits {
        Limits {
            quote_window_micros: DAY,
            window_micros: 31 * DAY,
            rows: 2_000_000,
            bars: 200_000,
        }
    }
}

/// What a client asks for.
#[derive(Debug, Clone, Deserialize)]
pub struct TapeQuery {
    /// The venue.
    pub venue: String,
    /// One instrument, or every instrument where the dataset allows it.
    pub ticker: Option<String>,
    /// Start, venue micros, inclusive.
    pub from: i64,
    /// End, venue micros, exclusive.
    pub to: i64,
}

/// Why an answer was not given.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
#[non_exhaustive]
pub enum Refusal {
    /// A dataset not served.
    #[error("{0:?} is not a dataset served here; ask for one of {1}")]
    UnknownDataset(String, String),
    /// A window whose end is not after its start.
    #[error("the window [{from}, {to}) runs backwards or is empty")]
    Backwards {
        /// Its start.
        from: i64,
        /// Its end.
        to: i64,
    },
    /// Quotes asked for with no ticker.
    #[error("quotes are served one ticker at a time; name one")]
    QuotesNeedTicker,
    /// A window longer than its dataset's limit.
    #[error("the window is longer than the {limit_days}-day limit for {kind}; ask again in parts")]
    WindowTooLong {
        /// The dataset.
        kind: String,
        /// Its limit, in days.
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
    /// A period not offered, in the bars module's words.
    #[error("{0}")]
    Period(String),
    /// More bars than one answer holds.
    #[error(
        "the answer would hold {bars} bars, more than the limit of {limit}; ask for a shorter window or a longer period"
    )]
    TooManyBars {
        /// What it would hold.
        bars: usize,
        /// The limit.
        limit: usize,
    },
    /// The tape would not read.
    #[error("the tape could not be read: {0}")]
    Unreadable(String),
}

impl IntoResponse for Refusal {
    fn into_response(self) -> Response {
        let status = match self {
            Refusal::Unreadable(_) => StatusCode::INTERNAL_SERVER_ERROR,
            _ => StatusCode::BAD_REQUEST,
        };
        (status, format!("{self}\n")).into_response()
    }
}

/// A dataset by the name a client used, where it is one served here.
pub fn dataset(name: &str) -> Result<Kind, Refusal> {
    Kind::from_str(name)
        .ok()
        .filter(|kind| DATASETS.contains(kind))
        .ok_or_else(|| {
            Refusal::UnknownDataset(name.to_string(), DATASETS.map(|k| k.as_str()).join(", "))
        })
}

/// An answer: one schema, carrying where the tape ends, and its rows.
#[derive(Debug, Clone)]
pub struct Answer {
    /// The dataset's schema, with the bound in its metadata.
    pub schema: SchemaRef,
    /// Oldest first.
    pub batches: Vec<RecordBatch>,
}

/// **Read one dataset's window for one venue**, as the bound permits.
pub fn read(root: &Path, kind: Kind, query: &TapeQuery, limits: Limits) -> Result<Answer, Refusal> {
    if query.to <= query.from {
        return Err(Refusal::Backwards {
            from: query.from,
            to: query.to,
        });
    }
    let longest = if kind == Kind::Quotes {
        if query.ticker.is_none() {
            return Err(Refusal::QuotesNeedTicker);
        }
        limits.quote_window_micros
    } else {
        limits.window_micros
    };
    if query.to - query.from > longest {
        return Err(Refusal::WindowTooLong {
            kind: kind.as_str().to_string(),
            limit_days: longest / DAY,
        });
    }
    let current =
        schema_for(kind).ok_or_else(|| Refusal::Unreadable(format!("{kind} has no schema")))?;

    let scope = format!("kind={}", kind.as_str());
    let (bound, read) = match Reader::open(root, &[scope.as_str()]) {
        Ok(reader) => {
            let bound = reader.bound().of_venue(&query.venue);
            let batches = reader
                .view(Window {
                    kind,
                    from_micros: query.from,
                    to_micros: query.to,
                    ticker: query.ticker.clone(),
                })
                .map_err(|e| Refusal::Unreadable(e.to_string()))?;
            (bound, batches)
        }
        // Nothing written for this dataset yet: an empty answer, not an error.
        Err(ReadError::NoFrontier { .. }) => (None, Vec::new()),
        Err(e) => return Err(Refusal::Unreadable(e.to_string())),
    };

    let mut metadata = HashMap::new();
    metadata.insert(
        bound_key(&query.venue),
        bound.map_or_else(|| "none".to_string(), |b| b.to_string()),
    );
    let schema = Arc::new(Schema::new_with_metadata(
        current.fields().clone(),
        metadata,
    ));

    let mut rows = Vec::with_capacity(read.len());
    for batch in &read {
        let conformed = conform(batch, &schema)?;
        let mine = of_venue(&conformed, &query.venue)?;
        if mine.num_rows() > 0 {
            rows.push(mine);
        }
    }
    let held: usize = rows.iter().map(RecordBatch::num_rows).sum();
    if held > limits.rows {
        return Err(Refusal::TooManyRows {
            rows: held,
            limit: limits.rows,
        });
    }
    let batches = if rows.is_empty() {
        Vec::new()
    } else {
        vec![oldest_first(
            &concat_batches(&schema, &rows).map_err(unreadable)?,
        )?]
    };
    Ok(Answer { schema, batches })
}

/// The answer as an Arrow IPC stream.
pub fn encode(answer: &Answer) -> Result<Vec<u8>, Refusal> {
    let mut writer = arrow::ipc::writer::StreamWriter::try_new(Vec::new(), &answer.schema)
        .map_err(unreadable)?;
    for batch in &answer.batches {
        writer.write(batch).map_err(unreadable)?;
    }
    writer.into_inner().map_err(unreadable)
}

/// `GET /v1/tape/{kind}`.
pub async fn tape(
    State(markets): State<Shared>,
    UrlPath(kind): UrlPath<String>,
    Query(query): Query<TapeQuery>,
) -> Response {
    let kind = match dataset(&kind) {
        Ok(kind) => kind,
        Err(refusal) => return refusal.into_response(),
    };
    let root = markets.tape.clone();
    let limits = markets.limits;
    let answered = tokio::task::spawn_blocking(move || {
        read(&root, kind, &query, limits).and_then(|a| encode(&a))
    })
    .await;
    match answered {
        Ok(Ok(bytes)) => ([(header::CONTENT_TYPE, ARROW_STREAM)], bytes).into_response(),
        Ok(Err(refusal)) => refusal.into_response(),
        Err(join) => Refusal::Unreadable(join.to_string()).into_response(),
    }
}

fn unreadable(error: impl std::fmt::Display) -> Refusal {
    Refusal::Unreadable(error.to_string())
}

/// A batch in the current schema's columns: a column it lacks is null, which
/// only a nullable column may be.
fn conform(batch: &RecordBatch, schema: &SchemaRef) -> Result<RecordBatch, Refusal> {
    let columns: Vec<ArrayRef> = schema
        .fields()
        .iter()
        .map(|field| match batch.column_by_name(field.name()) {
            Some(column) if column.data_type() == field.data_type() => Ok(column.clone()),
            Some(column) => Err(Refusal::Unreadable(format!(
                "{} is {} in a tape file, {} in the dataset",
                field.name(),
                column.data_type(),
                field.data_type()
            ))),
            None => Ok(new_null_array(field.data_type(), batch.num_rows())),
        })
        .collect::<Result<_, _>>()?;
    RecordBatch::try_new(schema.clone(), columns).map_err(unreadable)
}

/// The rows of one venue.
fn of_venue(batch: &RecordBatch, venue: &str) -> Result<RecordBatch, Refusal> {
    let venues = batch
        .column_by_name("venue")
        .and_then(|c| c.as_any().downcast_ref::<StringArray>())
        .ok_or_else(|| Refusal::Unreadable("no venue column".into()))?;
    let mask: BooleanArray = venues.iter().map(|v| Some(v == Some(venue))).collect();
    filter_record_batch(batch, &mask).map_err(unreadable)
}

/// Oldest first, by venue time and then receipt time, so a row with no venue
/// time sits by when it arrived; ticker last.
fn oldest_first(batch: &RecordBatch) -> Result<RecordBatch, Refusal> {
    let key = |name: &str, nulls_first| -> Result<SortColumn, Refusal> {
        Ok(SortColumn {
            values: batch
                .column_by_name(name)
                .ok_or_else(|| Refusal::Unreadable(format!("no {name} column")))?
                .clone(),
            options: Some(SortOptions {
                descending: false,
                nulls_first,
            }),
        })
    };
    let indices = lexsort_to_indices(
        &[
            key("at_micros", false)?,
            key("recv_micros", false)?,
            key("ticker", false)?,
        ],
        None,
    )
    .map_err(unreadable)?;
    take_record_batch(batch, &indices).map_err(unreadable)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Decimal128Array, Int64Array};
    use galata_datawatch::tape::{Row, Tape};
    use galata_wire::{Envelope, Event, Funding, Num, Ticker, Venue};

    /// 2026-11-25 00:00 UTC.
    const T0: i64 = 1_795_564_800_000_000;
    const HOUR: i64 = 3_600_000_000;

    fn funding(venue: &str, ticker: &str, hour: i64, rate: &str, seq: u64) -> Row {
        let at = T0 + hour * HOUR;
        Row {
            stream_seq: seq,
            source_recv_micros: at + 1_000,
            envelope: Envelope::new(
                Venue::new(venue).unwrap(),
                Ticker::new(ticker).unwrap(),
                Some(at),
                at + 1_000,
                Event::Funding(Funding {
                    rate: Num::from_str(rate).unwrap(),
                    next_micros: None,
                    premium: None,
                }),
            ),
        }
    }

    /// A day of hourly funding for BTC and ETH on hyperliquid, written newest
    /// first, and one row of another venue's.
    fn a_day() -> tempfile::TempDir {
        let root = tempfile::tempdir().unwrap();
        let mut tape = Tape::open(root.path());
        for hour in (0..24).rev() {
            tape.take(funding(
                "hyperliquid",
                "BTC",
                hour,
                "0.0000125",
                100 + hour as u64,
            ));
            tape.take(funding(
                "hyperliquid",
                "ETH",
                hour,
                "-0.00000123456789",
                200 + hour as u64,
            ));
        }
        tape.take(funding("derive-xyz", "BTC-PERP", 3, "0.0001", 7));
        tape.commit().unwrap();
        root
    }

    fn query(ticker: Option<&str>, from: i64, to: i64) -> TapeQuery {
        TapeQuery {
            venue: "hyperliquid".into(),
            ticker: ticker.map(str::to_string),
            from,
            to,
        }
    }

    fn decode(bytes: &[u8]) -> (SchemaRef, Vec<RecordBatch>) {
        let reader = arrow::ipc::reader::StreamReader::try_new(bytes, None).unwrap();
        let schema = reader.schema();
        (schema, reader.map(Result::unwrap).collect())
    }

    #[test]
    fn a_day_of_btc_funding_comes_back_oldest_first_and_exact() {
        let root = a_day();
        let answer = read(
            root.path(),
            Kind::Funding,
            &query(Some("BTC"), T0, T0 + 24 * HOUR),
            Limits::default(),
        )
        .unwrap();
        let (schema, batches) = decode(&encode(&answer).unwrap());
        let batch = &batches[0];
        assert_eq!(batches.len(), 1);
        assert_eq!(batch.num_rows(), 24, "one venue, one ticker");
        let at = batch
            .column_by_name("at_micros")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert!(at.values().windows(2).all(|w| w[0] < w[1]), "oldest first");
        assert_eq!(at.value(0), T0);
        let rate = batch
            .column_by_name("rate")
            .unwrap()
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap();
        assert_eq!(
            rate.value_as_string(0),
            "0.000012500000000000",
            "exact, never a float"
        );
        // The venue's bound, across every ticker it wrote: ETH's rows reach 223.
        assert_eq!(
            schema
                .metadata()
                .get("galata.bound.hyperliquid")
                .map(String::as_str),
            Some("223")
        );
    }

    #[test]
    fn a_decimal_round_trips_exactly_through_the_stream() {
        let root = a_day();
        let answer = read(
            root.path(),
            Kind::Funding,
            &query(Some("ETH"), T0, T0 + HOUR),
            Limits::default(),
        )
        .unwrap();
        let (_, batches) = decode(&encode(&answer).unwrap());
        let rate = batches[0]
            .column_by_name("rate")
            .unwrap()
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap();
        assert_eq!(rate.value_as_string(0), "-0.000001234567890000");
    }

    #[test]
    fn every_ticker_of_one_venue_where_the_dataset_allows_it() {
        let root = a_day();
        let answer = read(
            root.path(),
            Kind::Funding,
            &query(None, T0, T0 + 2 * HOUR),
            Limits::default(),
        )
        .unwrap();
        assert_eq!(
            answer.batches[0].num_rows(),
            4,
            "BTC and ETH, two hours; not derive's"
        );
    }

    #[test]
    fn a_window_that_runs_backwards_is_refused_naming_it() {
        let root = a_day();
        let refusal = read(
            root.path(),
            Kind::Funding,
            &query(Some("BTC"), T0 + HOUR, T0),
            Limits::default(),
        )
        .unwrap_err();
        assert!(matches!(refusal, Refusal::Backwards { .. }));
        assert!(refusal.to_string().contains(&T0.to_string()));
    }

    #[test]
    fn quotes_for_every_ticker_are_refused() {
        let root = a_day();
        let refusal = read(
            root.path(),
            Kind::Quotes,
            &query(None, T0, T0 + HOUR),
            Limits::default(),
        )
        .unwrap_err();
        assert_eq!(refusal, Refusal::QuotesNeedTicker);
        assert!(refusal.to_string().contains("one ticker at a time"));
    }

    #[test]
    fn a_window_past_the_limit_is_refused_naming_the_limit() {
        let root = a_day();
        let refusal = read(
            root.path(),
            Kind::Quotes,
            &query(Some("BTC"), T0, T0 + 2 * DAY),
            Limits::default(),
        )
        .unwrap_err();
        assert!(
            refusal.to_string().contains("1-day limit for quotes"),
            "{refusal}"
        );
        let refusal = read(
            root.path(),
            Kind::Funding,
            &query(Some("BTC"), T0, T0 + 40 * DAY),
            Limits::default(),
        )
        .unwrap_err();
        assert!(refusal.to_string().contains("31-day limit"), "{refusal}");
    }

    #[test]
    fn an_answer_past_the_row_limit_is_refused_not_cut_short() {
        let root = a_day();
        let limits = Limits {
            rows: 10,
            ..Limits::default()
        };
        let refusal = read(
            root.path(),
            Kind::Funding,
            &query(Some("BTC"), T0, T0 + 24 * HOUR),
            limits,
        )
        .unwrap_err();
        assert_eq!(
            refusal,
            Refusal::TooManyRows {
                rows: 24,
                limit: 10
            }
        );
    }

    #[test]
    fn a_dataset_not_served_is_refused_listing_those_that_are() {
        let refusal = dataset("trades").unwrap_err().to_string();
        assert!(
            refusal.contains("quotes, candles, funding, marks, gaps, instruments"),
            "{refusal}"
        );
    }

    #[test]
    fn a_dataset_nothing_wrote_is_an_empty_answer_with_no_bound() {
        let root = tempfile::tempdir().unwrap();
        let answer = read(
            root.path(),
            Kind::Marks,
            &query(Some("BTC"), T0, T0 + HOUR),
            Limits::default(),
        )
        .unwrap();
        let (schema, batches) = decode(&encode(&answer).unwrap());
        assert!(batches.is_empty());
        assert_eq!(
            schema
                .metadata()
                .get("galata.bound.hyperliquid")
                .map(String::as_str),
            Some("none")
        );
    }

    #[test]
    fn a_file_from_before_a_column_was_added_reads_into_the_current_schema() {
        let schema = schema_for(Kind::Instruments).unwrap();
        let old_width = schema.fields().len() - 6;
        let old_fields: Vec<_> = schema.fields().iter().take(old_width).cloned().collect();
        let old_schema = Arc::new(Schema::new(old_fields));
        let columns: Vec<ArrayRef> = old_schema
            .fields()
            .iter()
            .map(|f| new_null_array(f.data_type(), 0))
            .collect();
        let old = RecordBatch::try_new(old_schema, columns).unwrap();
        let conformed = conform(&old, &schema).unwrap();
        assert_eq!(conformed.num_columns(), schema.fields().len());
        assert!(conformed.column_by_name("venue_symbol").is_some());
    }
}
