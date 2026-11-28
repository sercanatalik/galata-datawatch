//! **Candles at a period** (`market-periods`, design D4): the tape's candles,
//! folded and resampled by `galata_datawatch::tape::bars`, sent as Arrow IPC.
//!
//! Most of the tape's 1m rows are updates to a forming bar — about 36,000 a
//! day for one instrument — so a window is read **a day at a time** and each
//! day folded into the latest version of each source bar before the next is
//! read: what is held at once is a day's rows, not the window's. The answer is
//! limited by its bars, not by the window's length.
//!
//! The answer's metadata says where the tape ends (`galata.bound.<venue>`) and
//! which spans of the window no source covers (`galata.unsourced`, a JSON list
//! of `[from, to)` pairs), which hold no bars.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use arrow::array::{
    ArrayRef, BooleanArray, Decimal128Array, Int64Array, StringArray, UInt32Array, UInt64Array,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use axum::extract::{Query, State};
use axum::http::header;
use axum::response::{IntoResponse, Response};
use galata_datawatch::tape::bars::{self, Bar, Period, SCALE, Sources};
use galata_datawatch::tape::{ReadError, Reader, Window};
use galata_wire::Kind;
use serde::Deserialize;

use crate::Shared;
use crate::history::{ARROW_STREAM, Answer, Refusal, bound_key, encode};

const DAY: i64 = 86_400_000_000;

/// The schema metadata key naming the spans no source covers.
pub const UNSOURCED: &str = "galata.unsourced";

/// What a client asks for.
#[derive(Debug, Clone, Deserialize)]
pub struct CandleQuery {
    /// The venue.
    pub venue: String,
    /// The instrument: candles are served one at a time.
    pub ticker: String,
    /// One of `tape::bars::PERIODS`.
    pub period: String,
    /// Start, venue micros: the bar holding it is the first.
    pub from: i64,
    /// End, venue micros, exclusive.
    pub to: i64,
    /// `json` for the same bars as JSON (`move-watcher-onto-markets`, design
    /// D8); Arrow IPC where absent.
    #[serde(default)]
    pub format: Option<String>,
}

/// A source interval's spelling: `1m`, `4h`, `1d`.
pub fn interval_label(micros: i64) -> String {
    const MINUTE: i64 = 60_000_000;
    match micros {
        m if m % (24 * 60 * MINUTE) == 0 => format!("{}d", m / (24 * 60 * MINUTE)),
        m if m % (60 * MINUTE) == 0 => format!("{}h", m / (60 * MINUTE)),
        m => format!("{}m", m / MINUTE),
    }
}

/// The bars' schema: who, when, the values exact, and what each bar is.
pub fn schema(metadata: HashMap<String, String>) -> SchemaRef {
    let price = DataType::Decimal128(38, SCALE);
    Arc::new(Schema::new_with_metadata(
        vec![
            Field::new("venue", DataType::Utf8, false),
            Field::new("ticker", DataType::Utf8, false),
            Field::new("start_micros", DataType::Int64, false),
            Field::new("period", DataType::Utf8, false),
            Field::new("source_interval", DataType::Utf8, false),
            Field::new("open", price.clone(), false),
            Field::new("high", price.clone(), false),
            Field::new("low", price.clone(), false),
            Field::new("close", price.clone(), false),
            Field::new("volume", price, false),
            Field::new("trade_count", DataType::UInt64, true),
            Field::new("sources", DataType::UInt32, false),
            Field::new("sources_expected", DataType::UInt32, false),
            Field::new("complete", DataType::Boolean, false),
            Field::new("gap", DataType::Boolean, false),
            Field::new("is_final", DataType::Boolean, false),
        ],
        metadata,
    ))
}

/// **Read one instrument's candles at a period.**
pub fn read(root: &Path, query: &CandleQuery, max_bars: usize) -> Result<Answer, Refusal> {
    let period = Period::parse(&query.period).map_err(|e| Refusal::Period(e.to_string()))?;
    if query.to <= query.from {
        return Err(Refusal::Backwards {
            from: query.from,
            to: query.to,
        });
    }
    let first = period.start_of(query.from);
    let end = period.start_of(query.to - 1) + period.micros();
    let wanted = ((end - first) / period.micros()) as usize;
    if wanted > max_bars {
        return Err(Refusal::TooManyBars {
            bars: wanted,
            limit: max_bars,
        });
    }

    let (bound, sources) = sources(root, &query.venue, &query.ticker, first, end)?;
    let gaps = candle_gaps(root, &query.venue, &query.ticker, first, end)?;
    let built = bars::bars(&sources, period, first, end, &gaps);

    let mut metadata = HashMap::new();
    metadata.insert(
        bound_key(&query.venue),
        bound.map_or_else(|| "none".to_string(), |b| b.to_string()),
    );
    metadata.insert(
        UNSOURCED.to_string(),
        serde_json::to_string(&built.unsourced).map_err(|e| Refusal::Unreadable(e.to_string()))?,
    );
    let schema = schema(metadata);
    let batches = if built.bars.is_empty() {
        Vec::new()
    } else {
        vec![batch(&schema, query, period, &built.bars)?]
    };
    Ok(Answer { schema, batches })
}

/// **One instrument's candle sources over `[from, to)`**, read a day at a time
/// and folded as they are read, and the venue's tape bound.
pub fn sources(
    root: &Path,
    venue: &str,
    ticker: &str,
    from: i64,
    to: i64,
) -> Result<(Option<i64>, Sources), Refusal> {
    match Reader::open(root, &["kind=candles"]) {
        Ok(reader) => {
            let mut sources = Sources::default();
            let mut day = from - from.rem_euclid(DAY);
            while day < to {
                let batches = reader
                    .view(Window {
                        kind: Kind::Candles,
                        from_micros: day.max(from),
                        to_micros: (day + DAY).min(to),
                        ticker: Some(ticker.to_string()),
                    })
                    .map_err(|e| Refusal::Unreadable(e.to_string()))?;
                let folded = Sources::from_batches(&batches, venue, ticker)
                    .map_err(|e| Refusal::Unreadable(e.to_string()))?;
                sources.merge(folded);
                day += DAY;
            }
            Ok((reader.bound().of_venue(venue), sources))
        }
        Err(ReadError::NoFrontier { .. }) => Ok((None, Sources::default())),
        Err(e) => Err(Refusal::Unreadable(e.to_string())),
    }
}

/// The recorded gaps of the instrument's candles, `[from, to)`.
pub fn candle_gaps(
    root: &Path,
    venue: &str,
    ticker: &str,
    from: i64,
    to: i64,
) -> Result<Vec<(i64, i64)>, Refusal> {
    let reader = match Reader::open(root, &["kind=gaps"]) {
        Ok(reader) => reader,
        Err(ReadError::NoFrontier { .. }) => return Ok(Vec::new()),
        Err(e) => return Err(Refusal::Unreadable(e.to_string())),
    };
    // A gap is filed under the venue time it began at, which can be before the
    // window it touches: read a day earlier.
    let batches = reader
        .view(Window {
            kind: Kind::Gaps,
            from_micros: from - DAY,
            to_micros: to,
            ticker: Some(ticker.to_string()),
        })
        .map_err(|e| Refusal::Unreadable(e.to_string()))?;
    let mut gaps = Vec::new();
    for batch in &batches {
        let text = |name| {
            batch
                .column_by_name(name)
                .and_then(|c| c.as_any().downcast_ref::<StringArray>())
                .ok_or_else(|| Refusal::Unreadable(format!("gaps without {name}")))
        };
        let int = |name| {
            batch
                .column_by_name(name)
                .and_then(|c| c.as_any().downcast_ref::<Int64Array>())
                .ok_or_else(|| Refusal::Unreadable(format!("gaps without {name}")))
        };
        let (venues, series, from_us, to_us) = (
            text("venue")?,
            text("series")?,
            int("from_micros")?,
            int("to_micros")?,
        );
        for i in 0..batch.num_rows() {
            if venues.value(i) == venue && series.value(i) == "candles" {
                gaps.push((from_us.value(i), to_us.value(i)));
            }
        }
    }
    Ok(gaps)
}

fn batch(
    schema: &SchemaRef,
    query: &CandleQuery,
    period: Period,
    bars: &[Bar],
) -> Result<RecordBatch, Refusal> {
    let n = bars.len();
    let dec = |f: fn(&Bar) -> i128| -> Result<ArrayRef, Refusal> {
        Ok(Arc::new(
            Decimal128Array::from_iter_values(bars.iter().map(f))
                .with_precision_and_scale(38, SCALE)
                .map_err(|e| Refusal::Unreadable(e.to_string()))?,
        ))
    };
    let columns: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(vec![query.venue.as_str(); n])),
        Arc::new(StringArray::from(vec![query.ticker.as_str(); n])),
        Arc::new(Int64Array::from_iter_values(
            bars.iter().map(|b| b.start_micros),
        )),
        Arc::new(StringArray::from(vec![period.label(); n])),
        Arc::new(StringArray::from_iter_values(
            bars.iter()
                .map(|b| interval_label(b.source_interval_micros)),
        )),
        dec(|b| b.open)?,
        dec(|b| b.high)?,
        dec(|b| b.low)?,
        dec(|b| b.close)?,
        dec(|b| b.volume)?,
        Arc::new(UInt64Array::from_iter(bars.iter().map(|b| b.trade_count))),
        Arc::new(UInt32Array::from_iter_values(
            bars.iter().map(|b| b.sources),
        )),
        Arc::new(UInt32Array::from_iter_values(
            bars.iter().map(|b| b.sources_expected),
        )),
        Arc::new(BooleanArray::from_iter(
            bars.iter().map(|b| Some(b.is_complete())),
        )),
        Arc::new(BooleanArray::from_iter(bars.iter().map(|b| Some(b.gap)))),
        Arc::new(BooleanArray::from_iter(
            bars.iter().map(|b| Some(b.is_final)),
        )),
    ];
    RecordBatch::try_new(schema.clone(), columns).map_err(|e| Refusal::Unreadable(e.to_string()))
}

/// `GET /v1/candles`.
pub async fn candles(State(markets): State<Shared>, Query(query): Query<CandleQuery>) -> Response {
    let json = match query.format.as_deref() {
        None | Some("arrow") => false,
        Some("json") => true,
        Some(other) => {
            return Refusal::Period(format!(
                "format {other:?} is not offered; ask for json, or leave it out for Arrow IPC"
            ))
            .into_response();
        }
    };
    let root = markets.tape.clone();
    let max_bars = markets.limits.bars;
    if json {
        let answered = tokio::task::spawn_blocking(move || {
            read(&root, &query, max_bars).map(|a| as_json(&query, &a))
        })
        .await;
        return match answered {
            Ok(Ok(answer)) => axum::Json(answer).into_response(),
            Ok(Err(refusal)) => refusal.into_response(),
            Err(join) => Refusal::Unreadable(join.to_string()).into_response(),
        };
    }
    let answered =
        tokio::task::spawn_blocking(move || read(&root, &query, max_bars).and_then(|a| encode(&a)))
            .await;
    match answered {
        Ok(Ok(bytes)) => ([(header::CONTENT_TYPE, ARROW_STREAM)], bytes).into_response(),
        Ok(Err(refusal)) => refusal.into_response(),
        Err(join) => Refusal::Unreadable(join.to_string()).into_response(),
    }
}

/// **The same bars as JSON** (`market-periods`, design D8 of
/// `move-watcher-onto-markets`): each bar in the Arrow answer's columns,
/// decimals as exact strings; the tape's bound, `null` where it holds none of
/// the venue; and the spans no source covers, as `[from, to)` pairs.
pub fn as_json(query: &CandleQuery, answer: &Answer) -> serde_json::Value {
    let metadata = answer.schema.metadata();
    let bound = metadata
        .get(&bound_key(&query.venue))
        .and_then(|b| b.parse::<u64>().ok());
    let unsourced: serde_json::Value = metadata
        .get(UNSOURCED)
        .and_then(|u| serde_json::from_str(u).ok())
        .unwrap_or_else(|| serde_json::json!([]));
    let bars: Vec<crate::rows::Row> = answer
        .batches
        .iter()
        .flat_map(crate::rows::rows_json)
        .collect();
    serde_json::json!({
        "venue": query.venue,
        "ticker": query.ticker,
        "period": query.period,
        "from": query.from,
        "to": query.to,
        "bound": bound,
        "unsourced": unsourced,
        "bars": bars,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use galata_datawatch::tape::{Row, Tape};
    use galata_wire::{Candle, Envelope, Event, Num, Ticker, Venue};
    use std::str::FromStr;

    /// 2026-11-25 00:00 UTC.
    const T0: i64 = 1_795_564_800_000_000;
    const MINUTE: i64 = 60_000_000;
    const HOUR: i64 = 60 * MINUTE;
    const UNIT: i128 = 1_000_000_000_000_000_000;

    fn candle(interval: &str, at: i64, recv: i64, open: &str, is_final: bool) -> Row {
        Row {
            stream_seq: 1,
            source_recv_micros: recv,
            envelope: Envelope::new(
                Venue::new("hyperliquid").unwrap(),
                Ticker::new("BTC").unwrap(),
                Some(at),
                recv,
                Event::Candle(Candle {
                    interval: interval.into(),
                    open: Num::from_str(open).unwrap(),
                    high: Num::from_str(open).unwrap() + Num::from(5),
                    low: Num::from_str(open).unwrap() - Num::from(5),
                    close: Num::from_str(open).unwrap() + Num::from(1),
                    volume: Num::from_str("2").unwrap(),
                    trade_count: Some(1),
                    is_final,
                }),
            ),
        }
    }

    /// Venue hourly bars for 2024-01-01, and galata's minutes for the first
    /// hour of 2026-11-25, each minute stated twice: forming, then final.
    fn tape() -> tempfile::TempDir {
        let root = tempfile::tempdir().unwrap();
        let mut tape = Tape::open(root.path());
        let old = 1_704_067_200_000_000; // 2024-01-01
        for h in 0..24 {
            tape.take(candle(
                "1h",
                old + h * HOUR,
                old + 30 * 24 * HOUR,
                "42000",
                true,
            ));
        }
        for m in 0..60 {
            let at = T0 + m * MINUTE;
            tape.take(candle("1m", at, at + 30_000_000, "60000", false));
            tape.take(candle(
                "1m",
                at,
                at + MINUTE,
                &(60_000 + m).to_string(),
                true,
            ));
        }
        tape.commit().unwrap();
        root
    }

    fn query(period: &str, from: i64, to: i64) -> CandleQuery {
        CandleQuery {
            venue: "hyperliquid".into(),
            ticker: "BTC".into(),
            period: period.into(),
            from,
            to,
            format: None,
        }
    }

    #[test]
    fn the_json_form_holds_the_same_bars_as_the_arrow_answer() {
        let root = tape();
        let from = T0 - 2 * HOUR;
        let asked = query("5m", from, T0 + HOUR);
        let answer = read(root.path(), &asked, 1_000_000).unwrap();
        let json = as_json(&asked, &answer);
        let bars = json["bars"].as_array().unwrap();
        let batch = &answer.batches[0];
        assert_eq!(bars.len(), batch.num_rows());
        let closes = column::<Decimal128Array>(batch, "close");
        let starts = column::<Int64Array>(batch, "start_micros");
        for (i, bar) in bars.iter().enumerate() {
            assert_eq!(bar["start_micros"], starts.value(i));
            assert_eq!(
                bar["close"].as_str().unwrap(),
                crate::rows::decimal(closes.value(i), SCALE),
                "an exact string"
            );
        }
        assert_eq!(json["unsourced"][0][0], from, "the span no source covers");
        assert!(json["bound"].is_u64() || json["bound"].is_null());
    }

    fn column<'a, T: 'static>(batch: &'a RecordBatch, name: &str) -> &'a T {
        batch
            .column_by_name(name)
            .unwrap()
            .as_any()
            .downcast_ref::<T>()
            .unwrap()
    }

    #[test]
    fn five_minute_bars_where_minutes_are_held_and_the_rest_named_unsourced() {
        let root = tape();
        let from = 1_704_067_200_000_000; // 2024-01-01
        let answer = read(root.path(), &query("5m", from, T0 + HOUR), 1_000_000).unwrap();
        let batch = &answer.batches[0];
        assert_eq!(batch.num_rows(), 12, "only the hour of minutes");
        let source = column::<StringArray>(batch, "source_interval");
        assert!((0..12).all(|i| source.value(i) == "1m"));
        let open = column::<Decimal128Array>(batch, "open");
        assert_eq!(
            open.value(0),
            60_000 * UNIT,
            "each minute at its final version"
        );
        assert_eq!(open.value(1), 60_005 * UNIT);
        let unsourced: Vec<(i64, i64)> =
            serde_json::from_str(&answer.schema.metadata()[UNSOURCED]).unwrap();
        assert_eq!(unsourced, [(from, T0)], "2024 to the minutes, one span");
        assert!(column::<BooleanArray>(batch, "complete").value(0));
    }

    #[test]
    fn hourly_bars_over_the_older_span_are_the_venues_own() {
        let root = tape();
        let old = 1_704_067_200_000_000;
        let answer = read(root.path(), &query("1h", old, old + 24 * HOUR), 1_000_000).unwrap();
        let batch = &answer.batches[0];
        assert_eq!(batch.num_rows(), 24);
        let source = column::<StringArray>(batch, "source_interval");
        assert!((0..24).all(|i| source.value(i) == "1h"));
        assert_eq!(
            column::<Decimal128Array>(batch, "open").value(0),
            42_000 * UNIT
        );
        assert_eq!(answer.schema.metadata()[UNSOURCED], "[]");
    }

    #[test]
    fn a_period_not_offered_and_too_many_bars_are_refused_by_name() {
        let root = tape();
        let refusal = read(root.path(), &query("7m", T0, T0 + HOUR), 1_000).unwrap_err();
        assert!(refusal.to_string().contains("1m, 3m, 5m"), "{refusal}");
        let refusal = read(root.path(), &query("1m", T0, T0 + 24 * HOUR), 1_000).unwrap_err();
        assert_eq!(
            refusal,
            Refusal::TooManyBars {
                bars: 1_440,
                limit: 1_000
            }
        );
    }

    #[test]
    fn the_answer_round_trips_through_the_stream() {
        let root = tape();
        let answer = read(
            root.path(),
            &query("15m", T0 + 7 * MINUTE, T0 + HOUR),
            1_000,
        )
        .unwrap();
        let bytes = encode(&answer).unwrap();
        let reader = arrow::ipc::reader::StreamReader::try_new(bytes.as_slice(), None).unwrap();
        let batches: Vec<RecordBatch> = reader.map(Result::unwrap).collect();
        let start = column::<Int64Array>(&batches[0], "start_micros");
        assert_eq!(start.value(0), T0, "the bar holding 10:07 is whole");
        assert_eq!(batches[0].num_rows(), 4);
    }

    #[test]
    fn interval_labels_read_back() {
        assert_eq!(interval_label(60_000_000), "1m");
        assert_eq!(interval_label(15 * 60_000_000), "15m");
        assert_eq!(interval_label(4 * HOUR), "4h");
        assert_eq!(interval_label(24 * HOUR), "1d");
    }
}
