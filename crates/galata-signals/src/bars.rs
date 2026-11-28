//! **Bars and their log returns**, read off the tape exactly as `py/signals`
//! reads them through galata-research.
//!
//! - **Candles** (`gr.market.candles`): per (venue, ticker, interval, bar
//!   open) the latest receipt, by `recv_micros` and then `stream_seq`; a bar
//!   is closed when a later bar of its series exists anywhere in the record,
//!   or when one of its receipts arrived at or after its close; a bar with no
//!   trades is dropped. The close is DECIMAL(38,18) as a float.
//! - **Wider bars** (`py/signals/bars.py`): 30m from 1m and 1w from 1d on
//!   the UTC grid (a week from Monday), a bucket kept only when it holds every
//!   fine bar it should, closing at its last fine bar's close.
//! - **Log returns** (`gr.timeseries.returns(kind="log")`): ln of close over
//!   the previous close, null on a ticker's first bar and after a hole (the
//!   previous bar did not close where this one opens).

use std::collections::BTreeMap;
use std::path::Path;

use arrow::array::{Array, Decimal128Array, Int64Array, StringArray, UInt32Array, UInt64Array};
use arrow::record_batch::RecordBatch;
use galata_datawatch::tape::{ReadError, Reader, Window};
use galata_wire::Kind;

const DAY: i64 = 86_400_000_000;
const WEEK: i64 = 7 * DAY;

/// One closed bar's span and close.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Bar {
    /// Its open, in microseconds.
    pub ts: i64,
    /// Its close.
    pub close_ts: i64,
    /// Its closing price.
    pub close: f64,
}

/// Long returns: `ticker, ts, close_ts, return` with `None` where null.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Returns {
    /// Each row's instrument.
    pub ticker: Vec<String>,
    /// Each row's bar open.
    pub ts: Vec<i64>,
    /// Each row's bar close.
    pub close_ts: Vec<i64>,
    /// Each row's return.
    pub value: Vec<Option<f64>>,
}

impl Returns {
    /// The rows whose bar closed at or before `close`.
    pub fn through(&self, close: i64) -> Returns {
        let keep: Vec<usize> = (0..self.ts.len())
            .filter(|k| self.close_ts[*k] <= close)
            .collect();
        self.pick(&keep)
    }

    /// The rows with a return (`drop_nulls`).
    pub fn present(&self) -> Returns {
        let keep: Vec<usize> = (0..self.ts.len())
            .filter(|k| self.value[*k].is_some())
            .collect();
        self.pick(&keep)
    }

    fn pick(&self, keep: &[usize]) -> Returns {
        Returns {
            ticker: keep.iter().map(|k| self.ticker[*k].clone()).collect(),
            ts: keep.iter().map(|k| self.ts[*k]).collect(),
            close_ts: keep.iter().map(|k| self.close_ts[*k]).collect(),
            value: keep.iter().map(|k| self.value[*k]).collect(),
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct Receipt {
    recv: i64,
    seq: u64,
    close: i128,
    trades: Option<u32>,
}

/// Every closed, traded `interval` bar on the tape under `tape_root`, per
/// ticker, in time order. A ticker under two venues is refused: its returns
/// would interleave two markets.
pub fn candles(
    tape_root: &Path,
    interval: &str,
    width: i64,
) -> Result<BTreeMap<String, Vec<Bar>>, String> {
    let reader = match Reader::open(tape_root, &["kind=candles"]) {
        Ok(r) => r,
        Err(ReadError::NoFrontier { .. }) => return Ok(BTreeMap::new()),
        Err(e) => return Err(format!("the tape's candles cannot be read: {e}")),
    };
    let Some((first, last)) = date_span(&tape_root.join("kind=candles"))? else {
        return Ok(BTreeMap::new());
    };
    // (venue, ticker) -> bar open -> its latest receipt.
    let mut held: BTreeMap<(String, String), BTreeMap<i64, Receipt>> = BTreeMap::new();
    let mut day = first;
    while day <= last {
        let batches = reader
            .view(Window {
                kind: Kind::Candles,
                from_micros: day,
                to_micros: day + DAY,
                ticker: None,
            })
            .map_err(|e| format!("the tape's candles cannot be read: {e}"))?;
        for batch in &batches {
            fold(batch, interval, &mut held)?;
        }
        day += DAY;
    }
    let mut out: BTreeMap<String, Vec<Bar>> = BTreeMap::new();
    let mut venue_of: BTreeMap<String, String> = BTreeMap::new();
    for ((venue, ticker), bars) in held {
        if let Some(other) = venue_of.insert(ticker.clone(), venue.clone()) {
            return Err(format!(
                "{ticker} has {interval} candles under two venues, {other} and {venue}"
            ));
        }
        let latest_at = *bars.keys().next_back().expect("a series holds a bar");
        let kept: Vec<Bar> = bars
            .into_iter()
            .filter(|(at, r)| {
                (*at < latest_at || r.recv >= at + width) && r.trades.is_some_and(|t| t > 0)
            })
            .map(|(at, r)| Bar {
                ts: at,
                close_ts: at + width,
                close: r.close as f64 / 1e18,
            })
            .collect();
        if !kept.is_empty() {
            out.insert(ticker, kept);
        }
    }
    Ok(out)
}

fn date_span(dataset: &Path) -> Result<Option<(i64, i64)>, String> {
    let Ok(entries) = std::fs::read_dir(dataset) else {
        return Ok(None);
    };
    let mut days = Vec::new();
    for entry in entries.flatten() {
        let name = entry.file_name().to_string_lossy().to_string();
        if let Some(date) = name.strip_prefix("date=") {
            let d: jiff::civil::Date = date
                .parse()
                .map_err(|e| format!("{name}: not a date: {e}"))?;
            let at = d
                .to_zoned(jiff::tz::TimeZone::UTC)
                .map_err(|e| e.to_string())?
                .timestamp()
                .as_microsecond();
            days.push(at);
        }
    }
    Ok(days.iter().min().copied().zip(days.iter().max().copied()))
}

fn fold(
    batch: &RecordBatch,
    interval: &str,
    held: &mut BTreeMap<(String, String), BTreeMap<i64, Receipt>>,
) -> Result<(), String> {
    fn col<'a, T: 'static>(b: &'a RecordBatch, name: &str) -> Result<&'a T, String> {
        b.column_by_name(name)
            .and_then(|c| c.as_any().downcast_ref::<T>())
            .ok_or_else(|| format!("a candles batch has no {name} of the expected type"))
    }
    let venue: &StringArray = col(batch, "venue")?;
    let ticker: &StringArray = col(batch, "ticker")?;
    let iv: &StringArray = col(batch, "interval")?;
    let at: &Int64Array = col(batch, "at_micros")?;
    let recv: &Int64Array = col(batch, "recv_micros")?;
    let seq: &UInt64Array = col(batch, "stream_seq")?;
    let close: &Decimal128Array = col(batch, "close")?;
    if close.scale() != 18 {
        return Err(format!(
            "candles' close has scale {}, not 18",
            close.scale()
        ));
    }
    let trades: &UInt32Array = col(batch, "trade_count")?;
    for i in 0..batch.num_rows() {
        if iv.value(i) != interval || at.is_null(i) {
            continue;
        }
        let r = Receipt {
            recv: recv.value(i),
            seq: seq.value(i),
            close: close.value(i),
            trades: (!trades.is_null(i)).then(|| trades.value(i)),
        };
        let series = held
            .entry((venue.value(i).to_string(), ticker.value(i).to_string()))
            .or_default();
        match series.get(&at.value(i)) {
            Some(seen) if (seen.recv, seen.seq) > (r.recv, r.seq) => {}
            _ => {
                series.insert(at.value(i), r);
            }
        }
    }
    Ok(())
}

/// Wider bars from finer ones: a bucket on the UTC grid (a week from Monday)
/// kept only when it holds every fine bar, closing at its last one's close.
pub fn build(fine: &[Bar], fine_width: i64, coarse: i64) -> Vec<Bar> {
    let expected = coarse / fine_width;
    let bucket = |ts: i64| {
        if coarse == WEEK {
            // The epoch was a Thursday: a week from Monday starts 3 days earlier.
            ts - (ts + 3 * DAY).rem_euclid(WEEK)
        } else {
            ts - ts.rem_euclid(coarse)
        }
    };
    let mut out = Vec::new();
    let mut k = 0;
    while k < fine.len() {
        let start = bucket(fine[k].ts);
        let mut end = k;
        while end < fine.len() && bucket(fine[end].ts) == start {
            end += 1;
        }
        if (end - k) as i64 == expected {
            out.push(Bar {
                ts: start,
                close_ts: start + coarse,
                close: fine[end - 1].close,
            });
        }
        k = end;
    }
    out
}

/// Log returns per ticker, in ticker then time order.
pub fn log_returns(bars: &BTreeMap<String, Vec<Bar>>) -> Returns {
    let mut out = Returns::default();
    for (ticker, series) in bars {
        for (k, bar) in series.iter().enumerate() {
            let value = (k > 0 && series[k - 1].close_ts == bar.ts)
                .then(|| (bar.close / series[k - 1].close).ln());
            out.ticker.push(ticker.clone());
            out.ts.push(bar.ts);
            out.close_ts.push(bar.close_ts);
            out.value.push(value);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bar(ts: i64, w: i64, close: f64) -> Bar {
        Bar {
            ts,
            close_ts: ts + w,
            close,
        }
    }

    #[test]
    fn a_bucket_short_of_a_bar_is_not_a_bar() {
        let m = 60_000_000;
        let mut fine: Vec<Bar> = (0..30).map(|i| bar(i * m, m, 100.0 + i as f64)).collect();
        fine.extend((31..60).map(|i| bar(i * m, m, 100.0 + i as f64))); // the second half-hour lacks a minute
        let built = build(&fine, m, 30 * m);
        assert_eq!(
            built,
            vec![Bar {
                ts: 0,
                close_ts: 30 * m,
                close: 129.0
            }]
        );
    }

    #[test]
    fn a_week_runs_from_monday() {
        // 2026-11-23 was a Monday: 20,780 days after the epoch, and polars'
        // `truncate("1w")` takes 2026-11-26 to it.
        let monday = 20_780 * DAY;
        let fine: Vec<Bar> = (0..7)
            .map(|i| bar(monday + i * DAY, DAY, 1.0 + i as f64))
            .collect();
        assert_eq!(
            build(&fine, DAY, WEEK),
            vec![Bar {
                ts: monday,
                close_ts: monday + WEEK,
                close: 7.0
            }]
        );
        assert!(build(&fine[1..], DAY, WEEK).is_empty());
    }

    #[test]
    fn a_return_after_a_hole_is_null() {
        let d = DAY;
        let mut bars = BTreeMap::new();
        bars.insert(
            "BTC".to_string(),
            vec![bar(0, d, 100.0), bar(d, d, 110.0), bar(3 * d, d, 121.0)],
        );
        let r = log_returns(&bars);
        assert_eq!(r.value, vec![None, Some((110.0f64 / 100.0).ln()), None]);
    }
}
