//! The tape's candles, as the derive module reads them.
//!
//! Through [`crate::tape::reader::Reader`], so a derived statistic is bounded
//! by what is durable exactly as every other tape read is, and carries that
//! bound in its result.

use std::path::Path;

use arrow::array::{Array, BooleanArray, Decimal128Array, Int64Array, StringArray};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use galata_wire::Kind;

use super::{Bar, Horizon, Statistics, derive};
use crate::tape::reader::{ReadError, Reader, Window, unwritten};

/// A horizon's statistics and the tape position they were computed at.
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct Derived {
    /// Which venue.
    pub venue: String,
    /// The venue's durable bound on the candles tape: nothing past it was read.
    pub bound: Option<i64>,
    /// The statistics.
    pub statistics: Statistics,
}

/// Why the tape could not be read for a derivation.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum DeriveError {
    /// The reader refused.
    #[error(transparent)]
    Read(#[from] ReadError),
    /// A column was missing or of another type.
    #[error("the {kind} tape has no {column} column of the expected type")]
    Column {
        /// Which dataset.
        kind: &'static str,
        /// Which column.
        column: &'static str,
    },
}

/// A bar width as the venue spells it (`1m`, `5m`, `1h`, `1d`), in
/// microseconds. `None` for a spelling this build does not read.
pub fn width_micros(interval: &str) -> Option<i64> {
    let (n, unit) = interval.split_at(interval.len().checked_sub(1)?);
    let n: i64 = n.parse().ok()?;
    let secs = match unit {
        "m" => 60,
        "h" => 3_600,
        "d" => 86_400,
        _ => return None,
    };
    Some(n * secs * 1_000_000)
}

fn read(root: &Path, kind: Kind) -> Result<(Vec<RecordBatch>, Reader), ReadError> {
    let scope = format!("kind={}", kind.as_str());
    let reader = Reader::open(root, &[scope.as_str()])?;
    let batches = reader.view(Window {
        kind,
        from_micros: i64::MIN + 1,
        to_micros: i64::MAX,
        ticker: None,
    })?;
    Ok((batches, reader))
}

fn col<'a, T: 'static>(
    batch: &'a RecordBatch,
    kind: &'static str,
    column: &'static str,
) -> Result<&'a T, DeriveError> {
    batch
        .column_by_name(column)
        .and_then(|c| c.as_any().downcast_ref::<T>())
        .ok_or(DeriveError::Column { kind, column })
}

/// Derive a horizon's statistics for one venue from the tape under `root`.
pub fn from_tape(root: &Path, venue: &str, horizon: &Horizon) -> Result<Derived, DeriveError> {
    let mut bars = Vec::new();
    let mut bound = None;
    if unwritten(root, &["kind=candles"]).is_empty() {
        let (batches, reader) = read(root, Kind::Candles)?;
        bound = reader.bound().of_venue(venue);
        for batch in &batches {
            let venues = col::<StringArray>(batch, "candles", "venue")?;
            let tickers = col::<StringArray>(batch, "candles", "ticker")?;
            let at = col::<Int64Array>(batch, "candles", "at_micros")?;
            let recv = col::<Int64Array>(batch, "candles", "recv_micros")?;
            let intervals = col::<StringArray>(batch, "candles", "interval")?;
            let closes = col::<Decimal128Array>(batch, "candles", "close")?;
            let finals = col::<BooleanArray>(batch, "candles", "is_final")?;
            let scale = match closes.data_type() {
                DataType::Decimal128(_, s) => i32::from(*s),
                _ => {
                    return Err(DeriveError::Column {
                        kind: "candles",
                        column: "close",
                    });
                }
            };
            let divisor = 10f64.powi(scale);
            for i in 0..batch.num_rows() {
                if venues.value(i) != venue || at.is_null(i) {
                    continue;
                }
                let Some(width) = width_micros(intervals.value(i)) else {
                    continue;
                };
                bars.push(Bar {
                    ticker: tickers.value(i).to_string(),
                    start_micros: at.value(i),
                    width_micros: width,
                    // The door where a decimal becomes a float: logs and roots
                    // need one, and nothing past here is money.
                    close: closes.value(i) as f64 / divisor,
                    is_final: finals.value(i),
                    recv_micros: recv.value(i),
                });
            }
        }
    }

    Ok(Derived {
        venue: venue.to_string(),
        bound,
        statistics: derive(&bars, horizon),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_bar_width_is_read_as_the_venue_spells_it() {
        assert_eq!(width_micros("1m"), Some(60_000_000));
        assert_eq!(width_micros("15m"), Some(900_000_000));
        assert_eq!(width_micros("1h"), Some(3_600_000_000));
        assert_eq!(width_micros("1d"), Some(86_400_000_000));
        assert_eq!(width_micros("1w"), None);
        assert_eq!(width_micros(""), None);
    }
}
