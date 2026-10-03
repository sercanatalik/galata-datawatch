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
///
/// **Positive, or `None`.** A width read off the tape or a signal row is
/// external input: `0m` used to become a zero bucket and panic the watch on a
/// remainder by zero, and a last character outside ASCII panicked the split.
pub fn width_micros(interval: &str) -> Option<i64> {
    let unit = interval.chars().last()?;
    let n: i64 = interval[..interval.len() - unit.len_utf8()].parse().ok()?;
    let secs = match unit {
        'm' => 60,
        'h' => 3_600,
        'd' => 86_400,
        _ => return None,
    };
    n.checked_mul(secs)?
        .checked_mul(1_000_000)
        .filter(|width| *width > 0)
}

fn read(
    root: &Path,
    kind: Kind,
    from_micros: i64,
    to_micros: i64,
) -> Result<(Vec<RecordBatch>, Reader), ReadError> {
    let scope = format!("kind={}", kind.as_str());
    let reader = Reader::open(root, &[scope.as_str()])?;
    let batches = reader.view(Window {
        kind,
        from_micros,
        to_micros,
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
    let (bars, bound) = bars_from_tape(root, venue)?;
    Ok(Derived {
        venue: venue.to_string(),
        bound,
        statistics: derive(&bars, horizon),
    })
}

/// One venue's candles off the tape, every width, with the venue's durable
/// bound: read once for several horizons, as the watch's reconciliation does.
pub fn bars_from_tape(root: &Path, venue: &str) -> Result<(Vec<Bar>, Option<i64>), DeriveError> {
    let mut by_venue = bars_by_venue(root, &[venue], i64::MIN + 1, i64::MAX)?;
    Ok(by_venue.pop().unwrap_or_default())
}

/// Several venues' candles in `[from, to)` venue time, **from one read**, in
/// the order the venues are given, each with its durable bound.
///
/// Reading the whole candle dataset once per venue — every width, every
/// venue, all of history — and keeping one venue's rows each time was the
/// watch's hourly reconciliation. One bounded read, split as it goes, is the
/// same bars.
pub fn bars_by_venue(
    root: &Path,
    venues: &[&str],
    from_micros: i64,
    to_micros: i64,
) -> Result<Vec<(Vec<Bar>, Option<i64>)>, DeriveError> {
    let venues_wanted = venues;
    let mut out: Vec<(Vec<Bar>, Option<i64>)> = venues.iter().map(|_| (Vec::new(), None)).collect();
    if !venues.is_empty() && unwritten(root, &["kind=candles"]).is_empty() {
        let (batches, reader) = read(root, Kind::Candles, from_micros, to_micros)?;
        for (slot, venue) in out.iter_mut().zip(venues) {
            slot.1 = reader.bound().of_venue(venue);
        }
        for batch in &batches {
            let names = col::<StringArray>(batch, "candles", "venue")?;
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
                if at.is_null(i) {
                    continue;
                }
                let Some(slot) = venues_wanted.iter().position(|v| *v == names.value(i)) else {
                    continue;
                };
                let Some(width) = width_micros(intervals.value(i)) else {
                    continue;
                };
                out[slot].0.push(Bar {
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

    Ok(out)
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

    #[test]
    fn a_width_off_the_tape_never_panics_and_is_never_zero() {
        // External input: a zero bucket was a remainder by zero downstream,
        // and a multi-byte last character a split inside a char.
        assert_eq!(width_micros("0m"), None);
        assert_eq!(width_micros("-1h"), None);
        assert_eq!(width_micros("1µ"), None);
        assert_eq!(width_micros("µ"), None);
        assert_eq!(width_micros("99999999999999d"), None);
    }
}
