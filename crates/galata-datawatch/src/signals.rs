//! Market-data signals on the tape: the one dataset **computed, not
//! projected**.
//!
//! ```text
//!   tape/
//!     kind=signals/
//!       date=2026-09-28/                      the asof date
//!         t-<computed>_<computed>_<pid>_<seq>.parquet
//! ```
//!
//! A signal is a number computed from market data on a schedule (Tier 16):
//! a volatility, a correlation, a covariance. It is a **record**, because it
//! will not be recomputed — the model is refitted, and the refit is not the
//! figure an algo acted on at the time (legacy `design/datasignal`, *It is a
//! record, and that decides both stores*). So no rebuild writes it, no
//! replacement plans over it, and the tape's horizon does not expire it.
//!
//! **One row per value.** `measure` is a column rather than a column per
//! measure, so a second signal — funding carry, a beta — is rows and never a
//! schema change: the dataset is additive-only from its first row.
//!
//! **The one Arrow schema allowed floats.** A statistic is not money:
//! `value` and `n_eff` are `f64`, and nothing else is. It lives here, outside
//! `tape/schema.rs`, which `check-no-float-money.sh` rule 3 scans — the
//! exemption is by location, and the script says so.
//!
//! **Written by Python, owned here.** `py/signals` writes this schema; this
//! module is its one definition, and a fixture it writes is read back here.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::{Array, Int64Array, RecordBatch};
use arrow::compute::filter_record_batch;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use galata_segments::{Codec, Cursor, SegmentError};
use galata_wire::Kind;

use crate::calendar::date_of;
use crate::tape::layout::partition_of;

/// The footer label naming who wrote a signal segment: what its ranges are
/// compared within, as `galata.venue` is for a projected dataset.
pub const WRITER_LABEL: &str = "galata.writer";
/// The writer this dataset has.
pub const WRITER: &str = "signals";
/// The run that computed the segment's rows.
pub const RUN_LABEL: &str = "galata.run_id";
/// The code that computed them: galata-research's commit.
pub const CODE_LABEL: &str = "galata.code";

/// The dataset's schema.
///
/// | column | type | |
/// |---|---|---|
/// | `signal`, `horizon`, `measure` | Utf8 | what the number is: `varcov`, `4h`, `correlation` |
/// | `ticker_i`, `ticker_j` | Utf8, `j` nullable | a pair (i ≤ j) or one instrument |
/// | `h` | Int64 | bars ahead |
/// | `value` / `absent` | Float64 / Utf8, nullable | the number, or why there is none |
/// | `n_eff` | Float64, nullable | the effective sample the figure rests on |
/// | `asof_micros` | Int64 | the close the figure stands on |
/// | `target_micros` | Int64 | the bar a forecast is for |
/// | `computed_micros` | Int64 | when it became known: what a point-in-time reader joins on |
/// | `fitted_through_micros`, `fit_from_micros` | Int64, nullable | the fit's span; null when nothing was fitted |
/// | `model`, `params`, `fitted` | Utf8, Utf8 (JSON), Boolean | how it was computed |
/// | `after_gap` | Boolean | the first figure after a dropped bar |
/// | `code`, `run_id` | Utf8 | galata-research's commit and the run |
pub fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("signal", DataType::Utf8, false),
        Field::new("horizon", DataType::Utf8, false),
        Field::new("measure", DataType::Utf8, false),
        Field::new("ticker_i", DataType::Utf8, false),
        Field::new("ticker_j", DataType::Utf8, true),
        Field::new("h", DataType::Int64, false),
        Field::new("value", DataType::Float64, true),
        Field::new("absent", DataType::Utf8, true),
        Field::new("n_eff", DataType::Float64, true),
        Field::new("asof_micros", DataType::Int64, false),
        Field::new("target_micros", DataType::Int64, false),
        Field::new("computed_micros", DataType::Int64, false),
        Field::new("fitted_through_micros", DataType::Int64, true),
        Field::new("fit_from_micros", DataType::Int64, true),
        Field::new("model", DataType::Utf8, false),
        Field::new("params", DataType::Utf8, false),
        Field::new("fitted", DataType::Boolean, false),
        Field::new("after_gap", DataType::Boolean, false),
        Field::new("code", DataType::Utf8, false),
        Field::new("run_id", DataType::Utf8, false),
    ]))
}

/// Why a batch cannot be written as signals.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum SignalError {
    /// The batch's schema is not [`schema`].
    #[error("a signal batch must have the signals schema; it has {found}")]
    Schema {
        /// The schema it had.
        found: String,
    },
    /// A row whose `value` and `absent` are both set, or both null.
    #[error("row {row}: a signal is a value or the reason it is absent, exactly one of them")]
    ValueOrAbsent {
        /// Which row.
        row: usize,
    },
    /// The segment layer refused.
    #[error(transparent)]
    Segment(#[from] SegmentError),
}

/// Write one run's rows: one labelled segment per `asof` date, named at the
/// run's `computed_micros`.
///
/// The cursor is the time cursor at `computed_micros`, first and last alike:
/// computed time is this system's clock, two runs never share it, and asof
/// ranges would overlap across horizons by design. `seq` is 0 — one run writes
/// one segment per partition.
pub fn write(
    tape_root: &Path,
    computed_micros: i64,
    run_id: &str,
    code: &str,
    batch: &RecordBatch,
) -> Result<Vec<PathBuf>, SignalError> {
    if batch.schema() != schema() {
        return Err(SignalError::Schema {
            found: format!("{:?}", batch.schema().fields()),
        });
    }
    let value = batch.column(6);
    let absent = batch.column(7);
    for row in 0..batch.num_rows() {
        if value.is_null(row) == absent.is_null(row) {
            return Err(SignalError::ValueOrAbsent { row });
        }
    }
    let asof = batch
        .column(9)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("asof_micros is Int64 by the schema");
    let mut dates: Vec<String> = asof.values().iter().map(|m| date_of(*m)).collect();
    dates.sort();
    dates.dedup();
    let cursor = Cursor::Time {
        first_micros: computed_micros,
        last_micros: computed_micros,
        pid: std::process::id(),
        seq: 0,
    };
    let labels = [
        (WRITER_LABEL, WRITER),
        (RUN_LABEL, run_id),
        (CODE_LABEL, code),
    ];
    let mut written = Vec::new();
    for date in dates {
        let mask: arrow::array::BooleanArray =
            asof.iter().map(|m| m.map(|m| date_of(m) == date)).collect();
        let rows = filter_record_batch(batch, &mask).map_err(|e| SignalError::Schema {
            found: e.to_string(),
        })?;
        let first = asof
            .iter()
            .flatten()
            .find(|m| date_of(*m) == date)
            .expect("a date came from a row");
        let dir = tape_root.join(partition_of(Kind::Signals, first));
        written.push(galata_segments::write_segment_labelled(
            &dir,
            cursor,
            &rows,
            Codec::Zstd,
            &[],
            &labels,
        )?);
    }
    Ok(written)
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use arrow::array::{BooleanArray, Float64Array, StringArray};

    /// Rows as the flow writes them: a correlation, a variance, and a pair
    /// below its floor, stated absent.
    pub(crate) fn batch(asof: &[i64]) -> RecordBatch {
        let n = asof.len();
        let text = |v: &str| Arc::new(StringArray::from(vec![v; n])) as Arc<dyn Array>;
        let values: Vec<Option<f64>> = (0..n).map(|i| (i % 3 != 2).then_some(0.5)).collect();
        let absent: Vec<Option<&str>> = (0..n)
            .map(|i| (i % 3 == 2).then_some("GOLD: 12 returns, floor 20"))
            .collect();
        RecordBatch::try_new(
            schema(),
            vec![
                text("varcov"),
                text("4h"),
                text("correlation"),
                text("BTC"),
                Arc::new(StringArray::from(vec![Some("ETH"); n])),
                Arc::new(Int64Array::from(vec![1; n])),
                Arc::new(Float64Array::from(values)),
                Arc::new(StringArray::from(absent)),
                Arc::new(Float64Array::from(vec![Some(32.3); n])),
                Arc::new(Int64Array::from(asof.to_vec())),
                Arc::new(Int64Array::from(
                    asof.iter().map(|m| m + 14_400_000_000).collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from(vec![1_790_560_800_000_000; n])),
                Arc::new(Int64Array::from(vec![Some(1_790_550_000_000_000); n])),
                Arc::new(Int64Array::from(vec![Some(1_767_700_000_000_000); n])),
                text("gjr-t/dcc"),
                text("{\"a\":0.0055,\"b\":0.9838}"),
                Arc::new(BooleanArray::from(vec![true; n])),
                Arc::new(BooleanArray::from(vec![false; n])),
                text("2b5e6a4"),
                text("run-1"),
            ],
        )
        .unwrap()
    }

    #[test]
    fn statistics_are_the_only_floats_in_a_signal() {
        let schema = schema();
        let floats: Vec<&str> = schema
            .fields()
            .iter()
            .filter(|f| {
                matches!(
                    f.data_type(),
                    DataType::Float16 | DataType::Float32 | DataType::Float64
                )
            })
            .map(|f| f.name().as_str())
            .collect();
        assert_eq!(floats, vec!["value", "n_eff"]);
    }

    #[test]
    fn a_signal_is_a_value_or_the_reason_it_is_absent() {
        let s = schema();
        for name in [
            "value",
            "absent",
            "ticker_j",
            "n_eff",
            "fitted_through_micros",
            "fit_from_micros",
        ] {
            assert!(s.field_with_name(name).unwrap().is_nullable(), "{name}");
        }
        for name in [
            "signal",
            "horizon",
            "measure",
            "ticker_i",
            "asof_micros",
            "computed_micros",
        ] {
            assert!(!s.field_with_name(name).unwrap().is_nullable(), "{name}");
        }
        // Both set is refused before anything is written.
        let good = batch(&[1_790_553_600_000_000]);
        let mut columns: Vec<Arc<dyn Array>> = good.columns().to_vec();
        columns[7] = Arc::new(StringArray::from(vec![Some("both")]));
        let both = RecordBatch::try_new(schema(), columns).unwrap();
        let root = tempfile::tempdir().unwrap();
        assert!(matches!(
            write(root.path(), 1, "r", "c", &both),
            Err(SignalError::ValueOrAbsent { row: 0 })
        ));
        assert!(!root.path().join("kind=signals").exists());
    }

    #[test]
    fn a_written_signal_names_its_run_and_no_venue() {
        let root = tempfile::tempdir().unwrap();
        // Two asof dates in one run: one segment in each partition.
        let day = 86_400_000_000;
        let asof = [
            1_790_553_600_000_000,
            1_790_553_600_000_000 + 4 * 3_600_000_000,
            1_790_553_600_000_000 + day,
        ];
        let written = write(
            root.path(),
            1_790_560_800_000_000,
            "run-1",
            "2b5e6a4",
            &batch(&asof),
        )
        .unwrap();
        assert_eq!(written.len(), 2);
        for path in &written {
            let name = path.file_name().unwrap().to_str().unwrap();
            let cursor = Cursor::parse(name).unwrap();
            assert!(matches!(
                cursor,
                Cursor::Time {
                    first_micros: 1_790_560_800_000_000,
                    last_micros: 1_790_560_800_000_000,
                    seq: 0,
                    ..
                }
            ));
            assert!(path.starts_with(root.path().join("kind=signals")));
            assert_eq!(
                galata_segments::label(path, WRITER_LABEL)
                    .unwrap()
                    .as_deref(),
                Some("signals")
            );
            assert_eq!(
                galata_segments::label(path, RUN_LABEL).unwrap().as_deref(),
                Some("run-1")
            );
            assert_eq!(
                galata_segments::label(path, CODE_LABEL).unwrap().as_deref(),
                Some("2b5e6a4")
            );
            assert_eq!(
                galata_segments::label(path, crate::tape::VENUE_LABEL).unwrap(),
                None
            );
        }
        assert!(
            crate::tape::check_layout(root.path()).is_empty(),
            "{:?}",
            crate::tape::check_layout(root.path())
        );
    }
}
