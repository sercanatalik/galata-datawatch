//! **Rows of the signals schema** ([`galata_datawatch::signals::schema`]),
//! built as `py/signals/varcov.py` builds them, and the batch that carries them.

use std::sync::Arc;

use arrow::array::{ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray};
use arrow::record_batch::RecordBatch;
use galata_datawatch::signals;

/// One row. `value` xor `absent`, as the schema requires.
#[derive(Debug, Clone, PartialEq)]
pub struct Row {
    pub signal: String,
    pub horizon: String,
    pub measure: String,
    pub ticker_i: String,
    pub ticker_j: Option<String>,
    pub h: i64,
    pub value: Option<f64>,
    pub absent: Option<String>,
    pub n_eff: Option<f64>,
    pub asof_micros: i64,
    pub target_micros: i64,
    pub computed_micros: i64,
    pub fitted_through_micros: Option<i64>,
    pub fit_from_micros: Option<i64>,
    pub model: String,
    pub params: String,
    pub fitted: bool,
    pub after_gap: bool,
    pub code: String,
    pub run_id: String,
}

/// A float as Python's `repr` writes it in JSON (`0.94`, `1.0`), so `params`
/// reads as `py/signals` wrote it.
pub fn py_float(v: f64) -> String {
    format!("{v:?}")
}

/// The rows as one batch of the signals schema.
pub fn batch(rows: &[Row]) -> Result<RecordBatch, String> {
    let s = |f: fn(&Row) -> String| -> ArrayRef {
        Arc::new(StringArray::from(rows.iter().map(f).collect::<Vec<_>>()))
    };
    let so = |f: fn(&Row) -> Option<String>| -> ArrayRef {
        Arc::new(StringArray::from(rows.iter().map(f).collect::<Vec<_>>()))
    };
    let i = |f: fn(&Row) -> i64| -> ArrayRef {
        Arc::new(Int64Array::from(rows.iter().map(f).collect::<Vec<_>>()))
    };
    let io = |f: fn(&Row) -> Option<i64>| -> ArrayRef {
        Arc::new(Int64Array::from(rows.iter().map(f).collect::<Vec<_>>()))
    };
    let fo = |f: fn(&Row) -> Option<f64>| -> ArrayRef {
        Arc::new(Float64Array::from(rows.iter().map(f).collect::<Vec<_>>()))
    };
    let b = |f: fn(&Row) -> bool| -> ArrayRef {
        Arc::new(BooleanArray::from(rows.iter().map(f).collect::<Vec<_>>()))
    };
    RecordBatch::try_new(
        signals::schema(),
        vec![
            s(|r| r.signal.clone()),
            s(|r| r.horizon.clone()),
            s(|r| r.measure.clone()),
            s(|r| r.ticker_i.clone()),
            so(|r| r.ticker_j.clone()),
            i(|r| r.h),
            fo(|r| r.value),
            so(|r| r.absent.clone()),
            fo(|r| r.n_eff),
            i(|r| r.asof_micros),
            i(|r| r.target_micros),
            i(|r| r.computed_micros),
            io(|r| r.fitted_through_micros),
            io(|r| r.fit_from_micros),
            s(|r| r.model.clone()),
            s(|r| r.params.clone()),
            b(|r| r.fitted),
            b(|r| r.after_gap),
            s(|r| r.code.clone()),
            s(|r| r.run_id.clone()),
        ],
    )
    .map_err(|e| format!("the rows do not make a signals batch: {e}"))
}

#[cfg(test)]
mod tests {
    #[test]
    fn floats_read_as_python_writes_them() {
        assert_eq!(super::py_float(0.94), "0.94");
        assert_eq!(super::py_float(1.0), "1.0");
    }
}
