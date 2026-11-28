//! **Arrow rows as JSON**: what galata-markets' JSON answers are built from
//! (`add-markets-accounts`, and JSON candles in `move-watcher-onto-markets`).
//! Whole numbers are numbers, decimals exact strings, a null is null.

use arrow::array::{
    Array, BooleanArray, Decimal128Array, Int64Array, StringArray, UInt16Array, UInt32Array,
    UInt64Array,
};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use serde_json::{Map, Value};

/// One row, by column.
pub type Row = Map<String, Value>;

/// **A batch's rows as JSON objects**, in its columns: whole numbers as
/// numbers, decimals as exact strings, a null as null.
pub fn rows_json(batch: &RecordBatch) -> Vec<Row> {
    let schema = batch.schema();
    let mut rows: Vec<Row> = (0..batch.num_rows()).map(|_| Map::new()).collect();
    for (field, column) in schema.fields().iter().zip(batch.columns()) {
        for (i, row) in rows.iter_mut().enumerate() {
            row.insert(field.name().clone(), cell(column.as_ref(), i));
        }
    }
    rows
}

fn cell(column: &dyn Array, i: usize) -> Value {
    if column.is_null(i) {
        return Value::Null;
    }
    let any = column.as_any();
    let typed = match column.data_type() {
        DataType::Utf8 => any
            .downcast_ref::<StringArray>()
            .map(|a| Value::from(a.value(i))),
        DataType::Int64 => any
            .downcast_ref::<Int64Array>()
            .map(|a| Value::from(a.value(i))),
        DataType::UInt16 => any
            .downcast_ref::<UInt16Array>()
            .map(|a| Value::from(a.value(i))),
        DataType::UInt32 => any
            .downcast_ref::<UInt32Array>()
            .map(|a| Value::from(a.value(i))),
        DataType::UInt64 => any
            .downcast_ref::<UInt64Array>()
            .map(|a| Value::from(a.value(i))),
        DataType::Boolean => any
            .downcast_ref::<BooleanArray>()
            .map(|a| Value::from(a.value(i))),
        DataType::Decimal128(_, scale) => any
            .downcast_ref::<Decimal128Array>()
            .map(|a| Value::from(decimal(a.value(i), *scale))),
        _ => None,
    };
    typed.unwrap_or_else(|| {
        Value::from(arrow::util::display::array_value_to_string(column, i).unwrap_or_default())
    })
}

/// A decimal's exact text, trailing zeros after the point dropped:
/// `81213.500000000000000000` at scale 18 is `81213.5`.
pub fn decimal(value: i128, scale: i8) -> String {
    let scale = usize::try_from(scale).unwrap_or(0);
    let digits = value.unsigned_abs().to_string();
    let padded = format!("{digits:0>width$}", width = scale + 1);
    let (whole, fraction) = padded.split_at(padded.len() - scale);
    let fraction = fraction.trim_end_matches('0');
    let sign = if value < 0 { "-" } else { "" };
    if fraction.is_empty() {
        format!("{sign}{whole}")
    } else {
        format!("{sign}{whole}.{fraction}")
    }
}
