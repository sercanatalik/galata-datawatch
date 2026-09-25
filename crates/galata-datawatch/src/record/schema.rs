//! The record's columns. Eight, and they stay these eight however many
//! adapters exist, because it stores what arrived rather than what it meant.
//!
//! Two shapes, not two counts: a payload a venue sent names a `venue`, and one
//! this system computed names a `market` — which may span venues and therefore
//! names none of them. **Everything else is identical**, which is what makes a
//! segment written before the second addressing existed byte-for-byte still
//! valid, and which a test asserts rather than a comment claims.

use std::sync::Arc;

use arrow::array::{ArrayRef, BinaryBuilder, Int64Array, StringBuilder, UInt16Array, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use galata_wire::SCHEMA_VERSION;

use super::{Failure, Payload, PayloadAddress};

/// The column names, in order, for a payload a venue sent.
pub const PAYLOAD_COLUMNS: [&str; 8] = [
    "seq",
    "recv_micros",
    "venue",
    "channel",
    "symbol",
    "origin",
    "payload",
    "schema_version",
];

/// The same, for a payload addressed by market.
pub const MARKET_COLUMNS: [&str; 8] = [
    "seq",
    "recv_micros",
    "market",
    "channel",
    "symbol",
    "origin",
    "payload",
    "schema_version",
];

/// The failure sibling's columns. **The payload is not among them.**
pub const FAILURE_COLUMNS: [&str; 6] = [
    "seq",
    "recv_micros",
    "venue",
    "channel",
    "error",
    "schema_version",
];

fn shape(addressing: &'static str) -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("seq", DataType::UInt64, false),
        Field::new("recv_micros", DataType::Int64, false),
        Field::new(addressing, DataType::Utf8, false),
        Field::new("channel", DataType::Utf8, false),
        // Null where a payload covers many, as a universe fetch does.
        Field::new("symbol", DataType::Utf8, true),
        Field::new("origin", DataType::Utf8, false),
        Field::new("payload", DataType::Binary, false),
        Field::new("schema_version", DataType::UInt16, false),
    ]))
}

/// The record's columns for bytes a venue sent.
pub fn payload_schema() -> SchemaRef {
    shape("venue")
}

/// The record's columns for a payload addressed by market.
pub fn market_schema() -> SchemaRef {
    shape("market")
}

/// The record's columns for the addressing in play.
pub fn schema_for(address: &PayloadAddress) -> SchemaRef {
    match address {
        PayloadAddress::Venue(_) | PayloadAddress::Account(_) => payload_schema(),
        PayloadAddress::Market(_) => market_schema(),
    }
}

/// The failure sibling's columns.
pub fn failure_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("seq", DataType::UInt64, false),
        Field::new("recv_micros", DataType::Int64, false),
        Field::new("venue", DataType::Utf8, false),
        Field::new("channel", DataType::Utf8, false),
        Field::new("error", DataType::Utf8, false),
        Field::new("schema_version", DataType::UInt16, false),
    ]))
}

/// One partition's payloads, as a batch.
///
/// Every payload in a group shares a partition and therefore an addressing, so
/// the schema is decided once from the first rather than per row.
///
/// **The builders are sized from what the batch already knows.** `arrow`'s
/// binary and string builders reallocate their value buffer every 32 KB by
/// default, and the payload column here holds whole frames — so an unsized
/// flush of a few hundred of them reallocates repeatedly, copying everything
/// each time. Both the row count and the summed byte length are already in
/// hand; nothing is estimated.
pub fn payload_batch(payloads: &[Payload]) -> Result<RecordBatch, ArrowError> {
    let schema = match payloads.first() {
        Some(p) => schema_for(&p.address),
        None => payload_schema(),
    };

    let rows = payloads.len();
    let payload_bytes: usize = payloads.iter().map(|p| p.payload.len()).sum();
    let address_bytes: usize = payloads.iter().map(|p| p.address.value().len()).sum();
    let channel_bytes: usize = payloads.iter().map(|p| p.channel.len()).sum();
    let symbol_bytes: usize = payloads
        .iter()
        .map(|p| p.symbol.as_deref().map_or(0, str::len))
        .sum();

    let mut address = StringBuilder::with_capacity(rows, address_bytes);
    let mut channel = StringBuilder::with_capacity(rows, channel_bytes);
    let mut symbol = StringBuilder::with_capacity(rows, symbol_bytes);
    // Every origin written here is one of two short words.
    let mut origin = StringBuilder::with_capacity(rows, rows * 8);
    let mut bytes = BinaryBuilder::with_capacity(rows, payload_bytes);

    for p in payloads {
        address.append_value(p.address.value());
        channel.append_value(&p.channel);
        match &p.symbol {
            Some(s) => symbol.append_value(s),
            None => symbol.append_null(),
        }
        origin.append_value(p.origin.as_str());
        bytes.append_value(&p.payload);
    }

    let columns: Vec<ArrayRef> = vec![
        Arc::new(UInt64Array::from_iter_values(
            payloads.iter().map(|p| p.seq),
        )),
        Arc::new(Int64Array::from_iter_values(
            payloads.iter().map(|p| p.recv_micros),
        )),
        Arc::new(address.finish()),
        Arc::new(channel.finish()),
        Arc::new(symbol.finish()),
        Arc::new(origin.finish()),
        Arc::new(bytes.finish()),
        Arc::new(UInt16Array::from_iter_values(
            payloads.iter().map(|_| SCHEMA_VERSION),
        )),
    ];

    RecordBatch::try_new(schema, columns)
}

/// One partition's failures, as a batch.
pub fn failure_batch(failures: &[Failure]) -> Result<RecordBatch, ArrowError> {
    let rows = failures.len();
    let mut venue =
        StringBuilder::with_capacity(rows, failures.iter().map(|f| f.venue.len()).sum());
    let mut channel =
        StringBuilder::with_capacity(rows, failures.iter().map(|f| f.channel.len()).sum());
    let mut error =
        StringBuilder::with_capacity(rows, failures.iter().map(|f| f.error.len()).sum());

    for f in failures {
        venue.append_value(&f.venue);
        channel.append_value(&f.channel);
        error.append_value(&f.error);
    }

    let columns: Vec<ArrayRef> = vec![
        Arc::new(UInt64Array::from_iter_values(
            failures.iter().map(|f| f.seq),
        )),
        Arc::new(Int64Array::from_iter_values(
            failures.iter().map(|f| f.recv_micros),
        )),
        Arc::new(venue.finish()),
        Arc::new(channel.finish()),
        Arc::new(error.finish()),
        Arc::new(UInt16Array::from_iter_values(
            failures.iter().map(|_| SCHEMA_VERSION),
        )),
    ];

    RecordBatch::try_new(failure_schema(), columns)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn names(schema: &SchemaRef) -> Vec<&str> {
        schema.fields().iter().map(|f| f.name().as_str()).collect()
    }

    #[test]
    fn the_schema_is_what_it_says() {
        assert_eq!(names(&payload_schema()), PAYLOAD_COLUMNS);
        assert_eq!(names(&market_schema()), MARKET_COLUMNS);
        assert_eq!(names(&failure_schema()), FAILURE_COLUMNS);
    }

    #[test]
    fn a_failure_row_carries_no_payload_column() {
        // The bytes stay in the main segment. Filtering a record by parse
        // success would discard exactly the evidence a normalisation defect is
        // diagnosed from — and a failure sibling that MOVED the payload would
        // make the record lossy for the one case it exists to serve.
        assert!(!names(&failure_schema()).contains(&"payload"));
    }

    #[test]
    fn the_shapes_differ_only_in_how_they_are_addressed() {
        let venue_schema = payload_schema();
        let market_schema_ref = market_schema();
        let venue = names(&venue_schema);
        let market = names(&market_schema_ref);

        assert_eq!(venue.len(), market.len());
        let differing: Vec<_> = venue.iter().zip(&market).filter(|(a, b)| a != b).collect();
        assert_eq!(
            differing,
            [(&"venue", &"market")],
            "one column apart, so a segment written before the second addressing \
             existed is byte-for-byte still what it was"
        );

        assert!(
            !market.contains(&"venue"),
            "a market may span venues, so naming one would be the sentinel this avoids"
        );
        assert!(!venue.contains(&"market"));
    }

    #[test]
    fn an_empty_batch_still_has_a_schema() {
        // Not a segment anyone writes — the store refuses those — but the
        // batch builder must not panic on the way to being refused.
        let batch = payload_batch(&[]).unwrap();
        assert_eq!(batch.num_rows(), 0);
        assert_eq!(names(&batch.schema()), PAYLOAD_COLUMNS);
    }
}
