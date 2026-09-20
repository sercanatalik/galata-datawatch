//! One schema per dataset.
//!
//! **Every row carries the same five**, and two of them are clocks:
//!
//! ```text
//!   venue        whose data this is — a COLUMN, never a directory
//!   ticker       which instrument — a column, sorted, never a directory
//!   at_micros    THE VENUE'S clock. Nullable: some events have none.
//!   recv_micros  OUR clock. Never null.
//!   stream_seq   the archive row the bytes are in — provenance, and the
//!                road back from any row to what actually arrived
//! ```
//!
//! Two clocks, always both. A single timestamp column would silently pick one
//! question to answer, and the difference between them is the latency figure
//! nothing else can produce.

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use galata_wire::Kind;

/// `decimal(38,18)` — exact, and it aggregates natively in DuckDB and Polars.
///
/// The archive is the fallback for a question about the venue's own formatting:
/// a row here is a number, and the bytes it came from are still on disk.
const PRICE: DataType = DataType::Decimal128(38, 18);

/// The columns a tape read prunes on, in the order rows are sorted by.
///
/// Passed to the segment writer so the footer carries statistics for these and
/// nothing else. The archive prunes on `recv_micros` and the tape does not, and
/// neither inherits the other's answer.
pub const PRUNE_ON: [&str; 3] = ["venue", "ticker", "at_micros"];

/// The five every row carries.
fn common() -> Vec<Field> {
    vec![
        // A column rather than a partition level. See `crate::tape`.
        Field::new("venue", DataType::Utf8, false),
        Field::new("ticker", DataType::Utf8, false),
        // **Nullable on purpose.** An event the venue did not timestamp is not
        // *at* any venue time, and giving it ours would make a latency of zero
        // out of an absence of information.
        Field::new("at_micros", DataType::Int64, true),
        Field::new("recv_micros", DataType::Int64, false),
        Field::new("stream_seq", DataType::UInt64, false),
    ]
}

fn with(extra: Vec<Field>) -> Option<SchemaRef> {
    let mut fields = common();
    fields.extend(extra);
    Some(Arc::new(Schema::new(fields)))
}

/// The arrow schema for a dataset, or `None` for one the tape does not project.
///
/// **An option rather than an exhaustive match**, because [`Kind`] is
/// `#[non_exhaustive]` and this is a different crate: the compiler requires a
/// catch-all here and cannot be made to complain about a missing arm. So the
/// catch-all returns `None` — a loud refusal at the writer — and
/// `every_dataset_has_a_schema` iterates [`Kind::ALL`] to catch a dataset that
/// was added and never projected. The check moves from build time to test time,
/// which is what the vocabulary's own `#[non_exhaustive]` costs its consumers,
/// and is worth saying out loud rather than claiming a guarantee that is not
/// there.
pub fn schema_for(kind: Kind) -> Option<SchemaRef> {
    match kind {
        Kind::Trades => with(vec![
            Field::new("price", PRICE, false),
            Field::new("size", PRICE, false),
            // Which side crossed.
            Field::new("aggressor", DataType::Utf8, false),
            // The venue's own identity, so two receipts of one trade are one
            // trade. Null where the venue states none — and a consumer must
            // then not claim it can deduplicate.
            Field::new("trade_id", DataType::Utf8, true),
        ]),

        // **One row per price level touched.** One `Book` event carries many
        // levels and becomes many rows; snapshots and deltas share this dataset
        // because they are the same shape, and splitting them would force every
        // consumer to read both and merge them in time order — which is the
        // work this table already did.
        Kind::Book => with(vec![
            // Groups the rows of one message.
            Field::new("update_seq", DataType::Int64, false),
            // True → this update replaces all prior state.
            Field::new("is_snapshot", DataType::Boolean, false),
            Field::new("side", DataType::Utf8, false),
            Field::new("price", PRICE, false),
            // **Zero means remove this level**, not a level of no size.
            Field::new("size", PRICE, false),
            // Where the venue numbers its levels.
            Field::new("level", DataType::UInt32, true),
        ]),

        Kind::Candles => with(vec![
            Field::new("interval", DataType::Utf8, false),
            Field::new("open", PRICE, false),
            Field::new("high", PRICE, false),
            Field::new("low", PRICE, false),
            Field::new("close", PRICE, false),
            Field::new("volume", PRICE, false),
            Field::new("trade_count", DataType::UInt32, true),
            // **False while the bar is still forming.** This venue re-sends the
            // in-progress bar on every update, so a consumer that ignores this
            // column counts one minute many times.
            Field::new("is_final", DataType::Boolean, false),
        ]),

        Kind::Funding => with(vec![
            Field::new("rate", PRICE, false),
            Field::new("next_micros", DataType::Int64, true),
        ]),

        // **The cross-venue table.** A pushed `bbo` and a polled best-bid-ask
        // land in the same shape, so comparing one instrument across venues is
        // one predicate rather than a union of two schemas.
        //
        // Nullable here means *this venue never states it*, never *it was
        // missing*: an exchange states sizes and no spread, a broker states a
        // spread and no size.
        Kind::Quotes => with(vec![
            Field::new("bid_px", PRICE, true),
            Field::new("ask_px", PRICE, true),
            Field::new("bid_sz", PRICE, true),
            Field::new("ask_sz", PRICE, true),
            Field::new("bid_spread", PRICE, true),
            Field::new("ask_spread", PRICE, true),
        ]),

        // Value moving between addresses. **Not a trade**: it proves custody
        // moved, not that anything was bought.
        Kind::Transfers => with(vec![
            Field::new("from_address", DataType::Utf8, false),
            Field::new("to_address", DataType::Utf8, false),
            Field::new("amount", PRICE, false),
            Field::new("tx_hash", DataType::Utf8, false),
            // With the hash, the identity of the event on chain — which is what
            // makes a re-read of a block idempotent.
            Field::new("log_index", DataType::UInt32, false),
        ]),

        Kind::Mints => with(vec![
            Field::new("holder", DataType::Utf8, false),
            Field::new("amount", PRICE, false),
            // Issuance, or redemption.
            Field::new("is_issue", DataType::Boolean, false),
            Field::new("tx_hash", DataType::Utf8, false),
            Field::new("log_index", DataType::UInt32, false),
        ]),

        // Four prices, **none derivable from another**, each as the venue
        // printed it. A mark that disagrees with the index is the fact.
        Kind::Marks => with(vec![
            Field::new("mark", PRICE, true),
            Field::new("index", PRICE, true),
            Field::new("oracle", PRICE, true),
            Field::new("open_interest", PRICE, true),
        ]),

        // An absence, with the reason it happened — never inferred from
        // silence.
        Kind::Gaps => with(vec![
            Field::new("series", DataType::Utf8, false),
            Field::new("from_micros", DataType::Int64, false),
            Field::new("to_micros", DataType::Int64, false),
            Field::new("cause", DataType::Utf8, false),
            // What the interval was clipped against, so a consumer knows how
            // loose the bound is.
            Field::new("clipped", DataType::Utf8, false),
        ]),

        // **A row here always has bytes behind it**, named by `archive_seq`.
        Kind::Unparsed => with(vec![
            Field::new("channel", DataType::Utf8, false),
            Field::new("archive_seq", DataType::UInt64, false),
            Field::new("error", DataType::Utf8, false),
        ]),

        Kind::Sessions => with(vec![
            Field::new("session_start", DataType::Int64, false),
            Field::new("session_end", DataType::Int64, false),
            // `full` includes pre- and post-market; `regular` is a subset.
            // Neither is derivable from the other, so both are recorded.
            Field::new("session_kind", DataType::Utf8, false),
            // The IANA zone it was resolved under, **recorded rather than
            // assumed**, so a resolution done under a wrong zone is diagnosable
            // later instead of invisible.
            Field::new("tz", DataType::Utf8, false),
            Field::new("source", DataType::Utf8, false),
            Field::new("observed_at", DataType::Int64, false),
        ]),

        Kind::Instruments => with(vec![
            Field::new("tick_size", PRICE, false),
            Field::new("lot_size", PRICE, false),
            Field::new("min_size", PRICE, false),
            Field::new("contract_type", DataType::Utf8, false),
            Field::new("base", DataType::Utf8, false),
            Field::new("quote", DataType::Utf8, false),
            Field::new("hours", DataType::Utf8, false),
            Field::new("active", DataType::Boolean, false),
            // The venue's own index, which its wire protocol may use in place
            // of a name.
            Field::new("venue_index", DataType::UInt32, true),
            Field::new("ui_multiplier", PRICE, true),
            Field::new("observed_at", DataType::Int64, false),
        ]),

        // **The one absence that can be proved** rather than inferred: rows
        // previously written that the chain no longer holds.
        Kind::Reorgs => with(vec![
            Field::new("from_block", DataType::UInt64, false),
            Field::new("to_block", DataType::UInt64, false),
            Field::new("old_hash", DataType::Utf8, false),
            Field::new("new_hash", DataType::Utf8, false),
        ]),

        // A dataset this build does not know how to project. Not a panic and
        // not an empty schema: either would put rows nowhere while reporting
        // success.
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_dataset_has_a_schema() {
        // The check `#[non_exhaustive]` takes away from the compiler, taken
        // back here: a Kind added to the vocabulary and never projected fails
        // this rather than silently returning None at run time.
        for kind in Kind::ALL {
            assert!(
                schema_for(kind).is_some(),
                "{kind} is a dataset with no tape schema"
            );
        }
    }

    #[test]
    fn every_dataset_carries_the_common_five() {
        for kind in Kind::ALL {
            let schema = schema_for(kind).unwrap();
            for (index, expected) in ["venue", "ticker", "at_micros", "recv_micros", "stream_seq"]
                .iter()
                .enumerate()
            {
                assert_eq!(
                    schema.field(index).name(),
                    expected,
                    "{kind} column {index} — the common five come first, in order, so a \
                     projection over several datasets reads the same way"
                );
            }
        }
    }

    #[test]
    fn the_venue_clock_is_nullable_and_ours_is_not() {
        // An event the venue did not timestamp is not *at* any venue time.
        for kind in Kind::ALL {
            let schema = schema_for(kind).unwrap();
            assert!(schema.field(2).is_nullable(), "{kind}: at_micros");
            assert!(!schema.field(3).is_nullable(), "{kind}: recv_micros");
        }
    }

    #[test]
    fn no_dataset_names_a_column_for_its_own_partition_level() {
        // `kind` and `date` are the path. Repeating either as a column would
        // give it a value that depends on a reader flag — measured on DuckDB
        // 1.5.5, and the reason `venue` is a column and NOT a level.
        for kind in Kind::ALL {
            for field in schema_for(kind).unwrap().fields() {
                assert_ne!(field.name(), "kind", "{kind}");
                assert_ne!(field.name(), "date", "{kind}");
            }
        }
    }

    #[test]
    fn the_prune_columns_exist_in_every_dataset() {
        for kind in Kind::ALL {
            let schema = schema_for(kind).unwrap();
            for column in PRUNE_ON {
                assert!(
                    schema.column_with_name(column).is_some(),
                    "{kind} has no {column}, which a tape read prunes on"
                );
            }
        }
    }

    #[test]
    fn no_price_is_a_float() {
        // A float in one dataset would make its arithmetic differ from
        // another's, silently.
        for kind in Kind::ALL {
            for field in schema_for(kind).unwrap().fields() {
                assert!(
                    !matches!(field.data_type(), DataType::Float32 | DataType::Float64),
                    "{kind}.{} is a float",
                    field.name()
                );
            }
        }
    }
}
