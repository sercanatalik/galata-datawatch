//! Buffer, sort, write the segment, commit.
//!
//! **Nothing is durable until [`Tape::commit`] returns.** A crash before it
//! loses buffered rows, which costs nothing: the archive still holds every byte
//! they were derived from, and a rebuild produces them again. That is the whole
//! privilege of being a cache.
//!
//! A segment is named by the **stream-sequence range** it covers, so a rebuild
//! that re-derives the same rows writes the same name — and two segments
//! claiming overlapping ranges are visible from the listing alone rather than
//! double-counted silently.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use arrow::array::{
    ArrayRef, BooleanBuilder, Decimal128Builder, Int64Builder, StringBuilder, UInt32Builder,
    UInt64Builder,
};
use arrow::record_batch::RecordBatch;
use galata_segments::{Codec, Cursor, write_segment_pruned};
use galata_wire::{Envelope, Event, Kind, Num};

use crate::tape::layout::partition_of;
use crate::tape::schema::{PRUNE_ON, schema_for};

/// The scale every price and size is written at: `decimal(38,18)`.
const SCALE: u32 = 18;

/// Why the tape refused.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum TapeError {
    /// The segment store refused.
    #[error(transparent)]
    Segment(#[from] galata_segments::SegmentError),
    /// Arrow refused the batch.
    #[error("arrow: {0}")]
    Arrow(String),
    /// A number does not fit `decimal(38,18)`.
    #[error(
        "{value} does not fit decimal(38,18), so the tape would have to round it. The archive \
         still holds the bytes it came from"
    )]
    Precision {
        /// The offending number.
        value: String,
    },
    /// A dataset this build does not project.
    #[error(
        "{kind} is a dataset this build does not project. Its bytes are still in the archive, \
         and a rebuild after the projection is added will write them"
    )]
    NotProjected {
        /// Which one.
        kind: Kind,
    },
}

/// One row, and the archive position it came from.
#[derive(Debug, Clone, PartialEq)]
pub struct Row {
    /// The archive sequence of the payload this was normalised from — the road
    /// back from any row to the bytes that produced it.
    pub stream_seq: u64,
    /// The event.
    pub envelope: Envelope,
}

/// The tape writer.
#[derive(Debug)]
pub struct Tape {
    root: PathBuf,
    codec: Codec,
    buffered: Vec<Row>,
}

impl Tape {
    /// Open the tape under a root.
    pub fn open(root: impl Into<PathBuf>) -> Tape {
        Tape {
            root: root.into(),
            codec: Codec::Zstd,
            buffered: Vec::new(),
        }
    }

    /// Write with a declared codec rather than the default.
    pub fn with_codec(mut self, codec: Codec) -> Tape {
        self.codec = codec;
        self
    }

    /// The tree this writes into.
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Rows taken and not yet durable.
    pub fn buffered(&self) -> usize {
        self.buffered.len()
    }

    /// The furthest stream position the tape holds, **read from the segment
    /// names** — no file is opened, and no bookmark is kept.
    ///
    /// `None` on a fresh tape, which is not an error.
    pub fn resume_from(&self) -> Option<u64> {
        galata_segments::last_durable(&self.root).and_then(|(variant, position)| {
            match variant {
                galata_segments::Variant::Seq => u64::try_from(position).ok(),
                // A tape holds sequence-named segments and nothing else. A
                // time- or block-named one under this root is somebody else's
                // tree, and answering from it would be a resume point measured
                // in the wrong unit.
                _ => None,
            }
        })
    }

    /// The partitions a commit would write to, **before** committing.
    ///
    /// For a rebuild that replaces: it needs to know what it is about to write
    /// over, and it needs to know before it writes. Derived from the buffered
    /// rows rather than from the tree, so a partition nothing will be written
    /// to is not named — and therefore not removed.
    pub fn pending_partitions(&self) -> Vec<PathBuf> {
        let mut out: Vec<PathBuf> = self
            .buffered
            .iter()
            .map(|row| {
                let at = row.envelope.at_micros.unwrap_or(row.envelope.recv_micros);
                partition_of(row.envelope.kind(), at)
            })
            .collect();
        out.sort();
        out.dedup();
        out
    }

    /// The venues among the buffered rows, sorted.
    ///
    /// What a replacing rebuild is entitled to remove: a partition is shared by
    /// every venue that supplies its dataset, so *this run's partitions* is not
    /// *this run's rows*, and the venue is what tells them apart.
    pub fn pending_venues(&self) -> std::collections::BTreeSet<String> {
        self.buffered
            .iter()
            .map(|row| venue_of(&row.envelope).to_string())
            .collect()
    }

    /// Take a row. Nothing is durable until [`Tape::commit`].
    pub fn take(&mut self, row: Row) {
        self.buffered.push(row);
    }

    /// Write and commit everything buffered.
    pub fn commit(&mut self) -> Result<Vec<PathBuf>, TapeError> {
        let rows = std::mem::take(&mut self.buffered);
        let mut written = Vec::new();

        for (partition, kind, mut group) in group(rows) {
            // **Sorted by venue, then ticker, then venue time.** In that order
            // because it is decreasing cardinality and increasing selectivity:
            // a venue predicate skips whole row groups, a ticker predicate
            // skips within what is left, a time predicate within that. Sorting
            // by time first would interleave every ticker and leave a ticker
            // predicate nothing to prune on.
            //
            // `stream_seq` last, so the order is TOTAL and a rebuild over the
            // same rows produces byte-identical output.
            group.sort_by(|a, b| {
                venue_of(&a.envelope)
                    .cmp(venue_of(&b.envelope))
                    .then_with(|| ticker_of(&a.envelope).cmp(ticker_of(&b.envelope)))
                    .then(a.envelope.at_micros.cmp(&b.envelope.at_micros))
                    .then(a.stream_seq.cmp(&b.stream_seq))
            });

            let first = group.iter().map(|r| r.stream_seq).min().unwrap_or(0);
            let last = group.iter().map(|r| r.stream_seq).max().unwrap_or(0);
            let batch = batch_for(kind, &group)?;
            if batch.num_rows() == 0 {
                continue;
            }
            written.push(write_segment_pruned(
                &self.root.join(partition),
                Cursor::Seq { first, last },
                &batch,
                self.codec,
                &PRUNE_ON,
            )?);
        }
        Ok(written)
    }
}

/// The venue, or the empty string for a market-addressed event.
///
/// Every dataset the tape projects is venue-addressed today, so the empty
/// string is unreachable — but it is returned rather than unwrapped, because a
/// panic in a projection would lose a whole batch of rows over one odd
/// envelope, and the column is where an oddity should be visible.
fn venue_of(envelope: &Envelope) -> &str {
    envelope.venue().map(|v| v.as_str()).unwrap_or("")
}

/// The ticker, or the empty string for an event that names none.
///
/// Every event the tape projects carries one today. The empty string sorts
/// first and is written as-is rather than refused — losing a row from a cache
/// over a missing sort key would be a worse trade than an odd-looking one.
fn ticker_of(envelope: &Envelope) -> &str {
    envelope.ticker().map(|t| t.as_str()).unwrap_or("")
}

/// Group into partitions, by dataset and by the date the row belongs to.
fn group(rows: Vec<Row>) -> Vec<(PathBuf, Kind, Vec<Row>)> {
    let mut out: BTreeMap<(String, Kind), Vec<Row>> = BTreeMap::new();
    for row in rows {
        let kind = row.envelope.kind();
        // **Partitioned by the venue's own time where it states one**, and by
        // ours where it does not. A row's date is the date the event happened,
        // not the date we heard about it — otherwise a walk fetching last
        // March writes it under today, and a date predicate finds nothing.
        let at = row.envelope.at_micros.unwrap_or(row.envelope.recv_micros);
        let partition = partition_of(kind, at);
        out.entry((partition.to_string_lossy().to_string(), kind))
            .or_default()
            .push(row);
    }
    out.into_iter()
        .map(|((partition, kind), rows)| (PathBuf::from(partition), kind, rows))
        .collect()
}

/// A number, at the scale the column holds.
///
/// **Refused rather than rounded** when it does not fit. A tape that quietly
/// rounded would disagree with the archive, and the archive is the thing that
/// is right.
fn scaled(value: Num) -> Result<i128, TapeError> {
    let scale = value.scale();
    let mantissa = value.mantissa();
    if scale > SCALE {
        return Err(TapeError::Precision {
            value: value.to_string(),
        });
    }
    mantissa
        .checked_mul(10i128.pow(SCALE - scale))
        .ok_or_else(|| TapeError::Precision {
            value: value.to_string(),
        })
}

/// A decimal column, sized from the row count.
fn dec(rows: &[Row], f: impl Fn(&Envelope) -> Option<Num>) -> Result<ArrayRef, TapeError> {
    let mut builder = Decimal128Builder::with_capacity(rows.len())
        .with_precision_and_scale(38, SCALE as i8)
        .map_err(|e| TapeError::Arrow(e.to_string()))?;
    for row in rows {
        match f(&row.envelope) {
            Some(value) => builder.append_value(scaled(value)?),
            None => builder.append_null(),
        }
    }
    Ok(std::sync::Arc::new(builder.finish()))
}

/// A string column, sized from the row count **and the summed length**.
///
/// Both, because the default reallocates the value buffer every 32 KB and a
/// batch already knows how many bytes it holds.
fn text(rows: &[Row], f: impl Fn(&Envelope) -> Option<String>) -> ArrayRef {
    let values: Vec<Option<String>> = rows.iter().map(|r| f(&r.envelope)).collect();
    let bytes: usize = values.iter().flatten().map(String::len).sum();
    let mut builder = StringBuilder::with_capacity(values.len(), bytes);
    for value in values {
        builder.append_option(value);
    }
    std::sync::Arc::new(builder.finish())
}

fn int(rows: &[Row], f: impl Fn(&Envelope) -> Option<i64>) -> ArrayRef {
    let mut builder = Int64Builder::with_capacity(rows.len());
    for row in rows {
        builder.append_option(f(&row.envelope));
    }
    std::sync::Arc::new(builder.finish())
}

fn u64s(rows: &[Row], f: impl Fn(&Row) -> u64) -> ArrayRef {
    let mut builder = UInt64Builder::with_capacity(rows.len());
    for row in rows {
        builder.append_value(f(row));
    }
    std::sync::Arc::new(builder.finish())
}

fn u32s(rows: &[Row], f: impl Fn(&Envelope) -> Option<u32>) -> ArrayRef {
    let mut builder = UInt32Builder::with_capacity(rows.len());
    for row in rows {
        builder.append_option(f(&row.envelope));
    }
    std::sync::Arc::new(builder.finish())
}

fn flag(rows: &[Row], f: impl Fn(&Envelope) -> bool) -> ArrayRef {
    let mut builder = BooleanBuilder::with_capacity(rows.len());
    for row in rows {
        builder.append_value(f(&row.envelope));
    }
    std::sync::Arc::new(builder.finish())
}

/// The five every dataset carries.
fn common(rows: &[Row]) -> Vec<ArrayRef> {
    vec![
        text(rows, |e| Some(venue_of(e).to_string())),
        text(rows, |e| e.ticker().map(|t| t.as_str().to_string())),
        int(rows, |e| e.at_micros),
        int(rows, |e| Some(e.recv_micros)),
        u64s(rows, |r| r.stream_seq),
    ]
}

/// The batch for one dataset.
///
/// Matched **exhaustively**. A dataset added to [`Kind`] is a compile error
/// here, rather than a silent absence from the tape.
fn batch_for(kind: Kind, rows: &[Row]) -> Result<RecordBatch, TapeError> {
    // The book is the one dataset where one event becomes MANY rows, so its
    // rows are expanded before any column is built.
    if kind == Kind::Book {
        return book_batch(rows);
    }

    let mut columns = common(rows);
    match kind {
        Kind::Trades => {
            columns.push(dec(rows, |e| trade(e).map(|t| t.price))?);
            columns.push(dec(rows, |e| trade(e).map(|t| t.size))?);
            columns.push(text(rows, |e| {
                trade(e).map(|t| t.aggressor.as_str().to_string())
            }));
            columns.push(text(rows, |e| trade(e).and_then(|t| t.trade_id.clone())));
        }
        Kind::Candles => {
            columns.push(text(rows, |e| candle(e).map(|c| c.interval.clone())));
            columns.push(dec(rows, |e| candle(e).map(|c| c.open))?);
            columns.push(dec(rows, |e| candle(e).map(|c| c.high))?);
            columns.push(dec(rows, |e| candle(e).map(|c| c.low))?);
            columns.push(dec(rows, |e| candle(e).map(|c| c.close))?);
            columns.push(dec(rows, |e| candle(e).map(|c| c.volume))?);
            columns.push(u32s(rows, |e| candle(e).and_then(|c| c.trade_count)));
            columns.push(flag(rows, |e| candle(e).is_some_and(|c| c.is_final)));
        }
        Kind::Funding => {
            columns.push(dec(rows, |e| funding(e).map(|f| f.rate))?);
            columns.push(int(rows, |e| funding(e).and_then(|f| f.next_micros)));
        }
        Kind::Quotes => {
            columns.push(dec(rows, |e| quote(e).and_then(|q| q.bid_px))?);
            columns.push(dec(rows, |e| quote(e).and_then(|q| q.ask_px))?);
            columns.push(dec(rows, |e| quote(e).and_then(|q| q.bid_sz))?);
            columns.push(dec(rows, |e| quote(e).and_then(|q| q.ask_sz))?);
            columns.push(dec(rows, |e| quote(e).and_then(|q| q.bid_spread))?);
            columns.push(dec(rows, |e| quote(e).and_then(|q| q.ask_spread))?);
        }
        Kind::Transfers => {
            columns.push(u64s(rows, |r| {
                transfer(&r.envelope).map(|t| t.block).unwrap_or(0)
            }));
            columns.push(text(rows, |e| transfer(e).map(|t| t.from.clone())));
            columns.push(text(rows, |e| transfer(e).map(|t| t.to.clone())));
            columns.push(dec(rows, |e| transfer(e).map(|t| t.amount))?);
            columns.push(text(rows, |e| transfer(e).map(|t| t.tx_hash.clone())));
            columns.push(u32s(rows, |e| transfer(e).map(|t| t.log_index)));
        }
        Kind::Mints => {
            columns.push(u64s(rows, |r| {
                mint(&r.envelope).map(|m| m.block).unwrap_or(0)
            }));
            columns.push(text(rows, |e| mint(e).map(|m| m.holder.clone())));
            columns.push(dec(rows, |e| mint(e).map(|m| m.amount))?);
            columns.push(flag(rows, |e| mint(e).is_some_and(|m| m.is_issue)));
            columns.push(text(rows, |e| mint(e).map(|m| m.tx_hash.clone())));
            columns.push(u32s(rows, |e| mint(e).map(|m| m.log_index)));
        }
        Kind::Marks => {
            columns.push(dec(rows, |e| mark(e).and_then(|m| m.mark))?);
            columns.push(dec(rows, |e| mark(e).and_then(|m| m.index))?);
            columns.push(dec(rows, |e| mark(e).and_then(|m| m.oracle))?);
            columns.push(dec(rows, |e| mark(e).and_then(|m| m.open_interest))?);
        }
        Kind::Gaps => {
            columns.push(text(rows, |e| {
                gap(e).map(|g| g.series.as_str().to_string())
            }));
            columns.push(int(rows, |e| gap(e).map(|g| g.from_micros)));
            columns.push(int(rows, |e| gap(e).map(|g| g.to_micros)));
            columns.push(text(rows, |e| gap(e).map(|g| g.cause.as_str().to_string())));
            columns.push(text(rows, |e| {
                gap(e).map(|g| g.clipped.as_str().to_string())
            }));
        }
        Kind::Unparsed => {
            columns.push(text(rows, |e| unparsed(e).map(|u| u.channel.clone())));
            columns.push(u64s(rows, |r| {
                unparsed(&r.envelope).map(|u| u.archive_seq).unwrap_or(0)
            }));
            columns.push(text(rows, |e| unparsed(e).map(|u| u.error.clone())));
        }
        Kind::Sessions => {
            columns.push(int(rows, |e| session(e).map(|s| s.session_start)));
            columns.push(int(rows, |e| session(e).map(|s| s.session_end)));
            columns.push(text(rows, |e| {
                session(e).map(|s| s.session_kind.as_str().to_string())
            }));
            columns.push(text(rows, |e| session(e).map(|s| s.tz.clone())));
            columns.push(text(rows, |e| session(e).map(|s| s.source.clone())));
            columns.push(int(rows, |e| session(e).map(|s| s.observed_at)));
        }
        Kind::Instruments => {
            columns.push(dec(rows, |e| instrument(e).map(|i| i.tick_size))?);
            columns.push(dec(rows, |e| instrument(e).map(|i| i.lot_size))?);
            columns.push(dec(rows, |e| instrument(e).map(|i| i.min_size))?);
            columns.push(text(rows, |e| {
                instrument(e).map(|i| i.contract_type.clone())
            }));
            columns.push(text(rows, |e| instrument(e).map(|i| i.base.clone())));
            columns.push(text(rows, |e| instrument(e).map(|i| i.quote.clone())));
            columns.push(text(rows, |e| instrument(e).map(|i| i.hours.clone())));
            columns.push(flag(rows, |e| instrument(e).is_some_and(|i| i.active)));
            columns.push(u32s(rows, |e| instrument(e).and_then(|i| i.venue_index)));
            columns.push(dec(rows, |e| instrument(e).and_then(|i| i.ui_multiplier))?);
            columns.push(int(rows, |e| instrument(e).map(|i| i.observed_at)));
        }
        Kind::Reorgs => {
            columns.push(u64s(rows, |r| {
                reorg(&r.envelope).map(|o| o.from_block).unwrap_or(0)
            }));
            columns.push(u64s(rows, |r| {
                reorg(&r.envelope).map(|o| o.to_block).unwrap_or(0)
            }));
            columns.push(text(rows, |e| reorg(e).map(|o| o.old_hash.clone())));
            columns.push(text(rows, |e| reorg(e).map(|o| o.new_hash.clone())));
        }
        // Handled above, before any column was built.
        Kind::Book => unreachable!("the book expands its rows first"),

        // A dataset this build does not project. Refused by name rather than
        // written empty: rows that went nowhere while the run reported success
        // is the failure this whole store is built to avoid.
        other => return Err(TapeError::NotProjected { kind: other }),
    }

    let schema = schema_for(kind).ok_or(TapeError::NotProjected { kind })?;
    RecordBatch::try_new(schema, columns).map_err(|e| TapeError::Arrow(e.to_string()))
}

/// The book, expanded: **one row per price level touched.**
fn book_batch(rows: &[Row]) -> Result<RecordBatch, TapeError> {
    let mut flat: Vec<Row> = Vec::new();
    let mut levels = Vec::new();
    for row in rows {
        if let Event::Book(book) = &row.envelope.event {
            for level in &book.levels {
                flat.push(row.clone());
                levels.push((book.update_seq, book.is_snapshot, level.clone()));
            }
        }
    }

    let mut columns = common(&flat);
    let mut update_seq = Int64Builder::with_capacity(levels.len());
    let mut snapshot = BooleanBuilder::with_capacity(levels.len());
    let mut side = StringBuilder::with_capacity(levels.len(), levels.len() * 4);
    let mut price = Decimal128Builder::with_capacity(levels.len())
        .with_precision_and_scale(38, SCALE as i8)
        .map_err(|e| TapeError::Arrow(e.to_string()))?;
    let mut size = Decimal128Builder::with_capacity(levels.len())
        .with_precision_and_scale(38, SCALE as i8)
        .map_err(|e| TapeError::Arrow(e.to_string()))?;
    let mut depth = UInt32Builder::with_capacity(levels.len());

    for (seq, is_snapshot, level) in &levels {
        update_seq.append_value(*seq);
        snapshot.append_value(*is_snapshot);
        side.append_value(level.side.as_str());
        price.append_value(scaled(level.price)?);
        size.append_value(scaled(level.size)?);
        depth.append_option(level.level);
    }

    columns.push(std::sync::Arc::new(update_seq.finish()));
    columns.push(std::sync::Arc::new(snapshot.finish()));
    columns.push(std::sync::Arc::new(side.finish()));
    columns.push(std::sync::Arc::new(price.finish()));
    columns.push(std::sync::Arc::new(size.finish()));
    columns.push(std::sync::Arc::new(depth.finish()));

    let schema = schema_for(Kind::Book).ok_or(TapeError::NotProjected { kind: Kind::Book })?;
    RecordBatch::try_new(schema, columns).map_err(|e| TapeError::Arrow(e.to_string()))
}

macro_rules! project {
    ($name:ident, $variant:ident, $type:ty) => {
        /// The payload, where this envelope carries one.
        ///
        /// `None` is not an error here: rows are grouped by kind before a batch
        /// is built, so a mismatch cannot occur — and returning an option keeps
        /// the column builders total rather than panicking on a case the
        /// grouping already excluded.
        fn $name(envelope: &Envelope) -> Option<&$type> {
            match &envelope.event {
                Event::$variant(inner) => Some(inner),
                _ => None,
            }
        }
    };
}

project!(trade, Trade, galata_wire::Trade);
project!(candle, Candle, galata_wire::Candle);
project!(funding, Funding, galata_wire::Funding);
project!(quote, Quote, galata_wire::Quote);
project!(transfer, Transfer, galata_wire::Transfer);
project!(mint, Mint, galata_wire::Mint);
project!(mark, Mark, galata_wire::Mark);
project!(gap, Gap, galata_wire::Gap);
project!(unparsed, Unparsed, galata_wire::Unparsed);
project!(session, Session, galata_wire::Session);
project!(instrument, Instrument, galata_wire::Instrument);
project!(reorg, Reorg, galata_wire::Reorg);

#[cfg(test)]
mod tests {
    use super::*;
    use galata_wire::{Quote, Ticker, Venue};
    use std::str::FromStr;

    const DAY: i64 = 86_400_000_000;

    fn quote_row(seq: u64, venue: &str, ticker: &str, at: Option<i64>) -> Row {
        Row {
            stream_seq: seq,
            envelope: Envelope::new(
                Venue::new(venue).unwrap(),
                Ticker::new(ticker).unwrap(),
                at,
                at.unwrap_or(0) + 1_000,
                Event::Quote(Quote {
                    bid_px: Some(Num::from_str("81213.0").unwrap()),
                    ask_px: Some(Num::from_str("81214.5").unwrap()),
                    bid_sz: Some(Num::from_str("15.826").unwrap()),
                    ask_sz: None,
                    bid_spread: None,
                    ask_spread: None,
                }),
            ),
        }
    }

    fn read(path: &Path) -> arrow::record_batch::RecordBatch {
        let mut batches = galata_segments::read_segment(path).unwrap();
        assert_eq!(batches.len(), 1);
        batches.remove(0)
    }

    fn strings(batch: &arrow::record_batch::RecordBatch, name: &str) -> Vec<String> {
        use arrow::array::Array;
        let column = batch.column_by_name(name).unwrap();
        let array = column
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        (0..array.len())
            .map(|i| array.value(i).to_string())
            .collect()
    }

    #[test]
    fn nothing_is_durable_before_commit() {
        // Losing buffered rows costs nothing: the archive holds every byte they
        // came from, and a rebuild produces them again.
        let root = tempfile::tempdir().unwrap();
        let mut tape = Tape::open(root.path());
        tape.take(quote_row(1, "hyperliquid", "BTC", Some(DAY)));
        assert_eq!(tape.buffered(), 1);
        assert!(galata_segments::list_segments(root.path()).is_empty());

        let written = tape.commit().unwrap();
        assert_eq!(written.len(), 1);
        assert_eq!(tape.buffered(), 0);
    }

    #[test]
    fn rows_are_sorted_by_venue_then_ticker_then_time() {
        // Decreasing cardinality, increasing selectivity: a venue predicate
        // skips whole row groups, a ticker predicate skips within what is left.
        let root = tempfile::tempdir().unwrap();
        let mut tape = Tape::open(root.path());
        for row in [
            quote_row(4, "rh-crypto", "BTC", Some(DAY + 1)),
            quote_row(1, "hyperliquid", "ETH", Some(DAY + 9)),
            quote_row(2, "hyperliquid", "BTC", Some(DAY + 7)),
            quote_row(3, "hyperliquid", "BTC", Some(DAY + 2)),
        ] {
            tape.take(row);
        }
        let written = tape.commit().unwrap();
        let batch = read(&written[0]);

        assert_eq!(
            strings(&batch, "venue"),
            ["hyperliquid", "hyperliquid", "hyperliquid", "rh-crypto"]
        );
        assert_eq!(strings(&batch, "ticker"), ["BTC", "BTC", "ETH", "BTC"]);
        let at = batch.column_by_name("at_micros").unwrap();
        let at = at
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        assert!(
            at.value(0) < at.value(1),
            "BTC's two rows are in time order"
        );
    }

    #[test]
    fn a_segment_is_named_for_the_sequence_range_it_covers() {
        // So a rebuild over the same rows writes the same name, and two
        // segments claiming overlapping ranges are visible from the listing.
        let root = tempfile::tempdir().unwrap();
        let mut tape = Tape::open(root.path());
        tape.take(quote_row(17, "hyperliquid", "BTC", Some(DAY)));
        tape.take(quote_row(42, "hyperliquid", "ETH", Some(DAY)));
        let written = tape.commit().unwrap();

        let name = written[0]
            .file_name()
            .unwrap()
            .to_string_lossy()
            .to_string();
        assert!(name.starts_with("s-"), "{name}");
        let listed = galata_segments::list_segments(written[0].parent().unwrap());
        assert_eq!(
            listed[0].0,
            Cursor::Seq {
                first: 17,
                last: 42
            }
        );
    }

    #[test]
    fn the_venue_is_in_the_data_and_not_in_the_path() {
        // The whole correction. Measured on DuckDB 1.5.5: written both ways,
        // the value depends on whether the reader passes hive_partitioning.
        let root = tempfile::tempdir().unwrap();
        let mut tape = Tape::open(root.path());
        tape.take(quote_row(1, "hyperliquid", "BTC", Some(DAY)));
        let written = tape.commit().unwrap();

        let path = written[0].to_string_lossy().to_string();
        assert!(!path.contains("venue="), "{path}");
        assert!(path.contains("kind=quotes"), "{path}");
        assert_eq!(strings(&read(&written[0]), "venue"), ["hyperliquid"]);
    }

    #[test]
    fn a_row_is_dated_by_the_venues_clock_where_it_states_one() {
        // Otherwise a walk fetching last March writes it under today, and a
        // date predicate finds nothing.
        let root = tempfile::tempdir().unwrap();
        let mut tape = Tape::open(root.path());
        // Venue time on day 100; receipt a day later.
        let mut row = quote_row(1, "hyperliquid", "BTC", Some(100 * DAY));
        row.envelope.recv_micros = 101 * DAY;
        tape.take(row);
        let written = tape.commit().unwrap();
        assert!(
            written[0]
                .to_string_lossy()
                .contains(&format!("date={}", crate::calendar::date_of(100 * DAY))),
            "{}",
            written[0].display()
        );
    }

    #[test]
    fn an_event_with_no_venue_time_falls_back_to_ours_and_is_null_in_the_column() {
        // Two different questions: it must be FILED somewhere, and it is not
        // *at* any venue time.
        let root = tempfile::tempdir().unwrap();
        let mut tape = Tape::open(root.path());
        let mut row = quote_row(1, "hyperliquid", "BTC", None);
        row.envelope.recv_micros = 100 * DAY;
        tape.take(row);
        let written = tape.commit().unwrap();

        assert!(
            written[0].to_string_lossy().contains("date=1970"),
            "filed by our clock"
        );
        let batch = read(&written[0]);
        assert!(batch.column_by_name("at_micros").unwrap().is_null(0));
        assert!(!batch.column_by_name("recv_micros").unwrap().is_null(0));
    }

    #[test]
    fn a_number_too_precise_for_the_column_is_refused_rather_than_rounded() {
        // A tape that quietly rounded would disagree with the archive, and the
        // archive is the thing that is right.
        let root = tempfile::tempdir().unwrap();
        let mut tape = Tape::open(root.path());
        let mut row = quote_row(1, "hyperliquid", "BTC", Some(DAY));
        if let Event::Quote(q) = &mut row.envelope.event {
            // Twenty decimal places, against a column that holds eighteen.
            q.bid_px = Some(Num::from_str("0.00000000000000000001").unwrap());
        }
        tape.take(row);
        let error = tape.commit().unwrap_err();
        assert!(matches!(error, TapeError::Precision { .. }), "{error}");
        assert!(error.to_string().contains("archive"), "{error}");
    }

    #[test]
    fn two_datasets_land_in_two_partitions() {
        let root = tempfile::tempdir().unwrap();
        let mut tape = Tape::open(root.path());
        tape.take(quote_row(1, "hyperliquid", "BTC", Some(DAY)));
        tape.take(Row {
            stream_seq: 2,
            envelope: Envelope::new(
                Venue::new("hyperliquid").unwrap(),
                Ticker::new("BTC").unwrap(),
                Some(DAY),
                DAY,
                Event::Funding(galata_wire::Funding {
                    rate: Num::from_str("0.0000125").unwrap(),
                    next_micros: None,
                }),
            ),
        });
        let written = tape.commit().unwrap();
        assert_eq!(written.len(), 2);
        assert_eq!(crate::tape::check_layout(root.path()), Vec::new());
    }

    #[test]
    fn a_fresh_tape_resumes_from_nowhere_and_a_written_one_from_its_own_names() {
        let root = tempfile::tempdir().unwrap();
        let mut tape = Tape::open(root.path());
        assert_eq!(tape.resume_from(), None);

        tape.take(quote_row(9, "hyperliquid", "BTC", Some(DAY)));
        tape.commit().unwrap();
        assert_eq!(tape.resume_from(), Some(9));
    }

    #[test]
    fn a_book_event_becomes_one_row_per_level() {
        let root = tempfile::tempdir().unwrap();
        let mut tape = Tape::open(root.path());
        let level = |side, px: &str| galata_wire::BookLevel {
            side,
            price: Num::from_str(px).unwrap(),
            size: Num::from_str("1.5").unwrap(),
            level: Some(0),
        };
        tape.take(Row {
            stream_seq: 1,
            envelope: Envelope::new(
                Venue::new("hyperliquid").unwrap(),
                Ticker::new("BTC").unwrap(),
                Some(DAY),
                DAY,
                Event::Book(galata_wire::Book {
                    update_seq: 7,
                    is_snapshot: true,
                    levels: vec![
                        level(galata_wire::Side::Bid, "100"),
                        level(galata_wire::Side::Ask, "101"),
                    ],
                }),
            ),
        });
        let written = tape.commit().unwrap();
        let batch = read(&written[0]);
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(strings(&batch, "side").len(), 2);
    }
}
