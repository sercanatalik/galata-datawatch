//! **What in the record is no longer true?**
//!
//! A reorganisation says *what we saw has been replaced*, and until the two
//! datasets are read together that claim sits in the record unread. This joins
//! them.
//!
//! ```text
//! cargo run --example superseded -- <tape-root> <kind> <from-date> <to-date>
//! ```
//!
//! The same join in SQL, for a reader holding DuckDB rather than this crate,
//! is [`galata_datawatch::reorg::AS_SQL`].

use arrow::array::{Array, UInt64Array};
use arrow::record_batch::RecordBatch;
use galata_datawatch::calendar::midnight_of;
use galata_datawatch::reorg::{self, Reorganised, Row, Standing};
use galata_datawatch::tape::{self, Reader, Window};
use galata_wire::Kind;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let root = args
        .next()
        .ok_or("usage: superseded <tape-root> <kind> <from-date> <to-date>")?;
    let kind: Kind = args.next().ok_or("need a kind")?.parse()?;
    let from = midnight_of(&args.next().ok_or("need a from-date")?).ok_or("bad from-date")?;
    let to = midnight_of(&args.next().ok_or("need a to-date")?).ok_or("bad to-date")?;

    // **A tape with no `kind=reorgs` at all is the ordinary case**, and it is
    // not an error: a chain that has not reorganised since capture began has
    // written nothing there, and the bounded view refuses a scope with no
    // frontier rather than inventing one.
    //
    // **Asked of the store, not of the filesystem.** This used to stat the
    // directory, which reimplemented a rule the store owns — and got it
    // subtly wrong, because a partition can exist and hold nothing.
    let declared = [kind_scope(kind), "kind=reorgs"];
    let ever_reorganised = !tape::unwritten(std::path::Path::new(&root), &declared)
        .iter()
        .any(|scope| scope == "kind=reorgs");
    let mut scopes = vec![kind_scope(kind)];
    if ever_reorganised {
        scopes.push("kind=reorgs");
    }
    let reader = Reader::open(&root, &scopes)?;
    let window = |kind| Window {
        kind,
        from_micros: from,
        to_micros: to,
        ticker: None,
    };

    // **The reorganisations first.** They are the smallest dataset in the tree
    // — a chain produces one rarely — which is what makes deriving the marker
    // cheap enough that it never needs storing.
    let reorgs = if ever_reorganised {
        reorganisations(&reader.view(window(Kind::Reorgs))?)
    } else {
        Vec::new()
    };
    println!("{} reorganisation(s) in the window", reorgs.len());
    for r in &reorgs {
        println!(
            "  blocks {}..={} ({} deep), recorded at stream_seq {}",
            r.from_block,
            r.to_block,
            r.depth(),
            r.stream_seq
        );
    }

    let batches = reader.view(window(kind))?;
    let rows = data_rows(&batches);
    println!("{kind}: {} rows", rows.len());

    if reorgs.is_empty() {
        // Worth saying rather than printing a zero: nothing was contradicted
        // because nothing was known to contradict it. Every row stands
        // UNCONTRADICTED, which is not the same as confirmed.
        println!(
            "nothing to join — {}",
            if ever_reorganised {
                "no reorganisation falls in this window"
            } else {
                "this venue has never recorded a reorganisation"
            }
        );
        return Ok(());
    }

    let mut superseded = 0usize;
    for row in &rows {
        if let Standing::Superseded(by) = reorg::standing(&reorgs, *row) {
            superseded += 1;
            if superseded <= 10 {
                println!(
                    "  block {} at stream_seq {} superseded by the reorganisation at {}",
                    row.block, row.stream_seq, by.stream_seq
                );
            }
        }
    }
    println!(
        "{superseded} of {} rows superseded, {} not contradicted",
        rows.len(),
        rows.len() - superseded
    );
    // **Not "confirmed".** A row no reorganisation contradicts is only one no
    // KNOWN reorganisation contradicts, and the trail sees only the blocks
    // capture asked for.
    Ok(())
}

fn kind_scope(kind: Kind) -> &'static str {
    match kind {
        Kind::Transfers => "kind=transfers",
        Kind::Mints => "kind=mints",
        _ => "kind=transfers",
    }
}

fn u64s<'a>(batch: &'a RecordBatch, name: &str) -> Option<&'a UInt64Array> {
    batch
        .column_by_name(name)?
        .as_any()
        .downcast_ref::<UInt64Array>()
}

fn reorganisations(batches: &[RecordBatch]) -> Vec<Reorganised> {
    let mut out = Vec::new();
    for batch in batches {
        let (Some(from), Some(to), Some(seq)) = (
            u64s(batch, "from_block"),
            u64s(batch, "to_block"),
            u64s(batch, "stream_seq"),
        ) else {
            continue;
        };
        for i in 0..batch.num_rows() {
            if from.is_null(i) || to.is_null(i) || seq.is_null(i) {
                continue;
            }
            out.push(Reorganised {
                from_block: from.value(i),
                to_block: to.value(i),
                stream_seq: seq.value(i),
            });
        }
    }
    out
}

fn data_rows(batches: &[RecordBatch]) -> Vec<Row> {
    let mut out = Vec::new();
    for batch in batches {
        let (Some(block), Some(seq)) = (u64s(batch, "block"), u64s(batch, "stream_seq")) else {
            continue;
        };
        for i in 0..batch.num_rows() {
            if block.is_null(i) || seq.is_null(i) {
                continue;
            }
            out.push(Row {
                block: block.value(i),
                stream_seq: seq.value(i),
            });
        }
    }
    out
}
