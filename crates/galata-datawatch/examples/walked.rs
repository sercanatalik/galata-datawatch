//! What the walk actually put on disk, read back from the record.
//!
//! **Every figure in `design/measured.md` comes from a run.** This is the
//! program that reads one: it counts the *fetched* payloads per venue symbol
//! and reports the span of venue time each one covers — which is the question a
//! walk's report cannot answer, because a report says what was asked for and
//! this says what arrived.
//!
//! ```text
//! cargo run --example walked -- /tmp/walkrun/archive candles
//! ```

use std::collections::BTreeMap;
use std::path::PathBuf;

use arrow::array::{Array, BinaryArray, StringArray};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let root = PathBuf::from(args.next().ok_or("usage: walked <archive-root> <kind>")?);
    let kind = args.next().ok_or("usage: walked <archive-root> <kind>")?;

    let dir = root.join("venue=hyperliquid").join(format!("kind={kind}"));
    let mut segments: Vec<PathBuf> = Vec::new();
    collect(&dir, &mut segments)?;
    segments.sort();

    // symbol -> (payloads, rows, first venue micros, last venue micros)
    let mut per_symbol: BTreeMap<String, (usize, usize, i64, i64)> = BTreeMap::new();
    let mut fetched = 0usize;

    for path in &segments {
        for batch in galata_segments::read_segment(path)? {
            let origin = column::<StringArray>(&batch, "origin");
            let symbol = column::<StringArray>(&batch, "symbol");
            let payload = column::<BinaryArray>(&batch, "payload");
            for row in 0..batch.num_rows() {
                if origin.value(row) != "fetched" {
                    continue;
                }
                fetched += 1;
                let key = if symbol.is_null(row) {
                    "<none>".to_string()
                } else {
                    symbol.value(row).to_string()
                };
                let (first, last, rows) = span_of(payload.value(row));
                let entry = per_symbol.entry(key).or_insert((0, 0, i64::MAX, i64::MIN));
                entry.0 += 1;
                entry.1 += rows;
                if rows > 0 {
                    entry.2 = entry.2.min(first);
                    entry.3 = entry.3.max(last);
                }
            }
        }
    }

    println!("{} segments, {fetched} fetched payloads\n", segments.len());
    println!(
        "{:<14} {:>8} {:>8} {:>14}  covers",
        "symbol", "pages", "rows", "span"
    );
    for (symbol, (pages, rows, first, last)) in &per_symbol {
        let span = if *first == i64::MAX {
            "-".to_string()
        } else {
            format!("{:.2}d", (last - first) as f64 / 86_400_000.0)
        };
        println!("{symbol:<14} {pages:>8} {rows:>8} {span:>14}  {first}..{last}");
    }
    Ok(())
}

/// The first and last venue time in a page, and how many rows it held.
///
/// Reads the two shapes the walk fetches — an array of objects keyed `t`
/// (candles) or `time` (funding) — and nothing else. A page it cannot read
/// counts as zero rows rather than as a span it invented.
fn span_of(bytes: &[u8]) -> (i64, i64, usize) {
    let Ok(value) = serde_json::from_slice::<serde_json::Value>(bytes) else {
        return (0, 0, 0);
    };
    let Some(rows) = value.as_array() else {
        return (0, 0, 0);
    };
    let times: Vec<i64> = rows
        .iter()
        .filter_map(|r| {
            r.get("t")
                .or_else(|| r.get("time"))
                .and_then(serde_json::Value::as_i64)
        })
        .collect();
    match (times.iter().min(), times.iter().max()) {
        (Some(first), Some(last)) => (*first, *last, times.len()),
        _ => (0, 0, 0),
    }
}

fn column<'a, T: 'static>(batch: &'a arrow::record_batch::RecordBatch, name: &str) -> &'a T {
    batch
        .column_by_name(name)
        .unwrap_or_else(|| panic!("no column {name}"))
        .as_any()
        .downcast_ref::<T>()
        .unwrap_or_else(|| panic!("{name} is not the expected type"))
}

fn collect(dir: &std::path::Path, out: &mut Vec<PathBuf>) -> std::io::Result<()> {
    if !dir.is_dir() {
        return Ok(());
    }
    for entry in std::fs::read_dir(dir)? {
        let path = entry?.path();
        if path.is_dir() {
            collect(&path, out)?;
        } else if path.extension().is_some_and(|e| e == "parquet") {
            out.push(path);
        }
    }
    Ok(())
}
