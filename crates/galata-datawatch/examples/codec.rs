//! **What does each codec cost each store?**
//!
//! ```text
//! cargo run --release --example codec -- <archive-or-tape-root>
//! ```
//!
//! The two stores have opposite read/write ratios — the record is written
//! every few seconds and read rarely, the tape is written once per window and
//! read constantly — so the same codec cannot be right for both on anything
//! but luck. This measures the trade on **this tree's own bytes**, which is
//! the only measurement that settles it: a published benchmark answers a
//! question about somebody else's columns.
//!
//! Every segment under the root is read once into memory, then rewritten under
//! each codec and read back. Write and read are timed separately, because they
//! are the two halves of the trade.

use std::path::Path;
use std::time::Instant;

use arrow::record_batch::RecordBatch;
use galata_segments::{Codec, Cursor, read_segment, read_segment_range, write_segment};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let root = std::env::args()
        .nth(1)
        .ok_or("usage: codec <archive-or-tape-root>")?;

    let mut files = Vec::new();
    collect(Path::new(&root), &mut files);
    files.sort();
    if files.is_empty() {
        return Err(format!("no segments under {root}").into());
    }

    // Read once, up front. What is being measured is the codec, not the disk.
    let batches: Vec<RecordBatch> = files
        .iter()
        .filter_map(|path| read_segment(path).ok())
        .flatten()
        .filter(|b| b.num_rows() > 0)
        .collect();
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    let on_disk: u64 = files
        .iter()
        .filter_map(|p| std::fs::metadata(p).ok())
        .map(|m| m.len())
        .sum();

    println!("{}", root);
    println!(
        "  {} segments, {rows} rows, {:.2} MiB on disk as written",
        files.len(),
        on_disk as f64 / 1_048_576.0
    );
    println!();
    // **A narrow window, which is the tape's real access pattern.** A full
    // scan is what a rebuild does; a query asks for a minute inside a day, and
    // that is where row-group pruning decides how much gets decompressed.
    let (low, high) = span(&batches);
    let width = (high - low).max(1);
    let (from, to) = (low + width / 2, low + width / 2 + width / 60);
    println!("  codec          bytes      of raw   write ms    read ms  window ms");
    println!("  ──────────────────────────────────────────────────────────────────");

    let scratch = tempdir()?;
    let mut raw = 0u64;
    for (label, codec) in [
        ("uncompressed", Codec::Uncompressed),
        ("lz4", Codec::Lz4),
        ("zstd", Codec::Zstd),
    ] {
        let dir = scratch.join(label);
        std::fs::create_dir_all(&dir)?;

        let started = Instant::now();
        let mut written = Vec::new();
        for (i, batch) in batches.iter().enumerate() {
            let cursor = Cursor::Time {
                first_micros: i as i64,
                last_micros: i as i64,
                pid: 1,
                seq: i as u64,
            };
            written.push(write_segment(&dir, cursor, batch, codec)?);
        }
        let write_ms = started.elapsed().as_secs_f64() * 1_000.0;

        let bytes: u64 = written
            .iter()
            .filter_map(|p| std::fs::metadata(p).ok())
            .map(|m| m.len())
            .sum();
        if codec == Codec::Uncompressed {
            raw = bytes;
        }

        // **Read back every row**, which is what a query over the tape does and
        // what a rebuild over the archive does. Decompression is the half of
        // the trade a size table never shows.
        let started = Instant::now();
        let mut read_rows = 0usize;
        for path in &written {
            read_rows += read_segment(path)?
                .iter()
                .map(|b| b.num_rows())
                .sum::<usize>();
        }
        let read_ms = started.elapsed().as_secs_f64() * 1_000.0;
        assert_eq!(read_rows, rows, "a codec must not change the rows");

        // The same window, twenty times, because one narrow read is faster
        // than the clock's own resolution.
        let started = Instant::now();
        for _ in 0..20 {
            for path in &written {
                let _ = read_segment_range(path, from, to)?;
            }
        }
        let window_ms = started.elapsed().as_secs_f64() * 1_000.0 / 20.0;

        println!(
            "  {label:<12} {:>10} {:>8.1}% {:>10.1} {:>10.1} {:>10.2}",
            bytes,
            if raw == 0 {
                100.0
            } else {
                bytes as f64 * 100.0 / raw as f64
            },
            write_ms,
            read_ms,
            window_ms
        );
    }
    let _ = std::fs::remove_dir_all(&scratch);
    Ok(())
}

/// The span of the column reads prune on, across every batch.
fn span(batches: &[RecordBatch]) -> (i64, i64) {
    use arrow::array::{Array, Int64Array};
    let (mut low, mut high) = (i64::MAX, i64::MIN);
    for batch in batches {
        let Some(column) = batch.column_by_name("recv_micros") else {
            continue;
        };
        let Some(values) = column.as_any().downcast_ref::<Int64Array>() else {
            continue;
        };
        for i in 0..values.len() {
            if !values.is_null(i) {
                low = low.min(values.value(i));
                high = high.max(values.value(i));
            }
        }
    }
    if low > high { (0, 1) } else { (low, high) }
}

fn collect(dir: &Path, out: &mut Vec<std::path::PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            collect(&path, out);
        } else if path.extension().is_some_and(|e| e == "parquet") {
            out.push(path);
        }
    }
}

fn tempdir() -> Result<std::path::PathBuf, std::io::Error> {
    let dir = std::env::temp_dir().join(format!("galata-codec-{}", std::process::id()));
    std::fs::create_dir_all(&dir)?;
    Ok(dir)
}
