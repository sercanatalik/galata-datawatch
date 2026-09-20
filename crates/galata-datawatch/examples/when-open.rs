//! **Is this instrument ever closed?** Answered from the record, not from a
//! claim.
//!
//! Whether a venue has a calendar decides whether a gap written over a weekend
//! is a real loss or a lie, and it is the one question about an instrument that
//! must not be taken from documentation. Two published sources already
//! disagreed about a ticker on this dex, and the venue's own universe settled
//! it; this does the same for hours.
//!
//! It buckets the walked candle history by hour of the week in the named zone
//! and prints how many bars carried a trade. **An hour with bars but no trades
//! is not closed** — it is quiet, and the difference is the whole point.
//!
//! ```text
//! cargo run --example when-open -- /tmp/walkrun/archive xyz:XYZ100 America/New_York
//! ```

use std::path::{Path, PathBuf};

use arrow::array::{Array, BinaryArray, StringArray};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let root = PathBuf::from(
        args.next()
            .ok_or("usage: when-open <root> <symbol> <zone>")?,
    );
    let want = args
        .next()
        .ok_or("usage: when-open <root> <symbol> <zone>")?;
    let zone_name = args.next().unwrap_or_else(|| "UTC".to_string());
    let zone = jiff::tz::TimeZone::get(&zone_name)?;

    let mut segments = Vec::new();
    collect(
        &root.join("venue=hyperliquid").join("kind=candles"),
        &mut segments,
    )?;
    segments.sort();

    // (day of week, hour) -> (bars, bars carrying a trade, trades)
    let mut grid: std::collections::BTreeMap<(i8, i8), (u64, u64, u64)> = Default::default();

    for path in &segments {
        for batch in galata_segments::read_segment(path)? {
            let origin = column::<StringArray>(&batch, "origin");
            let symbol = column::<StringArray>(&batch, "symbol");
            let payload = column::<BinaryArray>(&batch, "payload");
            for row in 0..batch.num_rows() {
                if origin.value(row) != "fetched"
                    || symbol.is_null(row)
                    || symbol.value(row) != want
                {
                    continue;
                }
                let bars: Vec<Bar> = serde_json::from_slice(payload.value(row))?;
                for bar in bars {
                    let at = jiff::Timestamp::from_millisecond(bar.t)?.to_zoned(zone.clone());
                    let key = (at.weekday().to_monday_zero_offset(), at.hour());
                    let entry = grid.entry(key).or_default();
                    entry.0 += 1;
                    if bar.n > 0 {
                        entry.1 += 1;
                        entry.2 += bar.n;
                    }
                }
            }
        }
    }

    println!("{want} by hour of week in {zone_name}\n");
    println!("  bars with a trade / bars seen, per hour\n");
    print!("      ");
    for hour in 0..24 {
        print!("{hour:>4}");
    }
    println!();
    for day in 0..7 {
        print!("{:<6}", DAYS[day as usize]);
        for hour in 0..24 {
            match grid.get(&(day, hour)) {
                None => print!("   ."),
                Some((bars, traded, _)) if *traded == 0 => print!("{:>4}", format!("0/{bars}")),
                Some((_, traded, _)) => print!("{traded:>4}"),
            }
        }
        println!();
    }
    println!("\n  .  = no bar at all     0/n = n bars, none of them traded");
    Ok(())
}

const DAYS: [&str; 7] = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"];

/// One candle, in the venue's own field names.
#[derive(serde::Deserialize)]
struct Bar {
    /// Bar open time, milliseconds.
    t: i64,
    /// Trades in the bar. **This is the field that answers the question** —
    /// volume can be zero in an open market, but a venue that is closed prints
    /// no trades at all.
    n: u64,
}

fn column<'a, T: 'static>(batch: &'a arrow::record_batch::RecordBatch, name: &str) -> &'a T {
    batch
        .column_by_name(name)
        .unwrap_or_else(|| panic!("no column {name}"))
        .as_any()
        .downcast_ref::<T>()
        .unwrap_or_else(|| panic!("{name} is not the expected type"))
}

fn collect(dir: &Path, out: &mut Vec<PathBuf>) -> std::io::Result<()> {
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
