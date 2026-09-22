//! Read the tape through the bounded view.
//!
//! ```text
//! cargo run --example view -- <tape-root> <kind> <from-date> <to-date>
//! ```
use galata_datawatch::calendar::midnight_of;
use galata_datawatch::tape::{Reader, Window};
use galata_wire::Kind;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let root = args.next().ok_or("usage: view <root> <kind> <from> <to>")?;
    let kind: Kind = args.next().ok_or("need a kind")?.parse()?;
    let from = midnight_of(&args.next().ok_or("need a from-date")?).ok_or("bad from-date")?;
    let to = midnight_of(&args.next().ok_or("need a to-date")?).ok_or("bad to-date")?;

    // Every dataset the tape holds is a scope, so the bound is the MINIMUM
    // across them — a view complete for one and holed for another is the thing
    // this refuses to produce.
    let scopes: Vec<String> = std::fs::read_dir(&root)?
        .filter_map(|e| e.ok())
        .filter_map(|e| e.file_name().to_str().map(str::to_string))
        .filter(|n| n.starts_with("kind="))
        .collect();
    let borrowed: Vec<&str> = scopes.iter().map(String::as_str).collect();
    println!("scopes: {}", borrowed.join(", "));

    let reader = Reader::open(&root, &borrowed)?;
    println!("bound: stream_seq <= {}", reader.bound().position);

    let batches = reader.view(Window {
        kind,
        from_micros: from,
        to_micros: to,
        ticker: None,
    })?;
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    println!("{kind}: {rows} rows in {} batches", batches.len());
    Ok(())
}
