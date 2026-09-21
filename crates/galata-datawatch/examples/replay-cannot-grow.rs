//! **Does replaying the record grow the record?**
//!
//! ```text
//! cargo run --release --example replay-cannot-grow -- <archive-root> <venue>
//! ```
//!
//! The claim is structural: replay comes back as a [`Replayed`], which only
//! `replay` can construct, and `ingest_replayed` is the one path that takes
//! one. There is no `Origin::Replay` for a caller to set wrongly, because
//! **writing what you are reading grows the thing you are reading.**
//!
//! Structural claims are the ones worth running, because the compiler's
//! agreement is not the same as the file count not moving.

use galata_datawatch::adapters::{self, AdapterConfig};
use galata_datawatch::ingest::{ingest, ingest_replayed};
use galata_datawatch::record::Archive;
use galata_datawatch::sink::testing::RecordingSink;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let root = args
        .next()
        .ok_or("usage: replay-cannot-grow <archive-root> <venue>")?;
    let venue = args.next().ok_or("need a venue")?;

    let segments = |root: &str| -> usize {
        fn walk(dir: &std::path::Path, n: &mut usize) {
            let Ok(entries) = std::fs::read_dir(dir) else {
                return;
            };
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    walk(&path, n);
                } else if path.extension().is_some_and(|e| e == "parquet") {
                    *n += 1;
                }
            }
        }
        let mut n = 0;
        walk(std::path::Path::new(root), &mut n);
        n
    };

    let before = segments(&root);
    let replayed = galata_datawatch::replay::read_all(std::path::Path::new(&root))?;
    println!("{} segments, {} payloads replayed", before, replayed.len());

    // The adapter is built for its `normalise`, which is a pure function of
    // the bytes — no network, no transport.
    let config = galata_datawatch::config::Config::load(
        &galata_datawatch::config::FileSource::from_env("config/datawatch.toml")?,
        &Resolver,
    )?;
    let declared = config.venue.get(&venue).ok_or("venue not declared")?;
    let adapter = adapters::build(AdapterConfig::from_declared(
        &venue,
        declared,
        &galata_datawatch::config::EnvSecrets,
    )?)?;

    // **The same archive it was read from.** If replay writes anything, it
    // writes here, and the count moves.
    let mut archive = Archive::open(&root).scoped_to(&venue);
    let sink = RecordingSink::default();

    let mut events = 0usize;
    for one in replayed {
        events += ingest_replayed(&mut archive, adapter.as_ref(), &sink, one)?.emitted;
    }
    archive.flush()?;

    let after = segments(&root);
    println!("{events} events emitted from the replay");
    println!("segments before {before}, after {after}");
    if after != before {
        return Err(format!("the record grew by {} segments", after - before).into());
    }
    println!("the record did not grow");

    // **The positive control.** *Nothing was written* and *writing is broken*
    // look identical from a file count, so the ordinary path is exercised on
    // the same archive object: it must move the number the replay did not.
    //
    // Through `ingest`, not `Archive::append` — which is private, and refused
    // this example at compile time. That is `check-ingest-callers.sh`'s rule
    // held by the language rather than by the guard, which is where a rule is
    // best held.
    ingest(
        &mut archive,
        adapter.as_ref(),
        &sink,
        galata_datawatch::record::Payload {
            seq: 0,
            recv_micros: 1,
            address: galata_datawatch::record::PayloadAddress::Venue(venue.clone()),
            channel: "control".into(),
            kind: "trades".into(),
            symbol: None,
            origin: galata_wire::Origin::Streamed,
            payload: b"{}".to_vec(),
        },
    )?;
    archive.flush()?;

    let controlled = segments(&root);
    println!("after one ordinary append: {controlled}");
    if controlled > after {
        println!("the write path works; the replay path does not write");
        Ok(())
    } else {
        Err("the ordinary path wrote nothing either, so this proves nothing".into())
    }
}

struct Resolver;

impl galata_datawatch::config::Adapters for Resolver {
    fn supplies(&self, venue: &str, series: galata_wire::Series) -> bool {
        adapters::supplies(venue, series)
    }
    fn known(&self, venue: &str) -> bool {
        adapters::known().contains(&venue)
    }
    fn known_names(&self) -> Vec<&'static str> {
        adapters::known()
    }
}
