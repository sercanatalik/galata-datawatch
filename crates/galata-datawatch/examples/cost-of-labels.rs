//! What a per-venue bound costs as the tape grows.
//!
//! `Bound::of` reads every segment's `galata.venue` label from its footer, so
//! its cost is one footer per segment — and galata-tower computes it once a
//! second for every served kind, against a tape that retention keeps forever.
//! This builds tapes of increasing segment count and times the bound on each,
//! cold and through a warm `LabelCache`, so both figures are measurements.
//!
//! ```text
//!   cargo run --release --example cost-of-labels
//! ```

use std::str::FromStr;
use std::time::Instant;

use galata_datawatch::tape::{Bound, LabelCache, Row, Tape};
use galata_wire::{Envelope, Event, Num, Quote, Ticker, Venue};

fn quote(seq: u64, venue: &str) -> Row {
    Row {
        stream_seq: seq,
        source_recv_micros: 86_400_000_000 + seq as i64,
        envelope: Envelope::new(
            Venue::new(venue).unwrap(),
            Ticker::new("BTC").unwrap(),
            Some(86_400_000_000 + seq as i64),
            86_400_000_000 + seq as i64,
            Event::Quote(Quote {
                bid_px: Some(Num::from_str("1").unwrap()),
                ask_px: None,
                bid_sz: None,
                ask_sz: None,
                bid_spread: None,
                ask_spread: None,
            }),
        ),
    }
}

fn main() {
    for segments in [10u64, 100, 1_000] {
        let dir = tempfile::tempdir().unwrap();
        let mut tape = Tape::open(dir.path());
        // One commit per segment, two venues alternating: the shape a tape
        // takes when two venues project a day each, over many days.
        for n in 0..segments {
            let venue = if n % 2 == 0 { "venue-a" } else { "venue-b" };
            tape.take(quote(n * 10, venue));
            tape.commit().unwrap();
        }
        let runs = 20;
        let started = Instant::now();
        for _ in 0..runs {
            Bound::of(dir.path(), &["kind=quotes"]).unwrap();
        }
        let cold = started.elapsed() / runs;

        // What galata-tower's watch pays after its first second: every label
        // already read, and a `stat` per segment to prove none changed.
        let labels = LabelCache::default();
        Bound::of_cached(dir.path(), &["kind=quotes"], &labels).unwrap();
        let started = Instant::now();
        for _ in 0..runs {
            Bound::of_cached(dir.path(), &["kind=quotes"], &labels).unwrap();
        }
        let warm = started.elapsed() / runs;
        println!("{segments:>5} segments: cold {cold:?}, warm {warm:?}");
    }
}
