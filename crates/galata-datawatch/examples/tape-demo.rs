//! Write a tape and let DuckDB read it, as the exit criterion states it.
use galata_datawatch::tape::{Row, Tape};
use galata_wire::{Envelope, Event, Num, Quote, Ticker, Venue};
use std::str::FromStr;

fn main() {
    let root = std::env::args().nth(1).unwrap();
    let mut tape = Tape::open(&root);
    let day = 1_789_941_180_000_000i64;
    let mut seq = 0u64;
    for (venue, ticker, px) in [
        ("hyperliquid", "BTC", "81213.0"),
        ("hyperliquid", "ETH", "3100.5"),
        ("hyperliquid", "XYZ100", "6712.25"),
        ("rh-crypto", "BTC", "81250.0"),
        ("rh-chain", "BTC", "81240.0"),
        ("hyperliquid", "GOLD", "4101.5"),
    ] {
        seq += 1;
        tape.take(Row {
            stream_seq: seq,
            source_recv_micros: day + seq as i64 + 500,
            envelope: Envelope::new(
                Venue::new(venue).unwrap(),
                Ticker::new(ticker).unwrap(),
                Some(day + seq as i64),
                day + seq as i64 + 500,
                Event::Quote(Quote {
                    bid_px: Some(Num::from_str(px).unwrap()),
                    ask_px: Some(Num::from_str(px).unwrap() + Num::from_str("0.5").unwrap()),
                    bid_sz: Some(Num::from_str("1.25").unwrap()),
                    ask_sz: None,
                    bid_spread: None,
                    ask_spread: None,
                }),
            ),
        });
    }
    println!("wrote {:?}", tape.commit().unwrap());
}
