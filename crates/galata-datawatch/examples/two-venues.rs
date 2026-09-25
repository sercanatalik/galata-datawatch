//! **The Tier 8 exit criterion**: BTC quotes from two venues in one query.
//!
//! Writes one `kind=quotes` partition holding quotes normalised by *two
//! different adapters* — Hyperliquid's `bbo` frames and Robinhood Crypto's
//! `best_bid_ask` response — and prints what a single `read_parquet` sees.
//!
//! The point is not that two rows exist. It is that they have **the same
//! shape**: an exchange states sizes and no spread, a broker states a spread
//! and one quantity, and `NULL` means *this venue never states it* rather than
//! *it was missing*.
//!
//! ```text
//! cargo run --example two-venues -- /tmp/twovenues
//! ```
use std::collections::BTreeMap;

use galata_datawatch::adapters::{hyperliquid, rh_crypto};
use galata_datawatch::normalise::Normalise;
use galata_datawatch::tape::{Row, Tape};
use galata_datawatch::venue::{Adapter, Construct};
use galata_wire::{Ticker, Venue};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let root = std::env::args()
        .nth(1)
        .ok_or("usage: two-venues <tape-root>")?;
    let at = 1_789_941_180_000_000i64;
    let mut tape = Tape::open(&root);

    // ---- venue one: an exchange, pushing bbo frames ----------------------
    let hl = hyperliquid::Hyperliquid::new(
        hyperliquid::Config {
            market: hyperliquid::Market::Mainnet,
            instruments: vec![hyperliquid::Instrument::main("BTC")],
            candle_interval: "1m".into(),
        },
        None,
    )?;
    let frame = br#"{"channel":"bbo","data":{"coin":"BTC","time":1789941180000,
        "bbo":[{"px":"81213.0","sz":"15.82613","n":44},{"px":"81214.0","sz":"2.38926","n":7}]}}"#;
    for envelope in hl.normalise(&hl.classify(frame, at))? {
        tape.take(Row {
            stream_seq: 1,
            source_recv_micros: envelope.recv_micros,
            envelope,
        });
    }

    // ---- venue two: a broker, answering a poll ---------------------------
    let venue = Venue::new(rh_crypto::VENUE)?;
    let tickers = BTreeMap::from([("BTC-USD".to_string(), Ticker::new("BTC")?)]);
    let body = br#"{"results":[{"symbol":"BTC-USD",
        "bid_inclusive_of_sell_spread":"81190.50","sell_spread":"22.50",
        "ask_inclusive_of_buy_spread":"81235.50","buy_spread":"22.50",
        "quantity":"0.5","timestamp":"2026-09-21T10:33:00Z"}]}"#;
    let parsed = rh_crypto::wire::response(body)?;
    for envelope in rh_crypto::wire::read(&venue, &parsed, &tickers, at) {
        tape.take(Row {
            stream_seq: 2,
            source_recv_micros: envelope.recv_micros,
            envelope,
        });
    }

    println!("wrote {:?}", tape.commit()?);
    Ok(())
}
