# galata-wire

**The shared vocabulary of Galata: how an instrument is named, what a dataset
is, and what a normalised market-data event carries.**

Every Galata component speaks these types: capture, the tape, the broker, the
operator UI, and the research and trading layers to come. The crate links
`serde`, `serde_json`, `rust_decimal` and `thiserror`, and nothing else. A
process that only needs to *name* an event should not have to pull in a
columnar format to do it.

```toml
galata-wire = "0.1"
```

## Example

```rust
use galata_wire::{Envelope, Event, Quote, Ticker, Venue};

let envelope = Envelope::new(
    Venue::new("hyperliquid")?,
    Ticker::new("BTC")?,
    Some(1_758_326_400_000_000),   // venue time, when the venue states one
    1_758_326_400_012_000,          // our receipt time
    Event::Quote(Quote::default()),
);
# Ok::<(), galata_wire::TokenError>(())
```

## What is in the crate

| Type | Purpose |
|---|---|
| `Venue`, `Ticker` | validated identifiers; a character that would break a path or a subject is refused at construction |
| `Kind`, `Series` | datasets (`quotes`, `trades`, `candles`, `funding`, `marks`, `gaps`, …) and the series a venue declares |
| `Envelope` | one normalised event: venue, ticker, optional venue time, receipt time |
| `Event` | market events (`Trade`, `Quote`, `Book`, `Candle`, `Funding`, `Mark`), on-chain events (`Transfer`, `Mint`, `Reorg`), and the record's own facts (`Gap`, `Unparsed`, `Session`, `Instrument`) |
| `Num` | a decimal amount |

## Design rules

- **No floats for money.** `Num` is a `rust_decimal` that serialises as a
  string, so an amount round-trips exactly through JSON, the broker and a
  browser. No type that crosses a contract holds an `f64`.
- **Venue time is optional.** Many venue messages carry no timestamp. The
  envelope leaves that field empty rather than substituting the receipt time,
  which would turn missing information into a latency of zero.
- **Gaps are events.** An absence is a `Gap` with a cause and bounds, never
  something inferred from missing rows.

## Part of Galata

galata-wire is one of four crates in [galata-datawatch], the data layer of
Galata, a low-latency algorithmic trading framework in Rust. See the
[project README][galata-datawatch] for the architecture and roadmap.

Licensed under MIT.

[galata-datawatch]: https://github.com/sercanatalik/galata-datawatch
