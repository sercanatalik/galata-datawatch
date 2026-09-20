# galata-wire

The shared vocabulary of [galata-datawatch]: how an instrument is named, what a
dataset is, and what a normalised market-data event carries.

Links `serde`, `serde_json`, `rust_decimal` and `thiserror` — and nothing else.
A process that only needs to *name* an event must not inherit a columnar format
to do it.

```rust
use galata_wire::{Envelope, Event, Quote, Ticker, Venue};

let envelope = Envelope::new(
    Venue::new("hyperliquid")?,
    Ticker::new("BTC")?,
    Some(1_758_326_400_000_000),   // venue time
    1_758_326_400_012_000,          // our time
    Event::Quote(Quote::default()),
);
# Ok::<(), galata_wire::TokenError>(())
```

Licensed under MIT.

[galata-datawatch]: https://github.com/sercanatalik/galata-datawatch
