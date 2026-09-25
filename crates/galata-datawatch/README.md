# galata-datawatch

**Multi-venue market data capture: a record every payload lands in verbatim,
the one path each payload takes to get there, and a queryable Parquet tape
rebuilt from that record.**

```toml
galata-datawatch = { version = "0.1", features = ["rh-chain"] }
```

## The one path

```text
  transport ─▶ archive ─▶ Adapter::normalise ─▶ Envelope ─▶ sink
               durable       pure: no clock,       galata-wire   NATS, or none
               first         no I/O
```

Archive, then normalise, then emit. This is implemented once, so the order
holds by construction rather than by convention at six call sites. A payload
is durable *before* anything tries to parse it, so an adapter that meets a
shape it was not written for costs a parse, never the bytes. Gaps, parse
failures and reconnections are recorded as events, not inferred from silence.

## Features

| Feature | Default | Enables |
|---|---|---|
| `capture` | yes | the capture loop, its runtime and the venue transports |
| `hyperliquid` | yes | Hyperliquid perps over WebSocket, including HIP-3 dexes |
| `rh-crypto` | yes | the Robinhood Crypto Trading API, as a signed REST poll |
| `rh-chain` | no | Robinhood Chain, as a block cursor over `eth_getLogs`, bounded at finalized |
| `bin` | yes | the operator binaries |

With `default-features = false`, the crate is the record, the tape schema,
the bounded reader and the venue seam, with **no transport linked**. That is
how an operator UI or a research host reads the tape without being able to
reach a venue.

A venue can also live in your own crate. `Adapter` carries a worked example
that compiles, and a test builds it as a separate crate to prove that nothing
private is needed.

## Binaries

| Binary | Purpose |
|---|---|
| `galata-datawatch` | capture one venue from a configuration file |
| `galata-tape-rebuild` | project the archive into the tape |
| `galata-compact` | compact closed days |
| `galata-watch` | judge the record's freshness and completeness |
| `galata-retain` | report what a retention horizon would expire |

## Part of Galata

galata-datawatch is the data layer of Galata, a low-latency algorithmic
trading framework in Rust. Signal generation, deterministic risk controls and
agentic execution are all built on the record this crate keeps. See the
[project README][galata-datawatch] for the architecture, operations guide and
roadmap.

Licensed under MIT.

[galata-datawatch]: https://github.com/sercanatalik/galata-datawatch
