# galata-datawatch

Market data capture and parquet archival, in Rust.

One process per venue. Bytes are made durable **before** anything tries to
parse them, gaps are published as events rather than inferred from silence, and
the store answers *how far am I durable* from a directory listing without
opening a file.

> **Pre-0.1.0.** Nothing is published yet. The roadmap is
> [`design/roadmap.md`](./design/roadmap.md).

## The crates

| crate | holds | links |
|---|---|---|
| `galata-wire` | the vocabulary: `Envelope`, `Event`, `Kind`, `Series`, `Ticker`, `Num` | `serde` only |
| `galata-broker` | `Publisher`/`Subscriber` and the NATS implementation | `galata-wire` |
| `galata-segments` | durable parquet segments: write, sync, rename, compact | `arrow`, `parquet` |
| `galata-datawatch` | the record, the venue seam, the capture loop, the tape | all three |

A downstream process that only wants to *hear* about market data takes
`galata-wire` and `galata-broker` and links no columnar format:

```toml
galata-wire   = "0.1"
galata-broker = "0.1"
```

## Venues are features, not crates

```sh
cargo add galata-datawatch --features rh-chain
```

`hyperliquid` (WebSocket), `rh-chain` (block cursor over `eth_getLogs`) and
`rh-crypto` (signed REST poll) ship in-tree. `Adapter` and `Source` are public,
so a venue can also be implemented out-of-tree without forking this one.

## Two stores

```
  var/archive/                    THE RECORD — one row per payload, verbatim
   venue=hyperliquid/               before any parse was attempted
     kind=quotes/
       date=2026-09-20/
         1758326400000000-1758326460000000-4711-3.parquet
         failures/                  same seq, no payload column

  var/tape/                       THE CACHE — one row per event, rebuildable
   kind=quotes/                     from the record at any time
     venue=hyperliquid/
       date=2026-09-20/
         part-000000123456-000000234567.parquet
```

`venue` sits above `kind` in the archive and below it in the tape. The
archive's unit is the capture — a venue's bytes are retained, replayed or
dropped as a subtree. The tape's unit is the dataset, so *this dataset across
every venue* is one prefix.

## Building

```sh
cargo test                 # no network is touched
scripts/check-all.sh       # format, lints, guards, the guard harness, tests
scripts/test-guards.sh     # proves every guard can fail
```

## Licence

MIT. See [LICENSE-MIT](./LICENSE-MIT).
