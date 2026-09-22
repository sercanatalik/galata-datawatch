# galata-datawatch

[![check](https://github.com/sercanatalik/galata-datawatch/actions/workflows/check.yml/badge.svg)](https://github.com/sercanatalik/galata-datawatch/actions/workflows/check.yml)

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
`rh-crypto` (signed REST poll) ship in-tree.

**A venue can also live in your own crate.** `Adapter` carries a worked example
that compiles, and the claim itself is checked by
[`tests/out_of_tree_venue.rs`](./crates/galata-datawatch/tests/out_of_tree_venue.rs)
— cargo builds that file as its own crate, so it sees exactly what a stranger
sees. If a venue needs something private, the compiler says which thing there
rather than in somebody's repository. The venue it implements is fictional on
purpose: one resembling an in-tree venue would tempt reuse of its helpers, and
reuse is what makes a test pass for the wrong reason.

docs.rs is told `all-features = true`, so every venue appears and every gated
item carries a badge naming the feature it needs.

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

## Reading the tape

```sql
SELECT * FROM read_parquet('var/tape/kind=quotes/**/*.parquet');
```

Two things to know before filtering it.

**`at_micros` is null where the venue did not timestamp the event**, which is
not rare: measured over a 24-minute run, 100% of `marks` and 89% of `funding`
carry no venue time, against 0% of `quotes` and `trades`. Giving those rows our
receipt time would turn an absence of information into a latency of zero, so
the column is left null — and a `WHERE at_micros BETWEEN …` drops all of
`marks` without saying so. Filter on `recv_micros`, or use the bounded reader,
which keeps such a row once the partition holding it is in range.

**An execution can arrive twice.** A venue that sends recent history on
subscribe redelivers it on every reconnection — measured at 1.55% of a
24-minute run, once per session rotation. Both receipts are recorded because
both arrived; group on `trade_id` to count each execution once.

## Building

```sh
cargo test                 # no network is touched
scripts/check-all.sh       # format, lints, guards, the guard harness, tests
scripts/test-guards.sh     # proves every guard can fail
```

**CI runs `check-all.sh` and nothing else**, so the badge above and the command
above cannot disagree. Everything after the dependency fetch runs `--offline`:
the workspace is provable without a network, and an accidental network
dependency should fail rather than succeed quietly.

## Publishing

Not published yet. When it is, **the order is forced by the dependency graph**
and getting it wrong fails partway through a sequence that cannot be undone —
a crates.io version is permanent.

```text
  galata-wire        no internal dependencies   ─┐
  galata-segments    no internal dependencies   ─┴─ either order
  galata-broker      needs wire
  galata-datawatch   needs wire, segments, broker
```

`cargo package` on `galata-broker` or `galata-datawatch` **fails today**, and
correctly so — it cannot resolve a dependency that is not on the registry:

```text
  error: failed to prepare local package for uploading
  Caused by: no matching package named `galata-wire` found
```

All four are verified before any publish happens, and by the gate rather than
by remembering:

```sh
cargo package --workspace     # every crate, built from its own tarball
```

`cargo package --workspace` builds a temporary registry under `target/package`,
publishes each crate into it, and compiles every unpacked tarball against the
*packaged* versions of the rest — not against the path dependencies this
workspace supplies. That distinction is the point: inside a workspace cargo
prefers the path dependency, so a crate can compile perfectly here while using
a sibling change its own manifest does not require.

Two guards, two questions. `scripts/check-package.sh` asks what a tarball would
*contain*, using `cargo package --list`, which resolves nothing — so it still
answers for a crate that will not build. `scripts/check-tarball-builds.sh` asks
whether it *compiles*, which is the half that could not be checked at all
before `--workspace` existed. About 19s warm, 95s on a cold tree, measured.

Verification builds default features; combinations are
`scripts/check-feature-matrix.sh`'s.

Allow a moment between publishes: the registry index needs to carry a crate
before the next one can resolve it.

## Licence

MIT. See [LICENSE-MIT](./LICENSE-MIT).
