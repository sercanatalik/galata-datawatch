# galata-datawatch — roadmap

*What gets built, in what order, and why that order. Written 2026-09-20 from an
exploration of `legacy/galata-legacy`'s datawatch slice, galata-vault,
galata-tower and cereyan.*

The ordering rule is **not** dependency alone. It is: *the things whose wrong
answer is unrecoverable come first.* Every byte not captured is gone forever,
and every gap row written before clipping exists is a row whose `clipped`
column lies. Those constraints, not the dependency graph, put sessions at
Tier 2 and the broker at Tier 4.

---

## What is decided

| | |
|---|---|
| workspace | `galata-datawatch/`, four published crates, lockstep version |
| licence | MIT |
| venues | `hyperliquid` · `rh-chain` · `rh-crypto`, as cargo features |
| instruments | BTC · ETH · HYPE (main dex) · WTIOIL · XYZ100 · GOLD (`xyz` HIP-3 dex) |
| capture | `bbo` only — no `l2Book` — plus trades, candles, `activeAssetCtx` |
| ticker | the venue's own symbol; the dex prefix is composed, never stored |
| config | galata-vault authoritative, file source for tests and library users |
| broker | NATS stays, in its own crate, so a consumer links no parquet |
| tape schema | a **public API** under semver — safe because it is rebuildable |
| scheduling | cereyan, by subprocess, with an exit-code taxonomy |
| UI | galata-tower: axum server and React in one repo |

## What is still open

- ~~**Tier 7's entry question.**~~ **Confirmed**, and the last piece landed
  2026-09-21: capture at the head, the reader bounded at finalized, reorgs as
  rows — *and* `crate::reorg` to apply them. Capturing at the head means the
  record holds rows the chain later replaces; until something said **which**, a
  reader had the contradiction and no way to use it.
- ~~**`bbo` volume.**~~ **Measured, and the worry was right**: both channels on
  one socket for 75 s put `bbo` at **4.0× the bytes** and 43× the messages of
  `l2Book`. Event-driven is the cost, not the saving. It is still the right
  choice, for the other reason — 43× as many distinct tops, and *the top of
  book as it moved* is what the dataset is. 5.5 MiB/day/ticker compressed,
  against the predecessor's 44 MB/day/ticker for candles.
- **Retention — a decision, not a question.** Absence of a `[retention]` block
  means keep everything. Nothing measurable resolves how long market data is
  worth keeping; it is the operator's, which is why no horizon has a default,
  `galata-retain` exits 3 with no block, and `--delete` is explicit. The only
  thing that changes it is a storage bill.

---

## The crates

```
  galata-wire        vocabulary                     serde only
  galata-broker      Publisher/Subscriber + NATS    wire + async-nats
  galata-segments    durable parquet segments       parquet + arrow
  galata-datawatch   record · seam · loop · tape    all three
```

A downstream algo project takes `galata-wire` and `galata-broker` and links no
parquet — which is the wall legacy's `algo-fast` reached for and did not get,
because 18 crates depend on `galata-ingest` just to reach the bus.

---

## Tier 0 — the spine

*Pure, offline, fully testable without a network. Nothing else compiles without
it.*

**`galata-wire`** — trimmed from legacy's 4,999 lines to roughly 1,200 by
leaving behind `trading.rs`, `statistics.rs` and the acting path.

- `Envelope` · `Event` · `Address` · `Origin` · `Kind` · `Series` · `Ticker` ·
  `Venue` · `Market` · `Num`/`Token` · `SCHEMA_VERSION`
- Token validation: `a-zA-Z0-9_-`, max 64. A subject is constructed, never
  interpolated.
- **New vocabulary:** `Series::{Quotes, Transfers, Mints}`,
  `Kind::{Quotes, Transfers, Mints, Reorgs}`,
  `GapCause::{PollFailed, Throttled, Reorg}`.

**`galata-segments`** — legacy's 1,165 lines plus the cursor generalisation.

- `SegmentWriter`: buffer → write → `sync_all` → rename → sync the directory.
  **Rename is the commit.**
- Row group size **16,384** — the measured knee. Statistics on `recv_micros`
  only; no page index, no embedded arrow schema.
- **The cursor generalises** from `i64` micros to
  `Cursor::{Time{micros}, Block{number}, Seq{stream}}`. The archive names a
  segment by its source's cursor; the tape by stream sequence.
- `list_segments` · `partitions` · `frontier` · `last_committed` ·
  `read_segment_range` (row-group pruning from footer statistics) ·
  `compact_partition` with an advisory `flock` hold · `overdue_closed`.

**Guards from day one:** `check-workspace-deps.sh`, `check-release-hygiene.sh`,
and `test-guards.sh` proving each guard can fail.

> **Exit:** `cargo test` green, no network touched, segments round-trip under
> every cursor variant.

---

## Tier 1 — the record, one venue, no broker, no tape

*The first thing that can be run against a live venue and soaked.*

- **`record/`** — `Payload` · `Archive` · `Failure` · `PayloadAddress` and the
  eight-column schema. Durability follows `Origin`, never a caller's choice.
  `.clean-shutdown` distinguishes a kill from a stop.
- **`venue/`** — the `Adapter` trait (pure: `normalise` · `classify` ·
  `declaration` · `channel_of`), `Declaration` · `Paging` · `Budget` ·
  `ConnectionPolicy` · `Symbols`.
- **`source/`** — the trait, and `Source::Stream` only. **This is the split
  legacy never made**: `capture/src/run.rs:645` calls
  `tokio_tungstenite::connect_async` directly, and the seam abstracts framing
  but not transport.
- **`ingest.rs`** — the one path: archive → normalise → emit, with
  `catch_unwind` around the adapter and a `NullSink`.
- **`capture/`** — the loop that owns the clock: `clock` · `coverage` ·
  `subscriptions` · `session` · `walk` · `status`.
- **`adapters/hyperliquid`** — BTC, ETH, HYPE. `bbo`, `trades`, `candle`,
  `activeAssetCtx`. `RotateAhead { observed 624s, rotate 480s, ping 20s }`.
- **`config/`** — file source only, `Origin::File`. Trimmed hard from legacy's
  6,679 lines: no `[algo]`, `[account]`, `[producer]`, `[allocator]`.
- **bin `galata-datawatch <venue>`**, and `measure` carried from legacy.

**Guards:** `check-ingest-callers.sh` (no sixth caller reaches past the one
path), `check-clock-discipline.sh` (nothing below capture reads a clock),
`check-venue-boundary.sh` (one module names a venue).

> **Exit:** a **4-hour soak**, three instruments. Answers the open `bbo` volume
> question and produces the per-ticker MB/day table this project sizes disk
> from. Bytes on disk; no broker, no vault, no tape.

---

## Tier 2 — the HIP-3 dex, and the calendar that turned out not to exist

*Done, and partly disproved. Kept in full because the disproof is the useful
part.*

- **HIP-3 dex support.** `Config` gained a per-instrument `dex`; `Symbols` maps
  `"xyz:XYZ100" → "XYZ100"` and the `:` never reaches a `Ticker`. Legacy's
  `symbols.everywhere(instrument, instrument)` self-mapping would have failed at
  `Ticker::new`, and does not exist here.
- **A duplicate ticker across dexes is refused at load**, naming both. HIP-3 is
  permissionless, so two dexes may each list `GOLD`, and within one venue
  `ticker` is a column, not a partition.
- **The universe check**, which was not planned and turned out to matter more
  than anything else in this tier: an unlisted coin is answered by a **hang-up**
  rather than a refusal, taking every other subscription on the socket with it.

### What was planned here and is NOT being built

This tier assumed `WTIOIL, XYZ100, GOLD` ran Sun 18:00 ET → Fri 17:00 ET and
planned `[hours.*]`, a `sessions` dataset and gap clipping against it.

**The record disproves the premise.** All three trade every hour of the weekend;
see `design/measured.md`. There is no calendar to clip against, every shipped
instrument is continuous, and `Clipped::Continuous` is already what they are
configured with.

Building the machinery anyway would mean a component with no caller and no test
that was not invented for it — and the *wrong* calendar would have been actively
harmful, clipping a real Saturday outage to a zero-length gap marked tight.

What is kept is the rule and the tool: **a calendar is measured before it is
declared**, and `examples/when-open.rs` is the measurement. The predecessor's
`design/datawatch/trading-hours.md` remains the design to build from when an
instrument with real hours arrives — most likely at Tier 7, where a venue serves
its hours from an endpoint rather than having them declared, which is a
different shape from the one planned here.

> **Exit:** six instruments on one socket, across two dexes, with the calendar
> question **answered from the record rather than assumed**. The thesis test —
> *a stack that assumes 24/7 for one crypto perp will guess catastrophically for
> a hundred instruments across five calendars* — survives intact and moves to
> the venue that first has a calendar. What this tier actually proved is the
> half nobody writes down: **guessing that a market is closed is the more
> dangerous guess.**

---

## Tier 3 — the tape, and the maintenance jobs

*The product. Done, except the watcher.*

- **`tape/`** — `kind=/date=`, rows sorted `(venue, ticker, at_micros)`, named by
  stream-sequence range. **Ticker is never a directory.**
- **`venue` is a column and NOT a partition level.** The roadmap said "a column,
  not *only* a partition level"; measured on DuckDB 1.5.5, a value carried both
  ways has a value that **depends on a reader flag**. One fact, one place.
- **The `quotes` dataset** — a pushed `bbo` and a polled best-bid-ask in one
  shape. Verified: cross-venue BTC is one predicate on one table.
- **`rebuild/`** — archive → **the one path** → tape. Determinism proved
  byte-for-byte on a frozen copy of a real archive.
- **`reader/`** — `view()` takes **no bound argument**. The bound is the minimum
  durable frontier across scopes, and on the first real tape it came out ten
  sequences short of the maximum, which is the invariant earning its keep.
- **`retain/`** — no horizon has a default; dry-run by default; `--delete`
  explicit; **unknown means untouched**.
- **bins** `galata-compact` · `galata-tape-rebuild` · `galata-retain`, with the
  **exit-code taxonomy legacy owed and never paid**: `0` done · `1` broken ·
  `2` bad argument · `3` held / nothing to do. Verified, all three.
- `check_layout` — a ticker directory, a **venue** directory, an unknown
  dataset, a date the calendar refuses, overlapping ranges.

### Still open in this tier

- ~~**`galata-watch`**~~ — **built**, in Tier 9 where it belongs: it watches the
  record rather than the scheduler.
- ~~**A `--replace` flag for the rebuild.**~~ **Done**, and it was not a
  convenience: Tier 9 runs the rebuild from a scheduler, every scheduler
  retries, and a retry after the archive has grown wrote both copies.
- ~~**`stream_seq` is not unique across restarts.**~~ **Fixed**: the loop seeds
  the archive from the clock it already reads, because
  `check-clock-discipline.sh` forbids the archive reading one itself. Verified
  on two real restarts — 2,337 payloads, 2,337 distinct sequences, zero
  collisions.

> **Exit, met:** `SELECT * FROM read_parquet('tape/kind=quotes/**/*.parquet')`
> in DuckDB returns six instruments with their venues **and needs no flags**,
> and `galata-tape-rebuild` run twice over a frozen archive writes identical
> segment names and identical bytes.

---

## Tier 4 — the broker

*Done.*

- **`galata-broker`** — `Publisher`/`Subscriber`, `NatsPublisher`,
  `NatsSubscriber`, `BrokerIdentity`, `Subject`, `encode`. Depends on
  `galata-wire` and **nothing else of this workspace**; measured at zero
  parquet and zero arrow crates, and held by `check-workspace-deps.sh`.
- **The boot asymmetry, verified against a real server.** Absent → warn and run
  on a `NullSink`. Identity refused → exit non-zero, naming the identity and the
  variable, never the secret.
- **`NatsSink`** bridges the **synchronous** `Sink::emit` to the async client
  across a bounded channel. `try_send`, never `send`: a full channel drops and
  counts rather than blocking the thread that is archiving. Drops appear on the
  status surface as `sink_dropped`.
- `Subject` lives in the broker rather than the vocabulary — a subject is a bus
  concept, and the record and the tape have none.

### Still open in this tier

- ~~**`status.<venue>` is not published yet.**~~ **Done**, and verified with a
  second process holding `status.>` against a real server. The file is still
  written **first**, because the surface that reports a broker outage must not
  be a publish.
- ~~**Grants.**~~ **Done**, and the predecessor's inverted-table bug was
  reproduced rather than taken on trust: `allow: []` means *allow everything*,
  and review, unit tests and `nats-server -t` all still miss it.
  `check-grant-coverage.sh` refuses a root granted to nobody.

> **Exit, met:** a second process subscribed `markets.hyperliquid.BTC.quotes`,
> received live envelopes carrying their archive sequence, and links no parquet.

---

## Tier 5 — the vault

*The seam is built. **The release-ordering blocker was a conflation and is
gone.***

This tier used to read *"blocked on galata-vault 0.1.0 reaching crates.io"*,
and then *"still unpublished"*. **Published 2026-09-21**: `cargo search
galata-vault` returns all ten crates at 0.1.0. Nothing in this tier is blocked
any more.

`Config::load_from_str` takes the text and its provenance rather than a path,
and `Origin::Document { name, version }` has existed since Tier 0. So a
vault-backed loader is three lines wherever the vault client already is:

```rust
let (text, version) = vault.get("datawatch").await?;
Config::load_from_str(&text, Origin::Document { name, version }, &Resolver)
```

**This crate takes no vault dependency to be vault-backed**, and therefore
publishes without one. The decision made three tiers ago to name the second
source before it existed turned out to buy more than tidy refusals.

### Done

- **`ConfigSource` / `SecretSource`**, with `FileSource` and `EnvSecrets`. A
  vault implementation of either needs nothing from here but the trait.
- **`Secret`** — no `Display`, and a `Debug` that withholds. A secret reaches a
  log through the most ordinary line somebody writes.
- **Both `GALATA_CONFIG` and `GALATA_CONFIG_DOCUMENT` set is refused**, naming
  both. The rule is a pure function, so it is tested — this workspace forbids
  `unsafe` and setting an environment variable is `unsafe` in edition 2024, so
  a rule that read the environment for itself could not have been.
- **`check-secret-reach.sh`** — a secret is read in one module, and no vault
  authentication variable is named anywhere. `GV_TOKEN` and `GV_TOKEN_FILE` are
  the vault's rule, stated once, in the vault.

### Still open

- ~~**A vault-backed binary**, which needs the vault published.~~ **Done**, and
  it is `galata-datawatch-vault` — a fifth workspace member that does not
  publish, so the four that do still link none of it. Measured: the vault costs
  **104** crates on this tree, against the predecessor's carried 225, which was
  its tree taken alone. The wall is `check-vault-reach.sh`, watched failing.
- ~~**A secret the vault holds.**~~ **Done.** The document came from the vault
  and the broker password came from the process environment, because `boot`
  named `EnvSecrets` in its own body. It now takes a `&dyn SecretSource`, and
  `VaultSecrets` is one. Measured against a real `gv-server local`: a `read`
  token serves it, and a `config` token is refused with the vault's own
  sentence — *"this credential can list names but cannot decrypt secrets"* —
  which is the scope working by cryptography rather than by a check.
- **Per-`(binary, venue)` capability** and child vaults per venue credential —
  both are shapes of the vault's own token model, and belong with it. Now
  **reachable and deliberately not chosen**: one `read` token, two tokens, and
  a child vault per binary are all askable, and which is right depends on what
  else is in an operator's vault.

> **Exit:** `hyperliquid` still builds with
> `--no-default-features --features hyperliquid` — no vault, no broker — and a
> configuration from a document refuses exactly as one from a file does.

---

## Tier 6 — galata-tower

*One repo, both halves. The cereyan pattern, already proven in this tree.*

- **server** — axum over `galata-segments` (partitions, frontier,
  `overdue_closed`, gaps, failures), `galata-broker` (status and market
  streams), and `galata-datawatch` with `default-features = false` for the tape
  schemas. **Never the capture loop.**
  - **Done here already**: `capture` is a feature, default on, and a tree built
    without it links **zero** transport crates — 541 down to 279.
    `check-no-transport.sh` holds it. The server can be written against the
    thin build from its first line rather than acquiring a dependency on
    something in the fat one.
- **contract** — `utoipa` → committed `openapi.snapshot.json` →
  `openapi-typescript` + `openapi-fetch`, with a `--check` mode failing CI on
  drift. This replaces legacy's hand-generated fixtures and two-sided drift
  test with one generated source of truth.
- **ui/** — the existing React app moved from the repo root. Symbol list per
  venue, record monitoring beside live status, `lightweight-charts` over the
  tape's candles.
- **`rust-embed`** with `#[folder = "$CARGO_MANIFEST_DIR/ui/dist"]` — one binary
  serves API and screen.
- ~~`check-no-float-money.sh` gains its **Rust twin**: no `f64` in any type
  that crosses the contract.~~ **Done, and it lives here** rather than in the
  tower: the types it guards are `galata-wire`'s. Three rules — no float field
  in the vocabulary, no `serde-float` on `rust_decimal`, no float arrow column
  — each watched failing on its own plant.

> **Exit:** `./galata-tower` on `:8777` shows six instruments, their ages, the
> record's segment counts per closed day, and a chart — with no node process
> running.

---

## Tier 7 — rh-chain

*Done, and it proved the seam. Verified against the live chain throughout.*

- **`Source::Cursor` over `eth_getLogs`, paging by block.** Measured: twenty
  consecutive blocks carry **four distinct timestamps**, so a time cursor names
  about nine blocks and asking for what follows skips eight of them.
- **The seam actually takes it now.** `Transport::{Stream, Cursor}` and
  `streaming() -> Option<&dyn Streaming>`. The four websocket methods left the
  general trait; rh-chain implements none of them rather than answering emptily.
- **Two frontiers.** Finalized measured at **19.6 minutes and 11,678 blocks**
  behind the head — not the ~13 minutes first written here, which is `safe`.
- **Reorgs by parent linkage**, published through the one path. The one absence
  this system can *prove*.
- **Decoding traps, all confirmed in real data**: 123 of 4,362 `Transfer` logs
  carry four topics (ERC-721), each of which would have decoded as a zero-amount
  transfer; per-contract decimals with no default; zero-address issuance.
- **`at_micros` is absent, and `block` is present.** A range fetch cannot know
  per-block times without a call per block, so the time is *recoverable* rather
  than guessed.
- **Backoff on refusal**, found by running it: 15 rate-limit refusals in 40 s
  became 4 in 60 s.

### Still open in this tier

- ~~**`ui_multiplier` (ERC-8056)** and the `instruments` dataset for the
  chain.~~ **Done.** Polled, not evented — 50,000 blocks carry no update log
  under any candidate signature — so `Adapter::reference` declares which
  symbols and how often, and the cursor loop reads them before the first block
  of a pass. NVDA's multiplier is 1.0008; a reverting contract records absent,
  never 1.0.
- ~~**A provider URL as a secret.**~~ **Done.** `rpc_url_var` names a variable
  and there is deliberately no `rpc_url` field; an `Endpoint` prints a public
  URL and withholds a held one; `without_url()` at construction closed five
  sites that would have logged a key. Measured: reqwest's `Display` carries
  the whole URL, path and query.
- ~~**Reorg rows are not yet joined to what they contradict.**~~ **Done**, and
  it needed the cursor to rewind first: without that, the replaced blocks had
  no replacement and the join had nothing to distinguish. `crate::reorg` tests
  block range **and** stream sequence, because after a rewind the same blocks
  appear twice. Derived, never a column — the archive is append-only.

> **Exit, met:** 47,311 transfers across 32,000 blocks and 36,204 transactions,
> rebuilt with 0 unparsed and **0 invented venue times**.

---

## Tier 8 — rh-crypto

*The signing and the poll semantics are done. **The live endpoint has not been
called**, and that is stated rather than implied.*

- **Ed25519 signing** over `api_key + timestamp + path + method + body`, with
  each documented trap refused **by name**: milliseconds (13 digits, not 10, and
  it fails every request), a 64-byte expanded keypair (the seed is its first
  half), a `0x30` prefix (an ASN.1 SEQUENCE, so PKCS#8). Verified against **RFC
  8032 vectors**, because an implementation checked only against its own output
  is a test that a bug and its mirror image agree.
- **Clock discipline acquires a correctness consequence.** The signature expires
  after thirty seconds, so skew is a `401` and not a latency figure — the first
  place in this system where a wrong clock stops capture rather than mislabelling
  it.
- **`GapCause::{PollFailed, Throttled}`, with exact bounds.** On a poll, our own
  action supplies the half a stream is missing, so silence *is* a gap and its
  width is the cadence. Three consecutive failures are **one** gap three
  intervals wide.
- **`best_bid_ask` normalises** to the same `quotes` shape as a pushed `bbo`.

> **Exit, met:** BTC from two venues in one `kind=quotes` query, with `NULL`
> meaning *this venue never states it* — an exchange gives sizes and no spread,
> a broker gives a spread and one quantity.

### Still open in this tier

- **The live endpoint.** No credentials were obtained and none should be. The
  response shape rests on published documentation that **disagrees with itself**
  about whether a top-level `price` exists; the decoder requires none, and the
  record is what will settle it.
- ~~**The poll loop itself**~~ — **done**: `Capture::run_poll`, with every poll
  archived (including unchanged ones), consecutive failures widening **one**
  gap, and a `429` backing off where an unreachable venue does not. Exercised
  without a network or a credential.

---

## Tier 9 — cereyan

- `py/flows/` under four invariants: the run store is **deletable** (fixed
  trailing windows, never cursors); **the schedule is the retry** (compaction
  sweeps every closed day, so a failed night is repaired by the next); the lane
  holds **no credential** beyond a `config` token; **registration is the
  exposure boundary** — `run_flow` can start anything registered, so
  `galata-retain --delete` is never a flow.
- `galata-tape-rebuild` is the one exception to the retry rule: nobody
  re-derives a range, so it runs as a backfill of **one run per date**.
- **Nothing watches the scheduler.** `galata-watch` watches the *record*: a
  heartbeat is a claim, a closed partition still holding 1,412 segments is a
  fact on disk. **Done** — and it closes the half-made decision from Tier 1,
  where the status surface refused to judge and nothing else did either.

---

## Tier 10 — publish

- `galata-wire`, `galata-segments`, `galata-broker`, `galata-datawatch` to
  crates.io, MIT, lockstep.
- `galata-segments` could publish **early**, after Tier 0 — it is standalone,
  useful to anyone, and real users would find the cursor API's rough edges
  before the version matters.
- ~~README, docs.rs examples, and the `Adapter`/`Source` traits documented as
  the out-of-tree extension point.~~ **Done**, and writing it found the README
  advertising a `rh-chain` feature that did not exist and **three feature
  combinations that did not build**. `check-feature-matrix.sh` holds nine of
  them now.

---

## Rough scale

Grounded in legacy's own line counts, `src` only, tests excluded.

```
  Tier 0   wire (trimmed) 1,200 · segments 1,400                  ~2,600
  Tier 1   record 900 · venue 750 · source 400 · ingest 700
           capture 2,900 · hyperliquid 1,800 · config 1,500       ~8,950
  Tier 2   dex 200 · hours/sessions/clipping 700                    ~900
  Tier 3   tape 1,400 · rebuild 400 · reader 950 · retain 500
           bins 500                                               ~3,750
  Tier 4   broker 1,100                                           ~1,100
  Tier 5   vault sources 600                                        ~600
  Tier 6   tower server 1,500 · UI repoint                        ~1,500
  Tier 7   rh-chain 1,800                                         ~1,800
  Tier 8   rh-crypto 900                                            ~900
  ────────────────────────────────────────────────────────────────────────
                                                                 ~22,000
```

Legacy's equivalent slice is about 27,000 lines of `src` and 8,400 of tests, so
this is the same system minus the parts that belong to a trading stack, plus
three things it never had: the transport split, sessions, and two non-WebSocket
venues.

---

## The invariants every tier inherits

1. **Archive before normalise, one path** — one function, a build check forbids
   a second caller.
2. **The record is verbatim, including failures** — the bytes stay in the main
   segment; the failure row names them by `seq`.
3. **Gaps are events, never absences** — dated from the last durable receipt,
   never inferred from silence. On-chain, *provable*.
4. **Level-triggered everywhere** — converge toward a declared set; status is a
   full snapshot on a timer.
5. **Reports, never judges** — no field says whether a number is bad.
6. **Rename is the commit**, and the filename carries the durability fact.
7. **The loop owns the clock** — and with signed requests, that is correctness,
   not only testability.
8. **The archive is a record; the tape is a cache** — which is what licenses the
   tape to collapse identical poll states while the archive keeps every one.

*Tests are named after the claim they defend. A test called `test_ingest_ok` can
survive the deletion of the invariant it was written for; one called
`a_rotation_publishes_no_gap` cannot.*
