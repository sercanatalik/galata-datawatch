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
| ledger | account state, in this workspace: perp positions and margin, fills, funding, liquidations and every transfer that touches perp margin. Hyperliquid first. Many venues, many accounts |
| accounts | declared by alias in the vault document; the address is a vault variable; Hyperliquid sub-accounts are **discovered** under a declared master |
| ledger scope | spot balances and vault equity are **out**. A transfer into either is still recorded, as an event that leaves perp margin |

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
- **A unified account's collateral is spot USDC** (Tier 12). Legacy measured it
  on 2026-09-03 (`legacy/galata-legacy/design/measured.md`, *the account skew,
  decomposed*): on a unified account, perps `accountValue` is margin held plus
  unrealised, **0 while flat**, and the realised cash sits in the spot USDC
  balance. Spot is out of scope, so a snapshot of a unified account shows its
  perp margin and not its equity. **Decided 2026-09-25:** a unified account's
  equity is recorded as *not held*, with the reason, and the fold's transfers
  (Tier 13) account for the cash. No spot figure is read.
- **Whether each HIP-3 dex keeps its own margin** (Tier 12). If
  `clearinghouseState` answers per `dex`, a snapshot is per (account, dex) and
  a move between dexes is a transfer. One live call on the `xyz` dex settles
  it; the design assumes per-dex until then, because the other assumption
  would sum two margins into one.

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
  **And the walk has a live form** (2026-09-25, `fill-mid-run-gaps`): the
  boot walk resumes from the latest receipt and never looks behind it, so a
  session lost mid-run left candles and funding the venue would hand back
  missing. Capture now queues each gap it publishes while running on a
  historical series and asks for it once the bar of the loss has closed, as
  its own task, one at a time at the walk's pace, taken between frames so the
  socket is never waited on or cancelled. The record held 216 gaps that day,
  every one a restart gap: nothing had needed this yet.
  **And it keeps the widths the venue lets go** (2026-09-25,
  `walk-the-coarse-candles`): the venue serves ~5,000 bars per width on a
  rolling window, and only `1m` was kept, so `1h` and `4h` history was
  leaving uncaptured a day at a time. `walk_candles = ["1h", "4h", "1d"]` asks
  each for its whole reach on every boot, and `--import` took the rescue saved
  that morning into the record through the one path.
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
  **And it took rows it had not rebuilt** (2026-09-25, `replace-by-source`):
  the tape dates rows by the venue's time and a boot's walk receives old
  history today, so a partition holds several receipt days and replacing one
  removed the others'. Measured on a copy of the real record — no order of
  per-day rebuilds converged. Replacement is now by venue **and** receipt day,
  each stated in the segment's footer (`design/measured.md`).
- ~~**`stream_seq` is not unique across restarts.**~~ **Fixed**: the loop seeds
  the archive from the clock it already reads, because
  `check-clock-discipline.sh` forbids the archive reading one itself. Verified
  on two real restarts — 2,337 payloads, 2,337 distinct sequences, zero
  collisions.

- **`bound-the-replay`**: **NOT PROPOSED.** Named 2026-09-25. `view()` is
  bounded at the durable frontier, and nothing can say *the view as it stood at
  T*, which a replay host needs. Carries forward legacy's `reader-replay` as a
  separate crate, with the position mapped from receipt time through the
  archive. It lands only with its first caller, `galata-research`. Written up
  in [`planning/bound-the-replay.md`](../planning/bound-the-replay.md).

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

*Built against the published `galata-vault 0.4` SDK.*

The first release was `0.1.0` on 2026-09-21. The current release is `0.4.0`,
which consolidates the former ten packages into one feature-gated crate. The
unpublished `galata-datawatch-vault` member now resolves that registry package;
its root API required no source migration.

`Config::load_from_str` takes text and provenance rather than a path, and
`Origin::Document { name, version }` has existed since Tier 0. The vault-backed
member fetches the document once, then sends the exact text through that same
validator:

```rust
let vault = Vault::from_env()?;
let config = VaultConfig::fetch(&vault, "datawatch")?;
```

**The published crates take no vault dependency to be vault-backed.** The
dependency stops at a fifth, unpublished workspace member, and
`check-vault-reach.sh` asks Cargo rather than trusting the boundary to a
comment.

### Done

- **`ConfigSource` / `SecretSource`**, with `FileSource`, `EnvSecrets`,
  `VaultConfig`, and `VaultSecrets`.
- **`Secret`** — no `Display`, and a `Debug` that withholds. A secret reaches a
  log through the most ordinary line somebody writes.
- **Both `GALATA_CONFIG` and `GALATA_CONFIG_DOCUMENT` set is refused**, naming
  both. The rule is a pure function, so it is tested — this workspace forbids
  `unsafe` and setting an environment variable is `unsafe` in edition 2024, so
  a rule that read the environment for itself could not have been.
- **`check-secret-reach.sh`** — a secret is read in one module, and no vault
  authentication variable is named anywhere. `GV_TOKEN` and `GV_TOKEN_FILE` are
  the vault's rule, stated once, in the vault.
- **One credential at boot.** A config-only document uses `config` scope. A
  broker-backed document uses one `read` credential, preferably restricted by a
  secret allow-list. A child vault is the deployment answer when cryptographic
  isolation is required; two role-specific tokens are not a supported mode.
- **The 0.4 dependency boundary.** Under Cargo 1.98.1, the vault resolves 268
  packages alone and adds 151 to the `bin` Datawatch tree (`335 -> 478`). The
  old `221 / 104 / 310 -> 414` table is retained in `design/measured.md` as the
  0.1 measurement it was, not as the current cost.

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

**Exit, met through the API — 2026-09-24**, against the release tower (on
`:8787`, because an operator's tower already held `:8777`) and the real
record: six instruments with their ages (`/v1/instruments`: BTC, ETH, HYPE,
GOLD, XYZ100, CL, each last seen 61.4 h before — capture has not run since
2026-09-22), segment counts per closed day (`/v1/overdue`), 19,041 candle rows
behind the chart (`/v1/tape/candles`), the page served by the binary alone,
and no vite or node server running. **Not observed: the chart rendering on
screen** — no browser was reachable from the session, and a pixel is not
something the API can vouch for.

**Noticed and left alone:** the real `var/archive` has never been compacted —
`venue=hyperliquid/kind=quotes/date=2026-09-20` holds 1,101 segments —
because the Tier 9 lane is not installed on this machine. Compacting the
operator's record is the operator's step.

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

- ~~**Nothing a user runs could reach any of it.**~~ **Done, 2026-09-24
  (`poll-a-venue`)**: `rh-crypto` was absent from `known()`, `AdapterConfig`
  and `build()`, its only adapter was a test fixture, `run_poll` had no caller
  outside tests — and `boot` sent *every* non-stream transport to the cursor
  loop, so a poll venue would have been captured as a chain. Now it is
  declarable (`poll_secs` required, keys by variable name), `boot` matches the
  transport three ways, and `Capture::run_polled` asks through one venue-free
  signed `GET` (`source/poll.rs`). Proved against a local server: the
  signature **verifies** over the path *with* its query, a `429` is a
  `Throttled` gap, and a polled archive rebuilds with no credential.
  Found on the way: `boot` resolved the adapter with a hard-coded
  `&EnvSecrets`, so under the vault binary a chain provider's URL came from
  the environment rather than the vault — fixed.
- **The live endpoint.** No credentials were obtained and none should be. The
  response shape rests on published documentation that **disagrees with itself**
  about whether a top-level `price` exists — and, since, about whether the path
  is `/api/v1/` or `/api/v2/`; the decoder requires no `price`, the path is one
  constant, and the record is what will settle both.
- ~~**The poll loop itself**~~ — **done**: `Capture::run_poll`, with every poll
  archived (including unchanged ones), consecutive failures widening **one**
  gap, and a `429` backing off where an unreachable venue does not. Exercised
  without a network or a credential.

---

## Tier 9 — cereyan

*Done, 2026-09-24, and run rather than only declared* — legacy's lane shipped
saying *"what cannot be verified here is that any flow RUNS."* This one ran
each flow against a copy of the real record before it was called done.

- **`py/flows/`, five flows, each a subprocess call** to a release binary:
  compaction nightly, the tape's projection nightly, the retention *report*
  weekly, the watch hourly, and an unscheduled history rebuild. `cereyan
  check --strict` and the lane's tests run in `check-all.sh`, offline.
- **The four invariants, kept.** The run store is deletable (fixed windows,
  never cursors); the schedule is the retry; the lane holds no credential;
  registration is the exposure boundary, so `galata-retain --delete` is never
  a flow.
- **"Holds no credential" became "passes none."** A job's environment is
  built — four variables — and nothing is inherited, so a token in the
  scheduler's environment cannot reach a tool whatever it holds.
  `check-python-flows.sh` reads the lane's import graph and refuses the
  alternatives; five plants in `test-guards.sh` watch it fail.
- **Departed: the rebuild is no longer the exception to the retry rule.** It
  was, because nobody re-derives a range. With `--replace` (Tier 3) and a
  fixed three-day window, the nightly projection is idempotent — identical
  bytes, measured — so a missed night is re-projected by the next. Only
  `rebuild-one-day`, for history outside the window, retries, and **only on
  exit 1**: the exit taxonomy is read per tool, which pays the debt legacy
  recorded as owed the day retries widened.
- **The watch fails on *nothing to check*.** For the maintenance tools `3` is
  a clean *nothing to do*; for `galata-watch` it is what an empty archive
  looks like when capture has silently stopped.
- **Nothing watches the scheduler.** `galata-watch` watches the *record*: a
  heartbeat is a claim, a closed partition still holding 1,412 segments is a
  fact on disk. It closes the half-made decision from Tier 1, where the status
  surface refused to judge and nothing else did either.
- **Freshness is judged per declared venue** (2026-09-24, `stale-per-venue`).
  It was the newest segment of the whole archive — the freshest venue's — so
  one capture process could die beside another still writing and the watch
  reported clean indefinitely; reproduced before the fix. A declared venue
  that has captured nothing is reported too. Legacy's `Kind::Silence` was per
  subject, which is per venue; the move into this watcher had lost that.

### Still open in this tier

- ~~**A rebuild run by hand while the lane compacts.**~~ **Done** — the
  rebuild holds the archive shared and the tape exclusive, and deletion holds
  both. Found on the way, and worse: `--replace` removed **every venue's**
  rows of a kind and day, because the tape partitions by `kind=/date=` and
  carries venue as a column. Replacement is now per venue — by a label the
  writer states in each segment's footer, not by a statistic — and so are the
  layout check and the bounded reader's bound, which compared one venue's
  sequence against another's (`design/measured.md`).
- ~~**A venue whose endpoint is a secret could not be rebuilt by the lane.**~~
  **Done** — replay builds adapters with `AdapterConfig::for_replay`, which
  takes no secret source and withholds a keyed provider (`Endpoint::Withheld`)
  rather than reading it or falling back to the public node.
  `check-secret-reach.sh` now holds that the four tools which do not connect
  name no secret source. Proved by the nightly flow projecting hyperliquid
  and rh-chain into one tape with `GV_TOKEN` set and the provider variable
  empty in the scheduler's own environment.
- **The service itself.** The README gives the launchd shape; loading it on a
  machine is the operator's step, and the record starts being maintained the
  night it is taken.

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

## Tiers 11 to 15 — the ledger, and what is derived from it

*Written 2026-09-25, from the tower's Portfolio design (positions, risk share,
VaR, a correlation matrix), which has no positions feed to draw from.*

The ledger is account state: what an account holds and what happened to it.
It widens this workspace past market data. Two kinds of fact are involved,
and they call for opposite treatment:

```
  EVENTS  happened once, never change        STATES  true at one moment
  fills, funding paid, liquidations,         perp positions, margin
  transfers in and out of perp margin
  the venue keeps their history              the venue answers only "now"
  → walked back, like candles                → history exists only if taken
```

So **positions over time are a fold of events**, as legacy's `galata-fold`
already argued (*"never a mutable ledger that components write in turn"*), and
a snapshot is the venue's statement to check the fold against, as
`galata-reconcile` did. Legacy built both inside the trading half. Here they
sit under the record, where research, the tower and a later risk layer read
the same fold.

**The ordering follows the rule at the top of this file.** A snapshot not taken
today can never be taken. Fills can be walked back later, up to the venue's
reach. So snapshots come first, before the event walk.

### Tier 11 — accounts

*Done 2026-09-25 (`ledger-accounts-and-snapshots`), with Tier 12: running on
the operator's master since 14:39 UTC, and measured over its first hour
(`design/measured.md`). The shapes it relies on were measured on mainnet
before any code.*

- `[ledger.account.<alias>]` in the vault document: `venue`, `address_var`
  and, on Hyperliquid, `dexes`. **The record knows an account by alias; only
  the vault knows its address.** The address never appears in a path, a
  subject, a log line, a status field or a tape column.
- **An address fingerprint in every segment's footer**, beside the venue
  label. Repointing an alias at another address in the vault would otherwise
  merge two accounts' histories without a trace. A changed fingerprint under
  an alias is refused by name.
- **Hyperliquid sub-accounts are discovered** under each declared master, at
  boot and on a timer. Each is given a stable ordinal (`main.s1`) on first
  sight, bound to its fingerprint and rebuilt from the record at every boot,
  with no cursor file (the rule `capture/walk.rs` already keeps). The venue's
  sub-account name is a label, because the owner can rename it. A sub-account
  that stops appearing keeps its history and is reported as not seen since.
- **Its own root, `var/ledger`**, with its own permissions and its own
  retention. The raw answers are archived as received (invariant 2), and a
  sub-account listing carries addresses. The root is the boundary.
- **One `galata-ledger` process per venue**, serving every account on it, with
  a token that reads only that venue's variables. Market data is the P0, and
  a ledger defect must not be able to stop capture.

### Tier 12 — snapshots

*Done 2026-09-25, as above. Snapshots every 300 s on the main dex and `xyz`:
864 segments a day, 4.8 weight a minute in the steady state. A gap names its
dex, and consecutive misses nest (the poll lane's rule), found by an outage
test that never touched capture.*

- Perp positions and margin per (account, dex), polled through the existing
  poll lane: a failed poll is a gap one cadence wide, and throttling is its
  own cause.
- **The cadence is declared and its cost stated.** `clearinghouseState` weighs
  2 against 1,200 per minute **per IP**, shared with capture's walk on the
  same machine (legacy `design/datawatch/venues.md`). So the ledger takes a
  declared share of the venue's budget, as `walk_share` does, rather than a
  budget of its own.
- ~~Open: a unified account's equity, and per-dex margin.~~ Both settled
  2026-09-25: equity is recorded as not held where the collateral is spot
  (`unifiedAccount`, `portfolioMargin`), and each HIP-3 dex keeps its own
  margin, measured.

### Tier 13 — events

- Fills, funding paid, liquidations and non-funding ledger updates, walked
  back at boot as candles are. Identity comes from content (the fill's id),
  so an overlapping re-fetch is free. The venue's reach is reported, never
  covered over in silence.
- **Every transfer that touches perp margin is recorded**, including those to
  spot, to a vault and between master and sub-account. What happens on the
  other side is out of scope.
- Legacy's reconciler counted 2,944 fill rows where the venue held 1,691
  fills (`measured.md`, 2026-09-03). The fold was idempotent, so no money was
  double-counted, but the dataset recorded rows twice. This tier's dataset
  holds one row per fill, and a test says so.

### Tier 14 — the fold

- Positions, basis and realised P&L folded from events per account, from an
  anchor: the account's first transfer in, when the walk reaches it, or else
  the first snapshot this ledger took. **Which anchor was used is stated with
  every figure.**
- Checked against each snapshot. Agreement within a declared tolerance is a
  result, and so is skew; both figures are carried. Legacy's
  `galata-reconcile` is the reference: 1,300 lines, and its rules carry over
  unchanged.
- cereyan: **`fold-the-ledger`**, scheduled after the projection, over a
  fixed trailing window with `--replace`. The projection runs hourly since
  2026-09-25; the fold's own cadence is Tier 14's to decide. The lane passes
  no credential, and needs none: the fold reads the record, not the venue.

### Tier 15 — derived statistics

- A grid of returns aligned across instruments: a return spanning a `gaps`
  row is dropped, never interpolated, and candles the venue backfilled are
  flagged. Every ρ, σ and β carries its n, its window, its backfilled share
  and the tape bound it was computed at. Legacy's `galata-statistics` (1,887
  lines, polars) is the reference, including its rule that a horizon admits
  only bars whose interval divides its bucket.
- The screen computes on request from the tape first, the way the tower folds
  candles. **`derive-the-closed-days`** becomes a cereyan flow only when
  something other than the screen needs stored statistics: the fold's own
  history, or research.
- The xyz instruments were measured continuous on 2026-09-20, so no session
  mask is needed for today's universe. The grid still takes one, for the first
  instrument that closes.

The tower's Portfolio view reads Tiers 11–15 as they land. Until Tier 12 its
positions are the viewer's own inputs. The risk arithmetic (risk share, VaR,
shocks) stays in the screen: it is a model over the fold and the statistics,
not a record.

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

The ledger tiers take back three of those trading-stack parts, the read-only
ones. Legacy's references are `fold` 837, `reconcile` 1,300 and `statistics`
1,887 lines. Tiers 11–13 have no legacy equivalent to count, so no line
estimate is given for them.

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
