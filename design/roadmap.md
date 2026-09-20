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

- **Tier 7's entry question.** Capture rh-chain at the head, or only at
  finality? Recommended: at the head, with the reader bounded at finalized and
  reorgs written as rows. Not yet confirmed.
- **`bbo` volume.** Unmeasured, and possibly larger than the `l2Book` it
  replaces: the public book is a throttled snapshot every **5.27 s**, while
  `bbo` is event-driven. Tier 1's soak answers it.
- **Retention.** Absence of a `[retention]` block means keep everything, which
  is the legacy answer and is reversible.

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

*The product. Until this exists, the archive is opaque bytes nobody can query.*

- **`tape/`** — the datasets, `kind=/venue=/date=`, rows sorted
  `(ticker, at_micros)` within a segment. **Ticker is never a directory.**
- **`venue` becomes a column**, not only a partition level. Legacy's archive
  carries it both ways and its tape does not; a tape segment read without
  `hive_partitioning=true` silently merges three venues' BTC.
- **The `quotes` dataset** — HL `bbo` and rh-crypto `best_bid_ask` share one
  shape: `bid_px` · `ask_px` · `bid_sz?` · `ask_sz?` · `bid_spread?` ·
  `ask_spread?`. Nullable means *this venue never states it*, never *it was
  missing*. Cross-venue BTC becomes one predicate on one table.
- **`rebuild/`** — archive → reader → replay → ingest → tape. Not a second
  feed: the nine venue-addressed datasets are a *projection*, which is what
  makes parity a property rather than a promise. Whole closed grid cells only;
  a window must divide 86,400.
- **`reader/`** — the bounded view. `view()` takes no bound argument; the bound
  is the minimum durable frontier across scopes.
- **`retain/`** — dry-run by default; `--delete` behind an explicit flag.
- **bins** `galata-compact` · `galata-tape-rebuild` · `galata-retain` ·
  `galata-watch`, with the **exit-code taxonomy legacy owed and never paid**:
  `0` done · `1` broken · `2` bad argument · `3` held / nothing to do.
- `check_layout` — wrong addressing, ticker-as-directory, overlapping ranges.

> **Exit:** `SELECT * FROM read_parquet('tape/kind=quotes/**/*.parquet')` in
> DuckDB returns six instruments across their venues, and
> `galata-tape-rebuild <date>` run twice writes identical segment names.

---

## Tier 4 — the broker

- **`galata-broker`** — `Publisher`/`Subscriber` traits, `NatsBroker`,
  `BrokerIdentity`, grants, `encode`. Depends on `galata-wire` and **nothing
  else**.
- Moving it here also dissolves the `grants ↔ ingest` dev-dependency cycle that
  legacy documents in `crates/ingest/Cargo.toml`.
- `ingest` emits to it; `status.<venue>` carries the full snapshot on a timer.
- **The boot asymmetry, kept verbatim:** a broker that is *absent* warns and the
  process runs on a `NullSink` — the record does not depend on the broker. A
  broker that *rejects the identity* refuses to boot, because that is a
  misconfiguration that will never fix itself, and running on would mean
  archiving everything, publishing nothing, and being unable to report it.

> **Exit:** a second process subscribes `markets.hyperliquid.BTC.quotes` and
> links no parquet.

---

## Tier 5 — the vault

*Blocked on **galata-vault 0.1.0 reaching crates.io**. `cargo publish` refuses
git dependencies.*

- `ConfigSource` / `SecretSource` traits with `File` and `Vault`
  implementations, so a breaking `0.y` of the SDK is a one-file change.
- `Origin::Vault { document, version }` in every refusal where the path appears.
  `expose()` into `load_from_str`, never `deserialize` — it would skip galata's
  own refusals.
- Both `GALATA_CONFIG` and `GALATA_CONFIG_DOCUMENT` set is refused, naming both.
  `GV_TOKEN`/`GV_TOKEN_FILE` are never read.
- **Per-`(binary, venue)` capability**, keyed off argv:
  `galata-datawatch hyperliquid` holds a `config` token only;
  `galata-datawatch rh-crypto` holds `config` + `read`.
- Child vaults per venue credential, because encryption rather than the server
  is what stops one `read` token reaching every secret.
- A source guard confining `Vault::secret`/`secrets`/`secret_version` to the
  credentials module.

> **Exit:** `gv-server local` on loopback, the fleet booting from
> `gv config get datawatch`, and `hyperliquid` still building with
> `--no-default-features --features hyperliquid` — no vault, no broker.

---

## Tier 6 — galata-tower

*One repo, both halves. The cereyan pattern, already proven in this tree.*

- **server** — axum over `galata-segments` (partitions, frontier,
  `overdue_closed`, gaps, failures), `galata-broker` (status and market
  streams), and `galata-datawatch` with `default-features = false` for the tape
  schemas. **Never the capture loop** — which needs `capture` to become a
  feature (default on).
- **contract** — `utoipa` → committed `openapi.snapshot.json` →
  `openapi-typescript` + `openapi-fetch`, with a `--check` mode failing CI on
  drift. This replaces legacy's hand-generated fixtures and two-sided drift
  test with one generated source of truth.
- **ui/** — the existing React app moved from the repo root. Symbol list per
  venue, record monitoring beside live status, `lightweight-charts` over the
  tape's candles.
- **`rust-embed`** with `#[folder = "$CARGO_MANIFEST_DIR/ui/dist"]` — one binary
  serves API and screen.
- `check-no-float-money.sh` gains its **Rust twin**: no `f64` in any type that
  crosses the contract.

> **Exit:** `./galata-tower` on `:8777` shows six instruments, their ages, the
> record's segment counts per closed day, and a chart — with no node process
> running.

---

## Tier 7 — rh-chain

*The venue that proves `Source::Cursor`, and the one with genuinely new
problems.*

- `Source::Cursor` over `eth_getLogs`, paging by **block number** — not by the
  `last_micros + 1ms` bump legacy uses, which would skip blocks on a chain whose
  block times are documented as *sub-second and irregular*.
- Archive segments named `Cursor::Block`, so backfill is idempotent by naming.
- **The payload unit is the `getLogs` response, never one log** — a trade is
  only provable by matching the stock and USDG `Transfer` inside one
  `transactionHash`, and `normalise` must stay pure.
- **Two frontiers.** The reader's bound is the **finalized** block (~13 min),
  not the head. Reorgs are rows in the gaps family: an absence you can *prove*.
- Decoding traps: **drop 4-topic logs** (ERC-721 shares topic0 with ERC-20
  `Transfer`); per-contract decimals (Stock Tokens 18, USDG 6); mint/burn is a
  `Transfer` to or from the zero address.
- `instruments` gains **`ui_multiplier`** (ERC-8056), point-in-time via
  `observed_at` — the cleanest corporate-action source available anywhere.
- A provider RPC URL is a **secret**, not config. The public node has no archive
  access and cannot backfill.

> **Exit:** a day of blocks rebuilt twice produces identical segment names, and
> a deliberately induced reorg appears as a row.

---

## Tier 8 — rh-crypto

*The cheapest possible proof of `Source::Poll`.*

- Ed25519 request signing over key + timestamp + path + method + body. The clock
  discipline acquires a correctness consequence: skew is a 401.
- Two endpoints, no history at all: `best_bid_ask` (repeated `?symbol=`, so all
  symbols in one request) and `estimated_price`.
- **Poll at 5 s**, ~12 requests/minute against an undocumented budget community
  -reported near 100/min. **Archive every poll**; collapsing identical states is
  the tape projection's job.
- Adaptive backoff on 429, because the venue states its limits *fluctuate*.
- `GapCause::{PollFailed, Throttled}` — with a poll, silence *is* a gap, and the
  bounds are exact.

> **Exit:** BTC quotes from two venues in one `kind=quotes` query.

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
  fact on disk.

---

## Tier 10 — publish

- `galata-wire`, `galata-segments`, `galata-broker`, `galata-datawatch` to
  crates.io, MIT, lockstep.
- `galata-segments` could publish **early**, after Tier 0 — it is standalone,
  useful to anyone, and real users would find the cursor API's rough edges
  before the version matters.
- README, docs.rs examples, and the `Adapter`/`Source` traits documented as the
  out-of-tree extension point — which is what makes *"add a venue later"* true
  rather than true-if-you-fork.

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
