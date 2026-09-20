# Measured

*Every figure this tree acts on, and where it came from. A number with no entry
here is a number nobody has justified.*

The rule: **a figure from someone else's benchmark is a hypothesis, not a
decision.** Carried figures are marked as such, so nothing reads as this tree's
own measurement until this tree has taken it.

---

## Carried from the predecessor — NOT measured here

`../legacy/galata-legacy`, measured against live venues over a 78-minute soak
and a 5-hour archive. Every one of these is acted on today and none has been
re-taken in this tree.

| figure | value | what it decided | where |
|---|---|---|---|
| row-group knee | **16,384 rows** | `MAX_ROW_GROUP_ROWS` | `writer.rs` |
| window decode at the library default | 10.034 MB | that the default was wrong for this workload | `writer.rs` |
| window decode at 16,384 | 0.373 MB | the knee | `writer.rs` |
| file-size cost of 16,384 | +26.5% | accepted deliberately | `writer.rs` |
| fixture the above was taken on | 1,757,937 rows · 24.9 MB · 5 hours | the scale the knee holds at | `writer.rs` |
| archive volume | 551 MB/day, 145,387 segments/day | that compaction ships with the writer | `compact.rs` |
| Hyperliquid public book cadence | 5.27 s | why `bbo` was chosen over `l2Book` | `wire.rs`, at `channel_of` |
| Hyperliquid session lifetime | ~10.4 min observed | `OBSERVED_LIFETIME_SECS` | `adapters/hyperliquid` |
| Hyperliquid candle reach | 5,000 bars per (coin, interval) | the candle `Paging` | `adapters/hyperliquid` |
| Hyperliquid funding page | 500 rows, forward from start | the funding `Paging` | `adapters/hyperliquid` |
| Hyperliquid request budget | 1,200 weight/min per IP | `Budget` | `adapters/hyperliquid` |
| per-ticker volume, Hyperliquid | ~111 MB/day (book 21, trades 45, candles 44) | disk sizing | not yet used |

**The row-group table's known weakness.** It was taken on **one** dataset and
then applied to all of them. A row count is not row-width independent: 16,384
narrow quote rows and 16,384 wide book rows are wholly different amounts of
bytes. See the open question below.

## Read elsewhere, acted on by nobody

Recorded so a later measurement has something to disagree with.

| claim | source | status |
|---|---|---|
| `sonic-rs` is 3–4× `serde_json`, 1.5–2× `simd-json` at deserialisation | its own benchmarks | **not adopted.** No capture path exists to measure against, and the normaliser already avoids a `Value` tree |
| `LZ4_RAW` decompresses markedly faster than ZSTD at a worse ratio | `arrow-rs` docs | **knob added, default unchanged.** `Codec` is declarable; both callers still say Zstd |
| ~1M rows per group is right for time-sorted range reads | general parquet guidance | **contradicted for this workload**, with reasoning, at `MAX_ROW_GROUP_ROWS` |
| a hot loop can spend a large share of its time in the panic-catching machinery (`__rust_try`, `__rust_maybe_catch_panic`, the panic-count thread-local) | a published profile of another program | **guard kept.** See the open question below |

## Measured in this tree

| figure | value | how |
|---|---|---|
| row groups selected by a 100 µs window over a 50,000-row segment | **1 of 4** | `a_narrow_range_decodes_few_groups`, `tests/durability.rs` |
| `bbo`'s frame shape | **`data.bbo` is a flat `[bid, ask]`**, not `l2Book`'s `levels: [[bids],[asks]]` | first live run, 2026-09-20 |
| an **unlisted** coin on the WS endpoint | **the venue closes the connection**, taking every other subscription with it | probed 2026-09-20 |
| `xyz:` HIP-3 subscriptions | **work on every channel** — trades, bbo, candle, activeAssetCtx | probed 2026-09-20 |
| `activeAssetCtx` on the HIP-3 dex | **yes** — carries funding and open interest for `xyz:` coins | probed 2026-09-20 |
| WTI crude's name on the `xyz` dex | **`CL`**, not `WTIOIL` | `meta` with `dex = "xyz"`, 123 assets |
| session resets over 18 s, six instruments incl. three HIP-3 | **17** | `/tmp/dw2.log` |
| session resets over 16 s, three main-dex instruments | **0**, 12/12 pairs live | `/tmp/dw3.log` |
| session resets over 25 s, **all six** instruments with `CL` for WTI | **0**, 24/24 subscriptions held, 24/24 pairs live | 2026-09-20 |

### The two things the first live run overturned

**`bbo` does not send `levels`.** Third-party sources describe it as
"functionally equivalent to `l2Book` with `nLevels: 1, strict: true`", which is
true of its *meaning* and false of its *shape*. Every frame failed to
normalise, and the record held all of them — which is what made the real shape
readable afterwards rather than guessable. This is the entry above in *answered
by reading, not by running* being overturned by running, which is why that
section exists.

**An UNLISTED coin kills the whole connection — and HIP-3 has nothing to do
with it.**

The first reading of this was wrong and is corrected here, because a
measurement recorded with the wrong cause is worse than none. The observation
was right — seventeen resets in eighteen seconds, every instrument dead — and
the conclusion drawn from it, *a HIP-3 subscription kills the connection*, was
not. Probing each form separately:

```text
  {"type":"bbo","coin":"BTC"}                       streams
  {"type":"bbo","coin":"xyz:XYZ100"}                streams
  {"type":"bbo","coin":"xyz:SP500"}                 streams
  {"type":"bbo","coin":"xyz:GOLD"}                  subscribes, quiet (closed)
  {"type":"bbo","coin":"xyz:WTIOIL"}                CONNECTION RESET
  {"type":"bbo","coin":"XYZ100","dex":"xyz"}        CONNECTION RESET
```

Every `xyz:` channel works — trades, bbo, candle and activeAssetCtx alike. The
one that fails is the one naming a coin the venue does not list, and `meta`
with `dex = "xyz"` shows why: the universe holds `xyz:CL` and `xyz:BRENTOIL`,
and no `WTIOIL`. Two published sources disagreed about WTI crude's name; the
venue's own universe settled it.

What remains true, and matters more than the name:

1. **An unknown coin is answered by a hang-up, not a refusal**, and it takes
   every other subscription on that socket with it. One wrong ticker in a
   configuration file stops all capture — and it looks like a network problem.
2. **The loop lets that happen.** `Subscribed::Refused` exists and nothing
   populates it, because this venue does not answer. A subscription that
   consistently precedes a close should be quarantined and reported refused
   rather than retried forever at the cost of everything else.
3. **A configuration should be checked against the venue's universe at boot.**
   `meta` answers it in one request. That needs the REST client, which is the
   next change — and this is now the strongest reason for it.

## The first soak — 38 minutes, six instruments, four series

Release build, Hyperliquid mainnet, 2026-09-20. BTC, ETH, HYPE on the main perp
dex; CL, XYZ100, GOLD on the `xyz` HIP-3 dex. Subscribed: trades, quotes
(`bbo`), candles (1m) and funding (`activeAssetCtx`).

| figure | value |
|---|---|
| elapsed | 38 min, to a clean stop |
| archive | **18 MB**, **4,510 segments** |
| rate | ~28 MB/hour → **~670 MB/day**, **~112 MB/day/instrument** |
| *(at 11 min)* | 5.1 MB, 1,280 segments — the rate held over the whole run |
| subscriptions held | 24 of 24, throughout |
| session failures | **0** |
| **rotations** | **at least three** — one per 8 min of the 38 |
| **gap segments written** | **0** |
| clean-shutdown marker | written on SIGINT, so the next start reports downtime rather than a crash |

### Rotate-ahead is free, and this is the measurement

A handover happened and **no gap was published, because none occurred.** The
predecessor measured the alternative: a naive reconnect left a p50 handover of
1.00 s, a maximum of 66.00 s and 0.371% of the day uncovered. Here the same
event cost nothing observable — no gap row, no failed session, no interruption
in any pair's coverage.

### `bbo` is not the volume problem it might have been

The open question was whether an event-driven top-of-book would cost far more
than the 5.27 s throttled snapshot it replaces. It does not:

```text
  kind=candles   1.2 MB   310 files
  kind=funding   1.2 MB   312 files
  kind=quotes    1.2 MB   312 files     <- bbo
  kind=trades    1.3 MB   312 files
  kind=pong      124 KB    31 files     <- 2.4%, keepalive echoes
```

All four are level. Being emitted only when the top of book changes **on a
block** bounds it, exactly as the venue's documentation implied — and unlike
the frame *shape*, that part was right.

The per-instrument rate, ~112 MB/day, is within a percent of the predecessor's
~111 MB/day/ticker on the same venue with a different series mix. Two
independent measurements agreeing is worth more than either alone.

### And one thing nobody asked

**Keepalive echoes are archived**, under `kind=pong`, at 2.4% of volume. That
is the record's rule working as written — a frame carrying no observation still
arrived, and the record records arrivals — and it is a real cost. It buys
something: a pong is evidence the connection was alive at that moment, which is
a claim about coverage rather than about the market. Left as is, recorded so
the cost is a decision rather than an accident.

## The walk — 12 requests, one cold start, 2026-09-20

A cold start into an empty record, release build, mainnet, all six instruments.
Read back off disk with `cargo run --example walked`, which counts *fetched*
payloads and the span of venue time each covers — because a walk's report says
what was **asked for**, and only the record says what arrived.

```
walk of candles at 1m: asked 7d, the venue holds 3d 11h 20m,
                       covered 3d 11h 20m in 6 requests
walk of funding, paged forward: asked 7d, no stated bound,
                       covered 7d in 6 requests
```

| series  | per instrument | rows  | span   | pages | elapsed |
|---------|----------------|-------|--------|-------|---------|
| candles | 1 request      | 5,001 | 3.47 d | 1     | ~1.4 s  |
| funding | 1 request      | 168   | 6.96 d | 1     | ~1.9 s  |

Identical for `BTC`, `ETH`, `HYPE` and for `xyz:CL`, `xyz:GOLD`, `xyz:XYZ100`
— **the HIP-3 dex serves history exactly as the main one does**, which was not
known before this run.

### The reach is one page, so the backward cap cannot bind

5,001 rows against a declared `max_rows_per_call` of 5,000 and a `max_rows` of
5,000. The two being equal is the whole shape of the backward walk here: the
venue's reach **is** one page, so a candle walk is one request per instrument
and `walk_cap` can never truncate it. The cap still exists and is still tested,
because the forward walk can hit it and because the next venue may not have
this property.

The off-by-one is the venue's, not ours: it returns the bar *containing* each
endpoint, so an inclusive range of 5,000 minutes holds 5,001 bars. Nothing
downstream cares — identity is content-derived and a duplicate bar is the same
bar.

### Funding stopped because the page was short, not because it was told to

168 hourly rows for 7 days, against a page size of 500. One page, a short one,
and the walk stopped — the condition that ends a forward walk is the page,
never a count the caller kept. A 7-day cold start will never page twice here;
the paging loop is exercised by the tests rather than by this run, which is
worth saying plainly rather than claiming the run covered it.

### What the record's clock cost

The record is dated by **receipt**, so all twelve of these pages sit under
`date=2026-09-20` whatever they cover. That is correct — the record records
arrivals — and it is exactly why `WalkInterval` distinguishes a width the
stream pushes from one it does not. At 1m the receipt clock tracks the truth
because live capture keeps it there. At 1h it would not, and an hourly walk
resuming from it would ask for one minute and report success.

## The `xyz` instruments have no calendar — measured 2026-09-20

**The roadmap was wrong, and the record settled it.** Tier 2 asserted that
WTIOIL, XYZ100 and GOLD run Sun 18:00 ET to Fri 17:00 ET. They do not. Three and
a half days of minute bars, walked from the venue and read back off disk, count
bars **carrying a trade** by hour of the week in `America/New_York`:

```
xyz:XYZ100        0   1   2   3   4   5  ...  16  17  18  19  20  21  22  23
Thu               .   .   .   .   .   .       60  60  60  60  60  60  60  60
Fri              60  60  60  60  60  60       60  59  56  51  57  56  60  54
Sat              51  52  57  55  57  60       60  60  60  60  60  60  60  60
Sun              60  60  60  60  60  60       60  54   .   .   .   .   .   .
```

Friday 17:00 ET — the claimed close — is 59 traded minutes of 60. Saturday, the
claimed dead day, never falls below 46. `xyz:CL` is 60/60 across the entire
weekend. BTC, a genuinely continuous crypto perp, reads the same, which is the
control that says the tool can read a continuous market.

### Quiet is not closed, and gold is why the distinction is in the tool

`xyz:GOLD` thins on Saturday morning — 29 traded minutes in the 06:00 hour, 31
at 04:00 — and never reaches zero for a single hour. A measure counting
**volume** would have called that a close. Counting **trades** shows it for what
it is: an open market with thin liquidity, ragged rather than contiguous. A real
close is a solid block of `0/60`, and nothing here shows one.

### Which direction the error would have run

This is the part that matters more than the constant. Everything about gap
clipping rests on an unknown schedule **overstating** a loss. A believed-but-false
calendar inverts it:

```
  believed closed, actually open  ──▶  a real Saturday outage is clipped to
                                       nothing. clipped = "sessions", a
                                       zero-length gap, and the row claims to
                                       be TIGHT.
```

The hole nobody looks for, written confidently by the component whose whole
purpose is to prevent it. So: **a calendar is measured before it is declared**,
and `cargo run --example when-open` is the measurement.

### What a calendar will need, when one is real

Not built — no instrument here has hours, so the machinery would have no caller.
Recorded so the next set does not start from an assumption:

- An **IANA zone**, never a three-letter abbreviation. `CST` is US Central,
  China and Cuba, and an abbreviation cannot express DST at all.
- A library that **exposes DST ambiguity**. `chrono` answers a civil time inside
  a spring-forward gap with `MappedLocalTime::None` and nothing further; `jiff`
  gives the RFC 5545 compatible strategy plus `tz::AmbiguousZoned`. A venue
  opening at 02:30 has a boundary that does not exist on one Sunday a year.
- A **bundled** tz database, against jiff's own default. A minimal container has
  no `/usr/share/zoneinfo`, and two processes resolving against different
  databases disagree about whether a market was open — silently, and only near a
  transition. The cost is that a rule change arrives on a release rather than a
  system update; pinned in `Cargo.lock`, that is auditable.

`jiff` is a **dev-dependency**: the library itself needs no tz conversion, and
putting a tz database in every consumer's binary for a feature nothing uses is
not a trade worth making yet.

## Answered by reading, not by running

Recorded because a design question resolved from documentation is still not a
measurement, and the distinction matters when the answer turns out to be wrong.

| question | answer | source |
|---|---|---|
| does `bbo` carry sizes? | **yes** — it is functionally `l2Book` with `nLevels: 1, strict: true` | the venue's own subscription documentation |
| how often does `bbo` fire? | **only when the top of book changes on a block** — bounded by block cadence and by change, not by every quote update | the same |
| does `activeAssetCtx` cover the HIP-3 `xyz` dex? | **unresolved.** HIP-3 assets are addressed `<dex>:<coin>` in subscriptions generally, and this one is not separately documented | — |

The last is the one that costs something if wrong: if `activeAssetCtx` is
main-dex only, WTIOIL, XYZ100 and GOLD carry no funding and no mark, and the
declaration must say so rather than the walk discovering it. **The first live
subscription answers it**, which is the soak's first job.

## Open, and waiting on a soak

- **What does `catch_unwind` cost on the one path?** Normalisation runs inside
  a panic boundary for every event of every payload, which is the hottest path
  in the system — the predecessor measured candles at 44 MB/day/ticker because
  Hyperliquid pushes one on *every* update, not once per bar. The boundary is
  kept: an adapter panic taking down capture is worse than the overhead, the
  published figure is old, and a profile of another program is a hypothesis
  here rather than a result.

  **The mitigation is recorded in advance so it is not invented under
  pressure:** wrap a *batch* of frames rather than each frame. That still costs
  only parses and never bytes, because the bytes are durable before
  normalisation runs either way — what changes is that one panic costs the
  batch's parses instead of one. Take the figure first.

- **A byte bound on row groups.** `parquet` 60 added `set_max_row_group_bytes`,
  which expresses `MAX_ROW_GROUP_ROWS`'s actual intent directly and is
  row-width independent. Left unset; when both are set the smaller limit wins,
  so enabling it later narrows groups rather than widening them.
- **Compression per store.** The record is written every few seconds and read
  rarely; the cache is written once per window and read constantly. Opposite
  trades, one knob, no measurement yet.
