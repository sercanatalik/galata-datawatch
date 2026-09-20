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

- **Is `bbo` cheaper or more expensive than the `l2Book` it replaces?** The
  public book is a throttled snapshot every 5.27 s; `bbo` is event-driven and
  fires on every top-of-book change. Unmeasured, and the answer decides disk
  sizing. Nothing should quote a MB/day figure for the shipped configuration
  until this is taken.
- **A byte bound on row groups.** `parquet` 60 added `set_max_row_group_bytes`,
  which expresses `MAX_ROW_GROUP_ROWS`'s actual intent directly and is
  row-width independent. Left unset; when both are set the smaller limit wins,
  so enabling it later narrows groups rather than widening them.
- **Compression per store.** The record is written every few seconds and read
  rarely; the cache is written once per window and read constantly. Opposite
  trades, one knob, no measurement yet.
