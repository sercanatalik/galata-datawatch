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

## `venue` cannot be both a column and a partition level — DuckDB 1.5.5, 2026-09-21

The roadmap said to fix legacy's tape by making `venue` *"a column, not only a
partition level"*. The diagnosis was right and the remedy was not, and one tree
settled it. Two files, each under a `venue=` directory, one of whose **column
deliberately disagrees with its path**:

| read | reported venue |
|---|---|
| `hive_partitioning=true` | the **path** |
| no argument at all (the default) | the **path** |
| `hive_partitioning=false` | the **column** |

So a value written both ways answers the same query two ways depending on a
flag, and nothing warns. The published discussion of this points at a
*duplicate column* error; that is a case-sensitivity bug (`FOO` against `foo`),
and the exact-match case does not error — it silently picks one.

**So the tape writes it once, in the data.** `tape/kind=<kind>/date=<date>/`,
with `venue` and `ticker` as columns sorted `(venue, ticker, at_micros)`.
Verified against a written tape, with **no flags at all**:

```sql
SELECT venue, ticker, bid_px FROM read_parquet('tape/kind=quotes/**/*.parquet')
```
```
hyperliquid  BTC      81213.000000000000000000
hyperliquid  ETH       3100.500000000000000000
rh-chain     BTC      81240.000000000000000000
rh-crypto    BTC      81250.000000000000000000
```

Cross-venue BTC is one predicate on one table, which is what the quotes dataset
was for. Pruning moves from directories to row-group statistics on a sorted
column — the same mechanism that already keeps ticker out of the path, applied
one level up.

### The cost, paid knowingly

`rm -rf` no longer deletes one venue's tape. The tape is a cache, a rebuild
filters on the column, and **the archive** — where retention actually
happens — keeps `venue=` above `kind=` precisely so a venue's bytes are one
subtree. The two stores order their levels differently because their units
differ, which was already true before this.

### And a guarantee that was claimed and is not available

`schema_for` was written as an exhaustive match, documented as *"a dataset added
to `Kind` is a compile error here"*. It is not: `Kind` is `#[non_exhaustive]`
and the tape is a different crate, so the compiler **requires** a catch-all and
can never complain about a missing arm. The claim was false as written.

It now returns `Option`, the catch-all refuses by name, and a test over
`Kind::ALL` catches a dataset that was added and never projected. The check
moves from build time to test time, which is what `#[non_exhaustive]` costs its
consumers — worth stating rather than claiming a guarantee that is not there.

## The rebuild, against a real archive — 2026-09-21

An hour and three quarters of live capture, rebuilt from the archive through
the one path.

```
253,044 payloads → 361,928 rows in 15 segments (0 unparsed)
      50 MB archive  →  6.9 MB tape
```

**Seven times smaller**, which is what the projection is for: the archive holds
whole JSON frames verbatim, the tape holds typed columns of what they meant.

| dataset | rows |
|---|---|
| quotes | 159,062 |
| trades | 70,477 |
| candles | 54,643 |
| funding | 39,377 |
| marks | 38,369 |

More rows than payloads, because a walked candle page is one payload and 5,001
bars — the one place in this system where the ratio is not near one.

### Determinism, proved on the real archive

Two runs over a **frozen copy** wrote the same 15 segment names and
byte-identical files. Two runs over the *live* archive did not, and correctly
so: the soak added ~1,900 payloads between them, and the segment names said so
(`s-0_252926` against `s-0_254858`). A name that carries the sequence range is
what makes the difference visible rather than silent.

### Rows are dated by the venue's clock, and here is the proof

Every archive segment in this run is named `date=2026-09-20`, because the
archive dates by **receipt**. The tape it rebuilt into:

```
  kind=candles/date=2026-09-17 … 2026-09-20      4 days
  kind=funding/date=2026-09-13 … 2026-09-20      8 days
  kind=quotes /date=2026-09-20                   1 day
```

The walked history landed under the dates it is *about*. Had the tape dated by
receipt like the archive, a date predicate over last week's funding would have
found nothing — the rows would all be filed under the day they were fetched.

### The two-clock latency, which is the figure only two clocks can produce

```sql
SELECT venue, ticker, avg(recv_micros - at_micros)/1000 AS lag_ms ...
```

| ticker | quotes | avg spread | lag ms |
|---|---|---|---|
| BTC | 38,568 | 1.1342 | 320.6 |
| ETH | 40,095 | 0.1093 | 321.8 |
| HYPE | 32,875 | 0.0034 | 321.6 |
| CL | 20,437 | 0.0043 | 318.3 |
| GOLD | 8,881 | 0.1118 | 319.8 |
| XYZ100 | 18,206 | 1.1486 | 318.9 |

**~320 ms, and flat across all six instruments** — including the three on the
HIP-3 dex. A figure that does not vary by instrument is not about the
instrument: it is the venue's own publishing cadence plus the path to us, which
is exactly what a single timestamp column could never have told apart from a
slow instrument.

### One cost worth naming

Deciding there was *nothing* to rebuild for an empty date took **25 seconds**,
because listing the partitions of a 12,000-segment archive walks the tree before
any name is compared. The two prunings work on the segments; nothing yet prunes
the *directory walk* by date, and a `date=` partition is right there in the
path. Not fixed here, and the figure is recorded so the fix has a baseline.

## The date pruning, and the bound — 2026-09-21

### 25 s → 12 ms

Deciding there was nothing to rebuild for an empty date was **25 seconds** over
a 17,000-segment archive, because the partition walk read every file name before
comparing anything. Pruning `date=` directories that cannot overlap the range,
before opening them:

```
  empty date, 17,000 segments    25 s  →  0.012 s
```

Two thousand times, and the change is a conditional on a directory name. The
knowledge lives in the store that writes the layout rather than in
`galata-segments`, because a segment store that knew one caller's partitioning
scheme would be wrong for the next.

### The bound is the minimum, and here is it costing something

Opened over the five datasets of a real rebuild:

```
  scopes: kind=quotes, kind=candles, kind=funding, kind=marks, kind=trades
  bound:  stream_seq <= 285041          the rebuild wrote 285051 payloads
```

**Ten short of the greatest.** One dataset's last commit lagged the others by
ten sequences, and the bound took the lesser. A view taken at the maximum would
have claimed coverage that one dataset does not have — complete for four,
silently holed for the fifth, with an absent row and a not-yet-written row
looking identical.

That is the whole argument for the minimum, and it is not hypothetical: it cost
ten rows on the first real tape this was pointed at.

### A unit mismatch the design did not survive

This was written intending *"a window past the bound is refused, not
truncated"*. It does not survive contact with the units.

A tape segment is named by the **sequence** range it covers, so a sequence is
what the store can state about itself. A window is in **venue time**. The tape
holds no mapping between them, so the window *selects* and the bound
*restricts*, both per row — and refusing a window past the bound is not offered,
because deriving the time it would need (the greatest `at_micros` below the
bound, say) would state a completeness the tape cannot know.

A smaller thing fell out of it, worth writing down because it will surprise
somebody: **a row with no venue time is filed by our clock**, because a row must
land in some partition. The time filter keeps it — it is not *at* any time — but
the `date=` pruning reaches it only where the window also covers the day it was
received. The alternative is scanning every partition on every read in case one
holds a timeless row, which is the pruning thrown away for a case that is rare
by construction.

## Compaction, against a real archive — 2026-09-21

Three hours of one venue, six instruments, four series: **92 MB in 22,009
segments**. Compacting every *closed* partition — today's are still being
written to and are left alone:

```
  14,719 segments  →  6        in 3.8 seconds
  whole tree        22,009 → 7,296 segments
                    94 MB   →  43 MB
```

**A 54% reduction from merging alone**, before any retention. The saving is not
the data — the rows are identical — it is per-file parquet overhead: a footer, a
schema and a set of column chunk headers, 22,000 times, against a mean segment
of a few kilobytes.

This is also the answer to a question the flush cadence raises. `flush_secs = 2`
is chosen so a crash converts at most two seconds into a gap, and it costs
~7,000 files an hour per venue. Compaction is what makes that trade affordable:
the durability window stays at two seconds, and the file count is paid down
afterwards, on closed partitions, where nothing is racing.

### Retention expires nothing, and that is the shipped behaviour

Run against the real tree with no `[retention]` block:

```
  exit 3 — no retention is declared, so nothing expires
```

With a policy declared and a partition genuinely expired, the **default still
removes nothing**:

```
  1 partitions, 0.0 MB, 1 unclassified — nothing was removed. Pass --delete
```

And the unclassified subtree — a directory put there by hand that no family
claims — survived `--delete` untouched, which is the behaviour that matters:
the tool removed exactly what a declared horizon selected and nothing else.

There is no `--dry-run`. Forgetting a flag that protects is a deletion;
forgetting one that destroys is a report.

## The soak, at three hours

```
  3h 00m · 90 MB · 21,439 segments · 0 gaps · 0 session failures
```

~30 MB/hour → **~720 MB/day**, ~120 MB/day/instrument, which holds the
11-minute and 38-minute figures out to three hours and agrees with the
predecessor's ~111 to within a tenth. Rotation continued to be free: roughly
twenty handovers, none of which published a gap.

## The nine-hour soak, and the defect it found — 2026-09-21

```
  8h 52m · 259 MB · 0 session-failure-free hours but ONE reset · 0 parse failures
```

One connection reset in nearly nine hours, at 03:07:41 — `Connection reset by
peer`. The system did what it was built to do: published **24 gaps**, one per
declared pair (6 instruments × 4 series), each recorded durably *before* it was
emitted, and carried on. About twenty rotations, none of which published
anything, because none of them lost coverage.

### And then the rebuild had no gaps in it

All 24 came back as `unparsed`:

```
  venue        ticker   channel   error
  hyperliquid  BTC      gaps      unrecognised channel "gaps"
```

The bytes survived; **the meaning did not**. Two causes, both ours:

1. `record_generated` stored the event as `format!("{:?}", …)` — a *readable
   rendering*, chosen deliberately in an earlier change, and not
   round-trippable.
2. The one path handed every replayed payload to the **venue adapter**, and a
   gap is not a venue frame. The adapter was right to refuse it.

A gap that cannot be rebuilt is an absence again, which is precisely what
recording it durably was supposed to prevent. The evidence of an outage survived
the outage and did not survive the rebuild.

### Fixed, and proved on a real crash rather than a fixture

A capture was killed with `SIGKILL` mid-buffer, restarted, and the restart
reported the window it had not been covering. Rebuilt:

```
  1,047 payloads → 32,790 rows in 17 segments (0 unparsed)
```

| ticker | series | cause | clipped | seconds |
|---|---|---|---|---|
| BTC | quotes | crash_unflushed | continuous | 7.5 |
| CL | trades | crash_unflushed | continuous | 7.5 |

24 rows, 6 instruments, 4 series — dated from the **last durable receipt**, not
from when the loss was noticed, which is what keeps the interval from
understating itself by exactly the buffer that was outstanding.

### What `Origin` was doing wrong

```rust
enum Origin { Streamed, Fetched, Replay }
```

The first two say **how the bytes came to exist**. The third says **which route
this payload is taking now**. One slot answering two questions is why a payload
the system generated had nowhere to say so.

The route is a type now — `Replayed`, which only `replay` constructs and which
cannot be opened by a caller — and it is *stronger* than what it replaced: the
old guard asked somebody to set a field correctly, and this one does not
compile if you get it wrong. **It caught the rebuild on the first build**, which
had been passing replayed payloads to the archiving entry point and relying on
the field to save it.

### And a precision trap avoided on the way past

`rust_decimal`'s ordinary serde emits a float-looking value and deserialises it
**through `f64`** — precision loss, in the type chosen precisely because `f64`
loses precision. The `serde-str` feature makes every `Decimal` round-trip as a
string, globally; per-field annotations were the alternative and were rejected
because thirty fields is thirty chances to forget one, silently.

Asserted: `0.000000000000000123` is stored as that string and read back equal.

## The broker, against a real server — 2026-09-21

`galata-broker` is the fourth crate. The wall it exists for, measured rather
than asserted:

```
  cargo tree -p galata-broker  →  0 parquet crates, 0 arrow crates
```

A component that reads the stream takes the vocabulary and the broker and gets
no store with them. In the predecessor tree eighteen crates reached the bus
through the crate that also owned the archive writer, so the one crate whose
entire justification was linking no history linked `parquet` transitively.
`check-workspace-deps.sh` now holds this, and the plant proves it fails.

### The boot asymmetry, three cases against `nats-server`

| case | behaviour | exit |
|---|---|---|
| broker absent (dead port) | warns, runs on `NullSink`, capture unaffected | — |
| wrong password | refuses to boot, naming identity and variable | **1** |
| correct | publishes | — |

```
Error: Rejected { addr: "nats://127.0.0.1:4222", identity: "datawatch",
                  var: "GALATA_PW", reason: "authorization violation" }
```

The password does not appear, because `BrokerIdentity` has no derived `Debug`.

Both cases arrived as one error in the predecessor, which is why *archiving
everything and publishing nothing for a day* looked exactly like *a broker
outage for a day*.

### A second process, linking no parquet

```
listening on markets.hyperliquid.BTC.quotes
seq=1332  hyperliquid BTC  at=Some(1789976093843000) quotes
seq=1334  hyperliquid BTC  at=Some(1789976093909000) quotes
```

and a wildcard across instruments:

```
listening on markets.hyperliquid.*.trades
seq=1692  hyperliquid ETH  ...
seq=1706  hyperliquid BTC  ...
```

The `seq` is the archive row the bytes are in — the road back from any message
to what actually arrived.

### The one place this design departs from the predecessor

`Sink::emit` is **synchronous**, so `NatsSink` hands over through a bounded
channel with `try_send`. A full channel drops and counts; it never blocks the
thread that is archiving. The predecessor awaits its publish inside the one
path, so a slow broker slows capture there.

The drop count was, briefly, a number only a unit test could see. It is on the
status surface as `sink_dropped`, because **a drop that is counted and never
published is a drop nobody sees** — which is exactly the failure the counter
exists to prevent.

## The vault blocker was a conflation — 2026-09-21

Tier 5 read *"blocked on galata-vault 0.1.0 reaching crates.io"*. It is still
unpublished; `cargo search galata-vault` returns nothing. What that blocks was
never checked, and checking it took one reading of the loader.

`Config::load_from_str` takes **the text and its provenance**, not a path, and
`Origin::Document { name, version }` has existed since Tier 0 with the comment
*"named so the shape of every refusal is settled before the second source
exists"*. So a vault-backed loader is:

```rust
let (text, version) = vault.get("datawatch").await?;
Config::load_from_str(&text, Origin::Document { name, version }, &Resolver)
```

and lives wherever the vault client already is. **This crate takes no vault
dependency to be vault-backed**, so it publishes without one. The blocker
applies to a vault-*backed binary*; those two were conflated.

Verified: `--no-default-features --features hyperliquid` builds and links
**zero NATS crates** — no vault, no broker.

### Three places the tooling was right and I was not

1. **A test that defeated itself.** An in-file test asserting *"no vault token
   is named in this crate"* named `GV_TOKEN` in its own assertion list, so the
   file contained it and the test failed against itself. That check belongs in
   a guard that reads each file only as far as its first `#[cfg(test)]`.

2. **A plant that proved nothing.** The new guard's plant *appended* its
   violation — landing after `#[cfg(test)]`, in the region the check
   deliberately ignores. The planted run **passed**. This is the exact failure
   `check-ingest-callers.sh` documents in its own header, and it was walked
   into anyway. The plant now inserts before the tests.

3. **A second plant the harness caught.** Appending `galata-segments` to the
   broker's manifest landed it in `[dev-dependencies]` — which correctly does
   **not** break the wall, because a dev dependency is not propagated to a
   consumer. The rule was right to check only `[dependencies]`; the plant was
   wrong, and `test-guards.sh` said so rather than letting a guard be trusted
   on the strength of a plant that never tested it.

### And a constraint that produced a better design

This workspace forbids `unsafe`, and `std::env::set_var` is `unsafe` in edition
2024 — so a rule that reads the environment for itself is a rule **no test can
exercise**. The two-source reconciliation therefore became a pure function
taking two `Option<String>`, with the environment read as one line above it.

The part with the judgement in it is now the part that is tested, which is the
right way round and would not have been arrived at without the constraint.

## The third wall — 541 crates to 279, 2026-09-21

A consumer that wants only the tape's schemas — the tower's server is the first,
and it never captures — compiled **541 crates**. It got a runtime, a websocket
stack, an HTTP client and a TLS provider, to read parquet.

```
  galata-datawatch --no-default-features    541  →  279 crates
  transport crates in that tree                     0
  tests still passing with no runtime at all      123
```

This is the same wall the workspace already had twice, one level in:

```
  galata-wire    may not link a columnar format   (everything names events)
  galata-broker  may not link a store             (everything reads the stream)
  galata-datawatch, capture off: no transport     (everything reads the tape)
```

The line is **does this need a runtime**. The record, the tape, replay, the
calendar, configuration, the venue seam and every adapter's `wire` and
`normalise` are outside it. That the *seam* is outside is not a new decision —
its documentation has said so since Tier 1: *a fact that needs a runtime to
state is a fact that cannot be asserted in a test.* The feature is that sentence
enforced by the build.

`check-no-transport.sh` asks **cargo**, not the manifest, because a feature
nobody verifies stops gating anything the first time a module forgets its `cfg`.

### A guard fragility this exposed, and it is worth writing down

Four guards read each file only as far as its first `#[cfg(test)]`, matching
that **literal string**. Gating a test module on a feature as well —

```rust
#[cfg(all(test, feature = "hyperliquid"))]
mod tests {
```

— is legitimate, and it made the literal disappear. Three guards silently began
scanning test code and went red on fixtures that were never violations.

They failed *loudly*, which is the good case. The bad case is the mirror image:
a guard whose marker moves and which then scans **less** than it should, passing
while checking nothing. All four now match any `#[cfg(...)]` whose predicate
mentions `test`.

The lesson is narrow and real: **a guard that keys on an exact string is a guard
with a silent failure mode**, and the thing that caught this was
`test-guards.sh` refusing to plant into a tree that was already red.

## Robinhood Chain, measured against the live chain — 2026-09-21

Chain **4663**, Arbitrum Orbit, settling to Ethereum. The public endpoint
`https://rpc.mainnet.chain.robinhood.com` needs no key, keeps no archive and
offers no SLA.

### A timestamp names nine blocks

**This is the finding that decides the design.** Twenty consecutive blocks
carry **four distinct timestamps**:

```
  1789978217  →  blocks 68642966 … 68642974     nine blocks
  1789978218  →  blocks 68642975 … 68642983     nine blocks
```

Block timestamps have one-second granularity and the chain produces about nine
blocks a second. The predecessor pages history by bumping `last_micros + 1ms`;
here that asks for *everything after the second the last block was in*, and
**eight blocks in nine disappear** — silently, because the venue answers every
such request correctly.

Not a risk to be mitigated. Arithmetic. `Source::Cursor` over block numbers is
the only correct shape, and `Cursor::Block { first, last }` has been in the
segment store since Tier 0 waiting for it.

### Two frontiers, and the quoted figure is the wrong one

Read within a minute of each other:

| tag | block | behind head | behind in time |
|---|---|---|---|
| `latest` | 68,642,714 | — | — |
| `safe` | 68,634,888 | 7,826 | 13.1 min |
| `finalized` | 68,631,036 | 11,678 | **19.6 min** |

The roadmap said *"the finalized block (~13 min)"*. **13 minutes is `safe`, not
`finalized`** — and `safe` can still be reorganised under a fault, so a bound
taken there can move backwards, and a bound that can move backwards is not a
bound. `Frontier` offers `Head` and `Finalized` and deliberately does not offer
`safe`.

Capture follows the **head**, because a block later reorganised away still
*arrived*, and the record records arrivals. The reader is bounded by finality.
Two questions, two answers — the same split as *the archive is a record, the
tape is a cache*.

### The ERC-721 trap is real, and it is 3% of transfers

Over three hundred blocks:

```
  4,362 logs carry the ERC-20 Transfer topic0
    4,239  three topics   ERC-20
      123  FOUR topics    ERC-721, which shares that topic0
```

The fixture captured for the test has `"data": "0x"` — **empty**, because an
ERC-721 keeps its token id in `topics[3]`. Decoded as ERC-20 it yields an
amount of **zero** and a transfer that never happened. Nearly three percent of
transfers, every one a plausible-looking zero.

Refused by **arity**, which is a property of the log, rather than by a contract
allow-list, which is a second thing to maintain.

### And the decoder was right where I was not

The first test asserted an amount of `0.0987`, computed by eye from
`0x015fb7f9b8c38000`. The decoder said `0.099`. The decoder was right — the
value is 99,000,000,000,000,000 exactly.

Worth recording because the failure mode is the dangerous direction: a fixture
whose expected value was guessed is a fixture that tests the guess. The
assertion now carries the full raw integer as well as the scaled decimal.

## The one absence that can be proved — 2026-09-21

Every gap this system writes is **inferred from an event it witnessed**: a
session lost, a crash, a restart. None is inferred from silence, because on a
stream *nothing arrived* and *nothing happened* are the same picture. That is
invariant 3.

A chain reorganisation is the single exception in the whole design. When the
chain replaces a block, the rows captured from the old one describe something
that **provably did not happen**, and the proof is two hashes at one height.

### Detection costs nothing, because the evidence was already fetched

```text
  captured:  … ─[A]─[B]─[C]
  arriving:           [D] parentHash = C   the chain agrees
  arriving:           [D] parentHash = X   X is not the C we hold — a fork
```

Every block carries its parent's hash. The alternative — remember hashes,
re-fetch them, compare — costs a request per block to learn the same thing
later. Linkage knows at the moment the replacement arrives, which is as early as
it can be known.

**Verified against the real chain**: six consecutive blocks, real hashes, every
`parentHash` linking to its predecessor, and the detector silent throughout.
That test matters more than the positive one — a detector whose cost of being
wrong is *a false alarm on every block* has to be shown staying quiet.

### What is deliberately not claimed

- **A first sight is not a fork.** A trail with nothing to link against cannot
  tell one from the other, and claiming one would be an inference dressed as a
  proof — in the one module whose entire value is that it does not infer.
- **The depth is not guessed.** An arriving block names only its parent, so the
  claim is exactly what the evidence supports: *this height changed, here are
  the two hashes.*
- **Rows from orphaned blocks are not deleted.** Those bytes arrived, and the
  record records arrivals. The reorganisation is a second fact recorded beside
  them. Deleting would make the record a thing that changes, which is the one
  property it must not have.

### And one false alarm avoided by asking

Nodes disagree about hex case. A comparison that was case-sensitive would report
a fork because one node said `0xAA` and another `0xaa` — on every block. The
comparison is case-insensitive and there is a test that says why.

## The seam abstracted framing, not transport — again — 2026-09-21

Worth recording because it was **diagnosed, written down as the thing to avoid,
and then done anyway.** The original exploration said of the predecessor:

> its capture loop opens a websocket directly, so `subscribe_frames` and a
> keepalive are baked in as assumptions. Two of the three venues on this roadmap
> are polled or cursor-driven, for which both are meaningless.

and concluded `Source` should be introduced now rather than retrofitted. The
`source` **module** was introduced in Tier 1. The **seam was not**:

```
  Adapter::channel_of         a websocket concept
  Adapter::subscribe_frames   a websocket concept
  Adapter::keepalive          a websocket concept
  Capture::run                constructed StreamSource unconditionally
```

Expressed through that trait, a chain adapter must answer an empty frame list
and a `Keepalive::None` — values that are not *false*, they are **meaningless** —
and a loop acting on them opens a socket and sends it nothing.

### The fix, and why `Option` rather than defaults

```rust
fn transport(&self) -> Transport;                     // Stream | Cursor
fn streaming(&self) -> Option<&dyn Streaming> { None } // the subscribing half
```

Defaulted `subscribe_frames` returning `Vec::new()` would have compiled just as
well and been worse: an empty list is **an answer a loop will act on**. `None`
is not, and the caller has to say what it does about it.

`Transport` is an enum rather than a trait object because the set is closed and
the loop has to match on it regardless — a stream loop and a cursor loop are
genuinely different programs, and pretending otherwise is how a chain adapter
would end up with a keepalive.

### The result

`RhChain` is an `Adapter` with **no subscription method at all** and a
`transport()` that carries the RPC endpoint, chain 4663, the block paging and
the measured finality lag. There is a test asserting `streaming().is_none()` —
which, before this change, was not a thing that could be true.

`Capture::run` now refuses a non-stream transport by name rather than opening a
socket for it.

### And the third wall caught a layering mistake, not a dependency one

`Transport` is a pure declaration, but it was written referencing `BlockPaging`
from the `source` module — which is behind the `capture` feature. The build with
that feature off refused it.

The guard was right and the placement was wrong: **how a provider serves ranges
is a declaration about a venue**, like a bar width or a rate limit. Only the
*fetching* is transport. The whole module moved from `source/cursor.rs` to
`venue/chain.rs`, where nothing in it needs a runtime — which was already true,
and was why it compiled.

Worth noting because the wall was built to stop a *reader linking a websocket
stack*, and what it actually caught first was a module in the wrong layer. The
same mistake wearing a different hat.

## The cursor loop, against the live chain — 2026-09-21

A minute of capture from Robinhood Chain's public node, rebuilt and queried:

```
  32 payloads → 96,512 rows in 2 segments (0 unparsed)

  venue     ticker      rows   first_block   last_block   transactions   venue_times
  rh-chain  TOKEN0BD7  47,311   67,883,240   67,915,239        36,204             0
```

**`venue_times: 0` is the point.** The column is a count of non-null venue
times, and it is zero by design: a `getLogs` response cannot know per-block
timestamps without a call per block, so none was invented. The block number is
there instead, so the time is recoverable. A query proving a negative is a
better assurance than a comment claiming one.

Mints split 24,501 issuances against 24,700 redemptions, with exact decimal
totals — the numbers crossed JSON, parquet and DuckDB without a float anywhere.

### The run found a defect the tests could not

The public node rate-limits, and the first run met it:

```
  15 × {"code":429,"message":"Too Many Requests"} in 40 seconds
```

The loop behaved correctly — it failed the pass, **did not advance the cursor**,
and kept going, which is the invariant that stops a retryable hole becoming a
permanent one. But it retried at the declared pace of 500 ms, which is precisely
what provoked the refusal. **A rate-limited loop that does not slow down never
recovers**, and adds load to a node already saying stop.

The remedy for being told to slow down is to slow down. The same `Backoff` the
reconnect path uses:

```
  before   15 refusals in 40 s
  after     4 refusals in 60 s
```

A pass that got anywhere clears the penalty; one refused throughout keeps it.
No test would have found this — it needs a real provider with a real opinion
about how often it wants to be asked.

## Three venues, three different answers about silence — 2026-09-21

rh-crypto completes a set, and the set is the interesting part:

```text
  stream   a quiet market and a dead socket look identical, because
           NOTHING HAPPENED either way
           → never infer a gap                              invariant 3

  chain    the chain hands back a different block at a height we recorded
           → PROVE the gap                                  two hashes

  poll     we asked at 12:00:05 and nothing came back
           → BOUND the gap                                  exactly one cadence
```

The poll is the case where **our own action supplies the missing half**. A
stream cannot tell silence from absence because it did nothing to tell them
apart with; a poll can, because we did something and its failure is an event we
witnessed.

So a poll gap is not a softer `SessionLost`. It is **the only gap in the system
whose width is known** rather than dated from the last thing that happened to
arrive.

Three decisions fell out of writing it, each with a test:

- **Three consecutive failures are one gap three intervals wide**, not three
  gaps. Three claims where there is one fact would make a consumer count an
  outage three times.
- **Nothing is claimed before the first answer** — the same rule as a
  first-ever start, for the same reason: a gap back to the beginning of time is
  not a fact.
- **A clock that went backwards claims nothing.** A gap that runs backwards is
  not a gap, and a system whose clock jumped is not one that should be inventing
  intervals.

### The signature, and where the clock stops being cosmetic

```text
  message = api_key + timestamp + path + method + body
  timestamp = UNIX SECONDS, expiring after 30
  key       = base64 of a RAW 32-BYTE SEED
```

Each of the three documented ways to get this wrong is refused **by name**
rather than discovered as a `401`:

| wrong thing | what it actually is | why naming it matters |
|---|---|---|
| milliseconds | 13 digits where 10 are wanted | fails *every* request |
| 64 bytes | an expanded keypair | the seed is its first half |
| starts with `0x30` | an ASN.1 SEQUENCE, so PKCS#8 | the raw seed is inside it |

*Signature invalid* tells nobody anything. **This is the first place in the
system where a drifting clock does not merely mislabel data** — past thirty
seconds it stops capture entirely, and a `401` from skew looks exactly like a
`401` from a revoked key.

Signing is checked against **RFC 8032 test vectors**, because an implementation
verified only against its own output is a test that a bug and its mirror image
agree. The *message shape* is tested separately, since the vectors cannot know
it.

### What is not verified, and will not be here

**No credentials were obtained and none should be.** The live endpoint has not
been called. Everything above rests on published vectors and the documented
message shape — which is a weaker claim than every other venue in this tree
carries, and is worth saying plainly rather than leaving to be discovered.

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
