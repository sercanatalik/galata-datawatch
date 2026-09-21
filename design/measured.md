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

## Capture, configured from a vault — against a real server, 2026-09-21

`gv-server local` on loopback, a project, an `acme/prod` environment and the
shipped configuration stored as document `datawatch` v1. Run rather than
reasoned about, because the three refusals were written from an SDK's error
kinds and two of them turned out to describe something else.

**It works end to end.** `galata-datawatch-vault hyperliquid`, with a `config`
token and no file anywhere, fetched the document, validated it through
`Config::load`, booted capture and walked candles off the live venue:

```
  INFO boot: no broker is configured; events are recorded and not published
  INFO capture::run: walk of candles at 1m: asked 7d, the venue holds 3d 11h 20m,
       covered 3d 11h 20m in 1 requests venue="hyperliquid"
```

### What the document costs at boot

Time from exec to the venue refusal — naming a venue the configuration does not
declare, so the process stops immediately after `Config::load` and the figure is
the load and nothing after it. Ten runs each.

```
  from a document (loopback vault)   median 37.0 ms   min 35.0   max 50.0
  from a file on disk                median 28.7 ms   min 26.6   max 32.3
  ──────────────────────────────────────────────────────────────────────
  the document costs                          8.3 ms
```

8.3 ms, once, at boot, for a process that then runs for days. Nothing was
published to compare against — the search for a loopback-versus-file figure
returned none — so this is taken rather than carried. The file binary's FIRST
run took **44 seconds**, which is not a config cost: it is the macOS first-exec
signature validation and cold page-in that galata-vault's own conformance script
was mis-diagnosing as a dead server. Dropped from the median, and recorded
because it would otherwise look like the file path was catastrophic.

### Two refusals described something that does not happen

- **`read` reads configuration documents.** The refusal said *a token whose
  scope includes `config`*. It is wrong: a `read` token serves the document
  perfectly well, and only `meta` is refused — *"this credential cannot read
  configs"*. The message named a scope because the scope seemed obvious from
  the name, and naming it restated a rule belonging to the vault. This is the
  same mistake `check-secret-reach.sh` refuses for `GV_TOKEN`, arriving a second
  time by a route the guard cannot see: **a copy of somebody else's rule
  disagrees rather than fails.** The scope is gone from the message; the vault's
  own sentence is the authority.

- **The ordinary unreachable case never reaches our `Unreachable`.**
  `Vault::from_env` opens the vault, so a stopped server is refused by the SDK
  before `VaultConfig::fetch` is called — in **0.04 s**, not at the 120 s
  timeout. Our variant now covers only a vault that disappears between opening
  and reading, and says so.

  The SDK's message carries the URL: `could not reach http://127.0.0.1:8751`.
  `check-endpoint-reach.sh` governs this tree's own messages and cannot reach a
  dependency's, which is worth knowing rather than worth fixing here.

### The refusals that do hold

```
  a meta token          the vault refused to serve datawatch: ... cannot read configs
  both variables set    GALATA_CONFIG names ... and GALATA_CONFIG_DOCUMENT names ...
                        unset one
  no document named     GALATA_CONFIG_DOCUMENT is not set, and it names the
                        document to capture from
```

The middle one is the same sentence the file binary gives, from the same
function, which is what moving `document_from_env` beside `FileSource::from_env`
bought.

---

## The fourth wall, and a carried figure that does not survive it — 2026-09-21

The vault's cost, taken here rather than believed from the predecessor.
`legacy/galata-legacy/planning/draw-from-the-vault.md` priced this on
2026-09-11 as *"a client tree of about 225 crates, much of it `age`'s
localisation stack, which no `age` feature set drops."*

Both halves of that were checked. One holds and one does not.

```
  galata-vault, locked on its own                       221 crates
  legacy's carried figure                              ~225   agrees

  galata-datawatch with `bin`                           310 crates
  the same, plus galata-vault                           414 crates
  ────────────────────────────────────────────────────────────────
  what the vault actually costs this tree               104 crates
```

**The carried figure is right and the decision it would have supported is
wrong.** 221 is the vault's tree measured alone; this tree already links serde,
tokio, reqwest and rustls, so more than half of it is already paid for. Pricing
the change at 225 would have over-stated it by more than twice — and 225 against
a 310-crate binary is the kind of number that stops a design, which is what it
did for eleven days.

The localisation half holds exactly as recorded. It is really there, and no
feature set drops it:

```
  age · age-core · fluent · fluent-bundle · fluent-langneg · fluent-syntax
  i18n-embed · i18n-embed-fl · i18n-embed-impl · intl-memoizer
  rust-embed · rust-embed-impl · rust-embed-utils · unic-langid · unic-langid-impl
```

Which is the argument for the wall rather than against the vault: a tape reader
must not compile a localisation framework to read parquet, and
`check-vault-reach.sh` names every one of these so the wall holds even if the
vault is ever vendored under another name. Planted, the guard goes red naming
13 of them.

**The blocker that was not one.** Legacy also measured that *"Cargo resolves an
optional dependency even with its feature off"*, so an unreachable git URL fails
the build regardless, and concluded that galata-vault needed a public remote
before galata could depend on it in any form. True when taken, and void now:
galata-vault is a registry dependency at 0.1.0. The finding did not expire
because it was wrong; it expired because somebody published the crate.

---

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

## Two venues, one query — 2026-09-21

The Tier 8 exit criterion, and the point of the `quotes` dataset:

```sql
SELECT venue, ticker, bid_px, ask_px, bid_sz, bid_spread, ask_spread
FROM read_parquet('tape/kind=quotes/**/*.parquet') WHERE ticker = 'BTC'
```
```
  hyperliquid  BTC  81213.00  81214.00  15.826  NULL   NULL
  rh-crypto    BTC  81190.50  81235.50   0.500  22.50  22.50
```

Two adapters, two transports — a pushed `bbo` frame and a polled
`best_bid_ask` — **one shape**. And the `NULL`s are the load-bearing part:
they mean *this venue never states it*, not *it was missing*. An exchange
states sizes and no spread; a broker states a spread and one quantity.

Which makes the question anyone actually has into one predicate:

```sql
SELECT venue, ask_px AS pay_to_buy FROM … WHERE ticker='BTC' ORDER BY ask_px LIMIT 1
→ hyperliquid  81214.00
```

The broker's ask is 21.50 higher and its bid 22.50 lower — the spread it
states, visible as the reason rather than inferred from the numbers.

### A documented disagreement, carried rather than resolved

One published description of `best_bid_ask` lists a top-level `price`. A bug
report against a client library says the endpoint **structurally has no
`price` field**.

This tree has been here before: Hyperliquid's `bbo` was documented as
*"functionally equivalent to `l2Book` with `nLevels: 1`"* — true of meaning,
false of shape — and every frame failed to normalise until a live run showed
the real form.

So nothing here requires `price`. The prices relied on are the two
spread-inclusive ones, which are the **tradeable** numbers anyway — what you
would actually pay and actually receive. A field the venue did not state is
**absent**, never zero, because zero is a price.

**Unverified against the live endpoint**, which needs credentials. Until then
the shape is a hypothesis with a test, not a measurement — a weaker claim than
every other venue here carries, and the record is the thing that will settle it
when someone does run it.

## The component that judges — 2026-09-21

The status surface was built in Tier 1 to **report and never judge**, on an
argument that named its own missing half:

> A threshold inside the capture process cannot be changed without a deploy,
> and is wrong for the next instrument anyway. The component that judges is a
> different one, and it can be changed without stopping capture.

That different one did not exist, so the decision was half made: capture refused
to judge and nothing else judged either. `galata-watch` is the other half.

### A heartbeat is a claim; a file on disk is a fact

```
  3572 segments in a closed partition (expected at most 64)
    — var/soak4/archive/venue=hyperliquid/kind=candles/date=2026-09-20
  the newest segment is 8706 s old (expected at most 300 s)
    — var/soak4/archive
```

Both findings are true of the real nine-hour archive, and both are **checkable**:
the number, the bound and the path. An alert saying *compaction overdue* makes
somebody go and find out; this has already done that.

All four exit codes verified against real trees: `0` nothing to report (13
partitions, no thresholds declared), `1` findings, `2` bad argument, `3` nothing
to check — and the last is separate on purpose, because *an empty archive* is
what capture having silently stopped looks like.

### The first real run found the watcher's own bug

Against the nine-hour archive it reported **46 overlapping-range findings**, all
false. The cause was mine: it ran the **tape's** layout check against the
**archive**.

Twenty-four gaps written in one flush all carry the same microsecond, so their
segments have identical time ranges — which the tape's rule, *sequence ranges
must not overlap*, reads as forty-six redeliveries. They are nothing of the
kind. The archive distinguishes those segments by **pid and flush sequence**
precisely so that writing many at one instant is legal.

Two stores, two shapes, two rules, and one of them was applied to the other.
The test that now pins it writes two segments at the same instant and asserts
silence.

## "Add a venue later" — checked, and it was not quite true — 2026-09-21

Tier 10 says the `Adapter` seam is an out-of-tree extension point, *"which is
what makes 'add a venue later' true rather than true-if-you-fork."*

That is a **claim about somebody else's repository**, and every adapter in this
tree lives *inside* the crate, where `pub(crate)` is reachable and nobody would
notice. So it was written as a test instead: a fictional venue in `tests/`,
which cargo compiles as **its own crate** and which therefore sees exactly what
a stranger sees.

**It failed on the first run**, and the failure was the point:

```
  error[E0432]: unresolved import `galata_datawatch::sink::testing`
  note: found an item that was configured out
```

`RecordingSink` was `#[cfg(test)]`. An adapter author writing their own venue
had **no test sink at all** — they would have had to write one to exercise the
one path their adapter has to cross. The claim was true for compiling and false
for *testing what you compiled*, which is the half that matters.

It is now behind a `testing` feature: available to a stranger, absent from a
production build. Three tests pass — the venue compiles from the public API
alone, reaches `ingest`, and boxes as the `dyn Adapter` the loop holds.

### And the fix broke the third wall, which said so immediately

Reaching the feature from `tests/` needs the crate to depend on **itself** with
`testing` on. Written the obvious way, that dev-dependency carries **default
features** — so a `--no-default-features` build got the transport back through
its own test dependency, and `check-no-transport.sh` went red:

```
  check-no-transport: with capture off, these link anyway:
    tokio  reqwest  rustls  hyper  tungstenite
```

`default-features = false` on the self-dependency fixes it. Worth recording
because the guard was written to catch *a module forgetting its `cfg`* and what
it actually caught, twice now, was something subtler: first a module in the
wrong layer, now a dependency edge that goes in a circle.

## The publishing order, from the dry runs

```
  galata-wire       dry-run CLEAN
  galata-segments   dry-run CLEAN
  galata-broker     cannot verify — galata-wire is not on crates.io
  galata-datawatch  cannot verify — galata-broker is not on crates.io
```

The last two are **lockstep ordering, not a defect**: `cargo publish` verifies
against the index, and the index has neither of the first two yet. Nothing can
be done about it except publish in order, and that is a decision for whoever's
name is on the registry account — publishing is irreversible and outward-facing.

The roadmap's suggestion that `galata-segments` could go **early** is supported
by this: it is standalone, it dry-runs clean today, and real users would find
the cursor API's rough edges while the version still costs nothing to change.

## A defect a test fixture walked into — 2026-09-21

Writing the `--replace` test needed an archive that **grows between two
rebuilds**, which is what a scheduled retry sees. The obvious fixture reopened
the archive and appended one payload. It failed, and the reason is a real
defect rather than a fixture problem.

```rust
pub fn next_seq(&mut self) -> u64 { let seq = self.next_seq; self.next_seq += 1; seq }
```

`Archive::open` sets `next_seq: 0` and **nothing ever reads it back from disk**.
So every restart hands out sequences starting at zero, colliding with the ones
already written.

### Why that matters beyond a fixture

The tape's `stream_seq` is documented as *"the road back from any row to the
bytes they came from"*. After one restart, two different archived payloads carry
`seq = 0`, and two tape rows point at both. **The road back forks.**

The archive itself is unharmed — its segments are named by *receipt time* and
the `seq` column only joins a failure row to its payload within one flush. It is
the **provenance claim** that is false, and only after a restart, which is why
nine hours of soak never showed it.

What is *not* broken, and worth stating so the scope is clear:

- Segment names do not collide. One rebuild reads a whole range and commits one
  segment per partition, so two payloads both numbered 0 land in the same file.
- `(recv_micros, stream_seq)` together are still near-unique, and both columns
  are on every tape row — so the road back exists, it is just not the one
  column the documentation names.

### Fixed, and the fix was decided by a guard

Three options, and one of them was ruled out by a rule already in the tree.

**Resume from disk** — the archive is partitioned by receipt time and indexed by
nothing else, so finding the greatest sequence means opening files, on every
boot, across a tree that reaches 22,000 segments in three hours.

**Seed inside `Archive::open`** — *it cannot*.
`scripts/check-clock-discipline.sh` forbids a clock read below the loop, and the
reason is the sharp one: a real `now()` in a helper does not make a test
**fail**, it makes the test **stop asking**. So the seed is handed down, which
is the shape this codebase already has.

**Amend the claim to name both columns** — `(recv_micros, stream_seq)` really is
near-unique and both are on every row. But the bus carries `Envelope::seq`
*alone*, so the claim would have to be weakened exactly where it is hardest to
use.

So: the loop seeds the archive from the clock it already reads, in microseconds.
Monotonic across restarts because time moves forward; unique unless two
processes open one archive scope in the same microsecond, which one process per
venue already rules out; and **no counter in a file**, because a counter on disk
is a second source of truth that can be deleted while the data stays — which is
this bug wearing a hat.

### Verified on two real restarts

Two captures against the live venue, twenty-five seconds each, then one rebuild:

```
  2,337 payloads · 2,337 distinct sequences · 0 collisions
  lowest 1789983568056379 · highest 1789983594381915
```

The first query tried was `count(*) - count(DISTINCT stream_seq)` over the
**tape**, which reported 33,053 collisions and meant nothing: one payload
legitimately becomes many rows — a walked candle page is 5,001 of them sharing
one sequence, which is the whole point of the column. The question is whether a
**payload** sequence repeats, and the archive is where payloads are.


## `status.<venue>` on the bus — 2026-09-21

Tier 4 said the snapshot rides its own subject root on a timer. It was written
to a **file** and went nowhere else, so a dashboard watching a fleet had to read
every host's disk.

Verified against a real `nats-server`, a second process holding `status.>`:

```
  status.hyperliquid
    "connection": "connected"
    "subs_held": 24
    "buffered": 66
    "sink_dropped": 0
```

and the local file unchanged beside it, still written **first** — because the
surface that reports a broker outage must not be a publish. Same shape as
archive-before-normalise: the thing that survives goes first.

### One channel, because backpressure is one decision

Status and events share the bounded queue and the drop counter. Two queues would
mean two capacities and two answers to *what happens when the broker is behind*,
and the interesting case is exactly when both are backed up at once. A dropped
snapshot is the least costly thing in that queue, because the file on disk still
has one.

### The first subscriber attempt failed, correctly

```
  Error: NotAnEnvelope("missing field `address` at line 234")
```

The example asked for `status.>` and decoded what arrived as an `Envelope`. A
status snapshot is **not** one — it is a report about a *process*, not an
observation of a *market* — and the decoder did exactly what it is built to do:
*a message that will not decode is an error, never a skip.*

Worth recording because the failure was the design working. A decoder that
shrugged and skipped would have shown an empty dashboard and no reason for it.

## The third transport is driven — 2026-09-21

Three shapes were designed; two were driven.

```
  Stream   Capture::run          hyperliquid, nine hours live
  Cursor   Capture::run_cursor   rh-chain, 47,311 transfers
  Poll     Capture::run_poll     ← this
```

`Cadence` knew how to bound a gap, `Credential` knew how to sign and
`best_bid_ask` knew how to normalise. Nothing asked anything, and **an answer
nothing exercises is an answer nobody has checked.**

### Every poll is archived, including the boring ones

A five-second poll against a market that has not moved returns the same two
prices. The tempting optimisation is to notice and skip it.

No. The record records **arrivals**, and *we asked at 12:00:05 and the venue
said 81190.50/81235.50* is an arrival. Collapsing identical states at capture
time destroys the difference between **the price did not move** and **we did not
ask** — which is precisely the difference the bounded gap exists to preserve.
Collapsing is a projection's job, where it is reversible.

### Consecutive failures widen one gap

Tested: three failures after one success produce gaps that all start at the last
successful poll and each end later. Not three gaps — three claims where there is
one fact would make a consumer count an outage three times.

And the gap is in the **record** before it is on the wire, checked by looking
for `kind=gaps` on disk rather than by trusting the sink.

### Throttled backs off; unreachable does not

`429` means *we asked too often* — ours to fix, and it gets worse if we keep the
rate. A venue that is down is not ours and does not improve by waiting longer.
`GapCause` keeps them apart so a consumer can tell *we were rate-limited* from
*the venue was down*: different facts, different remedies.

### A test suite people would have started skipping

The first run of these five tests took **15.15 seconds** — the loop was sleeping
real time at a five-second cadence. `#[tokio::test(start_paused = true)]`
auto-advances tokio's own timer:

```
  15.15 s  →  0.12 s
```

Worth recording because the failure mode is social rather than technical: a slow
test is one people run less often, and a test nobody runs is a guard that has
quietly stopped guarding. The same reason `check-all` exists at all.

## `allow: []` means allow everything — reproduced — 2026-09-21

The predecessor shipped an inverted permission table and recorded what failed to
catch it: review, unit tests, and `nats-server -t`. **All three were checked
again here, and all three still miss it.**

Same probe, two tables, a real `nats-server`:

| table | `nats-server -t` | what capture did | server log |
|---|---|---|---|
| `subscribe: { allow: [] }` | **valid, exit 0** | **received `markets.hyperliquid.BTC.quotes`** | nothing |
| `subscribe: { deny: [">"] }` | valid, exit 0 | heard nothing | `Subscription Violation` |

```
  VERDICT: FAILED — capture received markets.hyperliquid.BTC.quotes,
                    which it is denied
```

`allow: []` reads as *allow nothing* and means **allow everything** — an absent
restriction rather than a total one, which is the exact inverse of what a
component granted no rights should have. So *nothing* is spelled `deny: [">"]`.

### The probe had to publish, or it proved nothing

The first version of this test asserted only *capture heard nothing in three
seconds*. Under the **inverted** table that also passed — because nothing had
been published. A false negative that would have certified the broken table.

The probe now publishes a real message first, on a subject capture is granted
under **both** tables, so the two halves differ only in the thing being tested.
*I did not see any data* is not evidence of a denial; the server's own refusal
is.

### What the guard cannot do

`nats-server -t` calling the inverted table valid is the whole reason this
verification loads a running server. **A generated permission set that has never
been loaded into one is a decoration** — and a unit test asserting the string
`deny: [">"]` only checks that the renderer does what the renderer was written
to do.

## A raw token amount is not a share count — 2026-09-21

ERC-8056 applies a corporate action by updating a **display multiplier** rather
than minting, burning or migrating tokens:

```
  underlying shares = raw amount × uiMultiplier ÷ 1e18
```

Asked of the live chain, contract by contract:

| contract | symbol | `uiMultiplier()` |
|---|---|---|
| `0xd0601ce…` | **NVDA** | **1.0007751591646306** |
| `0x4a0e65a…` | SPCX | 1.000000 |
| `0x0bd7d30…` | WETH | reverts |
| `0x3429ddc…` | RSTOCK | reverts |

**NVDA's multiplier is not one.** A corporate action has already been applied,
so:

```
  1000 raw tokens  →  1000.775 underlying shares
  understated by      0.0775%
```

Every raw amount this system records is correct as *tokens* and wrong as
*shares*, by a factor that changes and has changed once already. Nothing in the
record is wrong — raw is what arrived — but a consumer summing those amounts and
calling them shares is, silently, and more so after each corporate action.

### Two things this measurement corrected

**The contract being captured was WETH.** The rh-chain demonstration earlier in
this tree captured `0x0bd7d30…` as `TOKEN0BD7`, which turns out to be **WETH**
and not a stock token at all. The capture was correct; the label was ignorance,
and asking the contract its own symbol is what settled it.

**The update event could not be named.** Four candidate signatures for
`UIMultiplierUpdated` were hashed and searched against 50,000 blocks of the NVDA
contract. **None matched.** So nothing listens for an event it cannot name: the
multiplier is **polled** and recorded with the moment it was read, which is
honest about the update itself not having been witnessed.

### `None` is not `1.0`

Most contracts here revert on the call. Recording `1.0` for them would claim
they implement the standard and are currently unscaled. They do not implement
it, and *this is not a stock token* is a different fact from *this multiplier is
one*.

### And a hand-typed constant was wrong, again

The test fixture's hex was converted by hand and decoded to
`1.000838949559649155` — close enough to look right, wrong by 64 parts per
million. It is now derived from the measured integer.

**Second time in this tree** a hand-computed expectation disagreed with correct
code, after the `0x015fb7f9b8c38000` decimal. The rule that follows: a fixture
whose value was guessed is a fixture that tests the guess.

### It is recorded during capture — end to end, live

A 30-second run against the public node with two contracts declared, then a
tape rebuild and a join:

```
$ galata-datawatch rh-chain                 # 30 s, no broker
$ galata-tape-rebuild rh-chain 2026-09-21
  12 payloads → 37791 rows in 3 segments (0 unparsed)
```

```
┌────────┬──────┬───────────────┬──────────────────────┬────────┬──────────────────────┐
│ ticker │ base │ contract_type │      tick_size       │ active │    ui_multiplier     │
├────────┼──────┼───────────────┼──────────────────────┼────────┼──────────────────────┤
│ NVDA   │ NVDA │ token         │ 0.000000000000000001 │ true   │ 1.000775159164630595 │
│ WETH   │ WETH │ token         │ 0.000000000000000001 │ true   │ NULL                 │
└────────┴──────┴───────────────┴──────────────────────┴────────┴──────────────────────┘
```

`NULL`, not `1.0`, survives all the way into the tape — the distinction the
module exists for is the one a consumer actually sees.

The join, over the same thirty seconds of real transfers:

| ticker | transfers | raw tokens | underlying shares |
|---|---|---|---|
| NVDA | 3,406 | 2330.930470173899256393 | 2332.737312289967 |
| WETH | 19,540 | 7270.635273220536073464 | 7270.635273220535 |

**1.81 shares in thirty seconds**, on one contract, from a multiplier that is
1.0008. That is the size of the error the record no longer forces on a
consumer.

### The join overflows without a cast

The obvious query fails:

```
Out of Range Error: Overflow in multiplication of DECIMAL(38)
  (189645268612509304019 * 1000775159164630595)
```

Both columns are `DECIMAL(38, 18)`, and DuckDB will not widen past 38 digits to
hold the product of two of them. `cast(ui_multiplier as double)` is what makes
it run — which is fine *here*, because the multiplier has 18 significant
digits and a double holds 15, and the shares are a display quantity. It would
not be fine for the amount. Recorded because whoever writes this join next will
hit it, and the fix that first comes to mind — casting the amount — is the
wrong one.

### The cadence is an hour, and that is a staleness bound

No update event exists to subscribe to, so a corporate action is learned when
the next read happens and not before. An hour bounds that; it does not shorten
it. The read runs **before the first block of a pass**, not after, so a
multiplier is never younger than the amounts recorded against it.

Three requests per instrument per hour, against a node that refused ranges
fifteen times in forty seconds under a tighter pace.

### What the chain cannot say about an instrument

`Instrument` has fields describing an order book and this venue has none, so
each is filled from what a token contract knows rather than left to a default:

| field | value | why |
|---|---|---|
| `tick_size`, `lot_size`, `min_size` | `10^-decimals` | the only increment a contract has; `contract_type = token` says it is a quantity, not a price |
| `quote` | empty | a token contract prices nothing. USDG is a fact about a *trade*, recovered by pairing transfers in one transaction — not about this instrument |
| `hours` | `continuous` | the record shows transfers at every hour; the assumed Sun 18:00–Fri 17:00 ET window was already disproved for the `xyz` instruments |
| `venue_index` | `NULL` | a chain addresses by contract, never by position in a list |
| `active` | did it answer at all | a contract that answers nothing is not one we can call listed |

## A provider URL is the credential — 2026-09-21

A keyed RPC provider puts its key in the **path**:

```
  https://arb-mainnet.g.alchemy.com/v2/<KEY>
  https://<slug>.arbitrum-mainnet.quiknode.pro/<TOKEN>/
```

so the widespread remedy — strip the query string — protects nothing here.

**Measured, reqwest 0.13.5**, a failed POST to a URL carrying a key in both
places:

| formatting | what it says |
|---|---|
| `Display` | `error sending request for url (https://…/v2/SUPERSECRETKEY123?api_key=ALSOSECRET)` |
| `Debug` | the same, plus `url: "…"` as a field |
| `without_url()` `Display` | `error sending request` |
| `without_url()` source chain | `client error (Connect) \| dns error \| failed to lookup address information…` |

**Redaction costs nothing diagnostic.** The source chain — the part that says
*why* — survives intact. The only thing lost is the URL, which the operator
configured and already knows.

### Where it would have leaked

| site | what it did |
|---|---|
| `ChainError::Http` | formats `{source}` — reqwest's Display, with the URL — and the cursor loop logs it on **every** refused range. The same loop already recorded fifteen refusals in forty seconds. |
| `ChainError::WrongChain` | interpolated the URL by hand, and it is the error that fires when a provider is *misconfigured* — precisely when the URL is freshly pasted |
| `FetchError::{Http,Status}` | a `url: String` field in the message |
| `SourceError::Connect` | the same |
| `capture/run.rs` | `tracing::warn!(url, …)` on a failed connect |

Five sites, none of which held a secret **yet**.

### And the redaction made a refusal worse before it made it better

With the URL gone, the top line of a failed boot was:

```
  the provider refused: rh-chain eth_chainId: error sending request
```

DNS, TLS and connection-refused are indistinguishable there. `main` returning a
`Result` prints an error's `Debug` and nothing beneath it, and
`CaptureError::Provider(e.to_string())` had already flattened the chain away.
Both fixed — the binary prints causes, and `Provider` carries them:

```
  the provider refused: rh-chain eth_chainId: error sending request:
    client error (Connect): dns error: error resolving DNS:
    failed to lookup address information: nodename nor servname provided
```

**The lesson is the general one:** a redaction that removes a line's only
content has to put the real content back, or the next person disables the
redaction to debug something.

### The variable name, not the host

A held endpoint renders as `the provider named by GALATA_RHCHAIN_RPC_URL`.

Not the host: QuickNode puts an identifying slug in the **hostname**, so
"scheme + host" is a rule that is right for Alchemy and wrong for QuickNode.
Not `<held>`: that says a thing is hidden, where the variable name says where
to look — and it is already committed in the configuration file.

### There is no `rpc_url` field, deliberately

Only `rpc_url_var`. A field that accepted a URL would sit one `_var` suffix
away from the safe one, in a committed file. A named variable that is **unset
refuses** rather than falling back to the public node, because a silent
fallback is how a process runs for a week against a provider nobody chose:

```
$ galata-datawatch rh-chain
GALATA_RHCHAIN_RPC_URL is not set. Nothing connects anonymously, and no
default is invented
```

### The rule is absolute because an exception is uncheckable

`Stream` and `Poll` endpoints are compiled-in public addresses that could
safely print. They became `Endpoint` anyway. A guard that said *no URL in an
error message, except the ones that are fine* is a guard nobody can run — and
the exception is where the next keyed URL gets added.

`check-endpoint-reach.sh` holds three rules and each was watched failing on its
own plant: a URL in an `#[error]` message, a `reqwest::Error` stored without
`without_url()`, and `expose()` called outside a connect site.

### One adjacent hole, closed while here

A NATS URL may carry `nats://user:pass@host`, the broker's password already has
a variable of its own, and the connect line logs the URL. Userinfo in
`broker.url` now refuses at load — and the refusal **does not echo the URL**,
since a refusal that exists to keep a credential out of a file should not print
it.

## A reorganisation now supersedes what it contradicts — 2026-09-21

`Event::Reorg` has been recorded since Tier 7 and **nothing acted on it**. Two
things were missing, and each made the other useless.

### The cursor did not rewind

It published the reorganisation and kept advancing, so the rows for a replaced
range were the *old chain's* and nothing ever replaced them. The record held a
claim it had itself already contradicted.

Published indexers — Envio, QuickNode Streams, the reorg-safety trackers —
converge on one answer: **mark superseded, rewind to the divergence, re-index
forward, never hard-delete.** The first and third are this tree's shape
already: the archive is append-only and the reorg row is a fact *about* the
record rather than an edit *to* it.

So the cursor now goes back to `from_block - 1`. The trail already dropped the
replaced heights, so no second reorganisation is invented for the same blocks.
The re-read is **bounded by finality — 11,678 blocks, about twelve
thousand-block ranges at worst — and reported rather than capped**, because a
cap would silently leave part of a replaced range unread, which is the failure
the rewind exists to prevent.

### Which makes the join need two clauses

Once the cursor rewinds, the same blocks appear twice:

```
  seq  block  what
  ───  ─────  ────────────────────────────────────────────
  100   4100  transfer          ← old chain
  101   4101  transfer          ← old chain
  150      —  REORG 4100..4101  ← the divergence is recorded
  151   4100  transfer          ← new chain, SAME BLOCK
  152   4101  transfer          ← new chain, SAME BLOCK
```

A block-range test alone marks all four superseded. The rule is:

```
  reorg.from_block <= row.block <= reorg.to_block
  row.stream_seq   <  reorg.stream_seq
```

**The sequence clause is what makes the rewind safe, and the rewind is what
makes the sequence clause necessary.** Neither works alone — which is why both
landed in one change.

Verified in DuckDB over the documented query, which gives the same answer as
the Rust join:

```
┌───────┬────────────┬───────────┬───────────────┐
│ block │ stream_seq │   note    │ superseded_by │
├───────┼────────────┼───────────┼───────────────┤
│ 4099  │ 99         │ untouched │ NULL          │
│ 4100  │ 100        │ old       │ 0xaa          │
│ 4101  │ 101        │ old       │ 0xaa          │
│ 4100  │ 151        │ new       │ NULL          │
│ 4101  │ 152        │ new       │ NULL          │
└───────┴────────────┴───────────┴───────────────┘
```

The SQL lives beside the implementation as `reorg::AS_SQL`, with a test
asserting it states both clauses — a second implementation of a rule does not
fail when it drifts, it disagrees.

### Derived, never a column

A `superseded` column would have to be written by editing rows already durable,
which this store does not do. Deriving costs nothing: `kind=reorgs` is the
smallest dataset in the tree.

It is also **honest about time**. A row is superseded *as of the
reorganisations known so far*. A stored column reads as a permanent property;
a derived one reads as what it is.

### Found while writing the consumer

**A tape that has never reorganised has no `kind=reorgs` partition, and the
bounded view refuses a scope with no frontier** — correctly, since it will not
invent one. So the join's first act is to check whether that directory exists
at all, and say *this venue has never recorded a reorganisation* rather than
printing a zero. Run against the live capture:

```
0 reorganisation(s) in the window
transfers: 22946 rows
nothing to join — this venue has never recorded a reorganisation
```

Which is the right answer, and not the same as *every row is confirmed*. A row
no reorganisation contradicts is only one no **known** reorganisation
contradicts, and the trail sees only the blocks capture actually asked for.

## The codec, settled on this tree's own bytes — 2026-09-21

Two constants carried *not measured yet* notes since Tier 0. There are real
segments per dataset now, so both are answered.

`cargo run --release --example codec -- <root>` is the instrument: every
segment under a root, rewritten under each codec, with write, full-scan read
and **windowed** read timed separately.

### The archive — 12 whole `eth_getLogs` responses, 278 MB raw

| codec | bytes | of raw | full read | window |
|---|---|---|---|---|
| uncompressed | 278,821,357 | 100.0% | 28.0 ms | 0.20 ms |
| lz4 | 37,054,667 | 13.3% | 122.5 ms | 0.21 ms |
| **zstd** | **16,192,334** | **5.8%** | 129.1 ms | 1.50 ms |

### The tape — 37,791 typed rows

| codec | bytes | of raw | full read | window |
|---|---|---|---|---|
| uncompressed | 3,215,766 | 100.0% | 4.7 ms | 1.32 ms |
| lz4 | 2,708,773 | 84.2% | 5.5 ms | 1.16 ms |
| **zstd** | **1,564,052** | **48.6%** | 11.6 ms | 1.32 ms |

### What this refutes

The note said:

> `LZ4_RAW` decompresses markedly faster at a worse ratio, which is plainly one
> store's trade and plainly not the other's.

**Both halves fail.**

On the archive lz4 is 5% faster to read and **2.3× larger**. There is no trade.
Whole JSON frames are what a dictionary coder is for, and lz4 leaves most of it
on the table.

On the tape the ratio half is nearly right — lz4 barely compresses, 84.2%,
because the columnar encodings already did the work — but the speed half is
answered by the only column that matters:

**The windowed read is flat: 1.16, 1.32, 1.32 ms.**

A query asks for a minute inside a day, so row-group pruning decompresses one
or two groups. **The decompression rate the note reasoned about is multiplied
by a quantity pruning already made small.** zstd halves the file for nothing a
reader can feel.

The full-scan cost is real — 11.6 ms against 4.7 — but a full scan of the tape
is a rebuild's shape, and the tape is a cache rebuilt from the archive anyway.

So `Zstd` on both, which is what both callers already passed. **The change is
that it is measured rather than defaulted**, and the reasoning that would have
moved it is recorded as refuted rather than left standing to be acted on later.

### What the instrument does NOT resolve

**Write time.** Tape figures swung between runs — 720.9 / 401.5 / 378.2 in one
pass, within 40 ms of each other in another — with no consistent ordering by
codec. Three segments of 1.3 MB are dominated by file creation and the atomic
rename. Recorded as unresolved rather than quoted, because a number from an
instrument that does not resolve it is worse than no number. The archive is the
one place compression is visible in the write: about 87 ms over 278 MB, for 17×
the ratio.

### And the row-group byte bound

`MAX_ROW_GROUP_ROWS` asked for encoded bytes per dataset. From the tape's own
parquet metadata:

| dataset | rows | bytes/row | 16,384 rows ≈ |
|---|---|---|---|
| transfers | 22,946 | 35.47 | 567 KiB |
| mints | 14,843 | 37.55 | 601 KiB |

**567–601 KiB**, sane by any general guidance, and close enough between the two
that a byte bound would change nothing for either. It stays unset — now for a
measured reason instead of an unanswered one.

What would turn it is named: **a wide dataset**. A `book` row is many times a
transfer's width, and the row-count table was measured on `quotes`. The first
partition of real book segments is that measurement.

## The panic boundary costs under 20 ns — 2026-09-21

`ingest` normalises inside `catch_unwind`, and that had been an open question
since Tier 1 on the strength of a Servo profile showing roughly half a hot loop
inside `__rust_try`, `__rust_maybe_catch_panic` and `PANIC_COUNT`.

### The end-to-end instrument cannot resolve it, and that IS the result

`cargo run --release --example unwind-cost` — real archived payloads through
the real chain normaliser, in two shapes, because the concern is a per-*call*
cost and a chain response is 26 MB:

| shape | run 1 | run 2 |
|---|---|---|
| as archived, 10 × 26 MB | +0.25% | −0.96% |
| split, 38,700 × ~800 B | −12.3% | −16.8% |

**The split figures are negative** — the boundary measuring *faster* than no
boundary. It does not make work faster. The variance simply swamps the effect.

### And a harness defect nearly became a finding

The first version ran `caught` first in every round. Across three repeats:

```
  +30.84%    −37.70%    −1.34%
```

**A single run said +30% and looked exactly like the regression the roadmap
feared.** Three runs said it was noise.

Two rules out of that. *One run of a benchmark is a number, not a measurement.*
And a harness that always runs one arm first hands the other a warm allocator —
so the arms now alternate which goes first.

### The figure, from an isolated measurement

Two million iterations of a virtual call returning a `Vec`:

```
  bare 36.3 ns/call   caught 29.2 ns/call   boundary  −7.0 ns
  bare 34.4 ns/call   caught 51.4 ns/call   boundary +17.0 ns
```

**Between −7 and +17 ns per call**, itself within noise and bounded under 20.
Against a normalise of hundreds of nanoseconds that is a fraction of a percent.

The Servo figure is superseded: `catch_unwind` was refactored so LLVM inlines
the try closure into the happy path, making it zero-cost unless a panic is
actually thrown.

### The mitigation is not taken, and that is the decision

The design pre-committed to one *if it proved costly*: wrap a **batch** of
frames rather than each frame. Not taken. Batching means buffering payloads
before normalising, which delays the anomaly a failed parse publishes and makes
one bad payload cost the batch's parses — real complexity and a real
behavioural change, bought for an effect no instrument here can detect.

Refiled in the code from *waiting* to *measured unnecessary*, with the figure.

## Three feature combinations did not build — 2026-09-21

Tier 10 asks for the `Adapter` seam documented as the out-of-tree extension
point. Writing that documentation found two things the documentation would
have been wrong about.

### The README advertised a feature that did not exist

```sh
cargo add galata-datawatch --features rh-chain
```

**There is no `rh-chain` feature.** `hyperliquid` and `rh-crypto` are features;
`pub mod rh_chain;` was unconditional. That command fails — cargo errors on an
unknown feature — so the README's central claim about venues was undeliverable
for one of the three.

Making it real exposed the second thing.

### The combinations between all-on and all-off were never built

`check-all.sh` builds `--all-features` and `--no-default-features`, and
`check-workspace-deps.sh` holds the dependency walls. Nothing built anything in
between, and **three combinations did not compile** — two of which predate this
change and would have failed at any point in the last five tiers:

| combination | what broke |
|---|---|
| `capture` alone | `match` over an `AdapterConfig` with no variants |
| `capture, rh-chain` | `match` over a `History` with no variants |
| `hyperliquid` without `capture` | reached the capture-gated `client` module, twice |

The first two are the same Rust subtlety: **a match through a reference to an
empty enum is not exhaustive**, because unreachability is not inferred through
the reference. The third is the feature split's own claim failing — `wire` and
`normalise` are supposed to be pure and runtime-free, and `client()` and
`funding_page_end` sat outside the gate that says so.

Making `rh-chain` a feature caused none of these. It simply produced
combinations nobody had built.

`check-feature-matrix.sh` now builds nine of them on every `check-all`, and was
watched going red on a removed `cfg`.

### Two guards were keyed too tightly to survive it

Gating the chain meant `#[cfg(all(test, feature = "rh-chain"))]` on a test
module and `#[cfg(feature = "rh-chain")]` in `capture/mod.rs`. Both tripped
guards that were right in spirit and wrong in their pattern:

- **`check-endpoint-reach` and `check-secret-reach` split on a literal
  `#[cfg(test)]`**, so a feature-gated test module was no longer recognised as
  tests — and the failure was *backwards*: the guard went red on an assertion
  written to prove its own rule. Both now match any `cfg` predicate mentioning
  `test`, which `check-venue-boundary` had already learned to do.
- **`check-venue-boundary` greps for a venue's name in quotes**, and a
  `#[cfg(feature = "rh-chain")]` contains one. But a feature name in a `cfg` is
  a **build-time gate, not a runtime dispatch** — the loop still holds a
  `dyn Adapter` and still cannot tell two venues apart. `cfg` attribute lines
  are now skipped.

### And docs.rs would have documented the default features only

Venues are features and docs.rs builds defaults unless told otherwise, so the
page for the crate whose README says *venues are features* would have been
missing one. `all-features = true` and `--cfg docsrs` on all four crates, and
`check-release-hygiene.sh` now requires both.

**The widely-copied incantation for this is obsolete.** Every guide says
`#![cfg_attr(docsrs, feature(doc_auto_cfg))]`; that feature was **removed in
Rust 1.92** and merged into `doc_cfg`, so the build fails with `E0557`. Caught
only by building the docs locally, which is worth doing before publishing
rather than after.

The badges are verified present in the generated HTML for `capture`,
`hyperliquid` and `rh-crypto`.

## `bbo` is 4x the bytes of the book it replaced — 2026-09-21

The roadmap deferred this to a soak: *unmeasured, and possibly larger than the
`l2Book` it replaces.* Both channels, same three coins, one socket, 75 seconds:

| channel | messages | bytes | msg/s | KiB/s | mean |
|---|---|---|---|---|---|
| `bbo` | 1,806 | 257,880 | 24.08 | **3.36** | 143 B |
| `l2Book` | 42 | 64,687 | 0.56 | 0.84 | 1,540 B |

**4.0× the bytes, 43× the messages.** The worry was right.

### Two independent cross-checks

- `l2Book` arrived once every **5.36 s** per coin, against the **5.27 s**
  throttle measured separately back in Tier 1.
- Scaled to six instruments this is 6.72 KiB/s; the 90-second archive capture
  recorded **5.3 KiB/s** for `quotes` across its six — the same number from a
  different instrument on a different market minute.

A figure that two unrelated measurements agree on is a figure.

### The config stated the reason backwards

It said `bbo` *is emitted ONLY when the top of book changes on a block, so its
rate is **bounded** by block cadence and by change* — which reads as an
argument that it is cheaper. **It is not.** The top of book changes far more
often than every 5.27 s, so event-driven is the **cost**.

What it buys is the dataset. `bbo` showed **43× as many distinct tops**; a
snapshot every 5.27 s shows one top in 43 and cannot say what happened between
them. *The top of book as it moved* is what `quotes` is, not a sample of it.

So the choice stands and the reasoning is corrected: **4× the bytes for 43× the
resolution.**

### The bill

```text
  6 instruments, quotes    6.72 KiB/s    567 MiB/day raw
  at zstd 5.8% (measured)                 32.9 MiB/day
                                           5.5 MiB/day/ticker
```

The predecessor's candles were 44 MB/day/ticker. Full-resolution quotes cost
**an eighth of that**, which is what makes the trade easy.

## Tier 7's entry question, closed — 2026-09-21

> Capture rh-chain at the head, or only at finality? Recommended: at the head,
> with the reader bounded at finalized and reorgs written as rows. **Not yet
> confirmed.**

| the recommendation | what holds it |
|---|---|
| capture at the head | `one_pass` plans from the cursor to `eth_blockNumber` |
| reader bounded at finalized | the measured 11,678-block lag on `Transport::Cursor` |
| reorgs written as rows | `Event::Reorg` through the one path |
| …and usable | `crate::reorg` — **which only landed today** |

The last row is why this stayed open rather than merely unticked. **Capturing
at the head means the record holds rows the chain later replaces**, and until
something said *which*, a reader held the contradiction with no way to apply
it. That was the missing piece.

## The decimal rule is now held by the build — 2026-09-21

`Num` is `rust_decimal` with `serde-str`, so an amount round-trips as text and
never through a double. That rule is the oldest in the tree and was held
**entirely by everyone remembering**.

Three ways to break it, none broken today, all now guarded:

| break | why nothing would notice |
|---|---|
| an `f32`/`f64` field in `galata-wire` | the vocabulary crosses the bus, the parquet schema and the HTTP contract — wrong in all three at once |
| `rust_decimal`'s `serde-float` feature | every `Num` serialises through a double with **no type change, no call-site change and no warning** |
| a `Float` arrow column | the tape asserts this in a test, which covers the tape and not `galata-segments` |

The second is the dangerous one. The workspace manifest has always carried a
comment saying `serde-str` *is not a default and is load-bearing*; nothing
checked that the comment was obeyed. `rust_decimal`'s own documentation says
not to enable `serde-float` for precision-critical data, and the guard now says
the same thing in a form that fails.

**A guard is worth writing while the rule still holds.** Afterwards the rows
are written and unrecoverable — a double that has lost digits cannot say which
ones.

The guard also requires `serde-str` to be *present*, not merely `serde-float`
absent. Without it a `Num` serialises as a JSON number, and the loss happens in
the consumer's parser rather than here, which is worse: the bytes this tree
wrote were right.

## Compaction was destroying rows, and the legacy review found it — 2026-09-21

A mechanical diff of the legacy datawatch slice against this tree: **183 of its
254 public names carried over**, most of the rest being trading-system types
the re-cut deliberately dropped — `VenueFill`, `AccountTruth`,
`FILL_ARCHIVE_SCHEMA`. One looked like a real capability: `finish_interrupted`.

It turned out to be **present and better placed** — folded into
`compact_partition`, which removes what a replacement already holds *before*
merging, so resumption is automatic rather than a call somebody has to
remember. An improvement on legacy, not a gap.

But reading that code to confirm it found two defects in it.

### One: an identical range is not containment, and compaction deleted it

`superseded` marked a segment contained by a wider one:

```rust
  w.first <= c.first && w.last >= c.last
```

**`<=` and `>=` alone call an *identical* range contained.** On the archive an
identical range is legal and common — this tree already learned it once:
twenty-four gaps flushed in one microsecond share `[t, t]` and are told apart
by pid and flush sequence. **They hold different rows.**

And `compact_partition` removes the doomed set *before* it merges anything, so
the removed segment was never merged in. Proved with real rows:

```
  two flushes at t=100, 3 rows and 5 rows
  after compact_partition:  3 rows
  assertion failed: left 3, right 8
```

**Five rows of eight, gone from the archive** — the one store that cannot be
rebuilt. `galata-compact` does this today on any partition holding two flushes
that share a microsecond, which the gap path produces routinely.

The fix is strict containment: wider in at least one direction. The cost is a
corner that stays undetected — a compaction interrupted in a partition whose
segments all share one range leaves a replacement with that same range, and
this will not see it. **Failing to spot an interruption costs a duplicate read;
deleting an unmerged segment costs the rows.**

### Two: the sweep walked past a container's first victim

`superseded` is an ordered sweep, which needs a container to be seen before
what it contains. The listing sorts by `(first, last)` **ascending**, so
`[100,199]` precedes `[100,299]` and escaped.

Worse than missing one: compaction would then merge the container back in with
a segment it already held, **doubling those rows**. Fixed by sorting the last
position descending, on a copy — every other caller wants range order.

### And the window before repair is no longer silent

Compaction repairs an interruption on its next sweep. Between the crash and the
sweep, nothing said it was there.

`galata-watch` covers the tape, via `check_layout`'s overlapping-sequence rule.
For the archive it deliberately does not — running the tape's rule there was
this watcher's first mistake, and the real archive found it: those same
twenty-four gaps were reported as forty-six redeliveries.

**That lesson is right and it closed the door on a different check that is
sound.** Nesting is not overlap. Two segments sharing a range is ordinary here;
one *containing* another cannot arise from concurrent flushes at all, because a
writer flushes in receipt order and a tape in sequence order, so segments
written normally abut. `galata-watch` now reports containment on **both**
stores and still reports overlap on only one.

It matters because a rebuild over an un-repaired archive partition doubles
those rows **silently**: the duplicated payloads keep their sequences, so
nothing downstream overlaps either.

## A soak found a livelock in the chain cursor — 2026-09-21

Twenty-five minutes of rh-chain capture beside a healthy hyperliquid one. The
chain stopped advancing:

```
  a range failed from=63352032 to=63353031
    error=rh-chain eth_getLogs: the node said
      {"code":-32000,"message":"logs matched by query exceeds limit of 50000"}
```

**Eighteen times, the same range**, until the process was killed.

The cursor deliberately does not advance past a failed range — advancing turns
a retryable hole into a permanent one, silently. That rule is right for a
timeout or a `429`. **It is wrong for this one**: a 1,000-block span holding
more than 50,000 logs holds more than 50,000 logs on every retry, for ever.

### The node states a row cap, and no block span satisfies it

`MAX_BLOCK_SPAN = 1_000` was documented as *conservative against a public node
that states no limit and enforces one by timing out*. The node does state a
limit, in the error — and **it is a row limit**. A busy stretch of chain
produces more logs per block, so no fixed span is right for both a quiet
stretch and a busy one.

Asked of the live node, on the exact range that hung:

| span | result |
|---|---|
| 1,000 | **refused** — exceeds limit of 50000 |
| 500 | ok, **26,224 logs** |
| 250 | ok, 13,611 logs |
| 125 | ok, 6,120 logs |

**One halving clears it.** The loop was one arithmetic operation away from
carrying on, and instead stood still.

### What changed

The refusal is classified. *Exceeds limit* and *timed out* are narrowable —
less to gather is less to time out on. A `429` is **not**: the range was
acceptable and the request was too soon, which the backoff already answers, and
narrowing there would make *more* requests at exactly the wrong moment.

The span halves on a narrowable refusal and doubles back towards the declared
cap after a clean pass — doubling rather than restoring, because a busy stretch
is a stretch and going straight back would refuse again on the next pass over
the same neighbourhood.

Narrowing has a floor. At one block the provider cannot serve this chain at
all, and capture says so and exits rather than spinning. **A `Gap` would have
been the wrong row**: its bounds are venue time, a `getLogs` response carries
none, and inventing one is what this tree refuses everywhere else.

### The rule the soak illustrates

The cursor's *do not advance past a failure* rule was correct and **incomplete**
— it distinguished failed from succeeded and not retryable from unretryable.
A loop that cannot tell those apart either loses data or stops, and this one
stopped, which is the better of the two and still not right.

## Compaction is transparent to a rebuild — 2026-09-21

The compaction fix above was proved by unit tests on a constructed case. This
is the same code over the soak's real archive.

Two identical snapshots of it. One compacted, one left alone, then both
rebuilt over the same window:

```
  compaction        3,403 segments -> 8
  archive rows      86,384 -> 86,384          nothing lost
  rebuild from A    86,379 payloads -> 151,819 rows, 15 segments, 0 unparsed
  rebuild from B    86,379 payloads -> 151,819 rows, 15 segments, 0 unparsed
  all 15 tape segments                        BYTE-IDENTICAL
```

**A tape built from a compacted archive is bit-for-bit the tape built from the
uncompacted one.** That is the property worth having: compaction is a storage
decision, and a storage decision that changed what a reader sees would not be
one.

### The soak did not reproduce the bug, and that is worth saying

The archive it produced holds **zero** partitions with two segments sharing a
time range, so the row-destroying case never arose here. It needs events
*generated* in one flush — the gap path writes a batch that shares a
microsecond — and this soak had no gaps.

So the real archive validates the fix without exercising the defect. **A clean
run over real data is not evidence that a bug is absent**, only that this run
did not meet it, and the unit tests are what hold that one.

## Four binaries reported compliance for a store they never read — 2026-09-21

Continuing the legacy review. Legacy's `retain` carries a type this tree does
not:

> One root that could not be scanned. **A missing store is a refusal naming the
> path, never an empty sweep that looks like compliance.**

Absent here, in four binaries. Every listing in `galata-segments` answers an
unreadable directory with an empty result — right for a *subtree*, since a
partition that vanished mid-walk is no reason to abandon the others, and wrong
for a **declared root**: no partitions means no candidates, which each binary
reports as *nothing to do* and exits 3.

A mistyped path, an unmounted volume or a permissions change is then
indistinguishable from a tidy store. A retention job on a cron would report
success for ever while expiring nothing.

### And compaction made the typo real

Pointing all four at a nonexistent path, before the fix:

```
  galata-retain        exit 1
  galata-compact       exit 3      ← "nothing to compact"
  galata-watch         exit 1
  galata-tape-rebuild  exit 3      ← "nothing to rebuild"
```

The last two were caused by the second. **`hold` creates the directory it
locks**, so `galata-compact` created the mistyped path — after which it is a
real, empty, perfectly scannable store, and `watch` and `tape-rebuild`
*honestly* agreed there was nothing to do.

So the check has to come before anything that could create a store, which is a
stronger statement than *check first*.

After:

```
  all four               exit 1
  cannot scan …/typo-archive: No such file or directory (os error 2).
  A store that cannot be read is not a store with nothing in it
  the path was not created
```

### The tree already knew

Three guard scripts have said it about themselves since Tier 0, verbatim:

> A guard handed a root it cannot scan reports success forever.
> … refusing to scan nothing and call it ok

It was applied to the scripts that check the code and not to the binaries that
act on the record. `check-scannable-roots.sh` now holds it for the binaries,
and `galata-datawatch` is exempt because capture **writes** its store and
creates it on first run.

### A store that does not exist yet is refused too

A fresh install is a one-time failure that says exactly what to do. A typo is
silent for months. **Nothing can tell them apart from the outside**, so the
noisy reading is the right one.

## A refusal that names one thing at a time — 2026-09-21

The legacy review again, in the reader. Legacy carried `unwritten_scopes`,
*exposed because nothing has happened yet and this venue is missing while the
others are live are different facts, and only the caller knows which one
matters to it.*

The **rule** it supports is carried here and is right: a declared scope that
has written nothing means no bound, not *ignore that one* — which is the
silently-holed read arrived at by a different route. Two things around it were
not.

### The refusal named only the first

`Bound::of` returned on the first unwritten scope it found. A caller with three
misconfigured scopes fixes one, re-runs, meets the next, and repeats — and
determining the full set costs one pass over the listing either way. It now
names all of them.

### And a caller could not ask before opening

Without a way to distinguish *nothing has happened yet* from *this one is
missing while the others are live*, a caller reaches for the filesystem.

**This tree had already done it.** The `superseded` example stats a directory
to decide whether a venue ever recorded a reorganisation:

```rust
  let ever_reorganised = Path::new(&root).join("kind=reorgs").is_dir();
```

That reimplements a rule the store owns, and gets it subtly wrong: **a
partition can exist and hold nothing**, so a directory left behind by a
rebuild that wrote no rows would read as *this venue has reorganised*. The
example now asks `tape::unwritten` and gives the same answer for the right
reason.

The gap was visible in this tree's own code and I wrote that line myself. A
missing query does not announce itself — it shows up as a caller doing the
store's job badly, which reads as ordinary code until something names the
query it should have been.

## `subs_held` said *delivering* and meant *sent* — 2026-09-21

The legacy review reached `capture/subscriptions.rs`, where legacy and this
tree disagree about whether a refusal survives a reconnect. Reading that
argument out found that **neither side of it occurs**, and something larger
underneath.

### The field claimed what the code did not do

```rust
/// How many subscriptions the venue is delivering.
pub subs_held: usize,
```

and the loop:

```rust
for subscription in &convergence.to_subscribe {
    self.held.mark_sent(subscription);
    // This venue confirms by delivering rather than by
    // acknowledging per subscription, so a sent subscription is
    // held until something says otherwise.
    self.held.mark_held(subscription);
}
```

Held on **send**. The surface would read `24/24` even if the venue delivered
none of them — and in the soak it read 24/24 and happened to be right, which is
the worst way for a field to be right.

**The comment justifying it is false.** The soak archived **96
`subscriptionResponse` frames**, each echoing one subscription:

```json
{"channel":"subscriptionResponse","data":{"method":"subscribe",
 "subscription":{"type":"activeAssetCtx","coin":"BTC"}}}
```

This venue acknowledges per subscription. The loop was told it does not — and
it would not have mattered either way, because **neither sending nor being
acknowledged is delivering**.

### Held now means delivering, and it is visible

A payload arriving for a declared `(ticker, series)` is what marks it held. No
seam change: the loop already resolves both from every payload, one line above,
to record coverage.

Watched on a live run, polling the status file every four seconds:

```text
  subs_held  6/24   pairs live  6
  subs_held 17/24   pairs live 17
  subs_held 24/24   pairs live 24
  subs_held 24/24   pairs live 24
```

**Eight seconds of climb**, where before there was an instant 24. The climb is
the information: a subscription the venue silently ignored would stop the count
short and stay short.

### Two outcomes had never occurred

- `Outcome::Pending` — `mark_sent` and `mark_held` ran in the same statement,
  so nothing was ever pending. It is now reachable and means *sent, nothing
  back yet*.
- `Outcome::Refused` — `mark_refused` has no caller outside its own tests.
  Hyperliquid does not refuse: an unlisted coin makes it **hang up**, taking
  every other subscription with it, measured at seventeen disconnections in
  eighteen seconds. That is why the universe check runs before anything
  connects.

Which makes the whole `connection_lost` question — *does a refusal survive a
reconnect?* — **an argument about a state that has never occurred.** Legacy
answered it one way, this tree the other, each with a test, and neither answer
has ever been exercised. Now recorded as that rather than presented as settled:
the first venue that actually refuses a subscription is what settles it.

## The handover lost a second of every instrument, every eight minutes — 2026-09-21

The legacy review reached `capture/session.rs`, where legacy says:

> The replacement is subscribed and delivering. **Only now** may the one it
> replaces be closed.

This tree has the whole state machine, and three separate doc comments
promising exactly that — on `ConnectionPolicy::RotateAhead`, on
`Act::OpenReplacement`, and on `Capture::run`. **The loop did the opposite.**

```rust
Act::OpenReplacement | Act::CloseReplaced => {
    // Reconnecting from the top of this loop opens and
    // subscribes the replacement before this one stops delivering;
    // the handover is therefore covered and publishes no gap.
    self.coverage.handover_completed(now);
    break;
}
```

Both acts collapsed into one, breaking to reconnect **the single socket it
holds** — and `StreamSource::connect` replaces any connection it held, so the
old one closes before the new one exists. `handover_completed` then advanced
every pair's coverage to that instant, which is what suppressed the gap.

### Measured, in the record the soak already had

Hyperliquid rotates every **eight minutes** (`ROTATE_AFTER_SECS = 8 * 60`,
inside a measured lifetime of 10 m 24 s). Arrival gaps in `kind=quotes`:

| minutes into run | lost |
|---|---|
| 8.01 | 935 ms |
| 16.01 | 765 ms |
| 24.03 | 1,292 ms |

**Three rotations, three holes, each at an exact boundary — and the run
reported zero gaps.** The clean soak reported two entries above was clean
because the loop said so.

### And after

A ten-minute run across one rotation, same query:

```text
  minutes into run   largest gap
  ───────────────────────────────
      7.938            458 ms
      7.999            437 ms
      8.007            459 ms   ← the rotation
      8.107            458 ms
```

**The rotation is no longer visible.** 459 ms against a 450 ms noise floor,
where before it was 935 ms against the same floor. The largest gap anywhere in
the run is 561 ms, at 6.51 minutes, which is a quiet market.

### The duplicate I predicted did not happen

Holding two subscribed sockets should mean the replacement receives frames the
old one also delivered, for the length of the overlap. Near the handover:

```text
  990 payloads, 990 distinct bodies, 0 repeats
```

Because `act()` returns `CloseReplaced` on the very next tick, the overlap is a
single loop iteration — the replacement is subscribed and taken over inside a
few milliseconds, and the venue sends it nothing in between. Recorded because
**a predicted cost that does not appear is worth as much as one that does**,
and the prediction would otherwise stand as a reason not to do this.

### What this says about a promise written three times

The claim was in three doc comments and one policy type, each stating it as
settled. None of them was checked against the record, and the record had the
answer the whole time — in a query of eleven lines over data already captured.

## `count_1m` was between 55% and 63% of a minute — 2026-09-21

The last capture area in the legacy review. Legacy's coverage carried
`window_age_micros` — *how long the current counting window has been open* —
beside **one window shared by every pair**. This tree had neither: each pair's
window began at that pair's first message and rolled on its own schedule.

So the name asserted a minute, the value was whatever had arrived since that
pair's window opened, and two counts on one snapshot were over different spans.

### Measured against the record

A snapshot, and what actually arrived in the sixty seconds before it:

| pair | `count_1m` said | arrived in 60 s | |
|---|---|---|---|
| BTC trades | 181 | 328 | 55% |
| BTC quotes | 379 | 616 | 62% |
| BTC candles | 74 | 123 | 60% |
| BTC funding | 37 | 59 | 63% |

**Differing per pair**, so an operator comparing BTC trades against BTC quotes
was comparing 33 seconds against 37 without being told.

### After

One window for all of them, its age stated, and the field named `count`:

```text
  count_window_secs 37

  pair            count   arrived in the stated window
  ───────────────────────────────────────────────────
  BTC trades        157        156
  BTC quotes        422        412
  BTC candles        80         79
  BTC funding        36         36
```

The residual is the window being truncated to whole seconds — the true window
was 37-point-something and the query used 37 flat. **2.4% at thirty-seven
seconds, shrinking as the window fills**, and now written on the field rather
than left to be discovered. A denominator with an unstated error is the thing
the field exists to remove.

### Three fields, one shape of mistake

This is the third field on this surface whose **name asserted more than the
code delivered**:

| field | said | meant |
|---|---|---|
| `subs_held` | the venue is delivering it | it was sent |
| `count_1m` | a minute | 55–63% of one, varying |
| coverage across a handover | continuous | reconnected, and a second lost |

All three were found by asking the record what the surface claimed, which is a
cheaper check than it sounds: each was a query of a dozen lines over data
already captured. **None would have been found by reading the code**, because
in each case the code matched its own comment — and the comment was the thing
that was wrong.

## The latency, and the tail that was not latency — 2026-09-21

Continuing the audit, this time on a claim that has never had a number. The
tree carries `at_micros` and `recv_micros` on every row because *the difference
between them is the number that matters* — and has never stated it.

### The measurement, and its tail

| series | n | p50 | p95 | p99 | max |
|---|---|---|---|---|---|
| quotes | 49,141 | 330 ms | 569 ms | 731 ms | 1,066 ms |
| trades | 31,653 | 350 ms | 736 ms | **10,325 ms** | **197,983 ms** |

A trade arriving **three minutes** after the venue timestamped it is not
latency.

### It is redelivery, and it is scheduled

Hyperliquid sends recent trade history on every `subscribe`, and the session
rotates every eight minutes. So each rotation redelivers trades already
captured:

```text
  minute of run    redelivered
  ─────────────────────────────
        8              163
       16              161
       24              167
```

**491 rows, 1.55% of the dataset, growing with run length.** Anyone summing
volume without grouping on `trade_id` overstates it by that much.

The design was already right: `trade_id` exists *so two receipts of one trade
are one trade*, and on this venue it is **never null** — 0 of 31,653. What was
missing is that redelivery actually happens, on a timer, with a number.

### The real latency

Excluding redelivery and the opening burst, both series agree:

| series | p50 | p95 | p99 | max |
|---|---|---|---|---|
| quotes | 330 ms | 569 ms | 731 ms | 1,066 ms |
| trades | **346 ms** | **628 ms** | **806 ms** | 1,445 ms |

**A third of a second, median, from venue clock to ours**, and under a second
and a half at worst. The 10 s p99 and the 198 s max were artefacts of counting
a redelivery as a late arrival.

### A candle recurring is not a trade redelivered

Candles repeat `(ticker, at_micros, interval)` at **17.7%**, which is not the
same thing: the live channel re-sends the open bar as it fills, and the
historical walk covers the same bars again. Each row is that bar *as it stood*.
A consumer takes the last by `recv_micros` per key. Quotes repeat **0** times
in 49,141 rows.

Three datasets, three different right answers — which is why *deduplicate the
tape* would have been the wrong fix for any of them.

### A correction

Two entries above I checked for duplicates across a handover and reported
**zero**. That check read `kind=quotes` payload bodies only, where there is
indeed no redelivery. It did not cover trades, which is where the redelivery
is. The claim was true of what it examined and narrower than it sounded.

### And one claim that was exactly right

`buffered` says it is *the live size of the window a crash would convert into a
gap*. Tested with a real `kill -9` at a moment when it read **106**:

```text
  696 payloads durable
    0 payloads durable after the last flush
  newest durable receipt   203,015 µs BEFORE the flush instant
```

and on restart, 24 gaps of cause `crash_unflushed` beginning at **exactly
−203,015 µs** relative to that flush — the same microsecond, arrived at
independently. **The gap is dated from the last durable receipt**, as designed,
not from when the loss was noticed. Recorded because after three claims that
overstated, one that holds to the microsecond is worth saying out loud.

## A window roll read as five instruments going quiet — 2026-09-21

The audit's next field, and this one I broke myself in the entry above.

`PairState::Live` was decided by *is the count above zero*, and the count is a
**tumbling** window — it resets. So every pair that had not spoken since the
reset read as stale **because of the reset**.

Sharing one counting window across pairs, which the previous change did so the
counts would be comparable, turned a staggered and invisible version of this
into a synchronised and obvious one. Sampled every second across a roll:

```text
  window=60s  live=24  stale= 0
  window= 1s  live=19  stale= 5    ← the counter reset
  window= 4s  live=20  stale= 4
  window= 7s  live=23  stale= 1
  window= 8s  live=24  stale= 0
```

**Five of twenty-four pairs, for eight seconds, once a minute, every one of
them healthy.**

### The documentation already said what it should mean

> `Stale` says nothing arrived in the counting window; it does not say that is
> wrong. A quiet instrument at four in the morning is stale and healthy.

The failure is in the word *counting*. Staleness is a question about **time
since the last message** — a trailing window, which does not reset — and the
count needs a tumbling one because a count must reset to be a count. Two
questions, two windows, one of them borrowed for the other.

### After

```text
  window=60s  live=24  stale= 0
  window= 1s  live=24  stale= 0
  window= 7s  live=24  stale= 0
```

### What this one adds to the pattern

The three before it were claims that had always been wrong. **This one I
introduced**, in the change immediately before, while fixing a different claim
on the same surface — and it was visible within a minute of looking, by the
same method that found the others.

A fix that makes an existing flaw *more visible* is not a regression, but it is
not finished either. Sharing the window was right; it exposed a second thing
borrowing that window for a question it could not answer.

## The store's own claims: two hold, one was unstated — 2026-09-21

The audit, turned from the status surface onto the record.

### `stream_seq` survives a crash

The Tier 3 fix was verified on **two clean restarts**. Re-checked on the
harder case, across the `kill -9` from the entry above:

```text
  1,819 payloads, 1,819 distinct sequences, 0 collisions
```

### No venue time is invented

Across 117,451 rows, `at_micros` is **never** equal to `recv_micros` and
**never** ahead of it. The minimum venue-to-receipt difference is 239 ms for
quotes and trades and 392 ms for candles — all positive, all plausible. If a
time were being manufactured from our clock, those would be zero.

### And a third that is true, and nobody had said how much

`at_micros` is nullable on purpose — *an event the venue did not timestamp is
not at any venue time, and giving it ours would make a latency of zero out of
an absence of information*. Correct, and the size of it was never written
down:

| dataset | rows | with no venue time |
|---|---|---|
| `marks` | 8,538 | **100.0%** |
| `funding` | 9,546 | **89.4%** |
| `quotes` | 49,141 | 0.0% |
| `trades` | 31,653 | 0.0% |

`marks` and the live half of `funding` come from one channel that carries no
timestamp at all; funding's other 1,008 rows come from the historical walk,
which does.

**The design is right and the consequence is a trap.** `Reader::view` keeps
such a row once its partition is in range — that is tested. But the README
invites a reader to query the parquet directly, and a hand-written
`WHERE at_micros BETWEEN …` drops **every row of `marks`** and says nothing.

So the README now says how to read the store before it says how to build it,
which it never did: the null venue times, and the redelivery from the entry
above. Both are things a consumer meets on their first real query.

### What the audit looks like from here

Six claims examined across the surface and the store. **Four were wrong**, all
four in the direction of claiming more than was true. **Two were right**, and
one of those — `buffered` — right to the microsecond.

The four wrong ones shared a shape: the code matched its own comment, and the
comment was the thing that was wrong. None was findable by reading. Each took
a query of about a dozen lines against data already on disk.

## The broker asymmetry, exercised at last — 2026-09-21

Three claims that no soak had ever run. Every capture in this tree so far has
had no `[broker]` block, so **`sink_dropped` has read 0 in every status file
ever produced** — which is indistinguishable from a counter that does not work.

Tested against `nats-server` 2.14.5, loaded with this tree's own generated
grant table.

### Absent → warn and carry on

```text
  WARN broker at nats://127.0.0.1:4919 did not answer:
       IO error: Connection refused (os error 61)
  still running, 25 segments captured
```

### Refused → exit non-zero, and say what to check

```text
  exit=1
  broker at nats://127.0.0.1:4919 refused identity datawatch-hyperliquid:
    authorization violation.
    The grant table and this process disagree. Check that
    GALATA_BROKER_PASSWORD_DATAWATCH_HYPERLIQUID is set, and that the table
    loaded into the server grants that identity.
```

Not merely non-zero: it names the identity, the variable, and both places the
disagreement can live.

### Events reach the bus, and the decimal discipline is visible on it

```json
{"address":{"Venue":{"venue":"hyperliquid","ticker":"BTC"}},
 "seq":1790001581010278,"at_micros":1790001592151000,
 "event":{"Quote":{"bid_px":"85851.0","ask_px":"85852.0", …}}}
```

`"85851.0"` — a **string**, not a JSON number, on the wire where a consumer's
parser would otherwise turn it into a double. That is `serde-str` doing the one
job it is there for, and this is the first time it has been seen leaving the
process.

### Never blocks, and drops only after the queue

The server killed mid-run, capture left alone:

| | |
|---|---|
| capture | still running |
| archive | 37 → 300 segments, **10,538 rows** written while the broker was dead |
| `buffered` | 4 — the record never noticed |
| `sink_dropped` after 25 s | **0** — the queue absorbing, nothing lost yet |
| `sink_dropped` after 115 s | **2,754** |

**The counter moves only after the declared queue is exhausted**, which is
exactly what the declaration says it is for: 8,192 events at roughly a hundred
a second is about eighty seconds of outage absorbed before a single event is
lost.

And the reporting is edge-triggered, as designed — one line per transition
rather than one per payload:

```text
  WARN  the sink is refusing events; payloads are still recorded
  INFO  the sink is taking events again
```

Those alternate while the broker is down, because the client's own buffer
drains and refills around the queue's limit. Worth knowing before reading a log
and concluding the broker recovered.

## Replay, run rather than reasoned about — 2026-09-21

The last unaudited surface, and the one whose claim is structural:

> Replay is **refused** durability, because replay reads the record back out
> and writing it would grow the thing it is reading.

Enforced by a type. `Replayed` is constructed only by `replay` and consumed
only by `ingest_replayed`; there is no `Origin::Replay` for a caller to set
wrongly, because the field was removed in favour of the route.

**A structural claim is the one most worth running.** The compiler agreeing
that a type is private is not the same as the file count not moving.

Over the soak's real archive:

```text
  3403 segments, 86384 payloads replayed
  151824 events emitted from the replay
  segments before 3403, after 3403         the record did not grow
  after one ordinary append: 3405          the write path works
```

### The control matters more than the result

*Nothing was written* and *writing is broken* are the same number. So the
ordinary path runs against the same archive object and must move what the
replay did not — otherwise the test proves only that something is broken.

### Writing the control found the guard holding

`Archive::append` is **private**, and the example was refused at compile time.
`check-ingest-callers.sh` exists to say *no caller reaches past the one path to
the record*, and here the language said it first. A guard that the language
also enforces is a guard that cannot be evaded by someone who does not know it
is there.

### And the control exercised the failure path by accident

The control payload was chosen to be unparseable, which produced two segments
rather than one:

```text
  kind=trades/date=1970-01-01/t-1_1_10718_1.parquet            the payload
  kind=trades/date=1970-01-01/failures/t-1_1_10718_2.parquet   the failure
```

| row | seq | channel | bytes | error |
|---|---|---|---|---|
| payload | 0 | control | 2 | — |
| failure | 0 | control | — | `unrecognised channel "control"` |

**The payload is archived although it would not parse** — filtering the record
by parse success would discard exactly the evidence a normalisation defect is
diagnosed from. The failure row **joins it on `seq`**, lives in a `failures/`
sibling, and carries **no payload column**. Three claims from Tier 1, none of
them the one being tested, all holding.

### The audit, closed

Fourteen claims examined across the status surface, the store, the broker and
replay.

| | |
|---|---|
| wrong | **4** — all four claiming more than was true |
| right | **10** — one of them to the microsecond |

Every one of the four was a case where **the code matched its own comment and
the comment was wrong**, so none was findable by reading. Each took about a
dozen lines of query against data already on disk — and three of the four were
found by asking a single question: *does the record agree with what the
surface says about it?*

## A rustdoc warning that hid for six changes — 2026-09-21

`cargo doc` had been reporting one warning since `doc_cfg` was turned on, and
it stayed through six changes because **rustdoc reports a redundant explicit
link without a file or a line**:

```text
  warning: redundant explicit link target
    = note: when a link's destination is not specified,
            the label is used to resolve intra-doc links
```

Two earlier attempts to find it went wrong in instructive ways. Stripping every
self-resolving-looking link at once produced **nine** warnings instead of one,
because most of those targets were genuinely needed. Fixing the two most likely
candidates individually turned each into an *unresolved* link — worse than the
warning being fixed.

Bisecting one at a time settled it in six builds: every candidate but one
gained an unresolved link when stripped, and the remaining one went to zero.

**Guessing cost more than bisecting would have**, twice, on a search space of
eight.

### And it is now held by the build

`check-docs.sh` runs `cargo doc` with `-D warnings`, the same bargain the rest
of the workspace makes. A broken intra-doc link is invisible locally and
permanent once published — the page shows ``[`Thing`]`` as literal text and
nobody who sees it can do anything about it.

Stable rather than nightly: `--cfg docsrs` only adds the feature badges, link
resolution is the same either way, and a guard needing a nightly toolchain is a
guard most machines skip.

Zero warnings across the workspace, and the guard was watched going red on a
link to something that does not exist.

## The handover fix, across three rotations — 2026-09-21

The defect was measured over **three** rotations and the fix validated on
**one**. That asymmetry is not a detail: a single clean handover is also what a
quiet minute looks like.

A 26-minute run, three handovers, zero warnings, 3,088 segments. The handovers
landed at 8.00, 16.00 and 24.01 minutes — and the largest gap within three
seconds of each:

| handover | before the fix | after |
|---|---|---|
| 1 — 8.00 min | 935 ms | **521 ms** |
| 2 — 16.00 min | 765 ms | **469 ms** |
| 3 — 24.01 min | 1,292 ms | **635 ms** |

### Against what?

The run's own gap distribution, over 60,923 intervals:

```text
  median      0 ms      quotes arrive in bursts
  p99       242 ms
  p99.9     545 ms
  max     1,762 ms      at 7.64 minutes — a quiet market, not a handover
```

**All three handover gaps sit at p99.9.** Two below it, one just above. Before
the fix they were 1.7×, 1.4× and 2.4× that figure, and each was the largest gap
in its neighbourhood by a clear margin.

The handover is no longer distinguishable from the market being quiet, which is
the strongest form the claim can take: not *smaller*, but *not findable*.

### Why bother, having already tested one

Because one measurement of a periodic effect cannot tell a fix from a lucky
sample, and the original defect was periodic. The comparison only means
anything because both sides used the same query over the same shape of run —
which is also why the before-figures were worth keeping rather than
summarising.

## What a publish would ship, and in what order — 2026-09-21

Publishing is postponed, which makes this the right moment: everything below
is checkable **before** the irreversible step, and useless after it.

### Two crates can be verified today, two cannot

```text
  galata-wire        no internal dependencies    packages and builds
  galata-segments    no internal dependencies    packages
  galata-broker      needs wire                  cannot package yet
  galata-datawatch   needs wire, segments, broker    cannot package yet
```

```text
  error: failed to prepare local package for uploading
  Caused by: no matching package named `galata-wire` found
```

**Correct, not a defect.** `cargo package` resolves dependencies from the
registry, and two of these are not on it. What it means is that **the publish
order is forced**, and getting it wrong fails partway through a sequence that
cannot be undone. That order was nowhere written down; it is now in the README.

### `--list` checks what `package` cannot

`cargo package --list` does not resolve dependencies, so it works for all four
— which makes *what would ship* holdable even for the crates that cannot yet be
built from a tarball.

`check-package.sh` holds three rules, each watched failing:

| rule | why |
|---|---|
| every crate declares `include` | without it cargo ships the directory minus gitignores — and **this tree's default archive root is `var/`**, which holds captured market data |
| the tarball carries its licence, README and source | `include` is a **whitelist**: a typo drops a file silently and the crate still publishes, just without its licence |
| nothing from `var/`, `target/` or a dotfile | the deny side, checked against the real file list rather than the whitelist's intent |

All four pass today. The first rule is the one with teeth: a crate that lost
its `include` line would publish somebody's order flow, permanently, and
nothing else in the tree would notice.

### And the plant is the point

The guard's plant removes `"/LICENSE-MIT"` from one `include` list. The crate
still builds. It still passes every other check. It still publishes — without
the licence it claims in its own manifest.

That is the shape of every publish defect worth guarding: **not something that
breaks, something that succeeds while being wrong.**

## Answered by reading, not by running

Recorded because a design question resolved from documentation is still not a
measurement, and the distinction matters when the answer turns out to be wrong.

| question | answer | source |
|---|---|---|
| does `bbo` carry sizes? | **yes** — it is functionally `l2Book` with `nLevels: 1, strict: true` | the venue's own subscription documentation |
| how often does `bbo` fire? | ~~bounded by block cadence and by change~~ — **24 msg/s over three coins, 4.0× the bytes of `l2Book`** | *superseded by measurement, 2026-09-21* |
| does `activeAssetCtx` cover the HIP-3 `xyz` dex? | ~~unresolved~~ **yes** — all three `xyz` instruments carry funding and marks | *answered by a soak, 2026-09-21* |

**Both of the deferred ones are now measured, and one of them was wrong.**

*How often `bbo` fires* was read from documentation as **bounded**, which reads
as an argument that it is cheap. Measured, it is 4.0× the bytes of the
`l2Book` it replaced. The reading was not false — it does fire only on change
— but the inference drawn from it was, and that is the failure mode this
section exists to catch.

*Whether `activeAssetCtx` covers the HIP-3 dex* is the one that would have
cost something: if it were main-dex only, CL, XYZ100 and GOLD would carry no
funding and no mark, and the declaration would have to say so rather than the
walk discovering it. A 24-minute soak settles it from the record rather than
from a probe:

```
  ticker    funding rows   distinct rates
  ──────────────────────────────────────
  BTC             1,591              332
  ETH             1,591              348
  HYPE            1,591               39
  CL              1,591              426   ← xyz
  XYZ100          1,591              292   ← xyz
  GOLD            1,591               91   ← xyz
```

Identical row counts and genuinely varying rates on all six. Not zeros, not
repeats — the HIP-3 instruments are served exactly as the main-dex ones are.

## The soak itself, 24 minutes and 24 subscriptions — 2026-09-21

Reported because *nothing went wrong* is a measurement too, and this tree has
recorded plenty of the other kind.

```
  24/24 subscriptions live      0 refused
  2,853 segments, 19 MB          0 warnings
  74,418 payloads → 135,535 rows 0 unparsed
  0 gaps                         0 dropped to the sink
  galata-watch: nothing to report (8 partitions)
```

The same soak livelocked the *chain* cursor within twenty-five minutes, which
is recorded above. **One venue clean and the other stuck, in one run**, is the
argument for soaking both at once rather than each alone.

## Was open, waiting on a soak — all three now measured

The soak happened. Each of these had carried a *not measured yet* note since
Tier 0 or Tier 1, and **two of the three refuted the reasoning they replaced**
— which is the argument for taking the figure rather than acting on the worry.

- ~~**What does `catch_unwind` cost on the one path?**~~ **Measured: under
  20 ns per call**, and the end-to-end instrument cannot resolve it — its
  variance is ±35%, orders of magnitude larger. The Servo figure is superseded
  by the inlining of the try closure into the happy path. The batching
  mitigation is **not** taken: it would delay the anomaly a failed parse
  publishes, for an effect nothing here can detect.

- ~~**A byte bound on row groups.**~~ **Measured**: 16,384 rows is 567 KiB of
  transfers and 601 KiB of mints, so a bound would change nothing for either.
  Still unset, and what would turn it is now named — a **wide** dataset, since
  the row count was measured on `quotes` and a `book` row is many times a
  transfer's width.
- ~~**Compression per store.**~~ **Measured, and it refuted the reasoning it
  replaced**: lz4 is 2.3× larger than zstd on the record for a 5% read saving,
  and on the tape the *windowed* read — the one that matters — is flat across
  every codec, because pruning bounds how much is ever decompressed. `Zstd` on
  both, now for a reason.
