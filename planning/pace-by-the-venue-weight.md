# pace-by-the-venue-weight

**BUILT 2026-09-29** (`pace-by-the-venue-weight`, archived), not yet
deployed. Named 2026-09-28, from switching on `walk_funding_days` in the
deployment (the last step of `walk-the-funding-history`), which the venue
answered with 429 on every instrument.

**What was built:** D, then B, as recommended below. The probe took 50 full
funding pages (2,250 documented weight) in 24 s with no 429; the 09-28 walk
met its first at ≈2,300 in ≈30 s (`design/measured.md`). The venue tolerates
about twice its stated minute in a burst, of a shape two points cannot fix, so
the pace is the stated 1,200 by each call's stated weight. `Budget` states
`weight_per_minute`, each `Paging` a `RequestCost`. A 429 waits for
`Retry-After` or a minute and asks the same page again (three times at boot;
a throttled fill holds every fill). The boot walk keeps only the live width;
declared widths and the funding depth are history fills in the live loop,
capped by `walk_cap` pages each and reported on completion. Funding still
re-asks the whole depth each boot (~30 min of background fills), as
`walk-the-funding-history` chose. **Still open:** the limiter's true shape,
whether a 429 carries `Retry-After`, and history fills on the status surface.

Tier 1, the walk. Blocks switching the funding walk on, and blocks the
Tier 10 publish, because it changes the public `Budget` type.

---

*The declaration paces Hyperliquid at 1,200 **requests** a minute. The venue
allows 1,200 **weight** a minute, and a page of history weighs 20 to 104. The
walk has been running 20–45× over budget, unseen, because the candle walks
are 18 requests and finish inside the venue's slack. The funding walk is
~200 pages, and it does not. Worse, a walk whose fetches failed reports
full coverage.*

---

## What happened, 2026-09-28 01:03Z

`walk_funding_days = 1300` in `var/datawatch.local.toml`, every reader's
`--check-config` ok, capture restarted.

```
  01:03:03  walk of candles at 1m  ·  6 requests
  01:03:33 → 01:03:42   fundingHistory answered 429, once per instrument
  01:03:42  walk of funding, paged forward: asked 1300d … covered 1300d
            (…) in 64 requests                    ← not true
  01:03:43 → 01:03:57   9 more 429s: the fill of the restart's own gap
```

| ticker | stopped at | settled funding gained |
|---|---|---|
| BTC | 2026-02-05 | 2023-05 → 2026-02 |
| ETH, GOLD | first page | none |
| HYPE | 2025-02-05 | 2024-12 → 2025-02 |
| XYZ100 | 2025-11-24 | part |
| CL | 2026-05-11 | part |

The walk was switched **off** again (commented out, with the reason) at
01:10Z, because it also starved the gap fill, which worked before. Nothing
live was lost: capture stayed up and quotes and trades stream over the
socket. The partial funding it took is in the archive, verbatim.

## The venue's rule, verbatim

From the Hyperliquid docs, *Rate limits and user limits*:

> REST requests share an aggregated weight limit of 1200 per minute.
> … `l2Book, allMids, clearinghouseState, orderStatus, spotClearinghouseState,
> exchangeStatus` have weight 2. … All other documented `info` requests have
> weight 20. … additional rate limit weight per 20 items returned:
> … `fundingHistory` … The `candleSnapshot` info endpoint has an additional
> rate limit weight per 60 items returned.

| call | base | per row | full page | pages/min at the whole budget |
|---|---|---|---|---|
| `fundingHistory`, 500 rows | 20 | 1/20 | **45** | 26 |
| `candleSnapshot`, 5,000 bars | 20 | 1/60 | **~104** | 11 |
| `clearinghouseState` (ledger) | 2 | — | 2 | 600 |

`adapters/hyperliquid/mod.rs:262` reads the 1,200 as *"requests a minute
for an info call"*. At `walk_share = 0.25` that paces 300 requests a minute,
~13,500 weight of funding pages against a budget of 1,200. The ledger's own
config comment already counts in weight (*"about 4.8 weight a minute"*), so
the error is confined to `Budget`.

## Two defects, not one

1. **The budget's unit.** `Budget { requests_per_minute }` cannot state a
   cost that depends on the call and on the size of the answer.
2. **A failed fetch is counted as covered.** `one_fetch` turns an error into
   `Ok(None)`, and its doc says *"the outcome reports what was reached either
   way"*. Neither caller does that:
   - `walk_forward` (`capture/run.rs`) treats `None` like "the adapter cannot
     read the page end" and sets `here = to_micros`: the whole range.
   - `walk_steps` ignores the `None`, and `Walk::outcome` computes `reached`
     from the **planned** steps, so a failed candle page reads as covered.

   This breaks the invariant `boot.rs` states over the cap check, *"a run that
   covered less than it was asked for is not a green one"*, and legacy's
   *reports-never-judges* only works if the report is true.

   **Fixed 2026-09-28** by `a-failed-fetch-is-not-coverage` (`ccce818`). What
   follows is defect 1 only.

## What it changes

- **`Budget` states weight.** `weight_per_minute`, plus a per-series request
  cost: `base` and `per_rows(n)` (Hyperliquid: funding `20 + ⌈rows/20⌉`,
  candles `20 + ⌈rows/60⌉`). The walk paces each call by its *expected* cost
  (the page size the paging already declares). A venue that states only
  requests declares `base = 1`, `per_rows = 0`. rh-chain and rh-crypto keep
  their numbers with that meaning.
- **A 429 is a pace fact, not a skipped range.** Wait (honour `Retry-After`
  if the venue sends one, which is unverified; otherwise one budget-minute),
  then retry the **same** page, bounded. A retry that still fails ends that instrument's
  walk *at `from`*.
- ~~The outcome is what was taken; the exit code stays zero.~~ Done in
  `a-failed-fetch-is-not-coverage`.

## Costs, at the corrected pace

At `walk_share = 0.25` (300 weight a minute):

```
  candle widths, 18 full pages × ~104     ≈ 1,870 weight   ≈ 6 min a boot
  funding at 1300 d, ~200 pages × 45       ≈ 9,000 weight   ≈ 30 min a boot
```

Thirty minutes of funding pages on **every** boot is the real question this
raises. `walk-the-funding-history` chose to re-ask the whole depth because
the record is dated by receipt. Its planning also named the other option:
*"reaches back a bounded stretch per boot and resumes from the oldest
settlement it holds"*. Funding is not rolling away (`startTime = 0` returns
2023-05-12), so there is no hurry, and resuming is cheap once. **Open:**
resume from the newest *settlement* (venue time, which the tape has), or keep
re-asking and accept the 30 minutes.

## The decision this is waiting on: the boot walk blocks the live stream

**Corrected 2026-09-28.** This page said above that the walk interleaves the
live stream. It does not, and neither does `walk-the-funding-history`'s
design, which says the same. `boot.rs` walks **before** `fill_with` and
`.run`, on purpose: *"the history, before the live loop and after the
restart gap — so a backfill is never mistaken for coverage the record already
had, and so the walk's own requests are paced against a venue nothing else is
yet talking to"*. The spec says it too: *History precedes the live
subscription*. So every minute the boot walk takes is a minute of no quotes
and no trades after each restart. On 2026-09-28 that was ~61 s.

**And that minute is not in the record as a gap.** Checked in the tape after
the 01:40Z projection: quotes and trades stop at 01:02:56Z and resume at
~01:03:57Z, but the `downtime` rows cover **01:02:56 → 01:02:59Z, 2.7 s**. The
restart gap is published before the walk and closes then. Nothing marks the
walk's own duration for the streamed series, so the tape implies coverage it
does not have for ~58 s. That breaks *gaps are events, never inferred from
silence*, and it has happened on every boot since the walk existed (~20 s
each at today's pace). **It is its own fix, and it comes first:** the
restart gap of a streamed series ends when its subscription is held, not when
the process came back. It is independent of the options below. Option B
shrinks the hole but does not remove the need to mark it.

Pacing by weight at `walk_share = 0.25` makes that minute **~6 (candles) to
~36 (with funding)**. That is a regression no one would choose, so the pacing
cannot land alone. The options:

| | What | Restart hole | Cost |
|---|---|---|---|
| **A** | Boot walk takes the **whole** budget (`share = 1.0` before the live loop, where nothing else talks to the venue, as the comment says) | candles ~1.6 min; +funding ~9 min | small; ledger polls share the IP |
| **B** | The boot walk keeps only the **live width's resume**. The declared widths and the funding depth are queued as **fills**, which run inside the live loop, one at a time and paced | ~seconds | a real change: settle reads the widths from the walk request, and fills report per page, not as one outcome |
| **C** | A, plus funding **resumes** from the newest settlement held (venue time) instead of re-asking 1,300 days | candles ~1.6 min | the resume `walk-the-funding-history` rejected, for reasons worth re-reading first |
| **D** | Measure first: the venue took 18 full candle pages (~1,870 documented weight) in ~20 s on every earlier boot with no 429, so its limiter may be a burst bucket | — | one controlled probe, off-hours, same IP as capture |

**Recommendation: D, then B.** D is cheap and tells us whether the documented
per-row weight is enforced as a per-minute window. B is the only option whose
restart hole does not grow with history depth. It is also where the fill's
own retry already lives, so a 429 becomes a paced retry instead of a
dropped range. A is the stopgap if B waits.

## The line to hold

- **Verbatim in.** A 429 should be in the record as a failure. **Unverified:**
  `one_fetch` takes nothing on `Err`, so whether the history client archives a
  refused response before returning its error needs reading.
- **The declaration is the venue's word.** The weights are the venue's stated
  figures, quoted where they are declared, not a number tuned until the 429s
  stop.

## Depends on, and depended on by

- **Depends on** nothing unbuilt.
- **Depended on by** switching `walk_funding_days` on, then `galata-research`
  charging funding in backtests (`gr.backtest.returns`, *position × rate*),
  and the Tier 10 publish, since `Budget` is public API.

## Test

- A declared weight of 45 a page at share 0.25 paces ~6.7 pages a minute.
- A fetch that answers 429 twice and then succeeds retries the same page and
  covers the range.
- A fetch that keeps failing on instrument 2 of 6 yields `reached` at its
  `from`, and the outcome's report names the missing range.
- A failed candle step leaves `reached` at the last step every instrument took.
