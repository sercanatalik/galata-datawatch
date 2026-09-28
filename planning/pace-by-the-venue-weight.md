# pace-by-the-venue-weight

**NOT PROPOSED.** Named 2026-09-28, from switching on `walk_funding_days`
in the deployment (the last step of `walk-the-funding-history`), which the
venue answered with 429 on every instrument.

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
- **The outcome is what was taken.** `reached` is the point up to which every
  instrument has a page that arrived. `walk_forward` leaves `here` at `from` on
  a failure. `walk_steps` returns the last step every instrument took, and
  `Walk::outcome` takes that and stops using the plan. A short outcome is
  logged as an error with the range missing.
- **The exit code stays zero** for a shortfall the venue caused. Under
  launchd's `KeepAlive` a non-zero exit is a restart, and restarting into a
  rate limit is a loop that hammers the venue. The cap stays non-zero, since
  that one is ours to raise.

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
re-asking and accept the 30 minutes. The walk interleaves the live stream,
so nothing live waits, but the restart's gap fill runs after it.

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
