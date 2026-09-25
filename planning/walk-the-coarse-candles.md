# walk-the-coarse-candles

**NOT PROPOSED.** Named 2026-09-25, from the `galata-research` planning pass,
which asked how much history research has and found it leaving.

Tier 1, the walk. Carries forward legacy's `walk_candles`.

---

*Hyperliquid serves the most recent ~5,000 bars of each candle interval, and
the window rolls. datawatch walks only its live interval, `1m`, so the `1h`,
`4h` and `1d` history the venue holds today is not in the record, and **the
oldest day of it leaves the venue every day**. Every other gap in this system
is a bytes-not-captured gap that has already happened. This one is still
happening.*

---

## Measured 2026-09-25, `candleSnapshot` from `startTime = 0`

```
  BTC   1m   5,152 rows   2026-09-21 →   3.6 days
        1h   5,002 rows   2026-03-01 → 208.4 days    ← loses a day a day
        4h   5,001 rows   2024-06-14 → 833 days      ← loses a day a day
        1d   2,229 rows   2020-08-19 → all of it (under the cap)

  ETH   1h from 2026-03-01 · 1d from 2020-08-19
  HYPE  1h from 2026-03-01 · 1d 660 rows from 2024-12-05 (its listing)
  xyz:GOLD    1d 278 rows from 2025-12-22 · xyz:XYZ100 1d 348 rows from 2025-10-13

  funding BTC   first row 2023-05-12 00:00 · HYPE 2024-12-05 10:00
```

**And a trap, found reading the bars rather than counting them.** BTC's first
921 daily bars (2020-08-19 → 2023-02-25) carry `v = 0.0` and `n = 0`: prices
from before the venue traded, with no trades behind them. The first day with
trades is **2023-02-26**. A backtest over "six years of Hyperliquid BTC" would
be three years of Hyperliquid and three years of something else. They are
recorded verbatim, as everything is, and the tape already carries
`trade_count`, so a reader can refuse them. **Refusing them is the reader's
job, not the walk's.**

## What already exists

- **The planner has the shape.** `WalkInterval::needed(interval, need)` asks for
  `now − need` at a width the stream does not push, clipped to the venue's
  reach, beside `WalkInterval::live`. Nothing constructs one: `boot.rs` builds
  only `WalkInterval::live(live_interval)`.
- **The tape holds several widths.** Every candle row carries `interval`,
  `trade_count` and `is_final`, so `1h` beside `1m` in `kind=candles` is two
  interval values, not a new dataset.
- **The declaration states the reach** per series and interval
  (`reach_micros`), which is how the walk above already reports *the venue
  holds 3d 11h 20m*.
- **Legacy declared it**: `walk_candles = ["1h"]` per venue, *"fetched by the
  walk on every boot, never subscribed live"*, with *"a walk-only interval
  refetches its whole reach on every boot"* stated as its cost.

## What it adds

- `[venue.<name>] walk_candles = ["1h", "4h", "1d"]`: widths the walk fetches
  beside `candle`, each asking for the venue's whole reach. The adapter
  refuses a width it cannot name, at load, the way `candle` is refused.
- **Why the whole reach and not a resume.** The record is dated by receipt, so
  where a *width* resumes is a question it cannot answer (the history-walk
  spec's own opening). Refetching the reach is ~20 requests per width across
  six instruments at the walk's pace, on each boot. The archive keeps
  both copies, and so does the tape, which stores a re-fetched bar as a second
  row; a duplicate carries the same content-derived identity, so a reader
  dedupes on `(ticker, interval, at_micros)`, taking the final bar. It is not
  collapsed on write.
- **The record then outlives the venue's window.** Each boot keeps what the
  venue still serves. Nothing is lost as long as capture boots at least once
  per reach, which is 208 days for `1h`. The live fill (`fill-mid-run-gaps`)
  does not change that: it fills gaps, not reaches.

## The line to hold

- **Verbatim in, judged out.** The zero-trade bars are archived and projected
  exactly as served. Dropping them at the walk would make the record disagree
  with the venue about what it said. A reader that wants a market filters
  `trade_count > 0`, and `galata-research` states that filter in every run's
  manifest.
- **Not subscribed live.** `candle` stays the one live width. A coarser width
  subscribed live would be a second stream of the same fact.

## Depends on, and depended on by

- **Depends on** nothing unbuilt.
- **Depended on by** `galata-research` step 0 (history), and by any lookback
  longer than 3.6 days at `1m`.

## Urgency, and the rescue already taken

The ordering rule, *what cannot be recovered comes first*, puts this ahead of
everything in research. Each day it waits, the venue drops another day of `1h`
from March 2026 and another six `4h` bars from June 2024.

**Rescued 2026-09-25 ~14:40 UTC**, so today's windows are kept whatever the
design takes: `var/rescue/2026-09-25-coarse-candles/`, 18 pages (six
instruments × `1h`, `4h`, `1d`) saved **verbatim** as the venue's response
bytes, with `manifest.json` holding each request, its send and receipt times,
a sha256 and the row range. 7.3 MB. At the ~5,000-bar cap, and so rolling:
all six at `1h`, and BTC and ETH at `4h`. Under the cap, and so complete from
listing: HYPE and the `xyz` instruments at `4h` and `1d`.

**Deliberately not written into the archive.** A second writer beside running
capture would seed its sequence from its own clock, so capture's later payloads
would sort *below* the rescue's, and the tape's per-venue bound and replay
ordering assume sequence order is receipt order. Stopping capture to walk in
its place fails differently: the next boot dates its restart gap from the last
durable receipt, which would be the walk's candles, and quotes and trades
would claim coverage nobody captured.

**So this change owns the import.** The rescued pages enter the archive
through the one path, as `Origin::Fetched`, from inside the capture process
after its restart gap and before its walk and live subscription, once, and
the manifest's hashes are checked first. **Their receipt time is the moment
the record takes them, not 14:40.** An older receipt under a newer sequence
would break the same order the second writer would have. The fetch time
stays in the manifest. Pages the walk's own fetch of the reach still covers
are duplicate rows a reader dedupes by identity. The ones that matter are the oldest days, which by then
the venue no longer serves.

(`xyz:WTIOIL` in this repo's roadmap is `xyz:CL` on the venue; the rescue's
first attempt met the venue's 500 for the old name, and the configuration
already says so.)
