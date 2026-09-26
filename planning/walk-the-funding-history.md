# walk-the-funding-history

**BUILT 2026-09-26**: `walk_funding_days`, asked every boot
(`walk-the-funding-history`), and `premium` carried on settled funding
(`carry-the-settled-premium`). Named 2026-09-25, from `galata-research`, whose every backtest has to
say *funding not charged* because the record holds five days of settled
funding.

Tier 1, the walk. Carries forward legacy's `walk-the-funding` (paging per
series, forward from the start) and extends it with a stated depth, as
`walk-the-coarse-candles` did for bar widths.

---

*Settled funding enters the record two ways: a `fundingHistory` walk and
nothing else, since the live `activeAssetCtx` rate is a prediction. The walk
resumes from the record's receipt watermark and reaches back only as far as
the cold start (`cold_start_days = 7`). So the record's settled funding
begins where capture began, 2026-09-20 22:00, while the venue holds every
hour since its first settlement.*

---

## Measured 2026-09-25, `fundingHistory` from `startTime = 0`

```
  coin        first settlement        per request        hourly rows to today
  BTC, ETH    2023-05-12 00:00        500 rows (~21 d)   ~29,900  → ~60 pages each
  HYPE        2024-12-05 10:00        500 rows           ~15,800  → ~32 pages
  xyz:GOLD    2025-12-22              500 rows           ~6,600   → ~14 pages
  every row: {coin, fundingRate, premium, time}
```

**Unlike candles, it is not rolling away.** `startTime = 0` returns the very
first settlement, so this is not "what cannot be recovered comes first". It
is still the one thing between research and a backtest that charges what a
position is actually charged: longs paid about 11.6% a year at the interest
floor, and every long-only result in galata-research is flattered by that.

## The change

- **A stated depth for funding**, `walk_funding_from` (or a days figure
  beside `walk_candles`), declared per venue, rather than resuming from the
  receipt watermark, for the reason `capture/walk.rs` gives about bar
  widths: *the record's clock is a receipt clock*, so a walk that resumes
  from today's receipt asks for nothing and reports success.
- **Its request count**, about 60 pages for BTC and ETH, set against
  `walk_cap = 200` and `walk_share`. At 6 instruments the first walk is
  about 230 requests. Either the cap is raised for the one-off catch-up, or
  the walk reaches back a bounded stretch per boot and resumes from the
  oldest settlement it holds. The record says which, and the reach is
  reported, never covered over in silence.
- **`premium` carried** on settled funding, as it now is on marks
  (`de22b3c`/`9303331`). `wire.rs` notes "premium is on the wire and not
  carried" for `fundingHistory`, and the event has no field for it.
- **A test**: a walk declared to 2023-05-12 on an empty record asks forward
  from there, pages by the last returned time, and a second boot asks only
  from the newest settlement held.

## What research does with it

`gr.market.funding` already returns settled rows on `ts`, one per
`(venue, ticker, hour)`. With history behind it, `gr.backtest.returns` can
charge `position × rate` at each hour a position is held. That is
galata-research's next change once the rows exist.
