# planning

**Where a feature lives before it becomes a change.** One file per feature,
`planning/<name>.md`, named the way the change will be named. A file is written
before anything is proposed, and stays after its feature is built, as the
argument for it; its first line says which.

```text
  planning/<name>.md        the feature, argued. NOT PROPOSED.
        ▼
  design/<component>.md     the mechanism, with an Archify chart
        ▼
  openspec/changes/<name>/  proposal · design · specs · tasks
        ▼
  openspec/specs/<cap>/     the requirement, once the change is archived
```

[`design/roadmap.md`](../design/roadmap.md) carries a one-line entry for each
feature at its tier, so the argument about ordering stays in one place. The
project-level roadmap is in the [README](../README.md#roadmap).

| Feature | Tier | Named | Status | Origin |
|---|---|---|---|---|
| [`bound-the-replay`](./bound-the-replay.md) | 3 | 2026-09-25 | **not proposed**; waits for a Rust replay host | `galata-research` needs a view at a replay position; carries forward legacy `reader-replay` |
| [`walk-the-coarse-candles`](./walk-the-coarse-candles.md) | 1 | 2026-09-25 | built 2026-09-25, deployed | `galata-research` found the venue's `1h`/`4h` history rolling away uncaptured; carries forward legacy `walk_candles` |
| [`walk-the-funding-history`](./walk-the-funding-history.md) | 1 | 2026-09-25 | built 2026-09-26, deployed 2026-09-29 | `galata-research` found settled funding starting at capture start while the venue holds it from 2023-05-12; carries forward legacy `walk-the-funding` |
| [`pace-by-the-venue-weight`](./pace-by-the-venue-weight.md) | 1 | 2026-09-28 | built and deployed 2026-09-29 | switching on `walk_funding_days` met 429 on every instrument; `Budget` counts requests where the venue counts weight, and a failed fetch reads as covered |
