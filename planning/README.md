# planning

**Where a feature lives before it becomes a change.** One file per feature,
`planning/<name>.md`, named the way the change will be named. Nothing here is
proposed yet.

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

| Feature | Tier | Named | Origin |
|---|---|---|---|
| [`bound-the-replay`](./bound-the-replay.md) | 3 | 2026-09-25 | `galata-research` needs a view at a replay position; carries forward legacy `reader-replay` |
| [`walk-the-coarse-candles`](./walk-the-coarse-candles.md) | 1 | 2026-09-25 | `galata-research` found the venue's `1h`/`4h` history rolling away uncaptured; carries forward legacy `walk_candles` |
