# galata-segments

**A durable Parquet segment store, where the filename carries the durability
fact.**

galata-segments is the storage engine under Galata's market-data archive and
tape. It writes append-only Parquet segments, reports how far a store is
durable from a directory listing alone, and compacts closed partitions. It
has no dependency on the rest of the workspace and is useful on its own.

```toml
galata-segments = "0.1"
```

## The commit protocol

```text
  write to a temporary  →  sync_all  →  rename  →  sync the directory
                                        ▲
                                        the commit
```

A segment is only *named* after its bytes are on disk, so a segment that
exists is a segment that is durable. A reader never sees a half-written file
and never has to ask whether one is finished: that state does not exist.

## How far am I durable?

The answer comes from the **directory listing**, without opening a file:

```rust
use galata_segments::last_durable;

// `t-1789941180000000_1789941183000000_4242_7.parquet` →  1789941183000000
let (variant, position) = last_durable(root).expect("a tree with segments");
```

Because the name carries the range, a range read skips whole segments before
opening anything, and then skips row groups using the footer statistics.

## Three kinds of position

Not every store is indexed by time, so a cursor states which unit it uses:

| Cursor | Filename | For |
|---|---|---|
| `Time { first, last, pid, seq }` | `t-…_…_…_…` | receipts, by microsecond |
| `Block { first, last }` | `b-…_…` | a blockchain, by block number |
| `Seq { first, last }` | `s-…_…` | a stream, by position |

Fields are separated by `_` after a variant tag, rather than by `-`, so a
**negative** microsecond round-trips. A `-`-separated name cannot represent a
pre-1970 timestamp, which is the kind of thing found the first time someone
backfills.

`Time` carries a process id and a flush counter because two flushes can share
a receipt microsecond. Without them, the second would overwrite the first and
the loss would be invisible.

## Row groups are sized, not defaulted

The library default is 1,048,576 rows, which put a whole compacted five-hour
partition in **one** row group. A segment with one row group prunes
all-or-nothing, however tight its statistics are. Measured against a real
1,757,937-row segment, 16,384 rows is the knee.

Statistics are written only for the columns reads actually prune on. A page
index over an opaque payload column is a real share of a small file,
multiplied by the whole file count.

## Compaction

```rust
use galata_segments::{compact_closed, hold, Codec};

let _held = hold(root)?;                       // one compactor at a time
compact_closed(root, today, Codec::Zstd)?;     // never today
```

Measured on a real archive: **14,719 segments → 6** in 3.8 seconds, and the
tree from 94 MB to 43 MB. The saving is per-file Parquet overhead (a footer, a
schema and column chunk headers), paid 22,000 times over segments that
averaged a few kilobytes.

Today's partition is never touched, because a partition still being written
is not closed.

## Part of Galata

galata-segments is one of four crates in [galata-datawatch], the data layer
of Galata, a low-latency algorithmic trading framework in Rust. See the
[project README][galata-datawatch] for the architecture and roadmap.

Licensed under MIT.

[galata-datawatch]: https://github.com/sercanatalik/galata-datawatch
