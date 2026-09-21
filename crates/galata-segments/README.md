# galata-segments

Durable parquet segments, where **the filename carries the durability fact**.

```text
  write to a temporary  →  sync_all  →  rename  →  sync the directory
                                        ▲
                                        the commit
```

A segment that exists is a segment that is durable, because it is only *named*
after its bytes are on the platter. A reader never sees a half-written file and
never has to ask whether one is finished — there is no such state.

## How far am I durable?

From the **directory listing**, without opening a file:

```rust
use galata_segments::last_durable;

// `t-1789941180000000_1789941183000000_4242_7.parquet` →  1789941183000000
let (variant, position) = last_durable(root).expect("a tree with segments");
```

The name carries the range, so a range read excludes whole segments before
opening anything, and then excludes row groups from the footer statistics.

## Three kinds of position

A store is not always indexed by time, so a cursor says which unit it is in:

| cursor | filename | for |
|---|---|---|
| `Time { first, last, pid, seq }` | `t-…_…_…_…` | receipts, by microsecond |
| `Block { first, last }` | `b-…_…` | a chain, by block number |
| `Seq { first, last }` | `s-…_…` | a stream, by position |

Separators are `_` with a variant tag rather than `-`, so a **negative**
microsecond round-trips. A `-`-separated name cannot parse any pre-1970
timestamp, which is the kind of thing found the first time somebody backfills.

## Row groups are sized, not defaulted

The library default is 1,048,576 rows, which put a whole compacted five-hour
partition in **one** row group — and a segment with one row group prunes to
all-or-nothing however tight its statistics are. Measured against a real
1,757,937-row segment, 16,384 is the knee.

Statistics are written for the columns a read actually prunes on and no others:
a page index over an opaque payload column is a real share of a small file,
times the whole file count.

## Compaction

```rust
use galata_segments::{compact_closed, hold, Codec};

let _held = hold(root)?;                       // one at a time
compact_closed(root, today, Codec::Zstd)?;     // never today
```

Measured on a real archive: **14,719 segments → 6** in 3.8 seconds, and the tree
from 94 MB to 43 MB. The saving is per-file parquet overhead — a footer, a
schema and column chunk headers, 22,000 times over a mean segment of a few
kilobytes.

Today is never touched, because a partition still being written to is not
closed.

---

Part of [galata-datawatch](https://github.com/sercanatalik/galata-datawatch),
and useful without it. MIT licensed.
