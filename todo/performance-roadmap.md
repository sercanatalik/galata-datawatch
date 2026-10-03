# Performance roadmap

*From a whole-codebase review on 2026-10-03. Eight reviewers swept every crate
and `py/signals`. The large items below were verified against the code; line
numbers are as of that review and will drift.*

The ordering rule: **costs that grow without bound come first.** A pass that
re-reads all history gets slower every day the process runs; a per-message
allocation costs the same on day 1,000 as on day 1. Fix the first kind before
the second.

Each item says what is slow, why it matters, and what to do instead.

---

## Tier 1 — costs that grow with uptime or history

### 1.1 Ledger re-folds the whole history every pass
- **Where:** `crates/galata-datawatch/src/ledger/run.rs:322-347` (`fold_all`),
  called every events cadence from `run.rs:977`; reads via
  `ledger/events.rs:56-75` → `replay::read_range(.., i64::MIN, i64::MAX)`.
- **Cost:** every pass reads, decompresses, normalises, sorts twice and folds
  every margin and position snapshot since the account was first polled (about
  8,640 per dex per day at `snapshot_secs = 10`), then rewrites the full
  projection. `check_snapshot` also scans every book per snapshot
  (`fold.rs:452-457`).
- **Do:** keep the fold state (`books` plus a high-water `at_micros`) per
  account between passes and fold only rows newer than it. Rebuild from scratch
  only at boot.

### 1.2 Ledger reads every kind, then filters
- **Where:** `ledger/events.rs:69-72`; `load()` at `ledger/run.rs:713`;
  `Bindings::read` at `adapters/hyperliquid/accounts.rs:443`.
- **Cost:** wanting only events still decodes and holds every snapshot segment.
- **Do:** pass the wanted kinds into `replay::read_range` and skip `kind=`
  partitions (`kind_of`, `replay.rs:119`) before opening any segment.

### 1.3 Python calculators read all candles since 2020
- **Where:** `py/signals/_src/galata_signals/bars.py:20,45`
  (`EVER = ("2020-01-01T00:00Z", "2100-01-01T00:00Z")`); callers
  `jumps.py:50`, `moments.py:55`, `varcov.py:116`, `backtest.py:73` (the last
  two once per horizon).
- **Cost:** a full-history read and 5m rebuild per call; moments then keeps
  about a week.
- **Do:** give `bars()` a lower bound and pass it to `gr.market.candles`. Cache
  the 1m frame per `source` within a run (`functools.lru_cache`).

### 1.4 Python scans the whole signals dataset per calculator
- **Where:** `varcov.py:100-107`; `history()` in `basis.py`, `liquidity.py`,
  `activity.py`; `backtest.stored_tails`.
- **Cost:** `kind=signals` only ever grows (one file per commit). Each
  calculator walks the tree and opens every footer, about 16 full scans per
  scheduled run.
- **Do:** glob only the `date=` directories inside the window. For
  `stored_asof`, read only the newest few, as `frontier()` already does.

### 1.5 Repeated archive walks
- **`watch`:** `watch.rs:151, 167-168, 188, 207` make 3-4 full archive walks
  per run. The `:151` result is used only for `.len()`.
- **Compaction:** `galata-segments/src/compact.rs:47, 55, 64` list each
  partition about four times. The closed-hours pass (`:143`) walks all
  ~2,230 dates to reach today's.
- **Tape `--replace`:** `tape/rebuild.rs:286-305` opens each footer twice per
  segment, and `bin/galata-tape-rebuild.rs:185` (`check_layout`) opens it a
  third time over the whole tape.
- **Do:**
  - Walk once with `partitions_with_segments` (`listing.rs:321`) and derive
    nesting, overdue counts and per-venue maxima from that listing.
  - Filter on `date=` while walking.
  - Add a `labels(path)` helper that returns every key-value label from one
    footer read.
  - Limit `check_layout` to the partitions the run touched.

### 1.6 Reconcile decodes the whole candle tape once per venue
- **Where:** `derive/tape.rs:59-69, 119`; `reconcile.rs:179-182`;
  `derive/mod.rs:290`.
- **Do:** read once, bounded by the targets' `[fit_from - width,
  fitted_through]`. Split by venue in one pass and group bars by ticker once.

---

## Tier 2 — bulk work that grows with a range or a book

### 2.1 `book_batch` clones the whole row once per level
- **Where:** `tape/writer.rs:503` (`flat.push(row.clone())`).
- **Cost:** a 500-level snapshot makes about 250k level copies, so the cost
  grows with the square of book depth.
- **Do:** build the shared columns from row indices, and expand only the
  per-level fields.

### 2.2 Tape reader ignores its own statistics
- **Where:** `tape/reader.rs:418-438`, row filter `:475-505`.
- **Cost:** a one-ticker, one-minute view decodes the whole day's segment. The
  writer records statistics for venue, ticker and `at_micros` that are never
  used.
- **Do:** open each file once, choose row groups from the ticker and
  `at_micros` statistics, and build the mask with arrow kernels. Hold a
  `LabelCache` in `Reader`.

### 2.3 Compaction's duplicate proof decodes whole containers
- **Where:** `galata-segments/src/compact.rs:286, 300-304`, then again in
  `merge` (`:408, 418`).
- **Cost:** one late segment inside a compacted day means decoding the
  container (about 1.76M rows, each copied into a `Vec<u8>`), then decoding it
  again for the merge.
- **Do:** read only the contained range with `read_segment_range`, key the set
  on a 64-bit row hash and confirm on a hit, and reuse the decoded batches in
  `merge`. Count rows rather than test membership (see the correctness note
  below).

### 2.4 Replay and tape rebuild hold the whole range in memory
- **Where:** `replay.rs:148-168, 214-237`; `tape/rebuild.rs:230, 247-262`.
- **Cost:** peak memory grows with the range requested; each row also
  allocates about five Strings, and the result gets one global sort.
- **Do:** stream one partition (or one receipt day) at a time with a k-way
  merge, and commit per day. Share kind and address through `Arc<str>`.

### 2.5 `ListingCache` copies every path on each hit
- **Where:** `galata-segments/src/listing.rs:352-363`.
- **Cost:** about 128k `PathBuf` copies a second on a warm watch.
- **Do:** store `Arc<Listed>` (or `Arc<[...]>` slices) so a hit is a reference
  count increment.

### 2.6 Ledger per-pass overhead
- **`identity()` builds a JSON string per row** (`ledger/events.rs:40-49`).
  Use a structured key or a row hash.
- **Each events page is parsed four times:**
  `adapters/hyperliquid/accounts.rs:137-139` and `ledger/run.rs:779, 789`.
  Parse once and return the rows.
- **The projection clones the whole event once per column**
  (`ledger/project.rs:256-259`, and `:281`, `:299`, `:317`, `:344`). Return
  `Option<&Fill>` and the like instead.
- **Venue calls run one at a time** (`ledger/run.rs:580-585`, `:526`,
  `:459-469`). Use `buffer_unordered(k)`, with `k` from the rate share.

### 2.7 Python per-ticker and per-minute loops
- **A full filter per ticker:** `q.filter(pl.col("ticker") == t)` in
  `leadlag.py:82`, `flow.py:64`, `liquidity.py:116`, `basis.py:122`,
  `carry.py:399`, `activity.py:83`, `cascade.py:92`. Call
  `partition_by("ticker", as_dict=True)` once instead.
- **The session check runs minute by minute** (`basis.py:171`,
  `moments.py:64`), about 43,200 timezone conversions per ticker per run. Check
  per hour (the boundaries fall on whole New York hours), or vectorise.
- **`liquidity.weighted_quantile` sorts Python lists.** Reuse
  `basis.weighted`.
- **`activity.py:123` uses `map_elements`.** Use `dt.hour()` and
  `dt.weekday()` instead.

### 2.8 `segments` writer
- **`same_bytes` reads both files whole** (`galata-segments/src/writer.rs:461`).
  Compare 64 KiB chunks.
- **`finish` drops the temporary file and reopens it only to sync it**
  (`:359, 367`). Sync the open handle instead.

---

## Tier 3 — the per-message hot path

Constant cost per message, but paid on every frame.

- **Payload copies along the path:**
  - `source/stream.rs:108` (`text.as_bytes().to_vec()`);
  - `ingest.rs:117` (`archive.append(payload.clone())`);
  - `sink.rs:227` (`envelope.clone()`);
  - `galata-broker/src/subscriber.rs:84` (`m.payload.to_vec()`);
  - `publisher.rs:205` (`body.to_vec()`).

  Carry `Bytes` throughout and give `Sink::emit` an owned `Envelope`.
- **Declaration cloned per frame:** `capture/run.rs:1840` (also `:1664`,
  `:1741`). Check `in_flight` and `next_start_micros` first, then borrow.
- **Pages cloned to read their end:** `capture/run.rs:1387, 1718`. Compute
  `page_end` before `take`.
- **Per-frame work in `take()`:** about `run.rs:400-450`. It clones the channel
  and tickers and calls `series_of_channel` twice. Use `get_mut` before
  `entry` in `Coverage::received`.
- **`Symbols::resolve` allocates two Strings per event**
  (`venue/symbols.rs:59`). Use a nested map keyed by `&str`, and keep a reverse
  map for `venue_symbol_for` (`:68`).
- **Partition grouping per payload:** `record/mod.rs:517-548` and
  `tape/writer.rs:134-145, 241-258` format a date, a path and Strings for every
  row. Key on `(kind, day index, &str)` and format once per group.
- **NATS subjects:** each is allocated twice per message
  (`galata-broker/src/subject.rs:23` then `publisher.rs:189`). Cache them per
  (venue, ticker, kind), or wrap `async_nats::Subject`.
- **Encoding:** `galata-broker/src/encode.rs:27` grows from an empty `Vec`.
  Size it from the previous message. Return `Result` rather than
  `unwrap_or_default`.
- **String columns in the tape writer** (`tape/writer.rs:305-313, 350-351`) make
  one String per row per column. Return `Option<&str>`, and build the constant
  `venue` column once per segment.
- **`rh_chain`:**
  - Each log page is parsed into a `Value` and serialised again
    (`client.rs:228`); borrow `RawValue` instead.
  - Logs hold six owned Strings each, and every address is lowercased twice
    (`normalise.rs:25`).
  - The three metadata `eth_call`s are awaited one after another; batch them.
  - The unfiltered `eth_getLogs` is deliberate; adding an `address` filter is a
    design decision, not a fix.

---

## Correctness bugs to fix alongside

These came out of the same review and were confirmed. Several sit in the code
the items above would change, so fix them together rather than twice.

| # | Bug | Where | Overlaps |
|---|---|---|---|
| C1 | `retention` days never validated; `0` deletes every past day | `config/mod.rs:361-373, 778`; `retain.rs:233` | — |
| C2 | partial flush drops buffered payloads and failures; restart gap understated | `record/mod.rs:365-371, 484-499` | 3 (grouping) |
| C3 | tape `--replace` leaves duplicates if a later group fails | `tape/writer.rs:166-202`; `tape/rebuild.rs:322-335` | 2.4 |
| C4 | reorg detection never fires at the default step | `rh_chain/trail.rs:117`; `capture/cursor.rs:331` | — |
| C5 | hung fill request blocks every fill (no timeout) | `capture/run.rs:1945`; `hyperliquid/client.rs:88` | — |
| C6 | throttled poll speeds up (250 ms backoff replaces the interval) | `capture/poll.rs:179-181` | — |
| C7 | stream reconnects with no delay after a closed session | `capture/run.rs:877, 993-1001` | — |
| C8 | failed fill step queues its next step again; failed attempts count toward `walk_cap` | `capture/run.rs:1907-1917` | — |
| C9 | RPC failure recorded as "no ERC-8056 multiplier"; HTTP status not checked | `rh_chain/client.rs:244, 282-284, 334-353` | 3 (`rh_chain`) |
| C10 | one unreadable segment stops the ledger | `ledger/run.rs:330, 977` | 1.1 |
| C11 | funding before the first fill gives a spurious break | `ledger/fold.rs:338` | 1.1 |
| C12 | `--redo` leaves the old value in activity and liquidity baselines | `activity.py:50-61`; `liquidity.py:57-67` | 1.4 |
| C13 | empty or mixed-width 1m data crashes before the empty checks | `bars.py:25` | 1.3 |
| C14 | candle-schema error reported as "no findings" | `reconcile.rs:179-182` | 1.6 |
| C15 | directory-sync failure swallowed before compaction deletes | `galata-segments/src/writer.rs:388, 518` | 2.8 |

Also worth deciding on (they depend on intent or input):
- **Duplicate proof ignores counts:** it checks set membership rather than
  multiplicity (`compact.rs:300`).
- **rh-crypto timestamps lose numeric UTC offsets** (`rh_crypto/wire.rs:211`).
- **A zero or non-ASCII bar width panics** in derive.
- **Retention follows symlinks** (`retain.rs:251, 264`).
- **`grants::table` takes unvalidated venues** (`galata-broker/src/grants.rs:84`).

---

## Progress

| Iteration | Items | Status |
|---|---|---|
| 1 | C1 retention validation · C5 fill timeout + bounded HTTP client · C6 throttled poll adds to cadence · C7 reconnect waits after an empty session | done |
| 2 | 1.1 incremental ledger history (`events::History`, read from the last receipt) · 1.2 kind-scoped reads · C10 unreadable account skipped and named · C11 first fill anchors after funding | done |
| 3 | 1.3 bounded candle reads for `moments` and `backtest` (`bars(.., start)`) · 1.4 signal history reads only the window's `date=` dirs (`frontier.signal_files`) · C12 newest computation per hour in `activity`/`liquidity` · C13 empty or mixed-width 1m handled in `bars.build`. **Left unbounded on purpose:** `jumps` and `varcov` fit over the whole history, so bounding them changes the estimate — a modelling decision, not a refactor | done |
| 4 | 1.5 one listing per store in `watch` (`partitions_listed`, `nested_in`, `overdue_in`, `last_durable_in`) · closed-hours compaction prunes other `date=` dirs unlisted · `compact_partition` lists once · 1.6 reconcile reads candles once, bounded to the targets' windows, split by venue in one pass · `derive` groups by ticker once · C14 an unreadable candle tape is a finding · D1 `width_micros` rejects 0, negatives, overflow and non-ASCII. **Still open from 1.5:** tape `--replace` footer reads, and a parallel walk (`std::thread::scope` per venue — no new dependency) if the single walk is still slow | done |
| 5 | C2 a part-failed flush re-buffers what it did not write and marks `.unflushed`, which dates the restart gap · C3 `Tape::commit` builds every batch before writing any · C15 directory sync errors propagate (EBADF/EINVAL ignored, as PostgreSQL's `fsync_fname`) · 2.8 `same_bytes` compares 64 KiB blocks, `finish` syncs the open handle · 8 the tape sort drops its per-step venue compare | done |
| 6 | 2.1 `book_batch` expands levels as `&Row`, no clone per level · 3 (tape string columns) every `text` column borrows `&str` from the row, sized in a first pass, no `String` per row per column; all column helpers take `&[impl Borrow<Row>]`. Dictionary-typed columns were considered and left: the tape schema is public API, and parquet already dictionary-encodes strings on disk | done |
| 7 | 2.2 tape `Reader::view` opens each segment once (`galata_segments::Segment`: label and row groups from one footer), decodes only row groups whose `at_micros` (or any null in it) and `ticker` statistics can hold the window (`Prune`), and builds its row mask with arrow `cmp`/`boolean` kernels instead of a closure per row | done |
| 8 | 2.3 duplicate proof reads the container only over the contained segments' receipt range (time cursors), keeps rows in arrow's row format by reference (one buffer per batch, no `Vec<u8>` per row) · S3 proof by **count**: a contained segment's rows must be held as often, and each proof spends the copies it used | done |
| 9 | 2.4 tape rebuild reads, derives and commits **one receipt day at a time** (`replay::receipt_days` lists only the days the archive holds, so an open range never steps through empty days); peak memory is one day, not the range · 1.5 (rest) the replacement plan opens each tape footer once for both labels (`Segment`), and is made before anything is written so a label refusal still touches nothing; a failed day removes what the days before it replaced. **Still open:** `replay::read_range` itself still collects a day into one `Vec` and sorts it; a streaming k-way merge over each partition's (already ordered) segments would bound it to a batch | done |

## Suggested order

1. **C1, C5-C7.** Small, local, and each one prevents data loss or a venue ban.
2. **1.1 + 1.2 + C10 + C11.** Make the ledger incremental.
3. **1.3 + 1.4 + C12 + C13.** Bound the Python reads.
4. **1.5 + 1.6 + C14.** Walk each tree once.
5. **C2, C3, C15.** Durability of writes.
6. **Tier 2**, by measured need.
7. **Tier 3**, only after profiling shows the hot path matters at current
   volume.
