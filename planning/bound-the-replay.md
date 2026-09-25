# bound-the-replay

**NOT PROPOSED.** Named 2026-09-25, from the `galata-research` exploration
(`../galata-research/planning/galata-research.md`), which needs a view at a
replay position and found none.

Tier 3, the reader. Carries forward legacy's `crates/reader-replay`.

---

*`Reader::view()` is bounded at the store's durable frontier, and that is the
right bound for anything live. A replay needs a different one. By replay time
the whole day is on disk, so the frontier sits at the end of it, and a strategy
replayed at 10:00 that takes a view sees 10:01 onwards. **Nothing in this tree
can say "the view as it stood at T"**, and the module states the absence itself:
"a replay that must be bounded in time has to drive by sequence until something
provides that mapping".*

---

## What legacy had, and what it did not do

```
  galata-reader          Reader::view()          any component may link
  galata-reader-replay   view_as_of(bound)       replay hosts only
                         └─ check-reader-linkage.sh: no component links it,
                            read from the RESOLVED graph; nothing outside the
                            two reader crates names View::__as_of
```

38 lines and a guard. The bound was `bound_micros`, a receipt time, because
legacy's archive frontier was `last_durable_recv_micros_for_scope`.

**And it had no caller.** `view_as_of` appears in `reader-replay/src/lib.rs` and
`reader-replay/tests/widening.rs` and nowhere else. The widening was designed,
guarded and tested, and no replay host ever took a view through it. That is the
failure pattern legacy recorded as the deepest one: protection with no call
site. **This plan does not land without its first caller** (see *The line to
hold*).

## What is different here

The new bound is **not a time**. `Bound` is a stream sequence **per venue**
(`tape/reader.rs`, `Bound::positions`), the minimum durable frontier across
the declared scopes. So legacy's `view_as_of(micros)` does not port as it
stands. It needs a mapping, and the mapping is already on disk:

- **Every tape row carries `recv_micros`** (our clock, never null) next to
  **`stream_seq`**, the archive row the bytes are in (`tape/schema.rs:77-78`).
- **The sequence is seeded from the boot's microseconds** and counts up by one
  per payload (`Archive::from_seq`, `capture/run.rs:177`). It is monotonic
  across restarts, because a process receives far fewer than one payload per
  microsecond. It is not a time, though: after boot it falls further behind the
  clock with every second.
- **Within a venue, `stream_seq` and `recv_micros` rise together**, except where
  the wall clock steps backwards (an NTP correction). Then the rows received by
  T are no longer a prefix of the sequence.

So a receipt time T becomes, per venue:

```
  position(venue, T) = the greatest s such that EVERY row of that venue
                       with stream_seq ≤ s has recv_micros ≤ T
                     = one below the first row, in sequence order, received after T
```

That is a **prefix** rule, not a "maximum seq with recv ≤ T" rule, so a clock
step withholds rows rather than revealing one from after T. The safe direction
is to withhold, as the reader already says about a row with no sequence.

**The replay bound is never above the durable frontier.** It is
`min(position(venue, T), Bound::of(...).of_venue(venue))`. A replay at a T
past what the store holds sees what the store holds, and says so.

## The shape

```
  galata-datawatch (published)                 galata-tape-replay (new, published)
  ───────────────────────────                  ───────────────────────────────────
  Reader::view(window)          ◀── any caller
  Bound::of(root, scopes)
  #[doc(hidden)] Reader::__at(bound) ◀──────── ReplayReader::at(root, scopes, T)
                                                ReplayReader::advance_to(T')   T' ≥ T
                                                ReplayReader::view(window)
                                                ReplayReader::position()  (T, per-venue seq)
```

- **A separate crate, not a feature.** Cargo features unify across the graph, so
  a `replay` feature enabled by any crate in a build is on for all of them, and
  a live binary would inherit the widening from a test dependency. A crate is
  an edge in the resolved graph, and an edge is something a guard can refuse.
- **`advance_to` moves forward only.** A replay host drives a virtual clock,
  and time does not go back. Forward-only also lets the position be extended
  incrementally, from the previous position, instead of rescanning from the
  start of the tape on every tick. An attempt to go back is an error naming
  both times. A host that wants T again opens a new reader.
- **The bound comes from T and nothing else.** There is no `at_seq(bound)`
  taking positions directly. A caller that can name a sequence can name one
  past T, and the sequence is an implementation fact of the record, not the
  replay host's clock.
- **Everything else is the live type's.** Windows, `NoFrontier`, `Unlabelled`,
  per-venue withholding, timeless rows kept, instruments matched whole: the
  widening changes where the bound comes from and nothing else, which is
  legacy's own sentence and still the right one.

## The one thing a receipt time cannot know

Live, a reader at T sees what was **received and durably renamed** by T.
Segments are buffered and committed by rename, and the tape holds no
per-row commit time: compaction rewrites segments, so an mtime says nothing.
**"Received by T" is therefore an upper bound on what a live process could have
seen**, optimistic by up to one flush interval.

Three honest answers, and the plan has to pick one:

1. **Declare a lag.** `at(root, scopes, T, lag)` bounds at `T − lag`, with the
   venue's flush interval as the lag. This guesses *against* the strategy
   (v/D-025), which is the direction a simulator is meant to guess.
2. **Record the commit.** Carry a `committed_micros` per segment in its footer
   label, beside the venue. That is exact, but it's a tape schema change, and
   compaction has to keep the *earliest* commit of the rows it merges.
3. **State it and stop there.** "Receipt-bounded, optimistic by ≤ one flush" as
   documentation. This is legacy's answer by omission.

Leaning 1 now and 2 later: 1 is one argument and conservative, and 2 is a real
change worth making only if `diff-the-views` ever needs tick-level parity with
the live path.

## Where research meets it

`galata-research` hands events to a strategy in order (v/D-029: *history is
handed, never scanned*). The strategy never holds a reader. The **host** does:
at each decision it takes the lookback through `ReplayReader::view`, and
advances the reader with the virtual clock. The no-lookahead property holds
twice:

```
  the strategy   cannot name a bound, because it holds no reader    (the seam)
  the host       cannot name one either: T is its virtual clock,    (this crate)
                 and advance_to refuses to go back
```

## The line to hold

- **It lands with its first caller.** The same change, or a change that lands
  together with it, puts `ReplayReader` in `galata-research`'s run host and
  runs one replay through it. A widening with no caller is legacy's
  `view_as_of` again.
- **The guard travels with the consumer.** Legacy's `check-reader-linkage.sh`
  read one workspace's graph. Here the widening is published and its consumers
  are other repos, so the rule has two halves:
  - in `galata-datawatch`: nothing outside the reader and `galata-tape-replay`
    names `Reader::__at` (a grep, as legacy did)
  - in each consumer: no live binary links `galata-tape-replay`, read from
    `cargo tree` of that binary. `galata-research` has no live binary, so it
    links freely. The future algo host's repo ships the guard on its first day.
- **`view()` does not change.** No live caller gains an argument, a default or
  an overload. The live reader stays unwidenable.

## Depends on, and depended on by

- **Depends on** nothing unbuilt. `Bound`, `LabelCache`, the per-venue frontier,
  and `recv_micros`/`stream_seq` on every row are all in place.
- **Depended on by** `galata-research` (the run host), and later `diff-the-views`
  and any warm-up that replays the last window.

## Open

- **The lag: 1, 2 or 3 above.**
- **Cost, and which store answers it. Checked 2026-09-25:** the position
  should come from the **archive**, not the tape. The tape sorts by `(venue,
  ticker, at_micros)`, and `galata-segments/src/reader.rs:49-53` says a store
  sorted that way has statistics that are "wide on arrival time". The archive
  is written in arrival order, so its `recv_micros` statistics are tight and
  `read_segment_range` picks a contiguous few row groups. Every payload there
  has a `seq` and a receipt time, including the ones the tape filters out, and
  a tape row's `stream_seq` *is* the archive's `seq` (`tape/rebuild.rs:18`).
  So `position(venue, T)` reads the archive near T, and the tape is bounded by
  it. The cost: the replay reader needs both roots, as legacy's
  `view_as_of(bound, archive_root, tape_root, scopes)` did. Measure one
  `advance_to` step over a day of quotes before promising it per tick.
- **Publish together or later.** `galata-tape-replay` in the 0.1.0 lockstep set
  (Tier 10), or held back until `galata-research` has run it. Leaning to hold
  it back: publishing is irreversible, and the first caller is what shows
  whether the API is right.
