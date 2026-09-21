//! What the store claims, asserted against a real filesystem.
//!
//! Every test is named after the claim it defends. A test called
//! `test_write_ok` can survive the deletion of the invariant it was written
//! for; one called `an_empty_writer_refuses_rather_than_claiming_a_range`
//! cannot.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use galata_segments::{
    Codec, Cursor, SegmentError, SegmentWriter, compact_partition, frontier, hold, last_durable,
    list_segments, overdue_closed, read_segment, read_segment_range, row_groups_for_range,
    write_segment,
};

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("recv_micros", DataType::Int64, false),
        Field::new("ticker", DataType::Utf8, false),
    ]))
}

/// A batch of `n` rows whose `recv_micros` runs `start, start+step, …`.
fn batch(start: i64, n: usize, step: i64) -> RecordBatch {
    let micros: Vec<i64> = (0..n as i64).map(|i| start + i * step).collect();
    let tickers: Vec<&str> = (0..n).map(|_| "BTC").collect();
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from(micros)),
            Arc::new(StringArray::from(tickers)),
        ],
    )
    .unwrap()
}

fn time(first: i64, last: i64, seq: u64) -> Cursor {
    Cursor::Time {
        first_micros: first,
        last_micros: last,
        pid: 4711,
        seq,
    }
}

fn micros_of(batches: &[RecordBatch]) -> Vec<i64> {
    let mut out = Vec::new();
    for b in batches {
        let col = b
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("recv_micros is Int64");
        out.extend(col.iter().flatten());
    }
    out
}

// ---- writing --------------------------------------------------------------

#[test]
fn a_committed_segment_is_complete() {
    let dir = tempfile::tempdir().unwrap();
    let path = write_segment(dir.path(), time(0, 9, 1), &batch(0, 10, 1), Codec::Zstd).unwrap();

    assert!(path.exists());
    assert_eq!(micros_of(&read_segment(&path).unwrap()).len(), 10);
    // And the name is what a listing sees.
    assert_eq!(list_segments(dir.path()).len(), 1);
}

#[test]
fn a_dropped_writer_leaves_no_segment() {
    let dir = tempfile::tempdir().unwrap();
    {
        let mut writer =
            SegmentWriter::create(dir.path(), time(0, 9, 1), schema(), Codec::Zstd).unwrap();
        writer.write(&batch(0, 10, 1)).unwrap();
        // Dropped without finishing: a crash between the write and the commit.
    }
    assert!(list_segments(dir.path()).is_empty(), "no segment committed");
    let leftovers: Vec<PathBuf> = std::fs::read_dir(dir.path())
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .collect();
    assert!(
        leftovers.is_empty(),
        "the temporary was not cleaned: {leftovers:?}"
    );
}

#[test]
fn an_empty_writer_refuses_rather_than_claiming_a_range() {
    let dir = tempfile::tempdir().unwrap();
    let writer = SegmentWriter::create(dir.path(), time(0, 9, 1), schema(), Codec::Zstd).unwrap();
    // A segment that says a range is covered and holds nothing is a lie a
    // reader cannot detect.
    assert!(matches!(writer.finish(), Err(SegmentError::Empty)));
    assert!(list_segments(dir.path()).is_empty());

    let empty = RecordBatch::new_empty(schema());
    assert!(matches!(
        write_segment(dir.path(), time(0, 0, 2), &empty, Codec::Zstd),
        Err(SegmentError::Empty)
    ));
}

#[test]
fn types_round_trip_without_embedded_arrow_metadata() {
    // The writer skips the embedded Arrow schema because every column type here
    // round-trips exactly from the parquet logical types. This holds the claim
    // rather than assuming it.
    let dir = tempfile::tempdir().unwrap();
    let written = batch(0, 10, 1);
    let path = write_segment(dir.path(), time(0, 9, 1), &written, Codec::Zstd).unwrap();
    let read = read_segment(&path).unwrap();

    assert_eq!(
        read[0].schema().fields().len(),
        written.schema().fields().len()
    );
    for (a, b) in read[0]
        .schema()
        .fields()
        .iter()
        .zip(written.schema().fields())
    {
        assert_eq!(a.name(), b.name());
        assert_eq!(a.data_type(), b.data_type(), "{} changed type", a.name());
    }
}

#[test]
fn a_small_flush_is_one_row_group() {
    // The declared 16,384 costs nothing on the write path: a two-second flush
    // writes a few rows, which is one group at any of the sizes measured.
    let dir = tempfile::tempdir().unwrap();
    let path = write_segment(dir.path(), time(0, 9, 1), &batch(0, 10, 1), Codec::Zstd).unwrap();
    let (_, total) = row_groups_for_range(&path, i64::MIN, i64::MAX).unwrap();
    assert_eq!(total, 1);
}

#[test]
fn every_codec_round_trips() {
    let dir = tempfile::tempdir().unwrap();
    for (i, codec) in [Codec::Zstd, Codec::Lz4, Codec::Uncompressed]
        .into_iter()
        .enumerate()
    {
        let path = write_segment(
            dir.path(),
            time(0, 9, i as u64 + 1),
            &batch(0, 10, 1),
            codec,
        )
        .unwrap();
        assert_eq!(
            micros_of(&read_segment(&path).unwrap()),
            (0..10).collect::<Vec<i64>>(),
            "{codec:?} did not round-trip"
        );
    }
}

// ---- pruning --------------------------------------------------------------

#[test]
fn a_pruned_read_holds_what_a_filtered_read_holds() {
    // The property, stated as an EQUALITY rather than as a performance claim:
    // for any range, the pruned read holds exactly what the full read holds
    // once filtered to the same range. A reader that pruned too eagerly fails
    // here; one that pruned nothing would pass, which is why the assertion
    // below on the group count is also needed.
    let dir = tempfile::tempdir().unwrap();
    let rows = 50_000; // > 16,384, so several row groups
    let path = {
        let mut w =
            SegmentWriter::create(dir.path(), time(0, rows - 1, 1), schema(), Codec::Zstd).unwrap();
        w.write(&batch(0, rows as usize, 1)).unwrap();
        w.finish().unwrap()
    };

    let all = micros_of(&read_segment(&path).unwrap());
    for (from, to) in [
        (0, 1),
        (0, rows),
        (100, 200),
        (rows - 1, rows),
        (rows, rows + 1_000), // wholly past the end
        (-1_000, 0),          // wholly before the start
        (16_000, 17_000),     // straddling a group boundary
        (20_000, 40_000),
    ] {
        let pruned: Vec<i64> = micros_of(&read_segment_range(&path, from, to).unwrap())
            .into_iter()
            .filter(|m| *m >= from && *m < to)
            .collect();
        let filtered: Vec<i64> = all
            .iter()
            .copied()
            .filter(|m| *m >= from && *m < to)
            .collect();
        assert_eq!(pruned, filtered, "range [{from}, {to}) disagreed");
    }
}

#[test]
fn a_narrow_range_decodes_few_groups() {
    // Pruning must actually happen. The equality above passes just as well
    // against a reader that decodes everything.
    let dir = tempfile::tempdir().unwrap();
    let rows = 50_000;
    let path = {
        let mut w =
            SegmentWriter::create(dir.path(), time(0, rows - 1, 1), schema(), Codec::Zstd).unwrap();
        w.write(&batch(0, rows as usize, 1)).unwrap();
        w.finish().unwrap()
    };
    let (selected, total) = row_groups_for_range(&path, 100, 200).unwrap();
    assert!(
        total > 1,
        "the fixture must span several groups, got {total}"
    );
    assert_eq!(
        selected, 1,
        "a 100-microsecond window selected {selected} of {total}"
    );
}

#[test]
fn a_group_with_no_statistics_is_read_not_skipped() {
    // A segment carrying no pruning column has no statistics to prune on. The
    // only answer pruning may give confidently is "no", so the answer here is
    // "read it" — pruning on an absence would drop rows silently.
    let dir = tempfile::tempdir().unwrap();
    let other: SchemaRef = Arc::new(Schema::new(vec![Field::new(
        "at_micros",
        DataType::Int64,
        false,
    )]));
    let b = RecordBatch::try_new(
        other.clone(),
        vec![Arc::new(Int64Array::from(vec![1_i64, 2, 3]))],
    )
    .unwrap();
    let path = write_segment(dir.path(), time(0, 2, 1), &b, Codec::Zstd).unwrap();

    // A range that could rule everything out if it were applied to at_micros.
    let read = read_segment_range(&path, 1_000_000, 2_000_000).unwrap();
    assert_eq!(
        read.iter().map(|b| b.num_rows()).sum::<usize>(),
        3,
        "a segment with no prune column must be read whole"
    );
}

// ---- listing and frontier -------------------------------------------------

fn write_at(root: &Path, scope: &str, first: i64, last: i64, seq: u64) {
    let dir = root.join(scope).join("kind=quotes").join("date=2026-09-20");
    write_segment(
        &dir,
        time(first, last, seq),
        &batch(first, 2, 1),
        Codec::Zstd,
    )
    .unwrap();
}

#[test]
fn the_frontier_is_the_minimum_not_the_maximum() {
    // The maximum would claim durability for a range one scope has not
    // written, so a view taken at it is complete for one scope and holed for
    // another — and an absent row and a not-yet-written row are the same
    // picture.
    let root = tempfile::tempdir().unwrap();
    write_at(root.path(), "venue=hyperliquid", 0, 100, 1);
    write_at(root.path(), "venue=rh-crypto", 0, 80, 1);

    let (_, position) = frontier(root.path(), &["venue=hyperliquid", "venue=rh-crypto"]).unwrap();
    assert_eq!(position, 80);
    // And the tree-wide maximum is a different, more dangerous number.
    assert_eq!(last_durable(root.path()).unwrap().1, 100);
}

#[test]
fn a_scope_that_wrote_nothing_yields_no_frontier() {
    let root = tempfile::tempdir().unwrap();
    write_at(root.path(), "venue=hyperliquid", 0, 100, 1);
    assert!(
        frontier(root.path(), &["venue=hyperliquid", "venue=rh-crypto"]).is_none(),
        "a silent scope must not inherit the other's position"
    );
}

#[test]
fn no_declared_scope_yields_no_frontier() {
    let root = tempfile::tempdir().unwrap();
    write_at(root.path(), "venue=hyperliquid", 0, 100, 1);
    assert!(frontier(root.path(), &[]).is_none());
}

#[test]
fn the_frontier_opens_no_file() {
    // A zero-byte file under a valid segment name is not readable parquet. If
    // the frontier parsed footers it would fail here; it reads names, so it
    // answers. This is what makes a boot over tens of thousands of files cheap.
    let root = tempfile::tempdir().unwrap();
    let dir = root.path().join("venue=hyperliquid");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join(time(0, 4_200, 1).file_name()), b"").unwrap();

    assert_eq!(last_durable(root.path()).unwrap().1, 4_200);
}

#[test]
fn a_temporary_is_not_a_segment_to_a_listing() {
    let root = tempfile::tempdir().unwrap();
    let dir = root.path().join("venue=hyperliquid");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join(time(0, 9, 1).temp_file_name()), b"partial").unwrap();

    assert!(list_segments(&dir).is_empty());
    assert!(last_durable(root.path()).is_none());
}

// ---- compaction -----------------------------------------------------------

/// Ten segments, each two rows, in one partition.
fn partition_of_ten(root: &Path) -> PathBuf {
    let dir = root.join("venue=hyperliquid/kind=quotes/date=2026-09-20");
    for i in 0..10i64 {
        write_segment(
            &dir,
            time(i * 10, i * 10 + 1, i as u64 + 1),
            &batch(i * 10, 2, 1),
            Codec::Zstd,
        )
        .unwrap();
    }
    dir
}

#[test]
fn many_segments_become_one_with_every_row() {
    let root = tempfile::tempdir().unwrap();
    let dir = partition_of_ten(root.path());
    assert_eq!(list_segments(&dir).len(), 10);

    let result = compact_partition(&dir, Codec::Zstd).unwrap();
    assert_eq!(result.segments_before, 10);
    assert_eq!(result.segments_after, 1);
    assert_eq!(result.rows, 20);

    let (_, path) = list_segments(&dir).pop().unwrap();
    let mut rows = micros_of(&read_segment(&path).unwrap());
    rows.sort_unstable();
    let expected: Vec<i64> = (0..10i64).flat_map(|i| [i * 10, i * 10 + 1]).collect();
    assert_eq!(rows, expected, "compaction is content-preserving repacking");
}

#[test]
fn an_interruption_leaves_rows_twice_never_missing() {
    // The replacement is committed BEFORE anything is removed. This constructs
    // that exact state: a wide segment beside everything it replaces.
    let root = tempfile::tempdir().unwrap();
    let dir = partition_of_ten(root.path());
    let all: Vec<i64> = (0..10i64).flat_map(|i| [i * 10, i * 10 + 1]).collect();
    let wide = batch(0, 1, 1); // stands in for the merged content
    write_segment(&dir, time(0, 91, 0), &wide, Codec::Zstd).unwrap();

    // Every original is still present: a duplicate read, never a loss.
    let mut seen = Vec::new();
    for (_, path) in list_segments(&dir) {
        seen.extend(micros_of(&read_segment(&path).unwrap()));
    }
    for micros in &all {
        assert!(
            seen.contains(micros),
            "{micros} was lost by an interruption"
        );
    }
}

#[test]
fn a_resumed_compaction_does_not_double_the_partition() {
    // Finishing an interrupted run is REMOVING what the replacement already
    // holds — not merging the duplicate in again, which would make every
    // resumed compaction double the partition it was meant to shrink.
    let root = tempfile::tempdir().unwrap();
    let dir = partition_of_ten(root.path());
    let expected: Vec<i64> = (0..10i64).flat_map(|i| [i * 10, i * 10 + 1]).collect();

    // A first run leaves one wide segment.
    compact_partition(&dir, Codec::Zstd).unwrap();
    // A second run over the result must change nothing.
    let again = compact_partition(&dir, Codec::Zstd).unwrap();
    assert_eq!(again.segments_after, 1);
    assert_eq!(again.rows, 0, "a single-segment partition is not rewritten");

    let (_, path) = list_segments(&dir).pop().unwrap();
    let mut rows = micros_of(&read_segment(&path).unwrap());
    rows.sort_unstable();
    assert_eq!(rows, expected, "rows were duplicated by the resume");
}

#[test]
fn a_single_segment_partition_is_untouched() {
    let root = tempfile::tempdir().unwrap();
    let dir = root
        .path()
        .join("venue=hyperliquid/kind=quotes/date=2026-09-20");
    let path = write_segment(&dir, time(0, 9, 1), &batch(0, 10, 1), Codec::Zstd).unwrap();
    let before = std::fs::metadata(&path).unwrap().len();

    let result = compact_partition(&dir, Codec::Zstd).unwrap();
    assert_eq!(result.segments_after, 1);
    assert_eq!(result.rows, 0);
    assert_eq!(
        std::fs::metadata(&path).unwrap().len(),
        before,
        "rewriting it would churn disk for nothing"
    );
}

#[test]
fn a_second_compaction_is_refused_by_name() {
    let root = tempfile::tempdir().unwrap();
    let first = hold(root.path()).unwrap();
    match hold(root.path()) {
        Err(SegmentError::Held { root: named }) => {
            assert_eq!(named, root.path());
        }
        other => panic!("a second hold must be refused, got {other:?}"),
    }
    drop(first);
    // Released with the holder, so a killed compaction leaves nothing to clear.
    assert!(hold(root.path()).is_ok());
}

#[test]
fn the_hold_file_is_not_a_segment() {
    let root = tempfile::tempdir().unwrap();
    let _held = hold(root.path()).unwrap();
    assert!(
        list_segments(root.path()).is_empty(),
        "the lock must not read as a segment"
    );
}

// ---- watching the record --------------------------------------------------

#[test]
fn a_closed_day_holding_too_many_segments_is_reported() {
    // A heartbeat says a process is alive. This says the work did not happen —
    // which catches the wrong var directory and the --dry-run left in, neither
    // of which a heartbeat notices.
    let root = tempfile::tempdir().unwrap();
    partition_of_ten(root.path());
    let overdue = overdue_closed(root.path(), "2026-09-21", 5);
    assert_eq!(overdue.len(), 1);
    assert_eq!(overdue[0].1, 10);
}

#[test]
fn todays_partition_is_never_overdue() {
    // Today's partitions are still being written, and a compaction racing the
    // writer is churn by design. The assertion and the action share one
    // definition of closed.
    let root = tempfile::tempdir().unwrap();
    partition_of_ten(root.path());
    assert!(overdue_closed(root.path(), "2026-09-20", 5).is_empty());
    assert!(overdue_closed(root.path(), "2026-09-19", 5).is_empty());
}

/// **The archive's legal case must survive COMPACTION**, not just the check.
///
/// Two flushes in one microsecond hold different rows. If compaction treats
/// one as superseded it is removed WITHOUT BEING MERGED, and those rows are
/// gone — from the record, which is the one store that cannot be rebuilt.
#[test]
fn compacting_two_flushes_in_one_microsecond_loses_no_rows() {
    let dir = tempfile::tempdir().unwrap();
    // Same microsecond, different flush, different rows.
    write_segment(
        dir.path(),
        time(100, 100, 1),
        &batch(100, 3, 0),
        Codec::Zstd,
    )
    .unwrap();
    write_segment(
        dir.path(),
        time(100, 100, 2),
        &batch(100, 5, 0),
        Codec::Zstd,
    )
    .unwrap();

    compact_partition(dir.path(), Codec::Zstd).unwrap();

    let rows: usize = list_segments(dir.path())
        .iter()
        .map(|(_, path)| {
            read_segment(path)
                .unwrap()
                .iter()
                .map(|b| b.num_rows())
                .sum::<usize>()
        })
        .sum();
    assert_eq!(rows, 8, "compaction must not drop a flush it did not merge");
}

/// **The archive's legal case must survive compaction.**
///
/// Twenty-four gaps flushed in one microsecond carry the SAME time range and
/// are told apart by pid and flush sequence. Neither holds the other's rows,
/// so neither may be treated as superseded by it.
#[test]
fn two_flushes_in_one_microsecond_are_both_kept() {
    let dir = tempfile::tempdir().unwrap();
    for seq in [1u64, 2] {
        let cursor = galata_segments::Cursor::Time {
            first_micros: 100,
            last_micros: 100,
            pid: 4711,
            seq,
        };
        std::fs::write(dir.path().join(cursor.file_name()), b"x").unwrap();
    }
    assert!(
        galata_segments::nested(dir.path()).is_empty(),
        "identical ranges are not containment: {:?}",
        galata_segments::nested(dir.path())
    );
}

/// **A container must be found however the listing sorts it.**
///
/// The ordinary listing sorts by `(first, last)` ascending, so a narrow
/// segment starting where its container starts precedes it — and an ordered
/// sweep that trusted that order walked straight past it. What followed was
/// worse than missing one: compaction would then merge the container back in
/// with the segment it already held, doubling those rows.
#[test]
fn a_narrow_segment_before_its_container_is_still_found() {
    let dir = tempfile::tempdir().unwrap();
    write_segment(
        dir.path(),
        time(100, 199, 1),
        &batch(100, 2, 1),
        Codec::Zstd,
    )
    .unwrap();
    write_segment(
        dir.path(),
        time(200, 299, 2),
        &batch(200, 2, 1),
        Codec::Zstd,
    )
    .unwrap();
    // The replacement an interrupted compaction had already written.
    write_segment(
        dir.path(),
        time(100, 299, 9),
        &batch(100, 4, 1),
        Codec::Zstd,
    )
    .unwrap();

    // BOTH narrow segments, not just the one that sorts after the container.
    assert_eq!(galata_segments::nested(dir.path()).len(), 2);

    compact_partition(dir.path(), Codec::Zstd).unwrap();
    let rows: usize = list_segments(dir.path())
        .iter()
        .map(|(_, p)| {
            read_segment(p)
                .unwrap()
                .iter()
                .map(|b| b.num_rows())
                .sum::<usize>()
        })
        .sum();
    // The replacement's four rows, with nothing merged back in on top.
    assert_eq!(
        rows, 4,
        "a resumed compaction must not double the partition"
    );
}

/// **A store that cannot be read is not a store with nothing in it.**
///
/// Every listing here answers an unreadable directory with an empty result,
/// which is right for a subtree and wrong for a declared root: the binaries
/// report no candidates as "nothing to do" and exit 3, so a mistyped path
/// looks exactly like a tidy store.
#[test]
fn a_root_that_cannot_be_read_is_refused_by_name() {
    let missing = std::path::Path::new("/nonexistent-store-a8f3/archive");
    // The listing still answers empty — that behaviour is deliberate.
    assert!(list_segments(missing).is_empty());
    // And `scannable` is what turns it into a refusal.
    let error = galata_segments::scannable(missing).unwrap_err();
    let said = error.to_string();
    assert!(said.contains("nonexistent-store-a8f3"), "{said}");
    assert!(said.contains("cannot scan"), "{said}");

    // An existing, empty store is genuinely empty and passes.
    let real = tempfile::tempdir().unwrap();
    assert!(galata_segments::scannable(real.path()).is_ok());
    assert!(list_segments(real.path()).is_empty());
}
