//! Compacting today's closed hours (`compact-closed-hours`).
//!
//! Capture flushes every two seconds, so today's partition reaches tens of
//! thousands of segments before the nightly compaction. A closed hour is
//! merged as a closed day is; the open hour is never read.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use galata_segments::{
    Codec, Cursor, HOUR_MICROS, compact_closed_hours, list_segments, nested, read_segment,
    write_segment,
};

const TODAY: &str = "2026-09-27";
/// 2026-09-27T00:00:00Z.
const MIDNIGHT: i64 = 1_790_467_200_000_000;

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("recv_micros", DataType::Int64, false),
        Field::new("ticker", DataType::Utf8, false),
    ]))
}

fn rows(micros: &[i64]) -> RecordBatch {
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from(micros.to_vec())),
            Arc::new(StringArray::from(vec!["BTC"; micros.len()])),
        ],
    )
    .unwrap()
}

fn at(hour: i64, micros: i64) -> i64 {
    MIDNIGHT + hour * HOUR_MICROS + micros
}

/// One flush, holding its own two endpoints as rows.
fn flush(dir: &Path, first: i64, last: i64, seq: u64) -> PathBuf {
    let cursor = Cursor::Time {
        first_micros: first,
        last_micros: last,
        pid: 4711,
        seq,
    };
    write_segment(dir, cursor, &rows(&[first, last]), Codec::Zstd).unwrap()
}

/// Three flushes in each of hours 10, 11 and 12 of today.
fn today_with_three_hours(root: &Path) -> PathBuf {
    let dir = root.join(format!("venue=hyperliquid/kind=quotes/date={TODAY}"));
    let mut seq = 1;
    for hour in 10..13 {
        for i in 0..3 {
            flush(
                &dir,
                at(hour, i * 2_000_000),
                at(hour, i * 2_000_000 + 1_000),
                seq,
            );
            seq += 1;
        }
    }
    dir
}

fn all_rows(dir: &Path) -> Vec<i64> {
    let mut out = Vec::new();
    for (_, path) in list_segments(dir) {
        for batch in read_segment(&path).unwrap() {
            let col = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            out.extend(col.iter().flatten());
        }
    }
    out.sort_unstable();
    out
}

fn files(dir: &Path) -> Vec<(String, Vec<u8>)> {
    let mut out: Vec<(String, Vec<u8>)> = list_segments(dir)
        .into_iter()
        .map(|(_, p)| {
            (
                p.file_name().unwrap().to_string_lossy().into_owned(),
                std::fs::read(&p).unwrap(),
            )
        })
        .collect();
    out.sort();
    out
}

fn hour_of(cursor: &Cursor) -> i64 {
    (cursor.last_position() as i64 - MIDNIGHT).div_euclid(HOUR_MICROS)
}

#[test]
fn closed_hours_become_one_segment_each_and_the_open_hour_is_untouched() {
    let root = tempfile::tempdir().unwrap();
    let dir = today_with_three_hours(root.path());
    let before = all_rows(&dir);
    let open_hour: Vec<(String, Vec<u8>)> = files(&dir)
        .into_iter()
        .filter(|(name, _)| {
            list_segments(&dir)
                .iter()
                .any(|(c, p)| p.file_name().unwrap().to_string_lossy() == *name && hour_of(c) == 12)
        })
        .collect();
    assert_eq!(open_hour.len(), 3);

    let done = compact_closed_hours(root.path(), TODAY, at(12, 0), Codec::Zstd).unwrap();

    assert_eq!(done.segments_before, 6);
    assert_eq!(done.segments_after, 2);
    assert_eq!(done.rows, 12);
    let hours: Vec<i64> = list_segments(&dir)
        .iter()
        .map(|(c, _)| hour_of(c))
        .collect();
    assert_eq!(hours.iter().filter(|h| **h == 10).count(), 1);
    assert_eq!(hours.iter().filter(|h| **h == 11).count(), 1);
    assert_eq!(hours.iter().filter(|h| **h == 12).count(), 3);
    for kept in &open_hour {
        assert!(
            files(&dir).contains(kept),
            "an open-hour flush was touched: {}",
            kept.0
        );
    }
    assert_eq!(
        all_rows(&dir),
        before,
        "compaction is content-preserving repacking"
    );
}

#[test]
fn a_second_run_changes_nothing() {
    let root = tempfile::tempdir().unwrap();
    let dir = today_with_three_hours(root.path());
    compact_closed_hours(root.path(), TODAY, at(12, 0), Codec::Zstd).unwrap();
    let after_first = files(&dir);

    let again = compact_closed_hours(root.path(), TODAY, at(12, 0), Codec::Zstd).unwrap();

    assert_eq!(
        again.segments_before, 0,
        "a merged hour is one segment and is skipped"
    );
    assert_eq!(files(&dir), after_first);
}

#[test]
fn a_late_segment_in_a_merged_hour_is_merged_in() {
    // A restart's first flush, or a clock step: a segment inside an hour
    // already merged, holding rows the merge does not. Contained by range,
    // but not a duplicate — so it is merged, never removed.
    let root = tempfile::tempdir().unwrap();
    let dir = today_with_three_hours(root.path());
    compact_closed_hours(root.path(), TODAY, at(12, 0), Codec::Zstd).unwrap();
    let late = at(10, 3_000_000);
    flush(&dir, late, late + 10, 99);
    let mut expected = all_rows(&dir);
    expected.sort_unstable();

    compact_closed_hours(root.path(), TODAY, at(12, 0), Codec::Zstd).unwrap();

    let tens = list_segments(&dir)
        .iter()
        .filter(|(c, _)| hour_of(c) == 10)
        .count();
    assert_eq!(tens, 1);
    assert_eq!(
        all_rows(&dir),
        expected,
        "the late segment's rows were lost"
    );
}

#[test]
fn an_interrupted_hour_is_finished_not_doubled() {
    // The state an interruption leaves: hour 10's replacement on disk beside
    // the originals it holds.
    let root = tempfile::tempdir().unwrap();
    let dir = today_with_three_hours(root.path());
    let originals: Vec<i64> = all_rows(&dir);
    let hour_ten: Vec<i64> = originals
        .iter()
        .copied()
        .filter(|m| *m < at(11, 0))
        .collect();
    let replacement = Cursor::Time {
        first_micros: *hour_ten.first().unwrap(),
        last_micros: *hour_ten.last().unwrap(),
        pid: 4711,
        seq: 0,
    };
    write_segment(&dir, replacement, &rows(&hour_ten), Codec::Zstd).unwrap();

    compact_closed_hours(root.path(), TODAY, at(11, 0), Codec::Zstd).unwrap();

    assert_eq!(all_rows(&dir), originals, "each row once, not twice");
}

#[test]
fn nothing_an_hour_merge_leaves_is_nested() {
    // A flush straddling the boundary belongs to the hour its range ends in,
    // so the two hours' merges abut and neither contains the other.
    let root = tempfile::tempdir().unwrap();
    let dir = today_with_three_hours(root.path());
    flush(&dir, at(11, HOUR_MICROS - 1_000_000), at(12, 1_000_000), 50);
    let before = all_rows(&dir);

    compact_closed_hours(root.path(), TODAY, at(13, 0), Codec::Zstd).unwrap();

    assert_eq!(list_segments(&dir).len(), 3, "one segment per closed hour");
    assert!(
        nested(&dir).is_empty(),
        "an hour merge contains another: {:?}",
        nested(&dir)
    );
    assert_eq!(all_rows(&dir), before);
}

#[test]
fn another_day_and_a_sequence_store_are_left_alone() {
    // A closed day is `compact_closed`'s; a sequence-cursor partition has no
    // receipt time in its names to call an hour closed by.
    let root = tempfile::tempdir().unwrap();
    let yesterday = root
        .path()
        .join("venue=hyperliquid/kind=quotes/date=2026-09-26");
    for i in 0..3 {
        flush(
            &yesterday,
            at(-10, i * 1_000_000),
            at(-10, i * 1_000_000 + 10),
            i as u64 + 1,
        );
    }
    let tape = root.path().join(format!("kind=quotes/date={TODAY}"));
    for i in 0..3u64 {
        write_segment(
            &tape,
            Cursor::Seq {
                first: i * 10,
                last: i * 10 + 5,
            },
            &rows(&[at(1, 0)]),
            Codec::Zstd,
        )
        .unwrap();
    }
    let (y, t) = (files(&yesterday), files(&tape));

    let done = compact_closed_hours(root.path(), TODAY, at(20, 0), Codec::Zstd).unwrap();

    assert_eq!(done.segments_before, 0);
    assert_eq!(files(&yesterday), y);
    assert_eq!(files(&tape), t);
}
