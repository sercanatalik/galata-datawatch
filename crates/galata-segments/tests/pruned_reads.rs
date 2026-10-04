//! A pruned read decodes fewer row groups and loses no row a filter would keep.

use std::sync::Arc;

use arrow::array::{Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use galata_segments::{Codec, Cursor, MAX_ROW_GROUP_ROWS, Prune, Segment, write_segment_labelled};

/// Three row groups: BTC at 0.., ETH at 0.., and ETH with a null time in it.
fn segment(dir: &std::path::Path) -> std::path::PathBuf {
    let n = MAX_ROW_GROUP_ROWS;
    let mut tickers = Vec::new();
    let mut at = Vec::new();
    for group in 0..3 {
        for i in 0..n {
            tickers.push(if group == 0 { "BTC" } else { "ETH" });
            at.push(if group == 2 && i == 7 {
                None
            } else {
                Some((group * n + i) as i64)
            });
        }
    }
    let schema = Arc::new(Schema::new(vec![
        Field::new("ticker", DataType::Utf8, true),
        Field::new("at_micros", DataType::Int64, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(tickers)),
            Arc::new(Int64Array::from(at)),
        ],
    )
    .unwrap();
    write_segment_labelled(
        dir,
        Cursor::Seq {
            first: 1,
            last: 3 * n as u64,
        },
        &batch,
        Codec::Zstd,
        &["ticker", "at_micros"],
        &[("venue", "x")],
    )
    .unwrap()
}

fn rows(batches: &[RecordBatch]) -> usize {
    batches.iter().map(RecordBatch::num_rows).sum()
}

#[test]
fn a_window_decodes_only_the_groups_that_can_hold_it_and_every_null_time() {
    let dir = tempfile::tempdir().unwrap();
    let path = segment(dir.path());
    let n = MAX_ROW_GROUP_ROWS as i64;

    // A window inside the first group: that group, plus the third, which holds
    // a row with no venue time and so cannot be ruled out.
    let window = [Prune::Int64Range {
        column: "at_micros",
        from: 10,
        to: 20,
        keep_nulls: true,
    }];
    let read = Segment::open(&path).unwrap().read_where(&window).unwrap();
    assert_eq!(rows(&read), 2 * n as usize);
    let nulls: usize = read.iter().map(|b| b.column(1).null_count()).sum();
    assert_eq!(nulls, 1, "the timeless row survived pruning");

    // Without keeping nulls, the first group alone.
    let strict = [Prune::Int64Range {
        column: "at_micros",
        from: 10,
        to: 20,
        keep_nulls: false,
    }];
    assert_eq!(
        rows(&Segment::open(&path).unwrap().read_where(&strict).unwrap()),
        n as usize
    );
}

#[test]
fn a_ticker_decodes_only_its_groups_and_a_label_needs_no_second_open() {
    let dir = tempfile::tempdir().unwrap();
    let path = segment(dir.path());
    let segment = Segment::open(&path).unwrap();
    assert_eq!(segment.label("venue").as_deref(), Some("x"));
    let btc = segment
        .read_where(&[Prune::Equals {
            column: "ticker",
            value: "BTC",
        }])
        .unwrap();
    assert_eq!(rows(&btc), MAX_ROW_GROUP_ROWS);
    let none = Segment::open(&path)
        .unwrap()
        .read_where(&[Prune::Equals {
            column: "ticker",
            value: "SOL",
        }])
        .unwrap();
    assert!(none.is_empty());
    // An unknown column rules nothing out.
    let all = Segment::open(&path)
        .unwrap()
        .read_where(&[Prune::Equals {
            column: "nope",
            value: "SOL",
        }])
        .unwrap();
    assert_eq!(rows(&all), 3 * MAX_ROW_GROUP_ROWS);
}
