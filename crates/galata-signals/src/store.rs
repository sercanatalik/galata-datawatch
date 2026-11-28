//! **The tape as the cursor, and one run as one commit.**
//!
//! [`stored_asof`] is the newest `asof` per horizon already on the tape for
//! `varcov`. [`commit`] writes a run through `signals::write`, and refuses a
//! run the tape already holds: on 2026-11-18 one run's hand-off was committed
//! three times by consecutive processes, three identical segments under one
//! `computed_micros`, and the tape's judge reported the overlaps hourly
//! (`design/measured.md`). Taken under the tape's exclusive hold, the cursor is
//! read again there, so of two runs at once at most one writes a horizon.

use std::collections::BTreeMap;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::time::Duration;

use arrow::array::{Array, Int64Array, StringArray};
use galata_datawatch::signals;
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use crate::VARCOV;
use crate::rows::{Row, batch};

/// How long a commit waits out a writer holding the tape: a projection has
/// taken 25 minutes under load (`py/signals/frontier.py`).
pub const PATIENCE: Duration = Duration::from_secs(50 * 60);

fn segments(tape_root: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    let Ok(dates) = std::fs::read_dir(tape_root.join("kind=signals")) else {
        return out;
    };
    for date in dates.flatten() {
        if let Ok(files) = std::fs::read_dir(date.path()) {
            for f in files.flatten() {
                let p = f.path();
                if p.extension().is_some_and(|e| e == "parquet") {
                    out.push(p);
                }
            }
        }
    }
    out.sort();
    out
}

/// The newest `asof` per horizon stored for `varcov`.
pub fn stored_asof(tape_root: &Path) -> Result<BTreeMap<String, i64>, String> {
    let mut out: BTreeMap<String, i64> = BTreeMap::new();
    for path in segments(tape_root) {
        let file = File::open(&path).map_err(|e| format!("{}: {e}", path.display()))?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)
            .map_err(|e| format!("{}: {e}", path.display()))?;
        let names = ["signal", "horizon", "asof_micros"];
        let schema = builder.parquet_schema();
        let indices: Vec<usize> = names
            .iter()
            .map(|n| {
                (0..schema.num_columns())
                    .find(|k| schema.column(*k).name() == *n)
                    .ok_or_else(|| format!("{}: no {n}", path.display()))
            })
            .collect::<Result<_, _>>()?;
        let mask = ProjectionMask::leaves(schema, indices);
        let reader = builder
            .with_projection(mask)
            .build()
            .map_err(|e| format!("{}: {e}", path.display()))?;
        for b in reader {
            let b = b.map_err(|e| format!("{}: {e}", path.display()))?;
            let signal = b
                .column_by_name("signal")
                .and_then(|c| c.as_any().downcast_ref::<StringArray>());
            let horizon = b
                .column_by_name("horizon")
                .and_then(|c| c.as_any().downcast_ref::<StringArray>());
            let asof = b
                .column_by_name("asof_micros")
                .and_then(|c| c.as_any().downcast_ref::<Int64Array>());
            let (Some(signal), Some(horizon), Some(asof)) = (signal, horizon, asof) else {
                return Err(format!("{}: not the signals schema", path.display()));
            };
            for i in 0..b.num_rows() {
                if signal.value(i) == VARCOV && !asof.is_null(i) {
                    let e = out.entry(horizon.value(i).to_string()).or_insert(i64::MIN);
                    *e = (*e).max(asof.value(i));
                }
            }
        }
    }
    Ok(out)
}

/// What a commit did.
#[derive(Debug, Clone, PartialEq)]
pub enum Committed {
    /// Rows written, in these segments.
    Written { rows: usize, segments: Vec<PathBuf> },
    /// Nothing: every horizon was stored by another run meanwhile.
    Nothing,
}

/// Write one run's rows, under the tape's exclusive hold. Refused when a
/// segment of this `computed_micros` is already on the tape; rows of a horizon
/// another run stored meanwhile are dropped.
pub fn commit(
    tape_root: &Path,
    rows: Vec<Row>,
    computed_micros: i64,
    run_id: &str,
    code: &str,
    patience: Duration,
) -> Result<Committed, String> {
    let _hold = galata_segments::wait(tape_root, galata_segments::Mode::Exclusive, patience)
        .map_err(|e| format!("the tape is held: {e}"))?;
    let prefix = format!("t-{computed_micros}_");
    if let Some(found) = segments(tape_root).into_iter().find(|p| {
        p.file_name()
            .is_some_and(|n| n.to_string_lossy().starts_with(&prefix))
    }) {
        return Err(format!(
            "run {run_id} is already on the tape ({}): one run is one commit",
            found.display()
        ));
    }
    let stored = stored_asof(tape_root)?;
    let rows: Vec<Row> = rows
        .into_iter()
        .filter(|r| stored.get(&r.horizon).is_none_or(|s| r.asof_micros > *s))
        .collect();
    if rows.is_empty() {
        return Ok(Committed::Nothing);
    }
    let n = rows.len();
    let segments = signals::write(tape_root, computed_micros, run_id, code, &batch(&rows)?)
        .map_err(|e| e.to_string())?;
    Ok(Committed::Written { rows: n, segments })
}
