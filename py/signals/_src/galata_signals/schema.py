"""The signals schema, as `galata_datawatch::signals::schema()` declares it.

A mirror, not a second definition: the commit compares fields (name, type,
nullability) and refuses a mismatch, and a fixture this package writes is read
back by a Rust test. `pa.string()` everywhere, never `large_string`: polars
produces large strings, and the commit refuses them by name.
"""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.ipc as ipc

SCHEMA = pa.schema(
    [
        pa.field("signal", pa.string(), nullable=False),
        pa.field("horizon", pa.string(), nullable=False),
        pa.field("measure", pa.string(), nullable=False),
        pa.field("ticker_i", pa.string(), nullable=False),
        pa.field("ticker_j", pa.string(), nullable=True),
        pa.field("h", pa.int64(), nullable=False),
        pa.field("value", pa.float64(), nullable=True),
        pa.field("absent", pa.string(), nullable=True),
        pa.field("n_eff", pa.float64(), nullable=True),
        pa.field("asof_micros", pa.int64(), nullable=False),
        pa.field("target_micros", pa.int64(), nullable=False),
        pa.field("computed_micros", pa.int64(), nullable=False),
        pa.field("fitted_through_micros", pa.int64(), nullable=True),
        pa.field("fit_from_micros", pa.int64(), nullable=True),
        pa.field("model", pa.string(), nullable=False),
        pa.field("params", pa.string(), nullable=False),
        pa.field("fitted", pa.bool_(), nullable=False),
        pa.field("after_gap", pa.bool_(), nullable=False),
        pa.field("code", pa.string(), nullable=False),
        pa.field("run_id", pa.string(), nullable=False),
    ]
)


def write(rows: list[dict], path: Path) -> Path:
    """One run's rows as an Arrow IPC file, through a temporary and a rename.

    A value and a reason are exclusive: a row with both, or neither, is
    refused here before the commit would refuse it there.
    """
    for i, row in enumerate(rows):
        if (row["value"] is None) == (row["absent"] is None):
            raise ValueError(f"row {i} ({row['horizon']} {row['measure']} {row['ticker_i']}/{row['ticker_j']}): a value or the reason it is absent, exactly one")
    table = pa.Table.from_pylist(rows, schema=SCHEMA)
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(f".{path.name}.writing")
    with pa.OSFile(str(temp), "wb") as sink, ipc.new_file(sink, SCHEMA) as writer:
        writer.write_table(table)
    temp.replace(path)
    return path
