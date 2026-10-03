"""One frame, split by instrument once.

Every calculator loops over its instruments and used to take each one's rows
with a fresh `filter(pl.col("ticker") == t)`: a full scan of the frame per
instrument, O(instruments × rows). `partition_by` makes one pass, keeps the
rows of each part in their original order, and leaves each loop a dictionary
lookup.
"""

from __future__ import annotations

from collections.abc import Callable

import polars as pl


def by(frame: pl.DataFrame, column: str = "ticker") -> Callable[[str], pl.DataFrame]:
    """`frame`'s rows for one value of `column`, in their order; an empty frame of its schema for a value it lacks."""
    if column not in frame.columns:
        return lambda _: frame
    parts = {key[0]: part for key, part in frame.partition_by(column, as_dict=True, maintain_order=False).items()}
    empty = frame.clear()
    return lambda value: parts.get(value, empty)
