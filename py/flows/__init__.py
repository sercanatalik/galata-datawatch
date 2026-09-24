"""The schedule for the record's maintenance: cadence, and nothing else.

Served with ``cereyan serve py``. Four invariants, carried from legacy's
``design/batch/batch.md``; every flow here keeps them:

1. **The run store is deletable.** Deleting cereyan's SQLite changes nothing
   about what a flow does next. No flow reads a cursor from it; the tape is
   projected over a fixed trailing window instead.
2. **The schedule is the retry.** Compaction folds every closed day and the
   projection replaces its whole window, so a failed night is repaired by
   the next. Only ``rebuild-one-day``, which is for history, retries.
3. **The lane holds no credential**, and passes none: a job's environment is
   built, never inherited (``_runner.job_env``).
4. **Registration is the exposure boundary.** cereyan's ``run_flow`` starts
   any registered flow, so ``galata-retain --delete`` is not a flow.

A flow may read a clock, name a date and spawn a tool. It may not compute a
figure: the Rust tools are authoritative for what they produce, and a number
worked out here would be a second opinion nobody checks.
"""
