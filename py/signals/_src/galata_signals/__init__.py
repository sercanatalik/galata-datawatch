"""Market-data signals over galata-datawatch's tape (roadmap Tier 16).

    galata-signals varcov --var var --out var/signals-staging/varcov.arrow

A calculator reads the tape through galata-research, computes, and hands one
run's rows to `galata-signals-commit` as an Arrow IPC file in the signals
schema. It never writes the tape: the commit, in Rust, owns that contract.
"""
