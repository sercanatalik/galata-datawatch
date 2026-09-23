#!/usr/bin/env bash
#
# Everything CI runs, in the order that fails cheapest first.
#
# The test suite must pass WITH NO NETWORK ACCESS. This tier's whole claim is
# that both crates are provable offline; an accidental network dependency would
# make that claim false silently on the machine where it was written and loudly
# on every other. `--offline` holds the Cargo half of it.
#
# Usage: check-all.sh [root]

set -euo pipefail
ROOT="${1:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
cd "$ROOT"

# **Timing, because deciding what to make faster needed it twice this week and
# the gate could not say it.** Once it was a guard spending 1m55s of a 2m17s
# run purging caches on every run; once it was a sibling repository's build,
# wedged for three hours, making every number here a lie. Neither was visible
# from output that says what passed and nothing about what it cost.
#
# `SECONDS` is the shell's own counter, not `EPOCHREALTIME`: the bash on a
# developer's macOS is **3.2.57**, where `EPOCHREALTIME` and `EPOCHSECONDS` do
# not exist — they arrived in bash 5. CI has bash 5, so a script written
# against them would work in CI and print nothing on the machine where the work
# is done. The cost is one-second resolution, which is the right resolution for
# a question about parts that take tens of seconds; a cached guard reporting
# `0s` is exactly the thing worth seeing.
#
# Two parallel arrays rather than one associative array, because bash 3.2 has
# no `declare -A`.
TIMED_NAMES=()
TIMED_SECS=()
part_started=0
part_name=""

finish_part() {
    [[ -z "$part_name" ]] && return 0
    TIMED_NAMES+=("$part_name")
    TIMED_SECS+=("$(( SECONDS - part_started ))")
    part_name=""
}

start_part() {
    finish_part
    part_name="$1"
    part_started=$SECONDS
}

# **Printed even when the gate fails**, via a trap: a part that fails is often
# the slow one, and `set -e` ending the script would lose the measurement at
# the moment it is most interesting.
report_timings() {
    finish_part
    (( ${#TIMED_NAMES[@]} == 0 )) && return 0
    printf '\n=== where the time went (whole seconds; slowest first)\n'
    local i
    for (( i = 0; i < ${#TIMED_NAMES[@]}; i++ )); do
        printf '%6s  %s\n' "${TIMED_SECS[$i]}s" "${TIMED_NAMES[$i]}"
    done | sort -rn
    printf '%6s  %s\n' "${SECONDS}s" "the whole gate"
}
trap report_timings EXIT

step() { start_part "$1"; printf '\n=== %s\n' "$1"; }
# A header with no timing of its own, for a step whose parts are timed
# individually — otherwise "guards" appears as 0s beside the guards that are
# the actual cost, which is worse than saying nothing.
header() { finish_part; printf '\n=== %s\n' "$1"; }

step "format"
cargo fmt --all -- --check

step "lints"
cargo clippy --all-targets --all-features --offline -- -D warnings

header "guards"
for guard in scripts/check-*.sh; do
    [[ "$guard" == "scripts/check-all.sh" ]] && continue
    printf '  %s\n' "$guard"
    # Each guard timed on its own: "guards" as one number would hide the one
    # that is worth looking at behind the twenty that are not.
    start_part "guard $(basename "$guard" .sh)"
    "$guard"
done
finish_part

step "the guards can fail"
./scripts/test-guards.sh

step "tests, offline"
cargo test --all-features --offline

printf '\nall green\n'
