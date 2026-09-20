#!/usr/bin/env bash
#
# Nothing below the loop reads a clock.
#
# Every timing invariant in this system is asserted by driving a controllable
# clock: a rotation at ten minutes, a flush at two seconds, a staleness window
# at a minute. One real `SystemTime::now()` in a helper below the loop silently
# removes that for whatever path it is on — and the test still passes, so
# NOTHING SAYS SO. That is the failure this exists to catch: not a wrong
# answer, a test that quietly stops asking.
#
# It acquires a second consequence later: a venue that signs its requests puts
# a timestamp in the signature, so clock skew becomes a rejected request rather
# than only a flaky test.
#
# Permitted: `capture/clock.rs`, which IS the clock.
#
# --- the protocol: check (default) | plant ---------------------------------

set -euo pipefail

VERB=check
if [[ $# -gt 0 ]]; then
    case "$1" in check|plant) VERB="$1"; shift ;; esac
fi
ROOT="${1:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
CRATE="$ROOT/crates/galata-datawatch/src"

# A guard handed a root it cannot scan reports success forever.
if [[ ! -d "$CRATE" ]]; then
    echo "$(basename "$0"): $ROOT has no crates/galata-datawatch/src — refusing to scan nothing and call it ok" >&2
    exit 2
fi

if [[ "$VERB" == "plant" ]]; then
    f="$CRATE/record/mod.rs"
    printf 'pub fn planted() -> u128 { std::time::SystemTime::now().elapsed().unwrap().as_micros() }\n' \
        | cat - "$f" > "$f.planted" && mv "$f.planted" "$f"
    exit 0
fi

non_test_lines() {
    awk '/#\[cfg\(test\)\]/{exit} {print FILENAME ":" FNR ": " $0}' "$1"
}

failures=()
while IFS= read -r file; do
    case "$file" in
        */capture/clock.rs) continue ;;   # the clock itself
        */tests.rs)         continue ;;   # wholly a test module
    esac
    hits=$(non_test_lines "$file" | grep -E 'SystemTime::now|Instant::now|UNIX_EPOCH' || true)
    [[ -n "$hits" ]] && failures+=("$hits")
done < <(find "$CRATE" -name '*.rs')

if (( ${#failures[@]} > 0 )); then
    echo "clock discipline: something below the loop reads a clock" >&2
    printf '%s\n' "${failures[@]}" >&2
    echo "  every timing invariant here is asserted by driving one that advances only when told" >&2
    exit 1
fi
