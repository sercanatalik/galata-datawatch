#!/usr/bin/env bash
#
# No caller reaches past the one path to the record or the sink.
#
# Archive -> normalise -> emit is implemented once, in `ingest.rs`, and every
# caller — live frames, a walk, a metadata fetch, replay — invokes that one
# function. Several call sites are several chances to reverse the order, and
# the reversal is invisible until a payload is lost to a parse that never
# happened.
#
# Half of this the compiler already holds: `Archive::append` and
# `append_failure` are `pub(crate)`, so no other CRATE can reach them. This
# guard holds the other half — no other MODULE in this crate may either, which
# the compiler cannot see.
#
# --- the protocol: check (default) | plant ---------------------------------
#
# The plant lives HERE, beside the check, rather than in the harness. It has to
# obey the same scanning rule the check does: this guard reads each file only
# as far as its first `#[cfg(test)]`, so a violation appended to a file that
# has tests lands in the region the check deliberately ignores, and a planted
# run would pass while reporting the GUARD as broken. That is a real failure
# mode, learned from a predecessor that hit it.

set -euo pipefail

VERB=check
if [[ $# -gt 0 ]]; then
    case "$1" in check|plant) VERB="$1"; shift ;; esac
fi
ROOT="${1:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"

CRATE="$ROOT/crates/galata-datawatch/src"

# A guard handed a root it cannot scan reports success forever. Exit 2, so a
# harness can tell "I refuse" from "I found a violation".
if [[ ! -d "$CRATE" ]]; then
    echo "$(basename "$0"): $ROOT has no crates/galata-datawatch/src — refusing to scan nothing and call it ok" >&2
    exit 2
fi

# The plant goes at the TOP of a file the check actually reads, for the reason
# in the header.
if [[ "$VERB" == "plant" ]]; then
    f="$CRATE/calendar.rs"
    printf 'pub fn planted(a: &mut crate::record::Archive, p: crate::record::Payload) { let _ = a.append(p); }\n' \
        | cat - "$f" > "$f.planted" && mv "$f.planted" "$f"
    exit 0
fi

# A source file, up to its first `#[cfg(test)]`. A test that trips a guard is a
# test doing its job.
# Any `#[cfg(...)]` whose predicate mentions `test`, not the literal
# `#[cfg(test)]`. A test module gated on a feature as well —
# `#[cfg(all(test, feature = "hyperliquid"))]` — is still a test module, and a
# guard keying on the exact string silently began scanning test code the first
# time somebody wrote a legitimate one. That happened.
non_test_lines() {
    awk '/^[[:space:]]*#\[cfg\(.*test.*\)\]/{exit} {print FILENAME ":" FNR ": " $0}' "$1"
}

failures=()
while IFS= read -r file; do
    case "$file" in
        */ingest.rs)  continue ;;   # the one path itself
        */tests.rs)   continue ;;   # wholly a test module
        */record/*)   ;;            # defines append; calls are still checked below
    esac
    hits=$(non_test_lines "$file" | grep -E '\.append\(|\.append_failure\(|\.emit\(' || true)
    if [[ -n "$hits" ]]; then
        failures+=("$hits")
    fi
done < <(find "$CRATE" -name '*.rs')

if (( ${#failures[@]} > 0 )); then
    echo "ingest callers: something reaches past the one path" >&2
    printf '%s\n' "${failures[@]}" >&2
    echo "  archive-then-normalise-then-emit has ONE implementation: ingest.rs" >&2
    exit 1
fi
