#!/usr/bin/env bash
#
# Exactly one module names a venue.
#
# Everything above the seam holds a `dyn Adapter` and cannot tell two venues
# apart. A venue name appearing elsewhere is a branch that will be wrong for the
# next venue, and it will be wrong QUIETLY — the code compiles, the tests pass,
# and one venue takes a path the other does not.
#
# Permitted: `src/adapters/` — the resolver and the adapter implementations.
#
# --- the protocol: check (default) | plant ---------------------------------
#
# The plant lives beside the check because it must obey the same scanning rule:
# this guard reads each file only as far as its first `#[cfg(test)]`, and skips
# a file that is wholly a test module.

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

# Every venue this tree knows how to name. A venue added to `adapters/` and not
# here would be unguarded, so the list is checked against the directory below.
VENUES=(hyperliquid rh-chain rh_chain rh-crypto rh_crypto)

if [[ "$VERB" == "plant" ]]; then
    f="$CRATE/sink.rs"
    printf 'pub fn planted() -> bool { matches!("hyperliquid", "hyperliquid") }\n' \
        | cat - "$f" > "$f.planted" && mv "$f.planted" "$f"
    exit 0
fi

# Any `#[cfg(...)]` whose predicate mentions `test`, not the literal
# `#[cfg(test)]`. A test module gated on a feature as well —
# `#[cfg(all(test, feature = "hyperliquid"))]` — is still a test module, and a
# guard keying on the exact string silently began scanning test code the first
# time somebody wrote a legitimate one. That happened.
non_test_lines() {
    awk '/^[[:space:]]*#\[cfg\(.*test.*\)\]/{exit} {print FILENAME ":" FNR ": " $0}' "$1"
}

pattern=$(IFS='|'; echo "${VENUES[*]}")
failures=()
while IFS= read -r file; do
    case "$file" in
        */adapters/*) continue ;;   # the permitted tree
        */tests.rs)   continue ;;   # wholly a test module
    esac
    hits=$(non_test_lines "$file" | grep -Ei "\"($pattern)\"" || true)
    [[ -n "$hits" ]] && failures+=("$hits")
done < <(find "$CRATE" -name '*.rs')

# An adapter present and unlisted would be unguarded. Catch that too.
for dir in "$CRATE"/adapters/*/; do
    [[ -d "$dir" ]] || continue
    name="$(basename "$dir")"
    listed=false
    for v in "${VENUES[@]}"; do
        [[ "${v//-/_}" == "${name//-/_}" ]] && listed=true
    done
    $listed || failures+=("adapters/$name exists and this guard does not know its name — it would be unguarded")
done

if (( ${#failures[@]} > 0 )); then
    echo "venue boundary: a venue is named outside the adapters tree" >&2
    printf '%s\n' "${failures[@]}" >&2
    echo "  everything above the seam holds a dyn Adapter and cannot tell two venues apart" >&2
    exit 1
fi
