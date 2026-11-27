#!/usr/bin/env bash
#
# NO PUBLISHED CRATE LINKS galata-margin (add-cross-venue-margin-optimiser, D10).
#
# galata-margin holds the request that asks Derive to simulate the owner's
# subaccount, and signs it with the session key. It is `publish = false` so the
# four published crates link none of it and a tape reader inherits no margin
# code. One line in a manifest is all that stands between the two, so this asks
# cargo, as check-vault-reach.sh does for the vault: a manifest is what somebody
# edits, and a resolved tree is what a consumer gets.
#
# **A cargo failure is a failure.** galata-margin depends on galata-datawatch,
# so a published crate in its own tree that reached back for it would make a
# cycle, and `cargo tree` would answer nothing. Read as "no galata-margin
# found", that would pass. Here it refuses instead.
#
# Usage: check-margin-reach.sh [check|plant] [root]

set -euo pipefail

VERB=check
if [[ $# -gt 0 ]]; then
    case "$1" in check|plant) VERB="$1"; shift ;; esac
fi
ROOT="${1:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
# Today every published crate is in galata-margin's tree, galata-broker through
# one of galata-datawatch's optional dependencies, and cargo counts optional
# edges when it looks for a cycle. So the planted edge is refused by cargo for
# the whole workspace (measured 2026-11-27), and the guard must turn that into
# a named failure, not an empty tree. Its "links galata-margin" branch covers
# the day galata-margin no longer depends on galata-datawatch.
PLANT="$ROOT/crates/galata-broker/Cargo.toml"

if [[ "$VERB" == plant ]]; then
    python3 - "$PLANT" <<'PLANTPY'
import sys, pathlib
path = pathlib.Path(sys.argv[1])
text = path.read_text()
marker = "[dependencies]\n"
assert marker in text, "the plant's target moved — the PLANT is wrong, not the guard"
at = text.index(marker) + len(marker)
path.write_text(text[:at] + 'galata-margin = { path = "../galata-margin" }  # planted by check-margin-reach.sh\n' + text[at:])
PLANTPY
    echo "planted in $PLANT" >&2
    exit 0
fi

cd "$ROOT"

# The four that publish, named explicitly and checked against the manifests, as
# check-vault-reach.sh does: a fifth publishing unobserved would leave this wall
# passing while no longer covering the tree.
PUBLISHED=(galata-wire galata-broker galata-segments galata-datawatch)
ACTUAL=$(python3 scripts/lib/publishable.py)
EXPECTED=$(printf '%s\n' "${PUBLISHED[@]}" | sort | tr '\n' ' ' | sed 's/ $//')
if [[ "$ACTUAL" != "$EXPECTED" ]]; then
    echo "check-margin-reach: the publishable set is not the set this guard covers." >&2
    echo "  expected: $EXPECTED" >&2
    echo "  actual:   $ACTUAL" >&2
    exit 1
fi

failed=0
for crate in "${PUBLISHED[@]}"; do
    # --all-features: a crate publishes every feature it declares.
    if ! tree=$(cargo tree -p "$crate" --all-features --prefix none --offline 2>&1); then
        echo "check-margin-reach: cargo could not resolve $crate's tree, so whether it links galata-margin is unknown:" >&2
        echo "$tree" | tail -3 | sed 's/^/    /' >&2
        failed=1
        continue
    fi
    if awk '{print $1}' <<< "$tree" | grep -qx 'galata-margin'; then
        echo "check-margin-reach: $crate links galata-margin." >&2
        failed=1
    fi
done

if [[ $failed -ne 0 ]]; then
    echo "  A published crate must reach none of galata-margin: it holds the request" >&2
    echo "  that simulates the owner's Derive subaccount, and does not publish." >&2
    exit 1
fi

echo "margin reach: ok. the four published crates link no galata-margin, at any depth"
