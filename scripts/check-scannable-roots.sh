#!/usr/bin/env bash
#
# **A binary that scans a declared store checks it can.**
#
# Every listing in `galata-segments` answers an unreadable directory with an
# empty result. That is right for a SUBTREE — a partition that vanished
# mid-walk is not a reason to abandon the others — and wrong for a DECLARED
# ROOT: no partitions means no candidates, which these binaries report as
# "nothing to do" and exit 3.
#
# A mistyped path, an unmounted volume or a permissions change then looks
# exactly like a tidy store, for as long as nobody checks. `galata-compact`
# was worse still: `hold` creates the directory it locks, so the typo was
# CREATED, after which it really is an empty, perfectly scannable store.
#
# The guards in this directory have said it about themselves since Tier 0 —
# *a guard handed a root it cannot scan reports success forever* — and the
# binaries that act on the record did not.
#
# So: any binary naming `paths.archive` or `paths.tape` must also name
# `scannable`.
#
# Usage: check-scannable-roots.sh [check|plant] [root]

set -euo pipefail

VERB=check
if [[ $# -gt 0 ]]; then
    case "$1" in check|plant) VERB="$1"; shift ;; esac
fi
ROOT="${1:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
BINS="$ROOT/crates/galata-datawatch/src/bin"
PLANT="$BINS/galata-watch.rs"

# A guard handed a root it cannot scan reports success forever.
if [[ ! -d "$BINS" ]]; then
    echo "$(basename "$0"): $ROOT has no binaries to check — refusing to scan nothing and call it ok" >&2
    exit 2
fi

if [[ "$VERB" == plant ]]; then
    python3 - "$PLANT" <<'PLANTPY'
import sys, pathlib
path = pathlib.Path(sys.argv[1])
text = path.read_text()
assert "scannable" in text, "the plant's target moved — the PLANT is wrong, not the guard"
path.write_text(text.replace("galata_segments::scannable", "let _unchecked = |_: &std::path::Path| Ok::<(), std::io::Error>(()); _unchecked"))
PLANTPY
    echo "planted in $PLANT" >&2
    exit 0
fi

problems=()
for file in "$BINS"/*.rs; do
    name="$(basename "$file")"
    # Capture WRITES its store and creates it; it does not sweep one.
    [[ "$name" == "galata-datawatch.rs" ]] && continue
    grep -qE 'paths\.(archive|tape)' "$file" || continue
    if ! grep -q 'scannable' "$file"; then
        problems+=("$name: reads a declared store and never calls scannable — an unreadable root would be reported as nothing to do")
    fi
done

if (( ${#problems[@]} > 0 )); then
    for problem in "${problems[@]}"; do
        echo "check-scannable-roots: $problem" >&2
    done
    exit 1
fi
