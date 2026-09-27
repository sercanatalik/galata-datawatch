#!/usr/bin/env bash
#
# Every place that deletes a file is listed here, with why it is safe.
#
# **Found the hard way, 2026-09-27.** Compaction removed every segment another
# contained by range, on a premise written when only a live writer and a
# compaction wrote a partition: that containment could only be an interrupted
# compaction. The settle then began writing fetched pages into live partitions,
# inside the live segments' ranges, and each nightly compaction deleted them
# unmerged: 200 of 402 settled pages in one day's candles
# (a-contained-page-is-not-a-duplicate). The deletion code never changed; the
# writers around it did, and nothing made anyone look at it again.
#
# So a deletion path cannot appear, or move, without being read: every
# `remove_file`, `remove_dir` and `remove_dir_all` outside tests must match an
# entry below, file and line text, each with the reason it is safe. A new one
# fails this guard until someone writes down why it cannot delete the record.
#
# Each file is read only as far as its first `#[cfg(test)]` or
# `#[cfg(all(test, ...))]`: tests delete their own fixtures.
#
# Usage: check-deletion-paths.sh [check|plant] [root]

set -euo pipefail

VERB=check
if [[ $# -gt 0 ]]; then
    case "$1" in check|plant) VERB="$1"; shift ;; esac
fi
ROOT="${1:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"

if [[ "$VERB" == plant ]]; then
    f="$ROOT/crates/galata-datawatch/src/calendar.rs"
    python3 - "$f" <<'PLANTPY'
import sys, pathlib
path = pathlib.Path(sys.argv[1])
text = path.read_text()
violation = "\n// planted by check-deletion-paths.sh\npub fn _planted(p: &std::path::Path) { let _ = std::fs::remove_file(p); }\n"
marker = "#[cfg(test)]"
at = text.index(marker) if marker in text else len(text)
path.write_text(text[:at] + violation + text[at:])
PLANTPY
    exit 0
fi

# file <TAB> the line, trimmed <TAB> why it cannot delete the record
IFS= read -r -d '' ALLOWED <<'LIST' || true
crates/galata-segments/src/compact.rs	std::fs::remove_file(path).map_err(|source| SegmentError::Write {	compaction: an original after the merged replacement holding it is durable, or a contained segment whose every row is proven in its container (proven_duplicates)
crates/galata-segments/src/writer.rs	let _ = std::fs::remove_file(&self.temp_path);	a segment writer's own temporary file, dropped unfinished; never a committed name
crates/galata-segments/src/writer.rs	let _ = std::fs::remove_file(&temp);	write_file's own temporary, after a failed write; never the committed file
crates/galata-datawatch/src/record/mod.rs	let _ = std::fs::remove_file(self.scope_path().join(CLEAN_SHUTDOWN));	the clean-shutdown marker, cleared at boot; holds no rows
crates/galata-datawatch/src/tape/rebuild.rs	std::fs::remove_file(&segment).map_err(|source| RebuildError::Replace {	the tape, a rebuildable cache: this venue's segments of the receipt days this run rewrites, by label, only after the run's own segments are committed, and never a path it wrote (a-replacement-never-leaves-a-hole)
crates/galata-datawatch/src/bin/galata-retain.rs	match std::fs::remove_dir_all(&candidate.path) {	retention: only with --delete, only partitions past a horizon the operator declared, holding both stores
LIST

non_test() {
    # `test` alone or first in an `all(...)`: code only a test build
    # compiles. Never `any(test, ...)` or `not(test)`, which ship.
    awk '/#\[cfg\((all\()?test[,)]/{exit} {print}' "$1"
}

failures=()
found=0
while IFS= read -r file; do
    rel="${file#$ROOT/}"
    while IFS= read -r line; do
        [[ -z "$line" ]] && continue
        trimmed="$(printf '%s' "$line" | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//')"
        # A comment naming one is not one.
        [[ "$trimmed" == //* ]] && continue
        found=$((found + 1))
        if ! printf '%s\n' "$ALLOWED" | awk -F'\t' -v f="$rel" -v l="$trimmed" '$1 == f && $2 == l {ok=1} END {exit !ok}'; then
            failures+=("$rel: $trimmed")
        fi
    done < <(non_test "$file" | grep -E 'remove_(file|dir|dir_all)\(' || true)
done < <(find "$ROOT/crates" -path '*/src/*' -name '*.rs' | sort)

# A guard that found nothing scanned nothing.
if (( found == 0 )); then
    echo "check-deletion-paths: no deletion found under $ROOT/crates; refusing to call an unscanned tree clean" >&2
    exit 2
fi

if (( ${#failures[@]} > 0 )); then
    echo "check-deletion-paths: a deletion that is not on the list:" >&2
    printf '  %s\n' "${failures[@]}" >&2
    echo "  add it to ALLOWED in scripts/check-deletion-paths.sh with why it cannot delete the record" >&2
    exit 1
fi
