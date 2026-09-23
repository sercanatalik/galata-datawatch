#!/usr/bin/env bash
#
# THE CHANGELOG'S NEWEST RELEASE IS THE VERSION THAT WILL PUBLISH.
#
# Four crates go to crates.io together from one `[workspace.package] version`
# (RELEASING.md). A changelog whose newest entry names a different number is
# one a reader cannot use to tell what they are getting — and the moment it
# happens is a version bump that forgot an entry, which is exactly when
# somebody is about to publish.
#
# **`[Unreleased]` is skipped by design.** It is where the next change goes,
# and an unreleased section is by definition not the released version.
#
# WHAT THIS CANNOT CHECK: whether the entry is TRUE. Nothing mechanical can
# read prose and say it describes the code. A guard that implied otherwise
# would be worse than none, so this holds the one thing that is falsifiable:
# the number.
#
# Usage: check-changelog.sh [check|plant] [root]

set -euo pipefail

VERB=check
if [[ $# -gt 0 ]]; then
    case "$1" in check|plant) VERB="$1"; shift ;; esac
fi
ROOT="${1:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
DOC="$ROOT/CHANGELOG.md"

if [[ ! -f "$DOC" ]]; then
    echo "$(basename "$0"): $ROOT has no CHANGELOG.md — four crates publish together and nothing says what is in them" >&2
    exit 2
fi

if [[ "$VERB" == plant ]]; then
    # **A version bump that forgot the changelog**, expressed from the other
    # side: the changelog naming a version the workspace does not. An append
    # could not produce it — the fault is a disagreement, not an addition.
    python3 - "$DOC" <<'PLANTPY'
import sys, pathlib, re
path = pathlib.Path(sys.argv[1])
text = path.read_text()
m = re.search(r"^## \[(\d+)\.(\d+)\.(\d+)\]", text, re.M)
assert m, "no released heading to move — the PLANT is wrong, not the guard"
bumped = f"## [{m.group(1)}.{int(m.group(2)) + 1}.0]"
path.write_text(text[:m.start()] + bumped + text[m.end():])
PLANTPY
    echo "planted in $DOC" >&2
    exit 0
fi

python3 - "$ROOT" "$DOC" <<'PY'
import sys, pathlib, re, tomllib

root, doc = pathlib.Path(sys.argv[1]), pathlib.Path(sys.argv[2])

ws = tomllib.loads((root / "Cargo.toml").read_text())
version = ws.get("workspace", {}).get("package", {}).get("version")
if not version:
    sys.exit("check-changelog: [workspace.package] has no version — refusing to compare against nothing")

# The first `## [x.y.z]`, which is the newest release; `## [Unreleased]` does
# not match the pattern and so is skipped without being special-cased.
found = re.search(r"^## \[(\d+\.\d+\.\d+)\]", doc.read_text(), re.M)
if not found:
    sys.exit("check-changelog: CHANGELOG.md has no released version heading")

newest = found.group(1)
if newest != version:
    sys.exit(f"check-changelog: CHANGELOG.md's newest release is {newest} and "
             f"[workspace.package] is {version}. The four crates publish from that "
             f"version, so whichever is wrong, a reader is told the wrong thing")

print(f"changelog: ok. its newest release is {newest}, which is the version the four crates publish from "
      f"(the prose is not checked and cannot be)")
PY
