#!/usr/bin/env bash
#
# RELEASING.md NAMES EXACTLY THE CRATES THAT PUBLISH.
#
# A release document is followed, so one naming the wrong set is worse than no
# document at all. Four crates here publish and one does not, and the one that
# does not is load-bearing: `galata-datawatch-vault` exists so that the other
# four take no vault dependency.
#
# **Both directions, for the reason `check-documented-routes.sh` gives one
# repository over.** A crate that publishes and is not named fails, and a name
# that does not publish fails. One direction would rot in the direction nobody
# notices: a crate added and never mentioned, which is the case the document is
# for.
#
# What it cannot check: whether the procedure is RIGHT. That came from running
# `cargo publish --workspace --dry-run` and reading the order it chose, and it
# is recorded in the document with the output rather than asserted here.
#
# Usage: check-release-doc.sh [check|plant] [root]

set -euo pipefail

VERB=check
if [[ $# -gt 0 ]]; then
    case "$1" in check|plant) VERB="$1"; shift ;; esac
fi
ROOT="${1:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
DOC="$ROOT/RELEASING.md"

if [[ ! -f "$DOC" ]]; then
    echo "$(basename "$0"): $ROOT has no RELEASING.md — four crates publish together and nothing says how" >&2
    exit 2
fi

if [[ "$VERB" == plant ]]; then
    # **Remove a crate from the table**, which is the violation that matters:
    # a publishable crate the document does not mention. Appending could not
    # produce it, because the fault is an absence.
    python3 - "$DOC" <<'PLANTPY'
import sys, pathlib
path = pathlib.Path(sys.argv[1])
text = path.read_text()
marker = "| `galata-broker` | yes | the bus |\n"
assert marker in text, "the plant's target moved — the PLANT is wrong, not the guard"
path.write_text(text.replace(marker, "", 1))
PLANTPY
    echo "planted in $DOC" >&2
    exit 0
fi

python3 - "$ROOT" "$DOC" <<'PY'
import sys, pathlib, re, subprocess, json

root, doc = pathlib.Path(sys.argv[1]), pathlib.Path(sys.argv[2])

meta = json.loads(subprocess.run(
    ["cargo", "metadata", "--no-deps", "--format-version", "1"],
    cwd=root, capture_output=True, text=True, check=True).stdout)

# `publish` is null when a crate publishes anywhere, and [] when it does not.
publishes = {p["name"] for p in meta["packages"] if p.get("publish") is None}
private = {p["name"] for p in meta["packages"] if p.get("publish") == []}

if not publishes:
    sys.exit("check-release-doc: cargo names no publishable crate — refusing to compare against nothing")

text = doc.read_text()
# Only the crates the table marks as publishing, so naming the private one in
# its own row (which the document does, deliberately) is not a claim.
named = {m.group(1) for m in re.finditer(r"^\|\s*`([a-z0-9-]+)`\s*\|\s*yes\s*\|", text, re.M)}

missing = sorted(publishes - named)
extra = sorted(named - publishes)

for name in missing:
    print(f"check-release-doc: {name} publishes and RELEASING.md does not say so", file=sys.stderr)
for name in extra:
    why = "does not publish" if name in private else "is not a workspace member"
    print(f"check-release-doc: RELEASING.md says {name} publishes, and it {why}", file=sys.stderr)

if missing or extra:
    sys.exit(1)

print(f"release doc: ok. RELEASING.md names the {len(publishes)} crate(s) that publish, and no others "
      f"({len(private)} held back by publish = false)")
PY
