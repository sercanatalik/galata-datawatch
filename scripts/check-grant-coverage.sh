#!/usr/bin/env bash
#
# A SUBJECT ROOT GRANTED TO NOBODY FAILS THE BUILD, AND IS NAMED.
#
# Every root the code can build a `Subject` for must appear in the generated
# authorization table. That makes adding a third root FORCE a decision about
# who may read it — at the moment the root is added, rather than whenever
# somebody notices a dashboard is empty.
#
# The predecessor records this guard firing as a genuine handover: a root
# arrived, the guard named it, and the person who added it decided. That only
# works if the guard exists before the root does.
#
# It also checks the two properties the table must never lose, because both
# were shipped wrong once:
#
#   1. `allow: []` MEANS ALLOW EVERYTHING in NATS, not "allow nothing". Review,
#      unit tests and `nats-server -t` all call the inverted table valid --
#      verified again on 2026-09-21, where a component granted `allow: []`
#      received a subject it was denied. "Nothing" is `deny: [">"]`.
#
#   2. No `no_auth_user` and no anonymous default. A table granting three
#      identities changes nothing if a fourth, nameless client keeps unlimited
#      authority.
#
# Usage: check-grant-coverage.sh [check|plant] [root]

set -euo pipefail

VERB=check
if [[ $# -gt 0 ]]; then
    case "$1" in check|plant) VERB="$1"; shift ;; esac
fi
ROOT="${1:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
TABLE="$ROOT/config/nats-authorization.conf"
DECLARED="$ROOT/crates/galata-broker/src/grants.rs"

if [[ "$VERB" == plant ]]; then
    # A root the code declares and the table does not carry.
    python3 - "$DECLARED" <<'PLANTPY'
import sys, pathlib
path = pathlib.Path(sys.argv[1])
text = path.read_text()
path.write_text(text.replace(
    'pub const ROOTS: [&str; 2] = ["markets.", "status."];',
    'pub const ROOTS: [&str; 3] = ["markets.", "status.", "views."];',
    1,
))
PLANTPY
    echo "planted in $DECLARED" >&2
    exit 0
fi

[[ -f "$TABLE" ]] || { echo "check-grant-coverage: $TABLE is missing — generate it" >&2; exit 1; }

python3 - "$DECLARED" "$TABLE" <<'PY'
import pathlib, re, sys

declared_path, table_path = pathlib.Path(sys.argv[1]), pathlib.Path(sys.argv[2])
declared_src = declared_path.read_text()
table = table_path.read_text()

# The roots the code declares, read from the constant rather than from a second
# list here -- a second list does not fail when it drifts, it disagrees.
match = re.search(r"pub const ROOTS: \[&str; \d+\] = \[(.*?)\];", declared_src, re.S)
if not match:
    print("check-grant-coverage: cannot find ROOTS in the broker", file=sys.stderr)
    sys.exit(1)
roots = re.findall(r'"([^"]+)"', match.group(1))

problems = []
for root in roots:
    if root not in table:
        problems.append(
            f"a subject root the code declares is granted to nobody\n"
            f"    {root} — declared in {declared_path.name}, absent from "
            f"{table_path.name}"
        )

# The two inversions, each shipped wrong once.
directives = [l for l in table.splitlines() if not l.strip().startswith("#")]
if any("allow: []" in line for line in directives):
    problems.append(
        "`allow: []` is in the table, and in NATS it means ALLOW EVERYTHING. "
        "Nothing is spelled `deny: [\">\"]`"
    )
for forbidden in ("no_auth_user", "allow_all"):
    if any(forbidden in line for line in directives):
        problems.append(
            f"{forbidden} is in the table: a nameless client would keep the "
            f"unlimited authority the table exists to remove"
        )

if problems:
    for p in problems:
        print(f"check-grant-coverage: {p}", file=sys.stderr)
    sys.exit(1)
PY
