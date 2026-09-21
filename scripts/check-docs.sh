#!/usr/bin/env bash
#
# **The documentation builds clean**, because docs.rs renders it to everyone.
#
# A broken intra-doc link is invisible locally and permanent once published:
# the page shows `[`Thing`]` as literal text, and nobody who sees it can do
# anything about it. `cargo doc` already reports them; nothing was failing on
# them.
#
# `-D warnings`, the same bargain the rest of the workspace makes. That also
# catches a redundant explicit link target — a `[`X`](path::to::X)` whose label
# resolves on its own — which is how the one this guard was written for was
# found, after it had been left standing for several changes because rustdoc
# reports it without a file or a line.
#
# Stable, not nightly: `--cfg docsrs` turns on `doc_cfg` for the feature
# badges, and docs.rs supplies its own nightly for that. Link resolution is the
# same either way, and a guard that needed a nightly toolchain would be a guard
# most machines skip.
#
# Usage: check-docs.sh [check|plant] [root]

set -euo pipefail

VERB=check
if [[ $# -gt 0 ]]; then
    case "$1" in check|plant) VERB="$1"; shift ;; esac
fi
ROOT="${1:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
PLANT="$ROOT/crates/galata-wire/src/lib.rs"

if [[ ! -f "$ROOT/Cargo.toml" ]]; then
    echo "$(basename "$0"): $ROOT is not a workspace — refusing to scan nothing and call it ok" >&2
    exit 2
fi

if [[ "$VERB" == plant ]]; then
    python3 - "$PLANT" <<'PLANTPY'
import re, sys, pathlib
path = pathlib.Path(sys.argv[1])
text = path.read_text()
violation = (
    "\n/// Planted by check-docs.sh: a link to something that does not exist,\n"
    "/// which renders as literal text on the published page.\n"
    "///\n"
    "/// See [`NoSuchThingAtAll`].\n"
    "pub struct Planted;\n"
)
found = re.search(r"#\[cfg\((?:test\)|all\(\s*test\b)", text)
at = found.start() if found else len(text)
path.write_text(text[:at] + violation + text[at:])
PLANTPY
    echo "planted in $PLANT" >&2
    exit 0
fi

cd "$ROOT"
if ! output=$(RUSTDOCFLAGS="-D warnings" cargo doc --workspace --all-features --no-deps 2>&1); then
    echo "check-docs: the documentation does not build clean" >&2
    echo "$output" | grep -E "^(error|warning)" -A 3 | head -20 | sed 's/^/  /' >&2
    exit 1
fi
