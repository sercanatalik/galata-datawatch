#!/usr/bin/env bash
#
# THE THIRD WALL. With `capture` off, galata-datawatch links no transport.
#
# Everything that reads the tape links this crate. Before the `capture` feature
# existed a tape reader compiled 541 crates to do it — a runtime, a websocket
# stack, an HTTP client and a TLS provider, to read parquet. With the feature
# off it compiles 279.
#
# This is the same wall the workspace already has twice:
#   galata-wire   may not link a columnar format
#   galata-broker may not link a store
#   and here      a reader may not link the transport
#
# A feature nobody verifies is a feature that quietly stops gating anything the
# first time a module forgets its `cfg`, so this asks cargo rather than reading
# the manifest.
#
# Usage: check-no-transport.sh [check|plant] [root]

set -euo pipefail

VERB=check
if [[ $# -gt 0 ]]; then
    case "$1" in check|plant) VERB="$1"; shift ;; esac
fi
ROOT="${1:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
PLANT="$ROOT/crates/galata-datawatch/src/calendar.rs"

if [[ "$VERB" == plant ]]; then
    # A module on the pure side reaching for the runtime. Inserted before any
    # `#[cfg(test)]` so it is part of the shipped build — a violation in a test
    # would not change what a consumer links.
    python3 - "$PLANT" <<'PLANTPY'
import sys, pathlib
path = pathlib.Path(sys.argv[1])
text = path.read_text()
violation = "\n// planted by check-no-transport.sh\npub fn _planted() -> Option<tokio::task::Id> { None }\n"
marker = "#[cfg(test)]"
at = text.index(marker) if marker in text else len(text)
path.write_text(text[:at] + violation + text[at:])
PLANTPY
    echo "planted in $PLANT" >&2
    exit 0
fi

cd "$ROOT"

# Does it still build without the transport? A `cfg` somebody forgot shows up
# here first.
if ! cargo build -p galata-datawatch --no-default-features --quiet 2>/tmp/check-no-transport.err; then
    echo "check-no-transport: galata-datawatch does not build with --no-default-features:" >&2
    head -20 /tmp/check-no-transport.err >&2
    exit 1
fi

# And does the tree stay clean? Asked of cargo, not of the manifest.
FOUND=$(cargo tree -p galata-datawatch --no-default-features --prefix none 2>/dev/null \
    | awk '{print $1}' | sort -u \
    | grep -xE 'tokio|tokio-tungstenite|tokio-util|reqwest|rustls|hyper|hyper-util|tungstenite' || true)

if [[ -n "$FOUND" ]]; then
    echo "check-no-transport: with capture off, these link anyway:" >&2
    echo "$FOUND" | sed 's/^/  /' >&2
    echo "  A reader of the tape must not compile a websocket stack to read parquet." >&2
    exit 1
fi
