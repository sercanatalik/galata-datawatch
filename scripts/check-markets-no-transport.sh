#!/usr/bin/env bash
#
# galata-markets LINKS NO VENUE TRANSPORT (`add-galata-markets`).
#
# It serves clients market data from the tape, the archive and the bus, and
# fills a hole from the record with the venues' own normalisers. It never
# connects to a venue, so it must not be able to: a capture loop it cannot
# link is a capture loop nobody can start inside it. It takes galata-datawatch
# without `capture`, and this asks cargo whether that still holds rather than
# reading the manifest.
#
# **Of the package built alone**, as its image builds it
# (`cargo build -p galata-markets`). A whole-workspace build unifies features
# across members, hands galata-datawatch `capture` for the capture binary, and
# would link it here too — that build is not the one deployed, and `cargo tree
# -p` resolves this package as its image does.
#
# The tokio runtime, rustls and hyper are not venue transport here: they are
# the bus client and the HTTP server, which this service is.
#
# Usage: check-markets-no-transport.sh [root]

set -euo pipefail

ROOT="${1:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
cd "$ROOT"

TREE=$(cargo tree -p galata-markets -e normal --prefix none 2>/dev/null) || {
    echo "check-markets-no-transport: cargo tree -p galata-markets failed" >&2
    exit 1
}

FOUND=$(echo "$TREE" | awk '{print $1}' | sort -u \
    | grep -xE 'tokio-tungstenite|tungstenite|reqwest' || true)
if [[ -n "$FOUND" ]]; then
    echo "check-markets-no-transport: galata-markets links a venue transport:" >&2
    echo "$FOUND" | sed 's/^/  /' >&2
    echo "  It serves what was captured; it connects to no venue. Take galata-datawatch" >&2
    echo "  with default-features = false and no \`capture\`." >&2
    exit 1
fi

if cargo tree -p galata-markets -e features --prefix none 2>/dev/null \
    | grep -q 'galata-datawatch feature "capture"'; then
    echo "check-markets-no-transport: galata-markets takes galata-datawatch with \`capture\`" >&2
    exit 1
fi
