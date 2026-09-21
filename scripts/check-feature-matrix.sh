#!/usr/bin/env bash
#
# **Every feature combination builds, not just all-on and all-off.**
#
# `check-workspace-deps.sh` holds the dependency walls, and `check-all.sh`
# builds `--all-features` and `--no-default-features`. Nothing checked the
# combinations in between, and when the venues were made into real features
# THREE of them did not compile:
#
#   capture (no venue)                 a match over an empty AdapterConfig
#   hyperliquid (no capture)           reached the capture-gated client module
#   capture + rh-chain (no hyperliquid) a match over an empty History
#
# None of those was caused by making `rh-chain` a feature; making it one simply
# produced combinations nobody had built. A venue whose feature does not build
# on its own is a venue the README's `cargo add --features <venue>` line cannot
# actually deliver.
#
# Only the library is built, and only `cargo check`: this runs on every
# `check-all` and the point is the type check, not a linked binary.
#
# Usage: check-feature-matrix.sh [check|plant] [root]

set -euo pipefail

VERB=check
if [[ $# -gt 0 ]]; then
    case "$1" in check|plant) VERB="$1"; shift ;; esac
fi
ROOT="${1:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
PLANT="$ROOT/crates/galata-datawatch/src/adapters/mod.rs"

if [[ "$VERB" == plant ]]; then
    # Remove a cfg, so a venue-less build stops compiling. Before the tests,
    # like every other plant here.
    python3 - "$PLANT" <<'PLANTPY'
import sys, pathlib
path = pathlib.Path(sys.argv[1])
text = path.read_text()
marker = '#[cfg(feature = "rh-chain")]\n        rh_chain::VENUE,'
assert marker in text, "the plant's target moved — the PLANT is wrong, not the guard"
path.write_text(text.replace(marker, "rh_chain::VENUE,", 1))
PLANTPY
    echo "planted in $PLANT" >&2
    exit 0
fi

# The combinations worth holding: each venue alone, each venue with the
# runtime, the runtime with no venue, and nothing at all.
COMBINATIONS=(
    ""
    "capture"
    "hyperliquid"
    "rh-chain"
    "rh-crypto"
    "capture,hyperliquid"
    "capture,rh-chain"
    "capture,rh-crypto"
    "hyperliquid,rh-chain,rh-crypto"
)

failed=0
for features in "${COMBINATIONS[@]}"; do
    args=(check --quiet -p galata-datawatch --lib --no-default-features)
    [[ -n "$features" ]] && args+=(--features "$features")
    if ! output=$(cd "$ROOT" && cargo "${args[@]}" 2>&1); then
        label="${features:-<none>}"
        echo "check-feature-matrix: galata-datawatch does not build with features: $label" >&2
        echo "$output" | grep -E "^error" | head -3 | sed 's/^/  /' >&2
        failed=1
    fi
done

exit "$failed"
