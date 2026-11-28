#!/usr/bin/env bash
#
# Ask every deployed binary whether it accepts a configuration document,
# BEFORE the document is written where they read it.
#
#   vet-config.sh [--bin-dir DIR] <candidate.toml>
#
# The document is parsed strictly (`deny_unknown_fields`) by capture, the
# ledger and the lane's tools, so a key a newer build added is refused by
# every older one. On 2026-09-26, writing `ledger.tape` after rebuilding only
# the ledger would have stopped the hourly lane at once and capture at its
# next restart; it was caught by reading the
# source (check-a-config-before-writing-it).
#
# Each binary answers `--check-config <path>` with its own rules: `ok:` or
# `refused: <reason>`. Anything else, such as a usage error from a build that
# predates the flag, is "cannot tell", never taken as acceptance.
#
# Exit: 0 when every binary accepts, 1 when any refuses or cannot tell,
# 2 on bad arguments.

set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN="$ROOT/target/release"
if [[ "${1:-}" == --bin-dir ]]; then BIN="$(cd "$2" && pwd)"; shift 2; fi
candidate="${1:-}"
[[ -f "$candidate" ]] || { echo "usage: vet-config.sh [--bin-dir DIR] <candidate.toml>" >&2; exit 2; }
candidate="$(cd "$(dirname "$candidate")" && pwd)/$(basename "$candidate")"

TOOLS=(galata-datawatch galata-ledger galata-compact galata-tape-rebuild galata-retain galata-watch galata-derive)
failed=0

verdict() {  # verdict <name> <answer>
    local name="$1" answer="$2"
    case "$answer" in
        ok:*) printf '%-22s ok\n' "$name" ;;
        refused:*) printf '%-22s REFUSED %s\n' "$name" "$(printf '%s' "${answer#refused: }" | sed "s|$candidate|${candidate##*/}|g")"; failed=1 ;;
        *) printf '%-22s cannot tell: this build predates --check-config; rebuild it\n' "$name"; failed=1 ;;
    esac
}

for tool in "${TOOLS[@]}"; do
    program="$BIN/$tool"
    if [[ ! -x "$program" ]]; then
        printf '%-22s not built\n' "$tool"
        continue
    fi
    # stdin closed and a scratch working directory: the answer is all that is
    # wanted, and an old build refuses the flag before any work (measured).
    answer="$(cd "$(mktemp -d)" && "$program" --check-config "$candidate" 2>/dev/null </dev/null | head -1)"
    verdict "$tool" "$answer"
done

exit "$failed"
