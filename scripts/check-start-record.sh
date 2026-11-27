#!/usr/bin/env bash
#
# EVERY START OF A SERVICE IS RECORDED WHERE THE DEPLOYMENT'S HEALTH READS IT.
#
# launchd retries a refused or exited service every ten seconds, and says so
# only in the service's log. The launcher records each start in the status
# folder (service-<service>.json) so that a refusal, and a restart loop, can be
# judged and alerted (add-service-health-monitoring, design D5). Each of these
# fails silently if it fails at all:
#
#   1. **A refusal is recorded with its reason.** The launcher is run into a
#      refusal here, a container tool that is not there, against a scratch
#      status folder: nothing real is started or asked.
#   2. **A check records nothing.** `--check` starts nothing, so a record of
#      it would read as a start.
#   3. **Ten are kept.** The eleventh start drops the oldest; the file does not
#      grow for ever under a loop.
#   4. **A start is recorded just before each hand-over**, both of them: the
#      plain `exec` and the one through `gv run` for a service with secrets.
#
# Usage: check-start-record.sh [root]

set -euo pipefail
ROOT="${1:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
LAUNCHER="$ROOT/scripts/run-service.sh"
RECORD="$ROOT/scripts/lib/start_record.py"
me="$(basename "$0")"
fails=0
say() { echo "$me: $1" >&2; fails=$((fails + 1)); }

[[ -f "$LAUNCHER" && -f "$RECORD" ]] || { echo "$me: no $LAUNCHER or $RECORD" >&2; exit 2; }

scratch="$(mktemp -d)"
trap 'rm -rf "$scratch"' EXIT

# outcomes <file>: each start's outcome and reason, one per line.
outcomes() {
    python3 - "$1" <<'PY'
import json, sys
for start in json.load(open(sys.argv[1]))["starts"]:
    print(start["outcome"], start.get("reason", ""), sep="\t")
PY
}

# --- 1. a refusal is recorded with its reason ------------------------------
status="$scratch/status"
GALATA_STATUS_DIR="$status" GALATA_CONTAINER_BIN="$scratch/absent/container" \
    bash "$LAUNCHER" flows >/dev/null 2>&1 && say "the launcher started flows with no container tool"
if [[ -f "$status/service-flows.json" ]]; then
    said="$(outcomes "$status/service-flows.json")"
    [[ "$said" == refused$'\t'*"no \`container\`"* ]] \
        || say "a refused start is recorded without its reason: $said"
else
    say "a refused start left no record in the status folder"
fi

# A venue's service is recorded under the name var/containers.conf gives it.
GALATA_STATUS_DIR="$status" GALATA_CONTAINER_BIN="$scratch/absent/container" \
    bash "$LAUNCHER" ledger derive-xyz >/dev/null 2>&1 || true
[[ -f "$status/service-ledger-derive-xyz.json" ]] \
    || say "a ledger's refusal is not recorded as ledger:derive-xyz"

# --- 2. a check records nothing --------------------------------------------
checked="$scratch/checked"
GALATA_STATUS_DIR="$checked" GALATA_CONTAINER_BIN="$scratch/absent/container" \
    bash "$LAUNCHER" --check flows >/dev/null 2>&1 || true
[[ ! -e "$checked" ]] || say "--check recorded a start: $(ls "$checked")"

# --- 3. ten are kept --------------------------------------------------------
kept="$scratch/kept"
for n in $(seq 1 11); do python3 "$RECORD" "$kept" flows refused "start $n"; done
said="$(outcomes "$kept/service-flows.json")"
[[ "$(wc -l <<< "$said" | tr -d ' ')" == 10 ]] || say "eleven starts keep $(wc -l <<< "$said" | tr -d ' '), not ten"
[[ "$(head -1 <<< "$said")" == refused$'\t'"start 2" && "$(tail -1 <<< "$said")" == refused$'\t'"start 11" ]] \
    || say "the eleventh start did not drop the oldest: $(head -1 <<< "$said") … $(tail -1 <<< "$said")"

# --- 4. a start is recorded just before each hand-over ----------------------
python3 - "$LAUNCHER" <<'PY' || fails=$((fails + 1))
import re, sys
text = open(sys.argv[1]).read()
for handover in ('exec "$@"', 'exec "$gv" run'):
    at = text.find(handover)
    if at < 0:
        sys.exit(f"check-start-record.sh: the launcher has no `{handover}` to hand over with")
    before = [l.strip() for l in text[:at].splitlines() if l.strip()]
    if not before or before[-1] != "record_start started":
        sys.exit(f"check-start-record.sh: `{handover}` is not preceded by `record_start started`")
PY

if (( fails > 0 )); then exit 1; fi
echo "$me: ok — a refused start is recorded with its reason under the service's name, a check records nothing, ten starts are kept, and each hand-over records its start"
