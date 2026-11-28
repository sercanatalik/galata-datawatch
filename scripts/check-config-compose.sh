#!/usr/bin/env bash
#
# THE DEPLOYMENT'S DOCUMENT IS COMPOSED, NEVER KEPT BY HAND
# (compose-deployment-config).
#
# var/datawatch.local.toml was once a copy of config/datawatch.toml kept by
# hand, plus this machine's [broker] and [ledger]. On 2026-11-28 four
# instruments committed to the one did not reach capture until copied into the
# other. Now it is composed — the committed file, then var/datawatch.overlay.toml,
# appended — at every start, by scripts/lib/compose_config.py. Each refusal
# below is a way the old drift, or a new one, would come back silently:
#
#   1. Composed: the committed file's tables and the overlay's, under a header,
#      and a committed change is in the next composition.
#   2. No overlay and no local document: the committed file is read.
#   3. A table in both is refused by name, and the document is unchanged.
#   4. A hand edit to the composed document is refused, never overwritten.
#   5. A hand-kept document with no overlay is refused, naming the migration.
#   6. --check refuses as a start would and writes nothing of the deployment's;
#      given a file, it composes there, so a check reads what a start would.
#   7. The migration turns a matching hand-kept document into an overlay,
#      reading exactly as before, and keeps it beside.
#   8. The migration of a drifted document changes nothing and names the drift.
#   9. Starts composing at once each leave a whole document.
#  10. required-secrets.sh reads the composed document, overlay and all.
#
# Usage: check-config-compose.sh [root]

set -uo pipefail
ROOT="${1:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
COMPOSE="$ROOT/scripts/lib/compose_config.py"
me="$(basename "$0")"
fails=0
say() { echo "$me: $1" >&2; fails=$((fails + 1)); }

[[ -f "$COMPOSE" ]] || { echo "$me: no $COMPOSE" >&2; exit 2; }
scratch="$(cd "$(mktemp -d)" && pwd -P)"
trap 'rm -rf "$scratch"' EXIT

COMMITTED='# the committed document
[paths]
archive = "var/archive"

[venue.hyperliquid]
series = ["quotes"]
instruments = [
  { ticker = "BTC" },
  [1, 2],
]
'
OVERLAY='# This machine.
[broker]
url = "nats://example:4222"
password_var = "GALATA_BROKER_PASSWORD_X"

[ledger]
root = "var/ledger"

[ledger.account.main]
address_var = "ADDR_MAIN"
'
fresh() {  # fresh <name>: a scratch root with the committed document
    local root="$scratch/$1"
    mkdir -p "$root/config" "$root/var"
    printf '%s' "$COMMITTED" > "$root/config/datawatch.toml"
    printf '%s' "$root"
}
compose() { python3 "$COMPOSE" "$@" 2>&1; }
# Portable to CI's Linux as well as macOS: no in-place sed, BSD stat or
# shasum, each of which differs between the two.
edit() {  # edit <file> <find> <replace>: once, and the find must be there
    python3 -c 'import sys, pathlib; p = pathlib.Path(sys.argv[1]); t = p.read_text(); assert sys.argv[2] in t, sys.argv[2]; p.write_text(t.replace(sys.argv[2], sys.argv[3], 1))' "$@"
}
mode() { python3 -c 'import os, stat, sys; print(oct(stat.S_IMODE(os.stat(sys.argv[1]).st_mode))[2:])' "$1"; }
digest() { cksum < "$1"; }
reads() {  # reads <file> <python expr over d>: true when it holds
    python3 -c 'import sys, tomllib; d = tomllib.load(open(sys.argv[1], "rb")); sys.exit(0 if eval(sys.argv[2]) else 1)' "$1" "$2"
}

# --- 1. composed ------------------------------------------------------------
r="$(fresh composed)"
printf '%s' "$OVERLAY" > "$r/var/datawatch.overlay.toml"
out="$(compose "$r")"
[[ "$out" == "$r/var/datawatch.local.toml" ]] || say "with an overlay, the path to read is not the composed document: $out"
head -1 "$r/var/datawatch.local.toml" 2>/dev/null | grep -q '^# GENERATED' || say "the composed document carries no generated header"
reads "$r/var/datawatch.local.toml" 'd["broker"]["url"] == "nats://example:4222" and d["ledger"]["account"]["main"]["address_var"] == "ADDR_MAIN" and d["venue"]["hyperliquid"]["series"] == ["quotes"]' \
    || say "the composed document is not the committed tables and the overlay's"
grep -q '^# the committed document' "$r/var/datawatch.local.toml" || say "the committed file's comments did not survive"
[[ "$(mode "$r/var/datawatch.local.toml")" == 600 ]] || say "the composed document is not 0600"
# A committed instrument is in the next composition.
edit "$r/config/datawatch.toml" '{ ticker = "BTC" },' $'{ ticker = "BTC" },\n  { ticker = "ETH" },'
compose "$r" >/dev/null
reads "$r/var/datawatch.local.toml" '{"ticker": "ETH"} in d["venue"]["hyperliquid"]["instruments"]' \
    || say "a committed instrument is not in the next composition"

# --- 2. no overlay ----------------------------------------------------------
r="$(fresh none)"
out="$(compose "$r")"
[[ "$out" == "$r/config/datawatch.toml" ]] || say "with no overlay, the committed document is not read: $out"
[[ ! -e "$r/var/datawatch.local.toml" ]] || say "with no overlay, a local document was written"

# --- 3. a table in both -----------------------------------------------------
r="$(fresh both)"
printf '%s' "$OVERLAY" > "$r/var/datawatch.overlay.toml"
compose "$r" >/dev/null
before="$(digest "$r/var/datawatch.local.toml")"
printf '\n[venue.hyperliquid]\nseries = ["candles"]\n' >> "$r/var/datawatch.overlay.toml"
out="$(compose "$r")" && say "an overlay declaring a committed table was accepted"
[[ "$out" == *REFUSED*venue.hyperliquid* ]] || say "a table in both is not refused by its name: $out"
[[ "$(digest "$r/var/datawatch.local.toml")" == "$before" ]] || say "a refused composition changed the document"
r="$(fresh bare)"
printf 'stray = 1\n%s' "$OVERLAY" > "$r/var/datawatch.overlay.toml"
out="$(compose "$r")" && say "an overlay with a key outside any table was accepted"
[[ "$out" == *REFUSED*stray* ]] || say "a key outside any table is not refused by its name: $out"

# --- 4. a hand edit ---------------------------------------------------------
r="$(fresh edited)"
printf '%s' "$OVERLAY" > "$r/var/datawatch.overlay.toml"
compose "$r" >/dev/null
edit "$r/var/datawatch.local.toml" 'series = ["quotes"]' 'series = ["quotes", "candles"]'
before="$(digest "$r/var/datawatch.local.toml")"
out="$(compose "$r")" && say "a hand-edited document was composed over"
[[ "$out" == *REFUSED*"edited by hand"* ]] || say "a hand edit is not refused as one: $out"
[[ "$(digest "$r/var/datawatch.local.toml")" == "$before" ]] || say "a hand edit was overwritten"

# --- 5. hand-kept with no overlay ---------------------------------------------
r="$(fresh handkept)"
printf '%s\n[broker]\nurl = "x"\n' "$COMMITTED" > "$r/var/datawatch.local.toml"
out="$(compose "$r")" && say "a hand-kept document with no overlay was accepted"
[[ "$out" == *REFUSED*--migrate* ]] || say "a hand-kept document is not refused naming the migration: $out"

# --- 6. --check writes nothing ------------------------------------------------
r="$(fresh check)"
printf '%s' "$OVERLAY" > "$r/var/datawatch.overlay.toml"
out="$(compose "$r" --check)"
[[ "$out" == "$r/var/datawatch.local.toml" ]] || say "--check does not say which document a start would read: $out"
[[ ! -e "$r/var/datawatch.local.toml" ]] || say "--check wrote the document"
out="$(compose "$r" --check "$scratch/check-into.toml")"
[[ "$out" == "$scratch/check-into.toml" ]] || say "--check FILE does not say it composed into FILE: $out"
head -1 "$scratch/check-into.toml" 2>/dev/null | grep -q '^# GENERATED' || say "--check FILE does not hold the composition a start would write"
[[ ! -e "$r/var/datawatch.local.toml" ]] || say "--check FILE wrote the deployment's document"
printf '\n[paths.more]\nx = 1\n' >> "$r/var/datawatch.overlay.toml"
compose "$r" --check >/dev/null && say "--check accepted what a start would refuse"

# --- 7. migration, matching -----------------------------------------------------
r="$(fresh migrate)"
printf '%s\n# This machine.\n[broker]\nurl = "nats://example:4222"\n\n[ledger]\nroot = "var/ledger"\n' "$COMMITTED" > "$r/var/datawatch.local.toml"
cp "$r/var/datawatch.local.toml" "$scratch/migrate-before.toml"
out="$(compose "$r" --migrate)" || say "a matching hand-kept document did not migrate: $out"
reads "$r/var/datawatch.overlay.toml" 'sorted(d) == ["broker", "ledger"]' || say "the migrated overlay is not exactly [broker] and [ledger]"
cmp -s "$r/var/datawatch.local.toml.hand-kept" "$scratch/migrate-before.toml" || say "the hand-kept document was not kept beside, as it was"
python3 -c 'import sys, tomllib; a, b = (tomllib.load(open(p, "rb")) for p in sys.argv[1:]); sys.exit(a != b)' \
    "$r/var/datawatch.local.toml" "$scratch/migrate-before.toml" || say "the migrated document does not read as the hand-kept one did"
compose "$r" >/dev/null || say "a migrated machine does not compose"

# --- 8. migration, drifted ------------------------------------------------------
r="$(fresh drifted)"
printf '%s\n[broker]\nurl = "x"\n' "${COMMITTED/\"quotes\"/\"quotes\", \"funding\"}" > "$r/var/datawatch.local.toml"
before="$(digest "$r/var/datawatch.local.toml")"
out="$(compose "$r" --migrate)" && say "a drifted hand-kept document was migrated"
[[ "$out" == *REFUSED*venue.hyperliquid* ]] || say "a drifted migration does not name what drifted: $out"
[[ "$(digest "$r/var/datawatch.local.toml")" == "$before" && ! -e "$r/var/datawatch.overlay.toml" && ! -e "$r/var/datawatch.local.toml.hand-kept" ]] \
    || say "a refused migration changed something"

# --- 9. at once -----------------------------------------------------------------
r="$(fresh atonce)"
printf '%s' "$OVERLAY" > "$r/var/datawatch.overlay.toml"
for _ in 1 2 3 4 5 6 7 8; do python3 "$COMPOSE" "$r" >/dev/null 2>&1 & done
wait
compose "$r" >/dev/null || say "after composing at once, the document is not a whole composition"
[[ -z "$(find "$r/var" -name '.datawatch.local.toml.*')" ]] || say "composing at once left a temporary file behind"

# --- 10. the secrets a deployment needs, from its composed document ----------
r="$scratch/secrets"
mkdir -p "$r/scripts" "$r/var"
cp -R "$ROOT/scripts/lib" "$r/scripts/lib"
cp "$ROOT/scripts/required-secrets.sh" "$r/scripts/"
cp -R "$ROOT/config" "$r/config"
printf '[ledger]\nroot = "var/ledger"\nfingerprint_key_var = "GALATA_LEDGER_FINGERPRINT_KEY"\n\n[ledger.account.main]\nvenue = "hyperliquid"\naddress_var = "GALATA_LEDGER_ADDRESS_MAIN"\n' \
    > "$r/var/datawatch.overlay.toml"
out="$("$r/scripts/required-secrets.sh" 2>&1)"
[[ "$out" == *"GALATA_LEDGER_ADDRESS_MAIN"* ]] \
    || say "required-secrets.sh does not read the overlay's [ledger] through the composed document: $out"

if (( fails )); then
    echo "$me: $fails failure(s)" >&2
    exit 1
fi
echo "$me: ok — composed from the committed file and the overlay; a table in both, a hand edit, a hand-kept document and a drifted migration are refused and change nothing; --check writes nothing; no half-written document"
