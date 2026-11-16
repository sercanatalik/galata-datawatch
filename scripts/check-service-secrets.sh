#!/usr/bin/env bash
#
# A SERVICE IS HANDED ITS SECRETS BY A `gv` THAT HONOURS ITS TOKEN, UNDER NAMES
# A PROCESS CAN HOLD.
#
# Three things, each of which fails silently if it fails at all:
#
#   1. **The version floor refuses an older gv.** Before galata-vault 0.5.0,
#      `gv run` ignored a token file and read through the owner key, so every
#      service got every secret. Nothing about such a start looks wrong. The
#      floor in scripts/lib/vault-tools.sh is the only thing between these
#      scripts and that, so it is run here against a `gv` that reports 0.4.9
#      (refused) and one that reports the floor (accepted).
#
#   2. **Every secret name the services ask for can be an environment
#      variable.** `gv run` skips, with a warning, a name that cannot — which
#      here would be a service started without its password. The broker's
#      identities carry dashes; `password_var` is what removes them.
#
#   3. **No script reaches for a galata-vault checkout.** The scripts run the
#      installed release (GALATA_VAULT_BIN). A path into a sibling source tree
#      is a deployment that runs whatever was last built there.
#
# What it cannot check: that the installed gv really is what its version says,
# and that the vault enforces a token's allow-list. Those are measured against
# a real server, in design/measured.md.
#
# Usage: check-service-secrets.sh [root]

set -euo pipefail
ROOT="${1:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
LIB="$ROOT/scripts/lib/vault-tools.sh"
me="$(basename "$0")"
fails=0
say() { echo "$me: $1" >&2; fails=$((fails + 1)); }

[[ -f "$LIB" ]] || { echo "$me: no $LIB" >&2; exit 2; }
# shellcheck source=scripts/lib/vault-tools.sh
source "$LIB"

# --- 1. the floor ---------------------------------------------------------
stubs="$(mktemp -d)"
trap 'rm -rf "$stubs"' EXIT
stub() {  # stub <version>: a directory holding a gv that reports it
    local dir="$stubs/$1"
    mkdir -p "$dir"
    printf '#!/bin/sh\necho "gv %s"\n' "$1" > "$dir/gv"
    chmod 755 "$dir/gv"
    printf '%s' "$dir"
}
accepted() { GALATA_VAULT_BIN="$1" gv_tool >/dev/null 2>&1; }

[[ "$GV_FLOOR" == 0.5.0 ]] \
    || say "the floor is $GV_FLOOR; 0.5.0 is the first gv that honours a token file, and a different floor needs this guard's reason restated"
accepted "$(stub 0.4.9)" && say "a gv reporting 0.4.9 is accepted: it ignores GV_TOKEN_FILE and reads through the owner key"
accepted "$(stub 0.4.0)" && say "a gv reporting 0.4.0 is accepted"
accepted "$(stub "$GV_FLOOR")" || say "a gv reporting the floor, $GV_FLOOR, is refused"
accepted "$(stub 0.10.0)" || say "a gv reporting 0.10.0 is refused: versions are compared as numbers, not text"
accepted "$(stub 1.0.0)" || say "a gv reporting 1.0.0 is refused"
accepted "$stubs/absent" && say "a directory with no gv in it is accepted"
mkdir -p "$stubs/silent"; printf '#!/bin/sh\nexit 0\n' > "$stubs/silent/gv"; chmod 755 "$stubs/silent/gv"
accepted "$stubs/silent" && say "a gv that reports no version is accepted"

# --- 2. the names ---------------------------------------------------------
# The broker identities run-service.sh starts services under, read from it so
# a new one is covered the day it is added.
identities="$(grep -Eo 'password_var "?[a-z$"-]+[a-z-]*' "$ROOT/scripts/run-service.sh" \
    | sed -E 's/password_var "?//' | grep -v '\$' | sort -u || true)"
identities="$identities
$(grep -Eo 'for id in [a-z -]+;' "$ROOT/scripts/run-service.sh" | sed -E 's/for id in //; s/;//' | tr ' ' '\n')
datawatch-hyperliquid"
count=0
while IFS= read -r id; do
    [[ -n "$id" ]] || continue
    name="$(password_var "$id")"
    count=$((count + 1))
    is_env_name "$name" || say "the broker identity $id becomes the secret name $name, which cannot be an environment variable: gv run would start the service without it"
done <<< "$identities"
(( count >= 3 )) || say "found only $count broker identities in run-service.sh — refusing to pass on nothing"

# The names minted by hand in mint-service-tokens.sh.
while IFS= read -r name; do
    [[ -n "$name" ]] || continue
    is_env_name "$name" || say "mint-service-tokens.sh mints a token for $name, which cannot be an environment variable"
done < <(grep -E '^mint [a-z-]+ ' "$ROOT/scripts/mint-service-tokens.sh" | cut -d' ' -f3- | tr ' ' '\n')

# The ledger's names, from the committed configuration (none declared is fine).
for venue in hyperliquid; do
    if names="$(python3 "$ROOT/scripts/lib/ledger_vars.py" "$ROOT/config/datawatch.toml" "$venue" 2>/dev/null)"; then
        while IFS= read -r name; do
            is_env_name "$name" || say "the ledger on $venue asks for $name, which cannot be an environment variable"
        done <<< "$names"
    fi
done

# --- 3. no checkout -------------------------------------------------------
# This file and test-guards.sh name the violation in order to describe and
# plant it; neither runs anything.
while IFS= read -r hit; do
    say "a script reaches for a galata-vault checkout: $hit"
done < <(grep -nE 'GALATA_VAULT_ROOT|galata-vault[^ ]*/target/|galata-vault-exec' \
            "$ROOT"/scripts/*.sh "$ROOT"/deploy/launchd/* 2>/dev/null \
         | grep -Ev "^$ROOT/scripts/($me|test-guards\.sh):" || true)

if (( fails > 0 )); then exit 1; fi
echo "$me: ok — an older gv is refused, every secret name can be a variable, and no script reaches for a checkout"
