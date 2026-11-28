#!/usr/bin/env bash
#
# Mint one read token per service, each able to read ONLY its own secrets, and
# write them to var/tokens/<service>.gvt (0600; *.gvt is ignored by git).
#
#   mint-service-tokens.sh [service ...]
#
# Every service's token, or only the services named (`ledger-derive-xyz`, say),
# so adding one service does not re-mint, and revoke, every other's. A name
# that is not a service here is refused before anything is minted.
#
# **They expire.** 365 days is the server's maximum for a read token, and an
# expired token is a service that cannot restart — capture above all. Run this
# again before the date it prints, then reinstall the services.
#
# **A re-mint revokes what it replaces.** Each token's id goes in
# var/tokens/<service>.id beside it; once the new token is in place, the id
# that file held before is revoked. Without that every re-mint left the old
# token live for the rest of its year. Running services are unaffected: each
# read its secrets at start, and exec left nothing holding the token.
#
# The restriction is the vault's, not this script's: a token minted with
# --only cannot read another name whatever it is asked for.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=scripts/lib/vault-tools.sh
source "$ROOT/scripts/lib/vault-tools.sh"
ENV="$(vault_env)"
TOKENS="$ROOT/var/tokens"
TTL="365d"

GV="$(gv_tool 2>&1)" || { echo "mint-service-tokens: $GV" >&2; exit 1; }
umask 077
mkdir -p "$TOKENS"

# The services named on the command line; none is every one.
ONLY_SERVICES=("$@")

# The ledger's venues, from the deployment's own [ledger] tables: one token per
# venue the accounts name, with or without a [venue.…] table
# (add-derive-account-ledger, D7); none where no ledger is declared.
CONFIG="$ROOT/var/datawatch.local.toml"
[[ -f "$CONFIG" ]] || CONFIG="$ROOT/config/datawatch.toml"
ledger_venues="$(python3 "$ROOT/scripts/lib/ledger_vars.py" "$CONFIG" --venues 2>/dev/null)" \
    || { ledger_venues=""; echo "no ledger declared in $CONFIG — no ledger token"; }

known=(nats capture-hyperliquid markets)
for venue in $ledger_venues; do known+=("ledger-$venue"); done
for asked in ${ONLY_SERVICES[@]+"${ONLY_SERVICES[@]}"}; do
    [[ " ${known[*]} " == *" $asked "* ]] \
        || { echo "mint-service-tokens: no service $asked here (${known[*]}) — nothing minted" >&2; exit 1; }
done

# Whether a service is one this run mints.
wanted() {
    (( ${#ONLY_SERVICES[@]} == 0 )) || [[ " ${ONLY_SERVICES[*]} " == *" $1 "* ]]
}

mint() {
    local service="$1"; shift
    wanted "$service" || return 0
    local only=()
    for name in "$@"; do only+=(--only "$name"); done
    # gv's own structured result: one JSON object on stdout carrying the
    # token and its id. Until galata-vault 0.5.0 this matched `gvt1_…` on one
    # stream and `token <32 hex>` on the other, which is reading a sentence
    # written for a person.
    local said="$TOKENS/$service.said.tmp" result="$TOKENS/$service.json.tmp"
    local fresh="$TOKENS/$service.gvt.tmp"
    if ! "$GV" token mint --env "$ENV" --scope read --ttl "$TTL" --json "${only[@]}" \
            >"$result" 2>"$said"; then
        cat "$said" >&2; rm -f "$said" "$result"
        echo "mint-service-tokens: gv would not mint for $service — var/tokens/$service.gvt left as it was" >&2
        exit 1
    fi
    rm -f "$said"
    # Both fields or neither file: a token stored without its id is one that
    # the next re-mint cannot revoke, and an id recorded without its token
    # points at nothing. The token goes to the file, never to this shell.
    local id
    if ! id="$(python3 - "$result" "$fresh" <<'PY'
import json, pathlib, re, sys
try:
    minted = json.loads(pathlib.Path(sys.argv[1]).read_text())
    token, ident = minted["token"], minted["id"]
except (ValueError, KeyError, TypeError):
    sys.exit("the mint result is not the object gv token mint --json documents")
if not (isinstance(token, str) and token.startswith("gvt1_")):
    sys.exit("the mint result carries no gvt1_ token")
if not (isinstance(ident, str) and re.fullmatch(r"[0-9a-f]{32}", ident)):
    sys.exit("the mint result carries no token id")
pathlib.Path(sys.argv[2]).write_text(token + "\n")
print(ident)
PY
    )"; then
        rm -f "$result" "$fresh"
        echo "mint-service-tokens: $service — var/tokens/$service.gvt and its id left as they were" >&2
        exit 1
    fi
    rm -f "$result"
    local previous=""
    [[ -f "$TOKENS/$service.id" ]] && previous="$(tr -d '[:space:]' < "$TOKENS/$service.id")"

    chmod 600 "$fresh"
    mv "$fresh" "$TOKENS/$service.gvt"
    printf '%s\n' "$id" > "$TOKENS/$service.id"
    echo "minted $service ($id): reads $*"

    # Revoke after replace, never before: a failed revoke is a leftover, not
    # an outage, so it is reported by id and the new token stands.
    if [[ -z "$previous" ]]; then
        echo "  no previous token id recorded for $service — revoke the old one by hand from: gv token ls --env $ENV"
    elif [[ "$previous" == "$id" ]]; then
        echo "  previous token $previous not revoked (it is the new one)" >&2
    elif "$GV" token revoke --env "$ENV" "$previous" >/dev/null 2>&1; then
        echo "  revoked the token it replaces ($previous)"
    else
        echo "  WARNING: could not revoke the token it replaces ($previous) — gv token revoke --env $ENV $previous" >&2
    fi
}

# The broker reads every identity's password the generated authorization file
# names, so its token is minted for exactly those (as run-service.sh reads them).
# Read whether or not the broker's token is asked for: a file naming no
# identity is wrong either way.
broker_names=()
while read -r id; do
    [[ -n "$id" ]] && broker_names+=("$(password_var "$id")")
done < <(awk '$1 == "user:" { print $2 }' "$ROOT/config/nats-authorization.conf")
(( ${#broker_names[@]} )) || { echo "mint-service-tokens: config/nats-authorization.conf names no identity" >&2; exit 1; }
mint nats "${broker_names[@]}"
mint capture-hyperliquid GALATA_BROKER_PASSWORD_DATAWATCH_HYPERLIQUID
# galata-markets: its bus password and its clients' tokens.
markets_names=()
while read -r name; do markets_names+=("$name"); done < <(markets_secrets)
mint markets "${markets_names[@]}"

# The ledger, per venue: that venue's account addresses and the fingerprint
# key, named by the deployment's own [ledger] tables (scripts/lib/ledger_vars.py)
# so this token and run-service.sh cannot disagree.
for venue in $ledger_venues; do
    if names="$(python3 "$ROOT/scripts/lib/ledger_vars.py" "$CONFIG" "$venue")"; then
        # shellcheck disable=SC2086  # one name per line, none with spaces
        mint "ledger-$venue" $names
    fi
done

echo "expire on $(date -v+365d +%Y-%m-%d 2>/dev/null || date -d '+365 days' +%Y-%m-%d) — re-run this before then"
