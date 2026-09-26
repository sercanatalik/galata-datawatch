#!/usr/bin/env bash
#
# Mint one read token per service, each able to read ONLY its own secrets, and
# write them to var/tokens/<service>.gvt (0600; *.gvt is ignored by git).
#
#   mint-service-tokens.sh
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
GV="${GALATA_VAULT_ROOT:-$(cd "$ROOT/.." && pwd)/galata-vault}/target/release/gv"
ENV="galata-datawatch/prod"
TOKENS="$ROOT/var/tokens"
TTL="365d"

[[ -x "$GV" ]] || { echo "mint-service-tokens: no gv at $GV" >&2; exit 1; }
umask 077
mkdir -p "$TOKENS"

mint() {
    local service="$1"; shift
    local only=()
    for name in "$@"; do only+=(--only "$name"); done
    local said="$TOKENS/$service.said.tmp"
    "$GV" token mint --env "$ENV" --scope read --ttl "$TTL" "${only[@]}" 2>"$said" \
        | grep -Eo 'gvt1_[A-Za-z0-9_-]+' | head -n 1 > "$TOKENS/$service.gvt.tmp"
    [[ -s "$TOKENS/$service.gvt.tmp" ]] || { cat "$said" >&2; echo "no token printed for $service" >&2; exit 1; }
    # gv says the id on stderr ("minted read token <id> for ..."); the token
    # alone goes to stdout.
    local id
    id="$(grep -Eo 'token [0-9a-f]{32}' "$said" | head -n 1 | cut -d' ' -f2 || true)"
    rm -f "$said"
    local previous=""
    [[ -f "$TOKENS/$service.id" ]] && previous="$(tr -d '[:space:]' < "$TOKENS/$service.id")"

    mv "$TOKENS/$service.gvt.tmp" "$TOKENS/$service.gvt"
    chmod 600 "$TOKENS/$service.gvt"
    if [[ -n "$id" ]]; then
        printf '%s\n' "$id" > "$TOKENS/$service.id"
    else
        # Refuse to overwrite the record with nothing: the previous id is
        # still the only way to find the token this one replaced.
        echo "WARNING: gv printed no token id for $service — var/tokens/$service.id left as it was" >&2
    fi
    echo "minted $service${id:+ ($id)}: reads $*"

    # Revoke after replace, never before: a failed revoke is a leftover, not
    # an outage, so it is reported by id and the new token stands.
    if [[ -z "$previous" ]]; then
        echo "  no previous token id recorded for $service — revoke the old one by hand from: gv token ls --env $ENV"
    elif [[ -z "$id" || "$previous" == "$id" ]]; then
        echo "  previous token $previous not revoked (no new id to replace it)" >&2
    elif "$GV" token revoke --env "$ENV" "$previous" >/dev/null 2>&1; then
        echo "  revoked the token it replaces ($previous)"
    else
        echo "  WARNING: could not revoke the token it replaces ($previous) — gv token revoke --env $ENV $previous" >&2
    fi
}

mint nats GALATA_BROKER_PASSWORD_DATAWATCH_HYPERLIQUID GALATA_BROKER_PASSWORD_DATAWATCH_RH_CHAIN GALATA_BROKER_PASSWORD_READER
mint capture-hyperliquid GALATA_BROKER_PASSWORD_DATAWATCH_HYPERLIQUID
mint tower GALATA_BROKER_PASSWORD_READER

# The ledger, per venue: that venue's account addresses and the fingerprint
# key, named by the deployment's own [ledger] tables (scripts/lib/ledger_vars.py)
# so this token and run-service.sh cannot disagree. None minted where no ledger
# is declared for the venue.
CONFIG="$ROOT/var/datawatch.local.toml"
[[ -f "$CONFIG" ]] || CONFIG="$ROOT/config/datawatch.toml"
for venue in hyperliquid; do
    if names="$(python3 "$ROOT/scripts/lib/ledger_vars.py" "$CONFIG" "$venue")"; then
        # shellcheck disable=SC2086  # one name per line, none with spaces
        mint "ledger-$venue" $names
    else
        echo "no ledger declared for $venue in $CONFIG — no ledger-$venue token"
    fi
done

echo "expire on $(date -v+365d +%Y-%m-%d 2>/dev/null || date -d '+365 days' +%Y-%m-%d) — re-run this before then"
