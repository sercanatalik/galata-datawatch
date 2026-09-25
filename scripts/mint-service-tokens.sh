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
    "$GV" token mint --env "$ENV" --scope read --ttl "$TTL" "${only[@]}" \
        | grep -Eo 'gvt1_[A-Za-z0-9_-]+' | head -n 1 > "$TOKENS/$service.gvt.tmp"
    [[ -s "$TOKENS/$service.gvt.tmp" ]] || { echo "no token printed for $service" >&2; exit 1; }
    mv "$TOKENS/$service.gvt.tmp" "$TOKENS/$service.gvt"
    chmod 600 "$TOKENS/$service.gvt"
    echo "minted $service: reads $*"
}

mint nats GALATA_BROKER_PASSWORD_DATAWATCH_HYPERLIQUID GALATA_BROKER_PASSWORD_DATAWATCH_RH_CHAIN GALATA_BROKER_PASSWORD_READER
mint capture-hyperliquid GALATA_BROKER_PASSWORD_DATAWATCH_HYPERLIQUID
mint tower GALATA_BROKER_PASSWORD_READER

echo "expire on $(date -v+365d +%Y-%m-%d 2>/dev/null || date -d '+365 days' +%Y-%m-%d) — re-run this before then"
