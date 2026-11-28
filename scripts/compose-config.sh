#!/usr/bin/env bash
#
# Compose the deployment's configuration document (compose-deployment-config).
#
#   compose-config.sh             compose var/datawatch.local.toml, print it
#   compose-config.sh --check     every refusal a start would meet; writes nothing
#   compose-config.sh --vet       compose, then ask every deployed binary
#                                 whether it accepts the result (vet-config.sh)
#   compose-config.sh --migrate   once per machine: a hand-kept local document
#                                 becomes var/datawatch.overlay.toml
#
# The document is the committed config/datawatch.toml followed by this
# machine's var/datawatch.overlay.toml ([broker], [ledger]: names, never
# values). Every service start composes it too (run-service.sh's use_config),
# so a committed change reaches a service at its next start. Edit the
# committed file or the overlay, never the composed document: a hand edit is
# refused, not overwritten. The rules are scripts/lib/compose_config.py's.

set -uo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE=(python3 "$ROOT/scripts/lib/compose_config.py" "$ROOT")

case "${1:-}" in
    "")        exec "${COMPOSE[@]}" ;;
    --check)   exec "${COMPOSE[@]}" --check ;;
    --migrate) exec "${COMPOSE[@]}" --migrate ;;
    --vet)
        path="$("${COMPOSE[@]}")" || exit 1
        exec "$ROOT/scripts/vet-config.sh" "$path" ;;
    *) echo "usage: compose-config.sh [--check | --vet | --migrate]" >&2; exit 2 ;;
esac
