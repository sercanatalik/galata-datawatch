#!/usr/bin/env bash
#
# Every secret this deployment's services read from the vault, by name.
#
#   required-secrets.sh [--config FILE]              one line per read: "<service><TAB><name>"
#   required-secrets.sh [--config FILE] --template   a file to fill in: each name once, with no value
#   required-secrets.sh [--config FILE] --missing    the names the vault's project does not hold yet
#
# **Names, never values.** Nothing here reads a secret. `--missing` asks the
# vault for the names it holds, as the project's owner, and compares.
#
# **From what the services themselves are started from**, so this list and a
# service's start cannot disagree: the identities in the generated
# authorization file (the broker reads every one's password, capture its own
# venue's), and the configuration's venues and
# `[ledger]` tables (scripts/lib/ledger_vars.py). A venue with a broker
# identity and no `[venue.…]` table has no capture here, so nothing reads its
# password but the broker. A ledger's venue is the one its accounts name, table
# or no table: a ledger-only venue has none (add-derive-account-ledger, D7).
#
# **The template is what a fresh project is filled from.** A new vault holds
# nothing, and a service started without a secret looks, for a while, like
# one started with it. config/secrets.env.example is `--template` for the
# committed configuration, and check-service-secrets.sh fails if the two
# differ or if the committed copy has gained a value.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=scripts/lib/vault-tools.sh
source "$ROOT/scripts/lib/vault-tools.sh"
ENV="$(vault_env)"
AUTH="$ROOT/config/nats-authorization.conf"

refuse() { echo "required-secrets: REFUSED — $1" >&2; exit 2; }

CONFIG=""
MODE=list
while [[ $# -gt 0 ]]; do
    case "$1" in
        --config) [[ -n "${2:-}" ]] || refuse "--config takes a file"; CONFIG="$2"; shift 2 ;;
        --template|--missing) MODE="${1#--}"; shift ;;
        *) refuse "usage: required-secrets.sh [--config FILE] [--template | --missing]" ;;
    esac
done
if [[ -z "$CONFIG" ]]; then
    # The deployment's own, composed as run-service.sh composes it
    # (compose-deployment-config): the committed file and this machine's
    # overlay, or the committed file alone when there is no overlay.
    CONFIG="$(python3 "$ROOT/scripts/lib/compose_config.py" "$ROOT" 2>&1)" \
        || refuse "${CONFIG#compose-config: REFUSED — }"
fi
[[ -f "$CONFIG" ]] || refuse "no configuration at $CONFIG"
[[ -f "$AUTH" ]] || refuse "no $AUTH"

venues="$(python3 - "$CONFIG" <<'PY'
import pathlib, sys, tomllib
print("\n".join(sorted(tomllib.loads(pathlib.Path(sys.argv[1]).read_text()).get("venue", {}))))
PY
)" || refuse "$CONFIG does not parse"

# "<service>\t<name>", one line per read.
required() {
    local id name venue names
    while read -r id; do
        [[ -n "$id" ]] || continue
        name="$(password_var "$id")"
        printf 'nats\t%s\n' "$name"
        case "$id" in
            markets) printf 'markets\t%s\n' "$name" ;;
            datawatch-*)
                venue="${id#datawatch-}"
                if grep -qx -- "$venue" <<< "$venues"; then printf 'capture:%s\t%s\n' "$venue" "$name"; fi ;;
        esac
    done < <(awk '$1 == "user:" { print $2 }' "$AUTH")
    # galata-markets' clients' tokens (scripts/lib/markets-clients).
    while read -r name; do
        [[ "$name" == GALATA_MARKETS_TOKEN_* ]] && printf 'markets\t%s\n' "$name"
    done < <(markets_secrets)
    while read -r venue; do
        [[ -n "$venue" ]] || continue
        if names="$(python3 "$ROOT/scripts/lib/ledger_vars.py" "$CONFIG" "$venue" 2>/dev/null)"; then
            while read -r name; do printf 'ledger:%s\t%s\n' "$venue" "$name"; done <<< "$names"
        fi
    done < <(python3 "$ROOT/scripts/lib/ledger_vars.py" "$CONFIG" --venues 2>/dev/null)
}

# Each name once, in the order first asked for.
names() { required | cut -f2 | awk '!seen[$0]++'; }

case "$MODE" in
    list)
        required
        ;;
    template)
        cat <<'HEAD'
# Every secret this deployment's services read from the vault, by name, with
# nothing filled in. Printed by scripts/required-secrets.sh --template; a
# guard keeps the committed copy, config/secrets.env.example, equal to it.
#
# Copy it to var/broker.env, mode 0600, and give each name a value:
# everything after the `=`, to the end of the line, with no quotes.
# scripts/provision-vault.sh imports from there. var/ is outside version
# control and outside every image.
#
# A broker password is this deployment's own invention; any long random
# string will do:
#   python3 -c 'import secrets; print(secrets.token_urlsafe(32))'
#
# A ledger's secrets are not inventions. An account's address is the
# account's, and the fingerprint key, once a ledger has written under it,
# can never be another: the record is keyed by it. Those may be left out of
# the file and set by hand, with the value typed and never stored:
#   gv set NAME --env galata-datawatch/mainnet
#
# The project has a section per network, galata-datawatch/mainnet and
# galata-datawatch/testnet. A deployment reads one: mainnet, unless
# GALATA_VAULT_ENV names the other.
HEAD
        while read -r name; do
            readers="$(required | awk -F'\t' -v n="$name" '$2 == n { printf "%s%s", sep, $1; sep = ", " }')"
            printf '\n# read by: %s\n%s=\n' "$readers" "$name"
        done < <(names)
        ;;
    missing)
        GV="$(gv_tool 2>&1)" || refuse "$GV"
        # The first word of each line `gv ls` prints is a name.
        held="$("$GV" ls --env "$ENV" 2>&1)" \
            || refuse "the vault would not list $ENV: $(printf '%s' "$held" | tail -1)"
        held="$(printf '%s\n' "$held" | awk '{ print $1 }')"
        missing=0
        while read -r name; do
            if ! grep -qx -- "$name" <<< "$held"; then
                readers="$(required | awk -F'\t' -v n="$name" '$2 == n { printf "%s%s", sep, $1; sep = ", " }')"
                printf '%s\t(read by: %s)\n' "$name" "$readers"
                missing=$((missing + 1))
            fi
        done < <(names)
        if (( missing )); then
            echo "required-secrets: $missing required secret(s) are not in $ENV — a service that reads one will not start" >&2
            exit 1
        fi
        echo "required-secrets: ok — $ENV holds every secret a service reads"
        ;;
esac
