#!/usr/bin/env bash
#
# Create this deployment's vault project and move the broker passwords into it.
# Run ONCE, against the deployment's vault.
#
#   provision-vault.sh <vault address>
#
#   <vault address>   where the vault answers, and the address the project is
#                     pinned to: the name its recovery kit and its tokens'
#                     vault carry from then on. For the hosted vault that is
#                     its https:// name on the tailnet, which galata-vault's
#                     `gv-container up` prints. GV_SERVER, when no argument is
#                     given.
#
# **No address is assumed.** This once said `http://127.0.0.1:8750`, the
# vault this deployment ran itself. A project provisioned at the wrong
# address is pinned there, and moving one is not something the vault does.
#
#   1. gv init galata-datawatch — the recovery kit is written to
#      var/galata-datawatch-recovery.gvkit (0600). It is the ONLY way to
#      recover the project: no account, no reset. MOVE IT to offline storage
#      or a password manager and delete that copy. gv refuses to finish until
#      "saved" is typed; this script types it, because the operator asked for
#      the kit to be written here and moved afterwards.
#   2. gv env add galata-datawatch/prod
#   3. every secret a service reads (scripts/required-secrets.sh) that
#      var/broker.env gives a value → gv set, value on stdin (never an
#      argument, where `ps` would show it)
#
# **A fresh project holds nothing**, so what it must hold is checked before
# it is created: every broker password this deployment's services read has
# to be in var/broker.env, or nothing is made. config/secrets.env.example is
# the file to fill in. A ledger's secrets may be left out of it and set by
# hand; the names still owed are listed at the end.
#
# Then scripts/mint-service-tokens.sh, then reinstall the services.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=scripts/lib/vault-tools.sh
source "$ROOT/scripts/lib/vault-tools.sh"
SERVER="${1:-${GV_SERVER:-}}"
SERVER="${SERVER%/}"
PROJECT="galata-datawatch"
ENV="$PROJECT/prod"
KIT="$ROOT/var/$PROJECT-recovery.gvkit"
SECRETS="$ROOT/var/broker.env"

refuse() { echo "provision-vault: REFUSED — $1" >&2; exit 1; }
GV="$(gv_tool 2>&1)" || refuse "$GV"
[[ -n "$SERVER" ]] \
    || refuse "say where the vault answers: provision-vault.sh <vault address> — for the hosted vault, the https:// name its \`gv-container up\` prints"
# The rule gv itself applies, said here before anything is created: https://,
# or plain http:// to this machine only.
case "$SERVER" in
    https://?*) ;;
    http://127.0.0.1|http://127.0.0.1:*|http://localhost|http://localhost:*|"http://[::1]"|"http://[::1]:"*) ;;
    *) refuse "'$SERVER' is not an address a vault client accepts: https://, or plain http:// to this machine only" ;;
esac
curl -s -o /dev/null --max-time 10 "$SERVER/" \
    || refuse "no vault answers at $SERVER — start the deployment's vault first (galata-vault: deploy/apple-container/gv-container up)"
[[ -e "$KIT" ]] && refuse "$KIT exists: this project was provisioned already"
[[ -f "$SECRETS" ]] || refuse "no $SECRETS to import — copy config/secrets.env.example there, mode 0600, and fill it in"

# What the services read, by name, and what the file gives a value. Checked
# before anything is created: a project made and then found a password short
# is a project to explain, and a service started from it is one started
# without its secret.
required="$("$ROOT/scripts/required-secrets.sh" | cut -f2 | awk '!seen[$0]++')" \
    || refuse "could not list the secrets the services read (scripts/required-secrets.sh)"
given() {  # given <name>: does the file give it a value?
    awk -F= -v n="$1" '$1 == n && length($0) > length(n) + 1 { found = 1 } END { exit found ? 0 : 1 }' "$SECRETS"
}
short=""
while read -r name; do
    [[ "$name" == GALATA_BROKER_PASSWORD_* ]] || continue
    given "$name" || short="$short $name"
done <<< "$required"
[[ -z "$short" ]] \
    || refuse "$SECRETS gives no value for:$short — nothing was created. config/secrets.env.example names every one"

umask 077
printf 'saved\n' | "$GV" init "$PROJECT" --server "$SERVER" --kit "$KIT"
"$GV" env add "$ENV"
owed=""
while read -r name; do
    [[ -n "$name" ]] || continue
    if given "$name"; then
        # Everything after the first `=`, to the end of the line.
        awk -v n="$name" 'index($0, n "=") == 1 { printf "%s", substr($0, length(n) + 2); exit }' "$SECRETS" \
            | "$GV" set "$name" --env "$ENV"
        echo "imported $name"
    else
        owed="$owed $name"
    fi
done <<< "$required"
if [[ -n "$owed" ]]; then
    echo
    echo "STILL OWED, set by hand (the value is typed, and stored nowhere else):"
    for name in $owed; do echo "  gv set $name --env $ENV"; done
fi
echo
echo "RECOVERY KIT: $KIT"
echo "  Move it to offline storage or a password manager, then delete this copy."
