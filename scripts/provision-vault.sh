#!/usr/bin/env bash
#
# Create this deployment's vault project and move the broker passwords into it.
# Run ONCE, with com.galata.vault running.
#
#   provision-vault.sh
#
#   1. gv init galata-datawatch — the recovery kit is written to
#      var/galata-datawatch-recovery.gvkit (0600). It is the ONLY way to
#      recover the project: no account, no reset. MOVE IT to offline storage
#      or a password manager and delete that copy. gv refuses to finish until
#      "saved" is typed; this script types it, because the operator asked for
#      the kit to be written here and moved afterwards.
#   2. gv env add galata-datawatch/prod
#   3. every GALATA_BROKER_PASSWORD_* in var/broker.env → gv set, value on stdin
#      (never an argument, where `ps` would show it)
#
# Then scripts/mint-service-tokens.sh, then reinstall the services.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
GV="${GALATA_VAULT_ROOT:-$(cd "$ROOT/.." && pwd)/galata-vault}/target/release/gv"
SERVER="http://127.0.0.1:8750"
PROJECT="galata-datawatch"
ENV="$PROJECT/prod"
KIT="$ROOT/var/$PROJECT-recovery.gvkit"
SECRETS="$ROOT/var/broker.env"

refuse() { echo "provision-vault: REFUSED — $1" >&2; exit 1; }
[[ -x "$GV" ]] || refuse "no gv at $GV — cargo build --release -p gv in galata-vault"
curl -s -o /dev/null "$SERVER/" || refuse "no vault at $SERVER — scripts/install-services.sh vault"
[[ -e "$KIT" ]] && refuse "$KIT exists: this project was provisioned already"
[[ -f "$SECRETS" ]] || refuse "no $SECRETS to import"

umask 077
printf 'saved\n' | "$GV" init "$PROJECT" --server "$SERVER" --kit "$KIT"
"$GV" env add "$ENV"
while IFS='=' read -r name value; do
    [[ "$name" == GALATA_BROKER_PASSWORD_* ]] || continue
    printf '%s' "$value" | "$GV" set "$name" --env "$ENV"
    echo "imported $name"
done < "$SECRETS"
echo
echo "RECOVERY KIT: $KIT"
echo "  Move it to offline storage or a password manager, then delete this copy."
