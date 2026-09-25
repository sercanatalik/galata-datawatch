#!/usr/bin/env bash
#
# The one program every service agent runs: set the environment one service
# needs, and `exec` it.
#
#   run-service.sh nats
#   run-service.sh capture <venue>
#   run-service.sh ledger <venue>
#   run-service.sh tower
#   run-service.sh flows
#   run-service.sh vault
#
# **Each service gets only its own secrets**, read from the deployment's vault
# (com.galata.vault) through a token minted for that service alone
# (var/tokens/<service>.gvt, 0600; scripts/mint-service-tokens.sh): NATS all
# three broker passwords, capture its own venue's, the tower the reader's, the
# ledger its own venue's account addresses and the fingerprint key, and
# the scheduling lane none — it is built to hold no credential.
# Secrets never go in a launchd plist, which is mode 0644 and readable by
# every user on the machine.
#
# **`exec`, so a stop reaches the service.** launchd stops a job with SIGTERM;
# a wrapper that stayed resident would take the signal itself and leave the
# binary to be SIGKILLed twenty seconds later, unflushed.
#
# launchd's environment is nearly empty, so PATH is set here and every path
# is absolute from this file's location.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TOWER="${GALATA_TOWER_ROOT:-$(cd "$ROOT/.." && pwd)/galata-tower}"
VAULT_BIN="${GALATA_VAULT_ROOT:-$(cd "$ROOT/.." && pwd)/galata-vault}/target/release"
export PATH="/opt/homebrew/bin:/usr/local/bin:$HOME/.local/bin:/usr/bin:/bin"
export NO_COLOR=1
cd "$ROOT"

refuse() { echo "run-service: REFUSED — $1" >&2; exit 2; }

# Start a command with named secrets from the vault, read through THIS
# SERVICE'S token — the vault refuses any name the token was not minted for.
# galata-vault-exec, not `gv run`: gv reads through the owner key, which this
# machine holds, so `gv run` would hand any service every secret.
from_vault() {
    local token="$ROOT/var/tokens/$1.gvt"; shift
    [[ -f "$token" ]] || refuse "no token at $token — scripts/mint-service-tokens.sh"
    local mode
    mode="$(stat -f %Lp "$token" 2>/dev/null || stat -c %a "$token")"
    [[ "$mode" == 600 || "$mode" == 400 ]] \
        || refuse "$token is mode $mode; a token must be 0600 or 0400"
    [[ -x "$ROOT/target/release/galata-vault-exec" ]] \
        || refuse "no galata-vault-exec — cargo build --release -p galata-datawatch-vault"
    export GV_SERVER="${GV_SERVER:-http://127.0.0.1:8750}"
    export GV_TOKEN_FILE="$token"
    exec "$ROOT/target/release/galata-vault-exec" "$@"
}

password_var() {
    # The broker's own naming: GALATA_BROKER_PASSWORD_<IDENTITY>, dashes to
    # underscores, upper case (galata_broker::password_var).
    local id="${1//-/_}"
    printf 'GALATA_BROKER_PASSWORD_%s' "$(printf '%s' "$id" | tr '[:lower:]' '[:upper:]')"
}

service="${1:-}"
case "$service" in
    nats)
        only=()
        for id in datawatch-hyperliquid datawatch-rh-chain reader; do
            only+=(--only "$(password_var "$id")")
        done
        from_vault nats "${only[@]}" -- \
            "$(command -v nats-server)" -c "$ROOT/config/nats-authorization.conf" -a 127.0.0.1 -p 4222
        ;;
    capture)
        venue="${2:-}"
        [[ -n "$venue" ]] || refuse "usage: run-service.sh capture <venue>"
        # This machine's broker block lives in the local configuration, which
        # is the committed one plus [broker]; without it, capture archives and
        # publishes nothing.
        if [[ -f "$ROOT/var/datawatch.local.toml" ]]; then
            export GALATA_CONFIG="$ROOT/var/datawatch.local.toml"
        else
            export GALATA_CONFIG="$ROOT/config/datawatch.toml"
        fi
        from_vault "capture-$venue" --only "$(password_var "datawatch-$venue")" -- \
            "$ROOT/target/release/galata-datawatch" "$venue"
        ;;
    ledger)
        venue="${2:-}"
        [[ -n "$venue" ]] || refuse "usage: run-service.sh ledger <venue>"
        if [[ -f "$ROOT/var/datawatch.local.toml" ]]; then
            export GALATA_CONFIG="$ROOT/var/datawatch.local.toml"
        else
            export GALATA_CONFIG="$ROOT/config/datawatch.toml"
        fi
        [[ -x "$ROOT/target/release/galata-ledger" ]] \
            || refuse "no galata-ledger — cargo build --release first"
        names="$(python3 "$ROOT/scripts/lib/ledger_vars.py" "$GALATA_CONFIG" "$venue")" \
            || refuse "$GALATA_CONFIG declares no ledger account on $venue"
        only=()
        while IFS= read -r name; do only+=(--only "$name"); done <<< "$names"
        from_vault "ledger-$venue" "${only[@]}" -- "$ROOT/target/release/galata-ledger" "$venue"
        ;;
    tower)
        [[ -x "$TOWER/target/release/galata-tower" ]] \
            || refuse "no tower release binary at $TOWER — cargo build --release there, or set GALATA_TOWER_ROOT"
        export GALATA_ARCHIVE="$ROOT/var/archive"
        export GALATA_TAPE="$ROOT/var/tape"
        export GALATA_TOWER_LISTEN="${GALATA_TOWER_LISTEN:-127.0.0.1:8777}"
        export GALATA_BROKER="${GALATA_BROKER:-127.0.0.1:4222}"
        cd "$TOWER"
        from_vault tower --only "$(password_var reader)" -- "$TOWER/target/release/galata-tower"
        ;;
    vault)
        # The deployment's secret store: loopback only, data in
        # ~/.local/share/galata-vault, no configuration. It stores ciphertext
        # and cannot read any of it — the binary links no decryption code.
        [[ -x "$VAULT_BIN/gv-server" ]] \
            || refuse "no gv-server at $VAULT_BIN — cargo build --release -p gv -p gv-server in galata-vault"
        exec "$VAULT_BIN/gv-server" local
        ;;
    flows)
        # No secret at all: the lane holds no credential, by design and by
        # check-python-flows.sh. Its own home, because a shared ~/.cereyan
        # imports and schedules other projects' flows too.
        export CEREYAN_HOME="$HOME/.cereyan-galata"
        export CEREYAN_NO_BROWSER=1
        # FROM py/, which cereyan's service guide asks for: a run executes in
        # an engine process that imports `flows.<module>` from its working
        # directory. Served from the repo root, the flows registered and every
        # run failed "No module named 'flows'" — found by the first run.
        cd "$ROOT/py"
        exec uv run --project . cereyan serve . --no-open --host 127.0.0.1 --port 4200
        ;;
    *)
        refuse "usage: run-service.sh vault | nats | capture <venue> | ledger <venue> | tower | flows"
        ;;
esac
