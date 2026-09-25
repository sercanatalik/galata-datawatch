#!/usr/bin/env bash
#
# The one program every service agent runs: set the environment one service
# needs, and `exec` it.
#
#   run-service.sh nats
#   run-service.sh capture <venue>
#   run-service.sh tower
#   run-service.sh flows
#
# **Each service gets only its own secrets.** var/broker.env (0600, never
# tracked) holds every broker password; NATS gets all of them, capture its
# own venue's, the tower the reader's, and the scheduling lane none — the lane
# is built to hold no credential, and the tower to read and never publish.
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
SECRETS="$ROOT/var/broker.env"
export PATH="/opt/homebrew/bin:/usr/local/bin:$HOME/.local/bin:/usr/bin:/bin"
export NO_COLOR=1
cd "$ROOT"

refuse() { echo "run-service: REFUSED — $1" >&2; exit 2; }

# The value of one variable in the secrets file, refusing a file others can
# read — the rule gv applies to a token file, applied here.
secret() {
    [[ -f "$SECRETS" ]] || refuse "no $SECRETS — generate the broker passwords first"
    local mode
    mode="$(stat -f %Lp "$SECRETS" 2>/dev/null || stat -c %a "$SECRETS")"
    [[ "$mode" == 600 || "$mode" == 400 ]] \
        || refuse "$SECRETS is mode $mode; it holds passwords and must be 0600 or 0400"
    local value
    value="$(grep -E "^$1=" "$SECRETS" | head -n 1 | cut -d= -f2-)"
    [[ -n "$value" ]] || refuse "$SECRETS has no $1"
    printf '%s' "$value"
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
        for id in datawatch-hyperliquid datawatch-rh-chain reader; do
            var="$(password_var "$id")"
            export "$var=$(secret "$var")"
        done
        exec nats-server -c "$ROOT/config/nats-authorization.conf" -a 127.0.0.1 -p 4222
        ;;
    capture)
        venue="${2:-}"
        [[ -n "$venue" ]] || refuse "usage: run-service.sh capture <venue>"
        var="$(password_var "datawatch-$venue")"
        export "$var=$(secret "$var")"
        # This machine's broker block lives in the local configuration, which
        # is the committed one plus [broker]; without it, capture archives and
        # publishes nothing.
        if [[ -f "$ROOT/var/datawatch.local.toml" ]]; then
            export GALATA_CONFIG="$ROOT/var/datawatch.local.toml"
        else
            export GALATA_CONFIG="$ROOT/config/datawatch.toml"
        fi
        exec "$ROOT/target/release/galata-datawatch" "$venue"
        ;;
    tower)
        [[ -x "$TOWER/target/release/galata-tower" ]] \
            || refuse "no tower release binary at $TOWER — cargo build --release there, or set GALATA_TOWER_ROOT"
        var="$(password_var reader)"
        export "$var=$(secret "$var")"
        export GALATA_ARCHIVE="$ROOT/var/archive"
        export GALATA_TAPE="$ROOT/var/tape"
        export GALATA_TOWER_LISTEN="${GALATA_TOWER_LISTEN:-127.0.0.1:8777}"
        export GALATA_BROKER="${GALATA_BROKER:-127.0.0.1:4222}"
        cd "$TOWER"
        exec "$TOWER/target/release/galata-tower"
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
        refuse "usage: run-service.sh nats | capture <venue> | tower | flows"
        ;;
esac
