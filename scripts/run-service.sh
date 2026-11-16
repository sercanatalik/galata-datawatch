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
#   run-service.sh --check <service> [<venue>]   every precondition, then `ready`;
#                                                starts nothing
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
# shellcheck source=scripts/lib/vault-tools.sh
source "$ROOT/scripts/lib/vault-tools.sh"
export PATH="/opt/homebrew/bin:/usr/local/bin:$HOME/.local/bin:/usr/bin:/bin"
export NO_COLOR=1
cd "$ROOT"

refuse() { echo "run-service: REFUSED — $1" >&2; exit 2; }

# **--check: would this service start?** Every precondition below, plus that
# its token opens and has not expired, and then `ready` in place of the exec.
# A restart is when a missing binary or a lapsed token is found otherwise:
# the tower's installed copy was missing for a day while --status read
# `pid 1770 yes` (say-a-service-cannot-restart).
CHECK=0
if [[ "${1:-}" == --check ]]; then CHECK=1; shift; fi

# The one exec every branch ends in: the program is tested first, so a missing
# one is refused by name rather than by bash, and under --check it is reported
# instead of run.
start() {
    [[ -x "$1" ]] || refuse "no program at $1"
    if (( CHECK )); then
        local note
        note="$(config_verdict "$1")"
        echo "ready: $*${note:+ ($note)}"
        exit 0
    fi
    exec "$@"
}

# **Would this binary accept the configuration it will be handed?** The
# document is parsed strictly, so a key the deployed build predates makes the
# restart fail, and nothing said so before 2026-09-26's near-miss
# (a-restart-checks-the-config). Asked only of programs that read the
# document, and only under --check. A build older than --check-config is
# already running on this config, so "cannot tell" is said, not refused.
config_verdict() {  # prints a note, or refuses
    local program="$1" answer
    [[ -n "${GALATA_CONFIG:-}" ]] || return 0
    case "${program##*/}" in
        galata-datawatch|galata-ledger) ;;
        galata-tower)
            # An older tower ignores its arguments and serves: ask only one
            # whose --version says it understands the flag.
            "$program" --version 2>/dev/null </dev/null | grep -q '^understands:.*--check-config' \
                || { echo "config not checked: this tower predates --check-config"; return 0; } ;;
        *) return 0 ;;
    esac
    answer="$("$program" --check-config "$GALATA_CONFIG" 2>/dev/null </dev/null | head -1)"
    case "$answer" in
        ok:*) ;;
        refused:*) refuse "${program##*/} refuses $GALATA_CONFIG: ${answer#refused: }" ;;
        *) echo "config not checked: this build predates --check-config" ;;
    esac
}

# Start a command with named secrets from the vault, read through THIS
# SERVICE'S token — the vault refuses any name the token was not minted for.
#
#   from_vault <service> --only NAME [--only NAME ...] -- <command> [args ...]
#
# `gv run`, from galata-vault 0.5.0 on: given a token file it uses that token
# or refuses, and it refuses a name outside the token's allow-list rather than
# starting the command without it. Before 0.5.0 it ignored the file and read
# through the owner key, which this machine holds — so the version is checked
# first (scripts/lib/vault-tools.sh), and an older gv is never handed a token.
# `gv run` execs, so the service replaces it and nothing resident holds the
# values.
#
# The token's expiry is judged before the start, by galata-token-expiry: a
# lapsed one is refused here, by name, and one inside the warning window logs
# a line and starts anyway — a valid token is never refused.
from_vault() {
    local service="$1" token="$ROOT/var/tokens/$1.gvt"; shift
    [[ -f "$token" ]] || refuse "no token at $token — scripts/mint-service-tokens.sh"
    local mode
    mode="$(stat -f %Lp "$token" 2>/dev/null || stat -c %a "$token")"
    [[ "$mode" == 600 || "$mode" == 400 ]] \
        || refuse "$token is mode $mode; a token must be 0600 or 0400"
    local judge="$ROOT/target/release/galata-token-expiry"
    [[ -x "$judge" ]] \
        || refuse "no galata-token-expiry — cargo build --release -p galata-datawatch-vault"
    local gv
    gv="$(gv_tool 2>&1)" || refuse "$gv"

    local names="" name
    while [[ $# -gt 0 && "$1" != -- ]]; do
        [[ "$1" == --only && -n "${2:-}" ]] || refuse "from_vault: expected --only NAME, got '$1'"
        name="$2"; shift 2
        # gv run skips, with a warning, a name that cannot be a variable; here
        # that would be a service started without its secret.
        is_env_name "$name" || refuse "'$name' cannot be an environment variable, so $service would start without it"
        names="${names:+$names,}$name"
    done
    [[ "${1:-}" == -- && -n "$names" && $# -ge 2 ]] \
        || refuse "from_vault: usage: <service> --only NAME ... -- <command>"
    shift

    export GV_SERVER="${GV_SERVER:-http://127.0.0.1:8750}"
    export GV_TOKEN_FILE="$token"
    # "<unix> <YYYY-MM-DD> <days> <ok|WARN|EXPIRED>"
    local expiry
    expiry="$("$judge" 2>&1 </dev/null)" \
        || refuse "$token does not open: ${expiry#galata-token-expiry: }"
    local date days state
    read -r _ date days state <<< "$expiry"
    case "$state" in
        ok) ;;
        WARN)
            (( CHECK )) || echo "run-service: WARNING $service's token expires on $date ($days days); after that the service cannot restart. Re-mint with scripts/mint-service-tokens.sh, then scripts/install-services.sh" >&2 ;;
        EXPIRED) refuse "$token has expired ($date) — scripts/mint-service-tokens.sh" ;;
        *) refuse "$token: galata-token-expiry said '$expiry', which is no verdict" ;;
    esac
    # Under --check, the program after `--` is what gv run would exec.
    (( CHECK )) && start "$@"
    [[ -x "$1" ]] || refuse "no program at $1"
    exec "$gv" run --only "$names" -- "$@"
}

# This machine's broker block lives in the local configuration, which is the
# committed one plus [broker]; without it, capture archives and publishes
# nothing. Every service that reads the document reads the same one.
use_config() {
    if [[ -f "$ROOT/var/datawatch.local.toml" ]]; then
        export GALATA_CONFIG="$ROOT/var/datawatch.local.toml"
    else
        export GALATA_CONFIG="$ROOT/config/datawatch.toml"
    fi
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
        [[ -n "$venue" ]] || refuse "usage: run-service.sh capture <venue> [--import <dir>]"
        use_config
        from_vault "capture-$venue" --only "$(password_var "datawatch-$venue")" -- \
            "$ROOT/target/release/galata-datawatch" "$venue" "${@:3}"
        ;;
    ledger)
        venue="${2:-}"
        [[ -n "$venue" ]] || refuse "usage: run-service.sh ledger <venue>"
        use_config
        [[ -x "$ROOT/target/release/galata-ledger" ]] \
            || refuse "no galata-ledger — cargo build --release first"
        names="$(python3 "$ROOT/scripts/lib/ledger_vars.py" "$GALATA_CONFIG" "$venue")" \
            || refuse "$GALATA_CONFIG declares no ledger account on $venue"
        only=()
        while IFS= read -r name; do only+=(--only "$name"); done <<< "$names"
        from_vault "ledger-$venue" "${only[@]}" -- "$ROOT/target/release/galata-ledger" "$venue"
        ;;
    tower)
        # The copy install-services.sh put in var/bin, never the tower
        # checkout's target/: a build there is not a deploy, and a restart
        # must not make it one. On 2026-09-25 a stray SIGTERM restarted this
        # service onto a build nobody had chosen to ship.
        bin="$ROOT/var/bin/galata-tower"
        [[ -x "$bin" ]] || refuse "no installed tower at $bin — scripts/install-services.sh tower"
        # Every path absolute. The tower's defaults are relative to its own
        # checkout, and from here its config/datawatch.toml does not exist:
        # the same day, latest prices went out empty for want of GALATA_CONFIG.
        export GALATA_ARCHIVE="$ROOT/var/archive"
        export GALATA_TAPE="$ROOT/var/tape"
        export GALATA_STATUS="$ROOT/var/status"
        use_config
        export GALATA_TOWER_LISTEN="${GALATA_TOWER_LISTEN:-127.0.0.1:8777}"
        export GALATA_BROKER="${GALATA_BROKER:-127.0.0.1:4222}"
        from_vault tower --only "$(password_var reader)" -- "$bin"
        ;;
    vault)
        # The deployment's secret store: loopback only, data in
        # ~/.local/share/galata-vault, no configuration. It stores ciphertext
        # and cannot read any of it — the binary links no decryption code.
        server="$(vault_tool gv-server 2>&1)" || refuse "$server"
        start "$server" local
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
        # The signal calculator's own environment (py/signals: numpy, arch,
        # galata-research), which the lane runs as a tool and never imports.
        # Synced from its lock before the lane serves, so the first :15 finds
        # it; --frozen, so a start never moves a pin.
        "$(command -v uv || echo uv)" sync --project signals --frozen --quiet \
            || refuse "could not sync py/signals from its lock — uv sync --project py/signals"
        start "$(command -v uv || echo uv)" run --project . cereyan serve . --no-open --host 127.0.0.1 --port 4200
        ;;
    *)
        refuse "usage: run-service.sh [--check] vault | nats | capture <venue> | ledger <venue> | tower | flows"
        ;;
esac
