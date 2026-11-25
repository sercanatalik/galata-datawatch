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
#   run-service.sh --check <service> [<venue>]   every precondition, then `ready`;
#                                                starts nothing
#
# **Each service gets only its own secrets**, read from the deployment's vault
# through a token minted for that service alone
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
# shellcheck source=scripts/lib/container-tools.sh
source "$ROOT/scripts/lib/container-tools.sh"
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
    # Two facts that must not read alike. A vault that did not answer is an
    # outage: start it and the next attempt works, with this same token. A
    # token the vault refuses will never work again, and the remedy is a
    # re-mint. launchd retries either way; the operator reads which.
    local expiry
    if ! expiry="$("$judge" 2>&1 </dev/null)"; then
        case "$expiry" in
            *"could not reach"*)
                refuse "the vault did not answer at $GV_SERVER, so $service's secrets cannot be read — start the vault; $service's token is not the problem (${expiry#galata-token-expiry: })" ;;
            *)
                refuse "$token does not open: ${expiry#galata-token-expiry: }" ;;
        esac
    fi
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

# **A service in a container of its own** (Apple `container`), when
# var/containers.conf lists it; natively, as below, when it does not. The line
# that lists it also states its CPU and memory, because a container given
# neither gets 4 CPUs and 1 GiB whatever the service is.
#
#   in_container <spec> <image> [container-run option ...]
#
# checks every precondition and sets CONTAINER_RUN to the words that start
# the container: `<tool> run --rm --name <name> --cpus N --memory M <options>
# <image>`. The caller appends the program and hands the line to `start` or
# to `from_vault`, so it is `exec`ed like any other service: `container run`
# stays in the foreground, launchd's SIGTERM reaches it, and it passes the
# stop to the container's process.
DATAWATCH_IMAGE="$(service_image capture)"
NATS_IMAGE="$(service_image nats)"
FLOWS_IMAGE="$(service_image flows)"
CONTAINER_RUN=()
in_container() {
    local spec="$1" image="$2"; shift 2
    local tool said limits cpus memory name
    tool="$(container_tool 2>&1)" || refuse "$tool"
    said="$(container_running "$tool" 2>&1)" || refuse "$said"
    said="$(container_image "$tool" "$image" 2>&1)" || refuse "$said"
    limits="$(container_limits "$spec" 2>&1)" || refuse "$limits"
    read -r cpus memory <<< "$limits"
    name="$(container_name "$spec")"
    # A container left by a crash, or by a client that lost its connection,
    # still holds the name. Not under --check, which starts and stops nothing.
    if (( ! CHECK )); then "$tool" rm -f "$name" >/dev/null 2>&1 || true; fi
    CONTAINER_RUN=("$tool" run --rm --name "$name" --cpus "$cpus" --memory "$memory" "$@" "$image")
}

# The `-v` words for a service's own roots, each read from the configuration
# (scripts/lib/config_roots.py) and seen to exist. A root that is missing is
# refused, never created here: an empty archive made by a typo in a path
# looks exactly like a capture that has just started.
#
#   root_mounts <key> [<key> ...]     sets ROOT_MOUNTS
ROOT_MOUNTS=()
root_mounts() {
    local lines host inside
    ROOT_MOUNTS=()
    lines="$(python3 "$ROOT/scripts/lib/config_roots.py" "$GALATA_CONFIG" "$ROOT" "$@" 2>&1)" \
        || refuse "$GALATA_CONFIG does not say where ${*} is${lines:+: $lines}"
    while IFS=$'\t' read -r host inside; do
        [[ -n "$host" ]] || continue
        [[ -d "$host" ]] || refuse "no folder at $host, which $GALATA_CONFIG names as a root — a container is given a folder that exists; mkdir -p it if the path is right"
        ROOT_MOUNTS+=(-v "$host:$inside")
    done <<< "$lines"
}

# The configuration a container reads: a copy, in a folder of this service's
# own, mounted read-only.
#
# A copy, because Apple's `container` mounts a FOLDER read-only and a file
# only read-write (measured 2026-11-24, 1.7.1), and the folder the
# deployment's configuration lives in is var/, which holds every service's
# token. Taken at each start, as a native service reads the file at each
# start; whole, never merged.
#
#   config_mount <name>     sets CONFIG_MOUNT, and GALATA_CONFIG's path inside
CONFIG_MOUNT=()
CONFIG_INSIDE=/app/config/datawatch.toml
config_mount() {
    local dir="$ROOT/var/run/$1"
    if (( ! CHECK )); then
        mkdir -p "$dir" || refuse "could not make $dir"
        chmod 700 "$dir"
        cp "$GALATA_CONFIG" "$dir/datawatch.toml" || refuse "could not copy $GALATA_CONFIG into $dir"
    fi
    CONFIG_MOUNT=(--mount "type=bind,source=$dir,target=/app/config,readonly"
                  -e "GALATA_CONFIG=$CONFIG_INSIDE" -e NO_COLOR=1)
}

# The lane's run store and its schedule: a volume of its own.
#
# The archive and the tape are folders on the host because other services and
# native tools read them. The lane's state is read by nothing but the lane, so
# it gets an Apple volume, which one container attaches at a time. Made at the
# first real start and handed once to the image's user, who cannot write to a
# new volume (measured 2026-11-24: mode 755, owned by root). Deletable, as the
# run store always was (py/flows/__init__.py, the first invariant).
FLOWS_STATE="${GALATA_FLOWS_STATE:-galata-flows-state}"
flows_state() {
    local tool
    tool="$(container_tool 2>&1)" || refuse "$tool"
    container_volume "$tool" "$FLOWS_STATE" && return 0
    "$tool" volume create "$FLOWS_STATE" >/dev/null \
        || refuse "could not create the volume $FLOWS_STATE"
    "$tool" run --rm --name galata-flows-state-init --cpus 1 --memory 256M -u 0 \
            -v "$FLOWS_STATE:/data" "$FLOWS_IMAGE" chown galata /data >/dev/null \
        || refuse "could not hand the volume $FLOWS_STATE to the lane's user — container volume rm $FLOWS_STATE, then start again"
}

service="${1:-}"
case "$service" in
    nats)
        only=()
        secrets=()
        for id in datawatch-hyperliquid datawatch-rh-chain reader; do
            only+=(--only "$(password_var "$id")")
            secrets+=("$(password_var "$id")")
        done
        if containerised nats; then
            # Reached by the host's tailnet name, so published on the host's
            # tailnet address and on no other. A tailnet that is down is a
            # reason not to start: the only other addresses are loopback,
            # which no container reaches, and the LAN.
            tailnet="$(tailnet_address 2>&1)" || refuse "$tailnet"
            # config/ read-only: the generated authorization file, which
            # names each password as a variable and holds none.
            in_container nats "$NATS_IMAGE" -i \
                --mount "type=bind,source=$ROOT/config,target=/etc/galata,readonly" \
                --publish "$tailnet:4222:4222"
            # -a 0.0.0.0, inside the container: the published port arrives on
            # the container's own address, not its loopback.
            from_vault nats "${only[@]}" -- \
                /bin/bash "$ROOT/scripts/lib/hand-secrets.sh" "${secrets[@]}" -- \
                "${CONTAINER_RUN[@]}" with-secrets "${secrets[@]}" -- \
                nats-server -c /etc/galata/nats-authorization.conf -a 0.0.0.0 -p 4222
        fi
        from_vault nats "${only[@]}" -- \
            "$(command -v nats-server)" -c "$ROOT/config/nats-authorization.conf" -a 127.0.0.1 -p 4222
        ;;
    capture)
        venue="${2:-}"
        [[ -n "$venue" ]] || refuse "usage: run-service.sh capture <venue> [--import <dir>]"
        use_config
        if containerised "capture:$venue"; then
            # An import reads a folder of rescued pages, which no container is
            # given: it is a one-off, run natively, beside a stopped service.
            [[ $# -le 2 ]] || refuse "capture:$venue runs in a container, and '${*:3}' needs a folder it is not given — run that natively: target/release/galata-datawatch $venue ${*:3}"
            secret="$(password_var "datawatch-$venue")"
            root_mounts paths.archive paths.status
            config_mount "$(container_name "capture:$venue")"
            # -i: the secret arrives on standard input (lib/hand-secrets.sh),
            # never with -e, which Apple's container writes to its own state.
            in_container "capture:$venue" "$DATAWATCH_IMAGE" -i "${ROOT_MOUNTS[@]}" "${CONFIG_MOUNT[@]}"
            from_vault "capture-$venue" --only "$secret" -- \
                /bin/bash "$ROOT/scripts/lib/hand-secrets.sh" "$secret" -- \
                "${CONTAINER_RUN[@]}" with-secrets "$secret" -- galata-datawatch "$venue"
        fi
        from_vault "capture-$venue" --only "$(password_var "datawatch-$venue")" -- \
            "$ROOT/target/release/galata-datawatch" "$venue" "${@:3}"
        ;;
    ledger)
        venue="${2:-}"
        [[ -n "$venue" ]] || refuse "usage: run-service.sh ledger <venue>"
        use_config
        names="$(python3 "$ROOT/scripts/lib/ledger_vars.py" "$GALATA_CONFIG" "$venue")" \
            || refuse "$GALATA_CONFIG declares no ledger account on $venue"
        only=()
        secrets=()
        while IFS= read -r name; do only+=(--only "$name"); secrets+=("$name"); done <<< "$names"
        if containerised "ledger:$venue"; then
            # Its record, its projection when one is declared, and the status
            # folder; never the archive or the tape, which are not its own.
            root_mounts ledger.root paths.status
            mounts=("${ROOT_MOUNTS[@]}")
            if python3 "$ROOT/scripts/lib/config_roots.py" "$GALATA_CONFIG" "$ROOT" ledger.tape >/dev/null 2>&1; then
                root_mounts ledger.tape
                mounts+=("${ROOT_MOUNTS[@]}")
            fi
            config_mount "$(container_name "ledger:$venue")"
            in_container "ledger:$venue" "$DATAWATCH_IMAGE" -i "${mounts[@]}" "${CONFIG_MOUNT[@]}"
            from_vault "ledger-$venue" "${only[@]}" -- \
                /bin/bash "$ROOT/scripts/lib/hand-secrets.sh" "${secrets[@]}" -- \
                "${CONTAINER_RUN[@]}" with-secrets "${secrets[@]}" -- galata-ledger "$venue"
        fi
        [[ -x "$ROOT/target/release/galata-ledger" ]] \
            || refuse "no galata-ledger — cargo build --release first"
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
        # The broker capture publishes to, read from the same [broker] url
        # (scripts/lib/broker_addr.py). Loopback only when the configuration
        # declares no broker at all, which is what it was before.
        if [[ -z "${GALATA_BROKER:-}" ]]; then
            GALATA_BROKER="$(python3 "$ROOT/scripts/lib/broker_addr.py" "$GALATA_CONFIG" 2>/dev/null)" \
                || GALATA_BROKER="127.0.0.1:4222"
        fi
        export GALATA_BROKER
        # Said under --check, on stderr: the address is in the environment,
        # not the command line, so `ready:` alone would not show it.
        if (( CHECK )); then echo "run-service: the tower connects to the broker at $GALATA_BROKER" >&2; fi
        from_vault tower --only "$(password_var reader)" -- "$bin"
        ;;
    vault)
        # Once a service here: `gv-server local`, on loopback, with its data
        # in ~/.local/share/galata-vault. The deployment's vault is now the
        # hosted one, which galata-vault runs in a container of its own and
        # publishes on this machine's loopback at the same port, so GV_SERVER
        # above still finds it. Said by name, because an old agent's plist
        # still asks for this.
        refuse "the vault is no longer a service of this deployment: start the hosted vault (galata-vault: deploy/apple-container/gv-container up), and remove the old agent with scripts/install-services.sh --uninstall vault"
        ;;
    flows)
        if containerised flows; then
            # The archive and the tape, which its tools maintain; its own
            # state; the configuration. No secret, no status folder, no
            # ledger, and no port: its interface can start any registered
            # flow, so it answers inside the container only
            # (`container exec galata-flows cereyan runs ls`).
            #
            # Five flows, not seven: the image leaves out the two that need
            # galata-research (deploy/container/Containerfile.flows), so they
            # stop running when the lane moves here.
            use_config
            root_mounts paths.archive paths.tape
            config_mount "$(container_name flows)"
            in_container flows "$FLOWS_IMAGE" "${ROOT_MOUNTS[@]}" "${CONFIG_MOUNT[@]}" -v "$FLOWS_STATE:/data"
            if (( ! CHECK )); then flows_state; fi
            start "${CONTAINER_RUN[@]}"
        fi
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
        refuse "usage: run-service.sh [--check] nats | capture <venue> | ledger <venue> | tower | flows"
        ;;
esac
