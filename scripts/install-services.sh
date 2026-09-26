#!/usr/bin/env bash
#
# Install — or remove — galata's long-running services as launchd agents.
#
#   install-services.sh [--uninstall] [service ...]
#   install-services.sh --status
#
#   services: vault  nats  capture:<venue>  ledger:<venue>  tower  flows
#             (default: all five below, vault first, with capture:hyperliquid;
#             a ledger is installed only when named, since it needs accounts
#             in the vault first)
#
# Legacy's shape (scripts/install-compact-job.sh), for long-running jobs:
# render the tracked template into ~/Library/LaunchAgents (untracked — it is
# nothing but this machine's paths), lint it, then bootout + bootstrap so an
# install REPLACES rather than stacks, and ask launchctl whether it loaded.
#
# **One process per service.** A hand-started instance of the same service —
# `nohup galata-datawatch hyperliquid`, say — is stopped first: two captures
# of one venue are two writers of one archive scope.
#
# macOS only. Elsewhere this is a systemd unit per service with the same
# ExecStart; run-service.sh does not care which manager runs it.

set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TOWER="${GALATA_TOWER_ROOT:-$(cd "$ROOT/.." && pwd)/galata-tower}"
TEMPLATE="$ROOT/deploy/launchd/com.galata.service.plist.in"
AGENTS="$HOME/Library/LaunchAgents"
DOMAIN="gui/$(id -u)"

refuse() { echo "install-services: REFUSED — $1" >&2; exit 1; }
[[ "$(uname)" == Darwin ]] || refuse "launchd is macOS; elsewhere write a systemd unit running scripts/run-service.sh"

# **A twin: the service's binary, running, and not launchd's.** Found by the
# executable's basename, plus the argument that picks the instance, and never
# by a regex over the whole command line. Those regexes drifted: they missed a
# debug-built tower that ran for a day beside the launchd one, the tower's own
# var/bin copy, and `galata-datawatch-vault <venue>`, which is a second writer
# of one archive scope (know-a-twin-by-its-binary). Matching the executable
# also cannot match the shell doing the matching. What launchd owns is the
# job's pid and its descendants: run-service.sh and galata-vault-exec exec, so
# the job's pid is the service itself.
#
#   twins_of <spec>    one "pid<TAB>command" line per twin
label_of() {
    case "$1" in
        capture:*) echo "com.galata.capture.${1#capture:}" ;;
        ledger:*) echo "com.galata.ledger.${1#ledger:}" ;;
        *) echo "com.galata.$1" ;;
    esac
}

owned_by_launchd() {  # the job's pid and every descendant, one per line
    local job
    job="$(launchctl print "$DOMAIN/$(label_of "$1")" 2>/dev/null | awk '$1 == "pid" && $2 == "=" {print $3; exit}')"
    [[ -n "$job" ]] || return 0
    ps -axo pid=,ppid= | awk -v root="$job" '
        { parent[$1] = $2 }
        END {
            owned[root] = 1; print root
            do { grew = 0
                 for (p in parent) if (!(p in owned) && (parent[p] in owned)) { owned[p] = 1; print p; grew = 1 }
            } while (grew)
        }'
}

twins_of() {
    local spec="$1" bins="" want=""
    case "$spec" in
        vault) bins="gv-server"; want="local" ;;
        nats) bins="nats-server"; want="$ROOT/config/nats-authorization.conf config/nats-authorization.conf" ;;
        capture:*) bins="galata-datawatch galata-datawatch-vault"; want="${spec#capture:}" ;;
        ledger:*) bins="galata-ledger galata-ledger-vault"; want="${spec#ledger:}" ;;
        tower) bins="galata-tower" ;;
        flows) ;;
    esac
    local owned
    owned=" $(owned_by_launchd "$spec" | tr '\n' ' ') "
    # argv[0]'s basename, not `comm`: the kernel keeps 16 characters of the
    # name (MAXCOMLEN), and `galata-datawatch-vault` is 22. `ww` so that no
    # command line is cut to the terminal's width.
    ps -axwwo pid=,args= | while read -r pid args; do
        [[ "$owned" == *" $pid "* || "$pid" == "$$" ]] && continue
        if [[ "$spec" == flows ]]; then
            # uv and python: no basename names the lane, so its arguments do,
            # and only a lane of THIS checkout (its path, or its working
            # directory) counts.
            [[ "$args" == *"cereyan serve"* ]] || continue
            if [[ "$args" != *"$ROOT"* ]]; then
                local cwd
                cwd="$(lsof -a -p "$pid" -d cwd -Fn 2>/dev/null | sed -n 's/^n//p')"
                [[ "$cwd" == "$ROOT"* ]] || continue
            fi
        else
            local argv0="${args%% *}"
            [[ " $bins " == *" ${argv0##*/} "* ]] || continue
            if [[ -n "$want" ]]; then
                # The instance argument, as a whole word: `hyperliquid`, not
                # a venue that merely contains it. `want` may list spellings.
                local found=0 word alt
                for word in ${args#"$argv0"}; do
                    for alt in $want; do [[ "$word" == "$alt" ]] && found=1; done
                done
                (( found )) || continue
            fi
        fi
        printf '%s\t%s\n' "$pid" "$args"
    done
}

# --status: every loaded com.galata.* agent, and whether this installer
# manages it. Added when legacy's com.galata.compact.testnet was found still
# loaded, exiting 127 on every run, reading as one of these in launchctl list
# because nothing listed what the deployment had loaded.
if [[ "${1:-}" == --status ]]; then
    printf '%-34s %-10s %s\n' LABEL "PID/EXIT" MANAGED
    launchctl list | awk '$3 ~ /^com\.galata\./ {print $1, $2, $3}' | sort -k3 | while read -r pid status label; do
        case "$label" in
            com.galata.vault|com.galata.nats|com.galata.tower|com.galata.flows|com.galata.capture.*|com.galata.ledger.*) managed=yes ;;
            *) managed="NO — not rendered by this installer" ;;
        esac
        if [[ "$pid" == "-" ]]; then state="exit $status"; else state="pid $pid"; fi
        printf '%-34s %-10s %s\n' "$label" "$state" "$managed"
    done

    # Restart: would each loaded service start again? run-service.sh --check
    # runs the start's own preconditions and starts nothing. A running service
    # hides a missing binary or a lapsed token until its next restart; the
    # tower ran a day with its installed copy missing.
    echo
    printf '%-34s %s\n' RESTART "WOULD IT START AGAIN"
    launchctl list | awk '$3 ~ /^com\.galata\./ {print $3}' | sort | while read -r label; do
        case "$label" in
            com.galata.capture.*) args=(capture "${label#com.galata.capture.}") ;;
            com.galata.ledger.*) args=(ledger "${label#com.galata.ledger.}") ;;
            com.galata.vault|com.galata.nats|com.galata.tower|com.galata.flows) args=("${label#com.galata.}") ;;
            *) continue ;;
        esac
        if said="$("$ROOT/scripts/run-service.sh" --check "${args[@]}" 2>&1)"; then
            printf '%-34s %s\n' "$label" "ready"
        else
            printf '%-34s %s\n' "$label" "NO — ${said#run-service: REFUSED — }"
        fi
    done

    # Twins: a managed service's binary running outside launchd. A debug
    # tower ran beside the launchd one for a day with nothing saying so.
    echo
    printf '%-34s %s\n' TWIN "PID  COMMAND"
    twin_count=0
    managed_specs=(vault nats tower flows)
    for plist in "$AGENTS"/com.galata.capture.*.plist "$AGENTS"/com.galata.ledger.*.plist; do
        [[ -f "$plist" ]] || continue
        l="$(basename "$plist" .plist)"
        case "$l" in
            com.galata.capture.*) managed_specs+=("capture:${l#com.galata.capture.}") ;;
            com.galata.ledger.*) managed_specs+=("ledger:${l#com.galata.ledger.}") ;;
        esac
    done
    for spec in "${managed_specs[@]}"; do
        while IFS=$'\t' read -r pid cmd; do
            [[ -n "$pid" ]] || continue
            printf '%-34s %s  %s\n' "$spec" "$pid" "${cmd:0:100}"
            twin_count=$((twin_count + 1))
        done < <(twins_of "$spec")
    done
    (( twin_count )) || echo "(none — every managed service runs only under launchd)"

    # The service tokens: a lapsed one is a service that cannot restart, and
    # nothing else says so before the day (warn-before-the-tokens-lapse). The
    # verdict — ok, WARN inside 30 days, EXPIRED — is galata-vault-exec's, so
    # the window is stated once.
    echo
    printf '%-34s %-10s %-5s %s\n' TOKEN EXPIRES DAYS STATE
    exec_bin="$ROOT/target/release/galata-vault-exec"
    for token in "$ROOT"/var/tokens/*.gvt; do
        [[ -f "$token" ]] || { echo "(no tokens in var/tokens — scripts/mint-service-tokens.sh)"; break; }
        name="$(basename "$token" .gvt)"
        if [[ ! -x "$exec_bin" ]]; then
            printf '%-34s %s\n' "$name" "unreadable: no galata-vault-exec — cargo build --release -p galata-datawatch-vault"
        elif line="$(GV_SERVER="${GV_SERVER:-http://127.0.0.1:8750}" GV_TOKEN_FILE="$token" "$exec_bin" --expiry 2>&1)"; then
            read -r _ date days state <<<"$line"
            printf '%-34s %-10s %-5s %s\n' "$name" "$date" "$days" "$state"
        else
            printf '%-34s %s\n' "$name" "unreadable: ${line#galata-vault-exec: }"
        fi
    done
    exit 0
fi

UNINSTALL=0
if [[ "${1:-}" == --uninstall ]]; then UNINSTALL=1; shift; fi
services=("$@")
(( ${#services[@]} )) || services=(vault nats capture:hyperliquid tower flows)


for spec in "${services[@]}"; do
    case "$spec" in
        vault|nats|tower|flows) service="$spec"; arg=""; label="com.galata.$spec" ;;
        capture:*) service=capture; arg="${spec#capture:}"; label="com.galata.capture.$arg"
                   [[ "$arg" =~ ^[a-z0-9-]+$ ]] || refuse "venue must be a token, got '$arg'" ;;
        ledger:*) service=ledger; arg="${spec#ledger:}"; label="com.galata.ledger.$arg"
                  [[ "$arg" =~ ^[a-z0-9-]+$ ]] || refuse "venue must be a token, got '$arg'" ;;
        *) refuse "unknown service '$spec' (vault, nats, capture:<venue>, ledger:<venue>, tower, flows)" ;;
    esac
    plist="$AGENTS/$label.plist"

    if (( UNINSTALL )); then
        launchctl bootout "$DOMAIN/$label" 2>/dev/null && echo "unloaded $label" || echo "$label was not loaded"
        rm -f "$plist"
        continue
    fi

    [[ -f "$TEMPLATE" ]] || refuse "no template at $TEMPLATE"
    case "$service" in
        capture) [[ -x "$ROOT/target/release/galata-datawatch" ]] || refuse "no release binary — cargo build --release first" ;;
        ledger) [[ -x "$ROOT/target/release/galata-ledger" ]] || refuse "no galata-ledger — cargo build --release first" ;;
        flows) [[ -x "$ROOT/target/release/galata-compact" ]] || refuse "the flows run release binaries — cargo build --release first" ;;
        tower) [[ -x "$TOWER/target/release/galata-tower" ]] \
                   || refuse "no tower release binary at $TOWER — cargo build --release there, or set GALATA_TOWER_ROOT" ;;
    esac

    # The four that hold a secret read it through their own vault token.
    # (Not `;;&` in the case above: macOS's bash is 3.2.)
    if [[ "$service" == capture || "$service" == ledger || "$service" == nats || "$service" == tower ]]; then
        [[ -x "$ROOT/target/release/galata-vault-exec" ]] \
            || refuse "no galata-vault-exec — cargo build --release -p galata-datawatch-vault first"
    fi

    mkdir -p "$AGENTS" "$ROOT/var/logs"
    log="$ROOT/var/logs/${label#com.galata.}.log"
    sed -e "s|@LABEL@|$label|g" -e "s|@REPO@|$ROOT|g" -e "s|@SERVICE@|$service|g" \
        -e "s|@ARG@|$arg|g" -e "s|@LOG@|$log|g" "$TEMPLATE" > "$plist"
    # An empty @ARG@ would hand the service an empty argument; drop that line.
    [[ -z "$arg" ]] && sed -i '' '/<string><\/string>/d' "$plist"
    plutil -lint "$plist" >/dev/null || refuse "rendered plist does not parse: $plist"

    # Replace, never stack — and never beside a hand-started twin.
    launchctl bootout "$DOMAIN/$label" 2>/dev/null || true
    twin_pids() { twins_of "$spec" | cut -f1 | tr '\n' ' '; }
    pids="$(twin_pids)"
    if [[ -n "${pids// }" ]]; then
        echo "stopping a hand-started $spec (pid ${pids% })"
        # SIGINT first: a capture built before SIGTERM was a clean stop still
        # stops cleanly on it. Then SIGTERM, because a process started with
        # `nohup … &` from a non-interactive shell inherits SIGINT IGNORED —
        # found when the tower, which installs no handler, would not stop.
        kill -INT $pids 2>/dev/null
        for _ in $(seq 1 10); do [[ -z "$(twin_pids | tr -d ' ')" ]] && break; sleep 1; done
        if [[ -n "$(twin_pids | tr -d ' ')" ]]; then
            kill -TERM $(twin_pids) 2>/dev/null
            for _ in $(seq 1 20); do [[ -z "$(twin_pids | tr -d ' ')" ]] && break; sleep 1; done
        fi
        [[ -z "$(twin_pids | tr -d ' ')" ]] || refuse "a hand-started $spec would not stop"
    fi
    # The tower runs a copy, taken here and only here: installing is the
    # deploy, and building the tower checkout is not. Copied while stopped,
    # through a rename, so no reader ever sees half a binary.
    if [[ "$service" == tower ]]; then
        mkdir -p "$ROOT/var/bin"
        cp "$TOWER/target/release/galata-tower" "$ROOT/var/bin/.galata-tower.new"
        mv -f "$ROOT/var/bin/.galata-tower.new" "$ROOT/var/bin/galata-tower"
        at="$(git -C "$TOWER" describe --always --dirty 2>/dev/null || echo unknown)"
        echo "$at" > "$ROOT/var/bin/galata-tower.source"
        echo "installed the tower, its checkout at $at"
    fi
    launchctl bootstrap "$DOMAIN" "$plist" || refuse "launchctl bootstrap failed for $plist"
    launchctl print "$DOMAIN/$label" >/dev/null 2>&1 || refuse "$label did not load"
    echo "loaded $label  (log: $log)"
done
