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
TEMPLATE="$ROOT/deploy/launchd/com.galata.service.plist.in"
AGENTS="$HOME/Library/LaunchAgents"
DOMAIN="gui/$(id -u)"

refuse() { echo "install-services: REFUSED — $1" >&2; exit 1; }
[[ "$(uname)" == Darwin ]] || refuse "launchd is macOS; elsewhere write a systemd unit running scripts/run-service.sh"

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
    exit 0
fi

UNINSTALL=0
if [[ "${1:-}" == --uninstall ]]; then UNINSTALL=1; shift; fi
services=("$@")
(( ${#services[@]} )) || services=(vault nats capture:hyperliquid tower flows)

# What a hand-started instance of each looks like, to stop it first.
pattern_of() {
    case "$1" in
        nats) echo "nats-server -c $ROOT/config/nats-authorization.conf|nats-server -c config/nats-authorization.conf" ;;
        capture:*) echo "galata-datawatch ${1#capture:}\$" ;;
        ledger:*) echo "galata-ledger ${1#ledger:}\$" ;;
        tower) echo "target/release/galata-tower\$" ;;
        flows) echo "cereyan serve py|cereyan serve $ROOT/py|cereyan serve \\. " ;;
        vault) echo "gv-server local" ;;
    esac
}

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
    pattern="$(pattern_of "$spec")"
    if pids="$(pgrep -f "$pattern")" && [[ -n "$pids" ]]; then
        echo "stopping a hand-started $spec (pid $(echo $pids | tr '\n' ' '))"
        # SIGINT first: a capture built before SIGTERM was a clean stop still
        # stops cleanly on it. Then SIGTERM, because a process started with
        # `nohup … &` from a non-interactive shell inherits SIGINT IGNORED —
        # found when the tower, which installs no handler, would not stop.
        kill -INT $pids 2>/dev/null
        for _ in $(seq 1 10); do pgrep -f "$pattern" >/dev/null || break; sleep 1; done
        if pgrep -f "$pattern" >/dev/null; then
            kill -TERM $(pgrep -f "$pattern") 2>/dev/null
            for _ in $(seq 1 20); do pgrep -f "$pattern" >/dev/null || break; sleep 1; done
        fi
        pgrep -f "$pattern" >/dev/null && refuse "a hand-started $spec would not stop"
    fi
    launchctl bootstrap "$DOMAIN" "$plist" || refuse "launchctl bootstrap failed for $plist"
    launchctl print "$DOMAIN/$label" >/dev/null 2>&1 || refuse "$label did not load"
    echo "loaded $label  (log: $log)"
done
