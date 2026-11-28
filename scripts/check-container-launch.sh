#!/usr/bin/env bash
#
# A SERVICE IN A CONTAINER STARTS WITH STATED LIMITS, ON A TAILNET THAT IS UP,
# AND IS HANDED NOTHING THAT IS NOT ITS OWN.
#
# Each of these fails silently if it fails at all:
#
#   1. **The preconditions refuse.** A container tool that is missing or
#      stopped, an image that was never built, a limit that was never stated
#      and a tailnet that is down are each refused by name. A start that
#      skipped one would look like a start: a container with no stated limit
#      runs, at 4 CPUs and 1 GiB; a broker published while the tailnet is down
#      would have to be published somewhere else, and that somewhere is the
#      LAN. A service with no line in var/containers.conf is refused, naming
#      the service and the missing limit: there is no native start left to
#      fall back to. Run here against stubs of `container` and of Tailscale's
#      command line, so nothing real is asked.
#
#   2. **No launch line hands a container a secret's file or a secret's
#      value.** The launcher reads a service's secrets on the host, with that
#      service's token, and the container inherits them by name. A mount of
#      var/ itself, of var/tokens or of var/broker.env would put every
#      service's token in one container; `-e NAME=value` for a password would
#      put the value in an argument list, where `ps` shows it.
#
#   3. **A port is published on the tailnet address and nowhere else.**
#      `container run -p 4222:4222`, with no address, binds every interface of
#      the Mac. The broker would then answer on the LAN.
#
#   4. **var/ stays out of the build context.** .dockerignore is the one line
#      between an image and this machine's tokens.
#
# What it cannot check: that Apple's `container` honours a limit it is given,
# or that a published port is unreachable from the LAN. Those are measured,
# in design/measured.md.
#
# Usage: check-container-launch.sh [root]

set -euo pipefail
ROOT="${1:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
LIB="$ROOT/scripts/lib/container-tools.sh"
me="$(basename "$0")"
fails=0
say() { echo "$me: $1" >&2; fails=$((fails + 1)); }

[[ -f "$LIB" ]] || { echo "$me: no $LIB" >&2; exit 2; }
# shellcheck source=scripts/lib/container-tools.sh
source "$LIB"
# password_var, for the broker's identities: the names, as the launcher makes
# them.
# shellcheck source=scripts/lib/vault-tools.sh
source "$ROOT/scripts/lib/vault-tools.sh"

stubs="$(mktemp -d)"
trap 'rm -rf "$stubs"' EXIT

# said <words> -- <function> [args]: the function refuses, and its reason
# holds the words. A refusal that names the wrong thing sends an operator to
# fix the wrong thing.
refuses_with() {
    local want="$1"; shift
    local out
    if out="$("$@" 2>&1)"; then return 1; fi
    [[ "$out" == *"$want"* ]]
}

# --- 1. the preconditions --------------------------------------------------
# A `container` that answers what it is told to: `system status` by its exit
# code, `image inspect <name>` for the one image it holds.
container_stub() {  # container_stub <dir> <running: 0|1> <the images it holds>
    mkdir -p "$1"
    cat > "$1/container" <<STUB
#!/bin/sh
case "\$1 \$2" in
    "system status") exit $2 ;;
    "image inspect") case " $3 " in *" \$3 "*) exit 0 ;; esac; exit 1 ;;
esac
exit 0
STUB
    chmod 755 "$1/container"
}
images="galata-datawatch:latest galata-nats:latest galata-flows:latest galata-markets:latest"
container_stub "$stubs/up" 0 "$images"
container_stub "$stubs/down" 1 "$images"

GALATA_CONTAINER_BIN="$stubs/up/container" container_tool >/dev/null 2>&1 \
    || say "a container tool that exists is refused"
GALATA_CONTAINER_BIN="$stubs/absent/container" refuses_with "install Apple container" container_tool \
    || say "a missing container tool is accepted, or refused without saying how to install it"
container_running "$stubs/up/container" 2>/dev/null \
    || say "a container tool whose services are up is refused"
refuses_with "container system start" container_running "$stubs/down/container" \
    || say "a container tool whose services are stopped is accepted, or refused without saying how to start them"
container_image "$stubs/up/container" galata-datawatch:latest 2>/dev/null \
    || say "an image the machine holds is refused"
refuses_with "scripts/build-images.sh" container_image "$stubs/up/container" galata-absent:latest \
    || say "an image the machine does not hold is accepted, or refused without saying how to build it"

# The limits, from a file of this deployment's own.
conf="$stubs/containers.conf"
cat > "$conf" <<'CONF'
# service              cpus  memory
capture:hyperliquid    2     1G
nats                   1     256M
#flows                 2     4G
ledger:hyperliquid     2
half                   0     1G
wordy                  2     1GB
CONF
limits() { GALATA_CONTAINERS_CONF="$conf" container_limits "$1"; }

[[ "$(limits capture:hyperliquid 2>/dev/null)" == "2 1G" ]] \
    || say "capture:hyperliquid's stated limits are not read back as '2 1G'"
[[ "$(limits nats 2>/dev/null)" == "1 256M" ]] \
    || say "nats's stated limits are not read back as '1 256M'"
refuses_with "no CPU or memory limit for capture:hyper," limits capture:hyper \
    || say "capture:hyper is given capture:hyperliquid's limits: a service is matched by its whole name, not a prefix"
refuses_with "no CPU or memory limit for flows," limits flows \
    || say "a commented-out service is given limits, or refused without naming the service and the missing limits"
refuses_with "no CPU or memory limit for markets," limits markets \
    || say "a service with no line is given limits, or refused without naming the service and the missing limits"
refuses_with "no memory limit for ledger:hyperliquid" limits ledger:hyperliquid \
    || say "a line with CPUs and no memory is accepted: half a limit is a default for the other half"
refuses_with "not a whole number" limits half \
    || say "zero CPUs is accepted"
refuses_with "512M or 2G" limits wordy \
    || say "a memory limit written '1GB' is accepted; Apple's container takes a number and one letter"
GALATA_CONTAINERS_CONF="$stubs/absent.conf" refuses_with "no CPU or memory limit is stated for nats" container_limits nats \
    || say "limits are read from a file that does not exist, or refused without naming the service"

[[ "$(container_name capture:hyperliquid)" == galata-capture-hyperliquid ]] \
    || say "capture:hyperliquid's container is not named galata-capture-hyperliquid"
[[ "$(container_name ledger:hyperliquid)" == galata-ledger-hyperliquid ]] \
    || say "ledger:hyperliquid's container is not named galata-ledger-hyperliquid"
[[ "$(container_name nats)" == galata-nats ]] || say "the broker's container is not named galata-nats"

# The tailnet: Tailscale's command line, stubbed in each state it can be in.
tailscale_stub() {  # tailscale_stub <dir> <BackendState> <address, or empty>
    mkdir -p "$1"
    cat > "$1/tailscale" <<STUB
#!/bin/sh
case "\$1" in
    status) printf '{\n  "Version": "1.104.7",\n  "BackendState": "%s",\n  "Self": {}\n}\n' "$2" ;;
    ip) [ -n "$3" ] && echo "$3" ;;
esac
exit 0
STUB
    chmod 755 "$1/tailscale"
}
tailscale_stub "$stubs/ts-up" Running 100.64.0.7
tailscale_stub "$stubs/ts-stopped" Stopped 100.64.0.7
tailscale_stub "$stubs/ts-login" NeedsLogin ""
tailscale_stub "$stubs/ts-noaddr" Running ""

[[ "$(GALATA_TAILSCALE_BIN="$stubs/ts-up/tailscale" tailnet_address 2>/dev/null)" == 100.64.0.7 ]] \
    || say "a tailnet that is up does not give its address"
GALATA_TAILSCALE_BIN="$stubs/ts-stopped/tailscale" refuses_with "the tailnet is down" tailnet_address \
    || say "a stopped Tailscale gives an address: its last one is not one to publish on"
GALATA_TAILSCALE_BIN="$stubs/ts-login/tailscale" refuses_with "the tailnet is down" tailnet_address \
    || say "a logged-out Tailscale gives an address"
GALATA_TAILSCALE_BIN="$stubs/ts-noaddr/tailscale" refuses_with "the tailnet is down" tailnet_address \
    || say "a running Tailscale with no address gives one"
GALATA_TAILSCALE_BIN="$stubs/ts-stopped/tailscale" refuses_with "another address" tailnet_address \
    || say "the refusal does not say that nothing is published elsewhere"

# --- 2. nothing that is not its own ----------------------------------------
# Read from the launcher's own text. This file and test-guards.sh name the
# violations in order to describe and plant them; neither starts anything.
LAUNCHER="$ROOT/scripts/run-service.sh"
[[ -f "$LAUNCHER" ]] || { echo "$me: no $LAUNCHER" >&2; exit 2; }
# The launcher with its comments removed: a comment may say `-e NAME` in order
# to say why it is not done.
code="$(sed -E 's/^[[:space:]]*#.*$//' "$LAUNCHER")"

# `-e` hands a container a variable. Written `-e KEY=value` with a literal
# KEY, it states one; written `-e NAME` or `-e "$name"`, it inherits one from
# the launcher's environment — which is where `gv run` has just put the
# secrets, and Apple's container writes what it inherits into its own state.
while IFS= read -r hit; do
    [[ -n "$hit" ]] || continue
    word="$(printf '%s' "$hit" | sed -E 's/^-e[[:space:]]+//; s/^"//')"
    key="${word%%=*}"
    if [[ "$word" != *=* || ! "$key" =~ ^[A-Z][A-Z0-9_]*$ ]]; then
        say "the launcher passes '$hit': -e with no literal KEY= inherits from the launcher's environment, which is how a secret would reach the container tool's state — secrets go over standard input (lib/hand-secrets.sh)"
    elif [[ "$key" =~ (PASSWORD|SECRET|TOKEN|FINGERPRINT|ADDRESS|^GV_) ]]; then
        say "the launcher passes '$hit': $key reads as a secret, and -e writes its value into the container tool's state"
    fi
done < <(printf '%s\n' "$code" | grep -oE '(^|[[:space:](])-e[[:space:]]+"?[^[:space:]")]+' | sed -E 's/^[[:space:](]+//' || true)

# A mount of var/ itself, or of anything in it that holds a secret, gives one
# container every service's token.
while IFS= read -r hit; do
    [[ -n "$hit" ]] || continue
    say "the launcher mounts '$hit': that folder holds tokens, and a container is given its own roots and nothing else"
done < <(printf '%s\n' "$code" | grep -oE '(-v[[:space:]]+"?|source=)[^[:space:]",]*(var/tokens|broker\.env|\.gvt|\.gvkit|\$ROOT/var[":,]|\$ROOT/var$|\$HOME[":,/]?)[^[:space:]",]*' || true)

# --- 3. published on the tailnet address ------------------------------------
# `--publish 4222:4222` binds every interface of the Mac. The address comes
# from tailnet_address, and a `--publish` word must begin with it.
#
# The long form only, in a line that starts a container: `-p` is also
# `mkdir -p`, `cargo build -p` and nats-server's own port flag, all of which
# this file would otherwise have to tell apart from a published port. So a
# container line that says `-p` is refused for saying it.
while IFS= read -r hit; do
    [[ -n "$hit" ]] || continue
    word="$(printf '%s' "$hit" | sed -E 's/^--publish[[:space:]]+//; s/^"//')"
    [[ "$word" == '$tailnet:'* || "$word" == '${tailnet}:'* ]] \
        || say "the launcher publishes '$hit': a port is published on the tailnet address (\"\$tailnet:port:port\") and on no other, or the service answers on the LAN"
done < <(printf '%s\n' "$code" | grep -oE '(^|[[:space:](])--publish[[:space:]]+"?[^[:space:]")]+' | sed -E 's/^[[:space:](]+//' || true)
# Each call of in_container, with its continuation lines joined.
while IFS= read -r line; do
    [[ -n "$line" ]] || continue
    say "a line that starts a container says -p: write --publish \"\$tailnet:port:port\", so that a published port can be told from every other -p — $(printf '%s' "$line" | cut -c1-100)"
done < <(printf '%s\n' "$code" | sed -e ':a' -e '/\\$/N' -e 's/\\\n//' -e 'ta' \
            | grep -E '(^|[[:space:];])in_container[[:space:]]' | grep -E '[[:space:]]-p[[:space:]]' || true)

# --- 5. the line the launcher would start -----------------------------------
# The launcher itself, run under --check in a root of its own: its scripts and
# the committed configuration, a token file that is only a file, and stubs for
# the vault tools and the token's judge. --check starts nothing and reads no
# secret, so what it prints is the whole line, and the line is what is checked.
scratch="$stubs/root"
mkdir -p "$scratch/scripts" "$scratch/var/tokens" "$scratch/var/archive" "$scratch/var/status" \
         "$scratch/target/release" "$scratch/vault-bin"
cp -R "$ROOT/scripts/lib" "$scratch/scripts/lib"
cp "$ROOT/scripts/run-service.sh" "$scratch/scripts/run-service.sh"
cp -R "$ROOT/config" "$scratch/config"
printf 'not a token\n' > "$scratch/var/tokens/capture-hyperliquid.gvt"
chmod 600 "$scratch/var/tokens/capture-hyperliquid.gvt"
printf '#!/bin/sh\necho "1830000000 2027-12-28 400 ok"\n' > "$scratch/target/release/galata-token-expiry"
# A gv above any floor: the floor has its own guard (check-service-secrets.sh).
printf '#!/bin/sh\necho "gv 99.0.0"\n' > "$scratch/vault-bin/gv"
chmod 755 "$scratch/target/release/galata-token-expiry" "$scratch/vault-bin/gv"
printf 'capture:hyperliquid    2     1G\n' > "$scratch/containers.conf"

would_start() {  # the launcher's verdict for capture, in the scratch root
    GALATA_VAULT_BIN="$scratch/vault-bin" GALATA_CONTAINER_BIN="$stubs/up/container" \
    GALATA_CONTAINERS_CONF="$scratch/containers.conf" \
        /bin/bash "$scratch/scripts/run-service.sh" --check capture hyperliquid 2>&1
}
secret=GALATA_BROKER_PASSWORD_DATAWATCH_HYPERLIQUID
line="$(would_start || true)"
if [[ "$line" != ready:* ]]; then
    say "capture, listed in containers.conf, does not check as ready in a root with everything in place: $line"
else
    has() { [[ "$line" == *"$1"* ]] || say "capture's container line lacks '$1': $2"; }
    has "hand-secrets.sh $secret -- " "its secret is handed over standard input, by name"
    has " with-secrets $secret -- galata-datawatch hyperliquid" "the image's entrypoint receives the name, then becomes capture"
    has " run --rm --name galata-capture-hyperliquid --cpus 2 --memory 1G " "it runs under its own name with the limits its line states"
    has " -i " "standard input is how the secret arrives, so the container is given one"
    has " -v $scratch/var/archive:/app/var/archive " "it is given the archive, where the configuration puts it"
    has " -v $scratch/var/status:/app/var/status " "it is given the status folder"
    has "source=$scratch/var/run/galata-capture-hyperliquid,target=/app/config,readonly" "its configuration is a copy in a folder of its own, read-only"
    has " -e GALATA_CONFIG=/app/config/datawatch.toml " "it is told where that copy is"
    [[ "$line" == *"/var/tape"* ]] && say "capture's container is given the tape, which is not its own root"
    [[ "$line" == *"var/tokens"* || "$line" == *".gvt"* ]] && say "capture's container line names a token"
    [[ "$line" =~ -e\ +\"?$secret ]] && say "capture's secret is passed with -e"
fi
# --check starts nothing and writes nothing: the copy of the configuration is
# taken at a real start.
[[ -e "$scratch/var/run" ]] && say "--check wrote $scratch/var/run: a check starts nothing and writes nothing"

# A root the configuration names and the machine does not have is refused,
# never created: an empty archive made by a typo looks like a capture that
# has just started.
rmdir "$scratch/var/archive"
line="$(would_start || true)"
[[ "$line" == *"REFUSED"*"no folder at $scratch/var/archive"* ]] \
    || say "capture is not refused for an archive folder that does not exist: $line"
[[ -d "$scratch/var/archive" ]] && say "the launcher created the missing archive folder"
mkdir -p "$scratch/var/archive"

# No line: refused, naming the service and the missing limit. There is no
# native start to fall back to, so nothing but the refusal may come out.
: > "$scratch/containers.conf"
line="$(would_start || true)"
[[ "$line" == *"REFUSED"*"no CPU or memory limit for capture:hyperliquid"* ]] \
    || say "capture, with no line in containers.conf, is not refused by its name and the missing limit: $line"
[[ "$line" == *"ready:"* || "$line" == *"target/release/galata-datawatch"* ]] \
    && say "capture, with no line in containers.conf, would start some other way: $line"

# The ledger, which holds more than one secret and has roots of its own. The
# committed configuration declares no ledger, so this root's overlay declares
# one: the names only, as the configuration holds. Every start composes the
# document from the committed file and the overlay (compose-deployment-config).
cat > "$scratch/var/datawatch.overlay.toml" <<'LEDGER'

[ledger]
root = "var/ledger"
tape = "var/ledger-tape"
fingerprint_key_var = "GALATA_LEDGER_FINGERPRINT_KEY"

[ledger.account.main]
venue = "hyperliquid"
address_var = "GALATA_LEDGER_ADDRESS_MAIN"
LEDGER
mkdir -p "$scratch/var/ledger" "$scratch/var/ledger-tape"
cp "$scratch/var/tokens/capture-hyperliquid.gvt" "$scratch/var/tokens/ledger-hyperliquid.gvt"
chmod 600 "$scratch/var/tokens/ledger-hyperliquid.gvt"
printf 'ledger:hyperliquid    2     2G\n' > "$scratch/containers.conf"
line="$(GALATA_VAULT_BIN="$scratch/vault-bin" GALATA_CONTAINER_BIN="$stubs/up/container" \
        GALATA_CONTAINERS_CONF="$scratch/containers.conf" \
        /bin/bash "$scratch/scripts/run-service.sh" --check ledger hyperliquid 2>&1 || true)"
both="GALATA_LEDGER_ADDRESS_MAIN GALATA_LEDGER_FINGERPRINT_KEY"
if [[ "$line" != ready:* ]]; then
    say "the ledger, listed in containers.conf, does not check as ready in a root with everything in place: $line"
else
    has() { [[ "$line" == *"$1"* ]] || say "the ledger's container line lacks '$1': $2"; }
    has "hand-secrets.sh $both -- " "every secret it holds is handed over standard input, by name"
    has " with-secrets $both -- galata-ledger hyperliquid" "the image's entrypoint receives both names, then becomes the ledger"
    has " run --rm --name galata-ledger-hyperliquid --cpus 2 --memory 2G " "it runs under its own name with the limits its line states"
    has " -v $scratch/var/ledger:/app/var/ledger " "it is given its record"
    has " -v $scratch/var/ledger-tape:/app/var/ledger-tape " "it is given its projection, when one is declared"
    has " -v $scratch/var/status:/app/var/status " "it is given the status folder"
    [[ "$line" == *"/var/archive"* || "$line" == *"/var/tape:"* ]] \
        && say "the ledger's container is given the archive or the market tape, which are not its own roots"
    [[ "$line" =~ -e\ +\"?GALATA_LEDGER_ ]] && say "one of the ledger's secrets is passed with -e"
fi
# The document is composed at a start, never by a check
# (compose-deployment-config). The start below reaches only stubs: the vault
# tool prints its version and exits, so nothing is started.
[[ -e "$scratch/var/datawatch.local.toml" ]] \
    && say "--check wrote var/datawatch.local.toml: a check composes in a file of its own"
GALATA_VAULT_BIN="$scratch/vault-bin" GALATA_CONTAINER_BIN="$stubs/up/container" \
GALATA_CONTAINERS_CONF="$scratch/containers.conf" \
    /bin/bash "$scratch/scripts/run-service.sh" ledger hyperliquid >/dev/null 2>&1 || true
head -1 "$scratch/var/datawatch.local.toml" 2>/dev/null | grep -q '^# GENERATED' \
    || say "a start with an overlay does not compose var/datawatch.local.toml under its generated header"
head -1 "$scratch/var/run/galata-ledger-hyperliquid/datawatch.toml" 2>/dev/null | grep -q '^# GENERATED' \
    || say "the ledger's copy of the configuration is not the composed document"
# A hand edit to the composed document refuses the start and is kept.
printf '\n# an operator was here\n' >> "$scratch/var/datawatch.local.toml"
edited="$(shasum "$scratch/var/datawatch.local.toml")"
line="$(GALATA_VAULT_BIN="$scratch/vault-bin" GALATA_CONTAINER_BIN="$stubs/up/container" \
        GALATA_CONTAINERS_CONF="$scratch/containers.conf" \
        /bin/bash "$scratch/scripts/run-service.sh" --check ledger hyperliquid 2>&1 || true)"
[[ "$line" == *"REFUSED"*"edited by hand"* ]] \
    || say "a start over a hand-edited configuration is not refused as one: $line"
[[ "$(shasum "$scratch/var/datawatch.local.toml")" == "$edited" ]] || say "a hand edit was overwritten by a start"
rm -f "$scratch/var/datawatch.local.toml"
: > "$scratch/containers.conf"
line="$(GALATA_VAULT_BIN="$scratch/vault-bin" GALATA_CONTAINER_BIN="$stubs/up/container" \
        GALATA_CONTAINERS_CONF="$scratch/containers.conf" \
        /bin/bash "$scratch/scripts/run-service.sh" --check ledger hyperliquid 2>&1 || true)"
[[ "$line" == *"REFUSED"*"no CPU or memory limit for ledger:hyperliquid"* ]] \
    || say "the ledger, with no line in containers.conf, is not refused by its name and the missing limit: $line"

# The broker: reached by the host's tailnet name, so published — on the
# tailnet address when the tailnet is up, and not started at all when it is
# down.
cp "$scratch/var/tokens/capture-hyperliquid.gvt" "$scratch/var/tokens/nats.gvt"
chmod 600 "$scratch/var/tokens/nats.gvt"
printf 'nats    1     256M\n' > "$scratch/containers.conf"
broker_would_start() {  # broker_would_start <tailscale stub>
    GALATA_VAULT_BIN="$scratch/vault-bin" GALATA_CONTAINER_BIN="$stubs/up/container" \
    GALATA_CONTAINERS_CONF="$scratch/containers.conf" GALATA_TAILSCALE_BIN="$1" \
        /bin/bash "$scratch/scripts/run-service.sh" --check nats 2>&1
}
# Every identity the generated authorization file names, in its order: the
# server refuses to start on a password variable it is not handed, so a
# launcher that hands over fewer is a broker that will not restart.
passwords="$(awk '$1 == "user:" { print $2 }' "$scratch/config/nats-authorization.conf" \
    | while read -r id; do password_var "$id"; echo; done | paste -sd ' ' -)"
[[ "$(wc -w <<< "$passwords")" -ge 3 ]] || say "the generated authorization file names fewer than three identities: $passwords"
line="$(broker_would_start "$stubs/ts-up/tailscale" || true)"
if [[ "$line" != ready:* ]]; then
    say "the broker, listed in containers.conf, does not check as ready with the tailnet up: $line"
else
    has() { [[ "$line" == *"$1"* ]] || say "the broker's container line lacks '$1': $2"; }
    has " --publish 100.64.0.7:4222:4222 " "its port is published on the tailnet address"
    has " run --rm --name galata-nats --cpus 1 --memory 256M " "it runs under its own name with the limits its line states"
    has "source=$scratch/config,target=/etc/galata,readonly" "it is given its configuration and the generated authorization file, read-only"
    has " -v galata-nats-store:/data " "its streams are stored on a volume of its own"
    has "hand-secrets.sh $passwords -- " "every identity's password is handed over standard input, by name"
    has " with-secrets $passwords -- galata-bus -- nats-server -c /etc/galata/nats-server.conf -a 0.0.0.0 -p 4222" \
        "the entrypoint receives every identity's name, then galata-bus applies the streams and becomes the server, listening where the published port arrives"
    [[ "$line" == *"127.0.0.1"* ]] && say "the broker's container line names loopback, which no container reaches"
    # Its store is the volume and nothing of var/: the container's own part of
    # the line, from `run`, names no path under the root's var/ — no root of
    # the record, no token, no store. The root's, not any `/var/`: on macOS the
    # scratch root itself is under /var/folders.
    container_part="${line#*" run --rm "}"
    [[ "$container_part" == *"$scratch/var"* ]] \
        && say "the broker's container is given something of var/: $container_part"
    [[ "$line" =~ -e\ +\"?GALATA_BROKER_PASSWORD ]] && say "one of the broker's passwords is passed with -e"
fi
line="$(broker_would_start "$stubs/ts-stopped/tailscale" || true)"
[[ "$line" == *"REFUSED"*"the tailnet is down"* ]] \
    || say "the broker is not refused while the tailnet is down: $line"
[[ "$line" == *"--publish"* ]] && say "the broker would be published while the tailnet is down"
: > "$scratch/containers.conf"
line="$(broker_would_start "$stubs/ts-up/tailscale" || true)"
[[ "$line" == *"REFUSED"*"no CPU or memory limit for nats"* ]] \
    || say "the broker, with no line in containers.conf, is not refused by its name and the missing limit: $line"
[[ "$line" == *"127.0.0.1"* ]] && say "the broker, with no line in containers.conf, would start on loopback: $line"

# The jobs lane: the archive and the tape, a volume of its own, and nothing
# handed over, because it holds no secret.
mkdir -p "$scratch/var/tape"
printf 'flows    2     2G\n' > "$scratch/containers.conf"
line="$(GALATA_CONTAINER_BIN="$stubs/up/container" GALATA_CONTAINERS_CONF="$scratch/containers.conf" \
        /bin/bash "$scratch/scripts/run-service.sh" --check flows 2>&1 || true)"
if [[ "$line" != ready:* ]]; then
    say "the jobs lane, listed in containers.conf, does not check as ready in a root with everything in place: $line"
else
    has() { [[ "$line" == *"$1"* ]] || say "the lane's container line lacks '$1': $2"; }
    has " run --rm --name galata-flows --cpus 2 --memory 2G " "it runs under its own name with the limits its line states"
    has " -v $scratch/var/archive:/app/var/archive " "it is given the archive, which its tools compact"
    has " -v $scratch/var/tape:/app/var/tape " "it is given the tape, which its tools project"
    has " -v galata-flows-state:/data " "its run store is a volume of its own"
    has "source=$scratch/var/run/galata-flows,target=/app/config,readonly" "its configuration is a copy in a folder of its own, read-only"
    [[ "$line" == *"galata-flows:latest" ]] || say "the lane's container line does not end at its image: the image's own command is what serves the flows"
    [[ "$line" == *"--publish"* ]] && say "the lane publishes a port: its interface can start any registered flow"
    [[ "$line" == *"hand-secrets"* || "$line" == *"with-secrets"* || "$line" == *" -i "* ]] \
        && say "the lane is handed something over standard input: it holds no secret"
    has " -v $scratch/var/status:/app/var/status " "it is given the status folder, where the record's judge writes its report"
    [[ "$line" == *"/var/ledger"* ]] \
        && say "the lane's container is given the ledger's record, which is not its own root"
fi
: > "$scratch/containers.conf"
line="$(GALATA_CONTAINER_BIN="$stubs/up/container" GALATA_CONTAINERS_CONF="$scratch/containers.conf" \
        /bin/bash "$scratch/scripts/run-service.sh" --check flows 2>&1 || true)"
[[ "$line" == *"REFUSED"*"no CPU or memory limit for flows"* ]] \
    || say "the jobs lane, with no line in containers.conf, is not refused by its name and the missing limit: $line"
[[ "$line" == *"cereyan serve"* ]] && say "the jobs lane, with no line in containers.conf, would serve natively: $line"

# galata-markets (add-galata-markets): the archive and the tape READ-ONLY,
# which it serves and must not be able to change; its port on the tailnet
# address; its bus password and its clients' tokens over standard input.
cp "$scratch/var/tokens/capture-hyperliquid.gvt" "$scratch/var/tokens/markets.gvt"
chmod 600 "$scratch/var/tokens/markets.gvt"
printf 'markets    2     2G\n' > "$scratch/containers.conf"
markets_names="$(markets_secrets | paste -sd ' ' -)"
[[ "$markets_names" == GALATA_BROKER_PASSWORD_MARKETS\ GALATA_MARKETS_TOKEN_* ]] \
    || say "galata-markets' secrets are not its bus password and then its clients' tokens: $markets_names"
line="$(GALATA_VAULT_BIN="$scratch/vault-bin" GALATA_CONTAINER_BIN="$stubs/up/container" \
        GALATA_CONTAINERS_CONF="$scratch/containers.conf" GALATA_TAILSCALE_BIN="$stubs/ts-up/tailscale" \
        /bin/bash "$scratch/scripts/run-service.sh" --check markets 2>&1 || true)"
if [[ "$line" != ready:* ]]; then
    say "galata-markets, listed in containers.conf, does not check as ready in a root with everything in place: $line"
else
    has() { [[ "$line" == *"$1"* ]] || say "galata-markets' container line lacks '$1': $2"; }
    has " run --rm --name galata-markets --cpus 2 --memory 2G " "it runs under its own name with the limits its line states"
    has "source=$scratch/var/archive,target=/app/var/archive,readonly" "it is given the archive read-only: it serves the record and must not be able to change it"
    has "source=$scratch/var/tape,target=/app/var/tape,readonly" "it is given the tape read-only"
    has " --publish 100.64.0.7:8780:8780 " "its port is published on the tailnet address"
    has "hand-secrets.sh $markets_names -- " "its bus password and its clients' tokens are handed over standard input, by name"
    has " with-secrets $markets_names -- galata-markets" "the entrypoint receives every name, then becomes galata-markets"
    [[ "$line" == *" -v $scratch/var/archive"* || "$line" == *" -v $scratch/var/tape"* ]] \
        && say "galata-markets is given the archive or the tape writable"
    has "source=$scratch/var/status,target=/app/var/status,readonly" "it is given the status folder read-only: it judges the reports in it, and must not change them"
    [[ "$line" == *" -v $scratch/var/status"* ]] \
        && say "galata-markets is given the status folder writable"
    # The ledger's projection, read-only (add-markets-accounts, design D7):
    # account history by alias. Its record never: the venues' answers in it
    # hold addresses. `/var/ledger` followed by anything but `-` is the record,
    # not `/var/ledger-tape`.
    has "source=$scratch/var/ledger-tape,target=/app/var/ledger-tape,readonly" "it is given the ledger's projection read-only: it serves account history from it, and must not change it"
    [[ "$line" == *" -v $scratch/var/ledger-tape"* ]] \
        && say "galata-markets is given the ledger's projection writable"
    [[ "$line" =~ /var/ledger([^-]|$) ]] \
        && say "galata-markets' container is given the ledger's record, which holds addresses and is not its to read"
    [[ "$line" =~ -e\ +\"?GALATA_(MARKETS_TOKEN|BROKER_PASSWORD) ]] && say "one of galata-markets' secrets is passed with -e"
fi
# A deployment that declares no ledger: galata-markets starts, given nothing
# of one.
mv "$scratch/var/datawatch.overlay.toml" "$scratch/var/datawatch.overlay.toml.aside"
line="$(GALATA_VAULT_BIN="$scratch/vault-bin" GALATA_CONTAINER_BIN="$stubs/up/container" \
        GALATA_CONTAINERS_CONF="$scratch/containers.conf" GALATA_TAILSCALE_BIN="$stubs/ts-up/tailscale" \
        /bin/bash "$scratch/scripts/run-service.sh" --check markets 2>&1 || true)"
mv "$scratch/var/datawatch.overlay.toml.aside" "$scratch/var/datawatch.overlay.toml"
if [[ "$line" != ready:* ]]; then
    say "galata-markets, with no ledger declared, does not check as ready: $line"
else
    [[ "$line" == *"/var/ledger"* ]] \
        && say "galata-markets, with no ledger declared, is given a ledger's folder: $line"
fi
: > "$scratch/containers.conf"
line="$(GALATA_VAULT_BIN="$scratch/vault-bin" GALATA_CONTAINER_BIN="$stubs/up/container" \
        GALATA_CONTAINERS_CONF="$scratch/containers.conf" GALATA_TAILSCALE_BIN="$stubs/ts-up/tailscale" \
        /bin/bash "$scratch/scripts/run-service.sh" --check markets 2>&1 || true)"
[[ "$line" == *"REFUSED"*"no CPU or memory limit for markets"* ]] \
    || say "galata-markets, with no line in containers.conf, is not refused by its name and the missing limit: $line"

# --- 4. the build context --------------------------------------------------
if [[ -f "$ROOT/.dockerignore" ]]; then
    grep -qxE 'var/?' "$ROOT/.dockerignore" \
        || say ".dockerignore does not exclude var/: an image build would be sent the service tokens, broker.env and the recovery kit"
    grep -qxE '\.git/?' "$ROOT/.dockerignore" \
        || say ".dockerignore does not exclude .git/"
else
    say "no .dockerignore: an image build is sent var/, with every token in it"
fi

if (( fails > 0 )); then exit 1; fi
echo "$me: ok — a missing tool, image, limit or tailnet is refused by name, and a service with no line has nothing to fall back to; no launch line inherits a variable, mounts a token or publishes off the tailnet; and var/ is outside the build context"
