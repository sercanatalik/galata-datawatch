# What a service needs to know before it starts in a container of its own:
# where Apple's `container` is and whether it is running, whether an image
# exists, what limits this deployment states for the service, and whether the
# tailnet is up. Sourced, never run; it sets nothing and starts nothing.
#
#   tool="$(container_tool 2>&1)" || refuse "$tool"
#   limits="$(container_limits capture:hyperliquid 2>&1)" || refuse "$limits"
#
# Each function prints the answer on stdout, or the reason on stderr and
# returns 1 — so the caller's own `refuse` says it, under the caller's name.
#
# **One path, not PATH.** launchd's PATH is nearly empty, and a name resolved
# through PATH at install time and again at start time can be two different
# programs. `GALATA_CONTAINER_BIN` and `GALATA_TAILSCALE_BIN` name the
# programs; the defaults are where their installers put them.
#
# **The limits are stated, never defaulted.** A container given no limit gets
# 4 CPUs and 1 GiB (measured 2026-11-24, Apple container 1.7.1), which is too
# much for the broker and too little for a tape rebuild that holds a range in
# memory. So every service has a line in var/containers.conf
# stating what it may use, and a service with no line does not start:
#
#   # service              cpus  memory
#   capture:hyperliquid    2     2G
#   nats                   1     256M
#   flows                  4     12G
#
# var/, because the figures belong to this machine.

# The file that states each service's limits.
containers_conf() {
    printf '%s' "${GALATA_CONTAINERS_CONF:-$ROOT/var/containers.conf}"
}

# container_tool: the path of Apple's `container`.
container_tool() {
    local path="${GALATA_CONTAINER_BIN:-/usr/local/bin/container}"
    if [[ ! -x "$path" ]]; then
        echo "no \`container\` at $path — install Apple container 1.7 or later (github.com/apple/container), or set GALATA_CONTAINER_BIN" >&2
        return 1
    fi
    printf '%s' "$path"
}

# container_running <tool>: are its services up? A launchd agent cannot answer
# a prompt, so the kernel is never installed from here: the first start is by
# hand.
container_running() {
    if ! "$1" system status >/dev/null 2>&1; then
        echo "the container services are stopped — \`container system start\`, once by hand so it can install its kernel" >&2
        return 1
    fi
}

# container_image <tool> <image>: does this machine hold it?
container_image() {
    if ! "$1" image inspect "$2" >/dev/null 2>&1; then
        echo "no image $2 — scripts/build-images.sh" >&2
        return 1
    fi
}

# service_image <spec>: the image a service's container runs. One name each,
# so a deployment can pin it and the launcher and the status report agree.
service_image() {
    case "$1" in
        nats) printf '%s' "${GALATA_NATS_IMAGE:-galata-nats:latest}" ;;
        flows) printf '%s' "${GALATA_FLOWS_IMAGE:-galata-flows:latest}" ;;
        markets) printf '%s' "${GALATA_MARKETS_IMAGE:-galata-markets:latest}" ;;
        *) printf '%s' "${GALATA_DATAWATCH_IMAGE:-galata-datawatch:latest}" ;;
    esac
}

# container_state <tool> <name>: "<state>\t<image>\t<image digest>" for the
# container under that name, and nothing when none holds it.
container_state() {
    "$1" inspect "$2" 2>/dev/null | python3 -c '
import json, sys

try:
    doc = json.load(sys.stdin)
except ValueError:
    sys.exit(0)
if isinstance(doc, list):
    doc = doc[0] if doc else None
if not isinstance(doc, dict):
    sys.exit(0)
image = (doc.get("configuration") or {}).get("image") or {}
state = (doc.get("status") or {}).get("state") or "unknown"
print("\t".join([state, image.get("reference", ""), (image.get("descriptor") or {}).get("digest", "")]))
' 2>/dev/null
}

# image_digest <tool> <image>: the digest the image name points at today. A
# running container whose digest differs was started before the last build.
image_digest() {
    "$1" image inspect "$2" 2>/dev/null | python3 -c '
import json, sys

try:
    doc = json.load(sys.stdin)
except ValueError:
    sys.exit(0)
if isinstance(doc, list):
    doc = doc[0] if doc else None
if isinstance(doc, dict):
    print(doc.get("id") or (doc.get("descriptor") or {}).get("digest") or "")
' 2>/dev/null
}

# image_source_tree <tool> <image>: the tree the image says it was built from
# (the label scripts/build-images.sh gives it). An image with no such label
# was built by hand, and says nothing about what is running.
image_source_tree() {
    local tree
    tree="$("$1" image inspect "$2" 2>/dev/null | python3 -c '
import json, sys

def find(node):
    if isinstance(node, dict):
        for key, value in node.items():
            if key == "org.galata.source-tree" and isinstance(value, str):
                return value
            found = find(value)
            if found:
                return found
    elif isinstance(node, list):
        for value in node:
            found = find(value)
            if found:
                return found
    return None

print(find(json.load(sys.stdin)) or "")
' 2>/dev/null)"
    if [[ -z "$tree" ]]; then
        echo "$2 carries no org.galata.source-tree label — build it with scripts/build-images.sh" >&2
        return 1
    fi
    printf '%s' "$tree"
}

# container_volume <tool> <name>: does this machine hold the volume?
container_volume() {
    "$1" volume inspect "$2" >/dev/null 2>&1
}

# container_limits <spec>: "<cpus> <memory>", as stated for this service.
# `spec` is what install-services.sh takes: nats, flows, capture:<venue>,
# ledger:<venue>. A service with no line is refused, naming it and both
# limits; a line that states only one of them is refused by name too: half a
# limit is a default for the other half, and the defaults are the thing being
# avoided.
container_limits() {
    local conf spec="$1" line cpus memory
    conf="$(containers_conf)"
    if [[ ! -f "$conf" ]]; then
        echo "no $conf, so no CPU or memory limit is stated for $spec — cp config/containers.conf.example $conf, then state each service's" >&2
        return 1
    fi
    if ! awk -v s="$spec" '$1 == s { found = 1 } END { exit found ? 0 : 1 }' "$conf"; then
        echo "$conf states no CPU or memory limit for $spec, so it does not start — add a line \"$spec <cpus> <memory>\" (config/containers.conf.example)" >&2
        return 1
    fi
    line="$(awk -v s="$spec" '$1 == s { print $2, $3; exit }' "$conf")"
    read -r cpus memory <<< "$line"
    if [[ -z "$cpus" ]]; then
        echo "$conf states no CPU limit for $spec" >&2
        return 1
    fi
    if [[ ! "$cpus" =~ ^[1-9][0-9]*$ ]]; then
        echo "$conf gives $spec '$cpus' CPUs, which is not a whole number of them" >&2
        return 1
    fi
    if [[ -z "$memory" ]]; then
        echo "$conf states no memory limit for $spec" >&2
        return 1
    fi
    if [[ ! "$memory" =~ ^[1-9][0-9]*[MG]$ ]]; then
        echo "$conf gives $spec '$memory' of memory; write it as a number and M or G, as in 512M or 2G" >&2
        return 1
    fi
    printf '%s %s' "$cpus" "$memory"
}

# container_name <spec>: the one name a service's container runs under, so a
# second start finds — and removes — what a crash left behind.
container_name() {
    case "$1" in
        capture:*) printf 'galata-capture-%s' "${1#capture:}" ;;
        ledger:*) printf 'galata-ledger-%s' "${1#ledger:}" ;;
        *) printf 'galata-%s' "$1" ;;
    esac
}

# tailnet_tool: the path of Tailscale's command line. The app's own binary
# first: `tailscale` in an interactive shell is usually an alias to it, and an
# alias is not something launchd has.
tailnet_tool() {
    local path
    for path in "${GALATA_TAILSCALE_BIN:-}" \
                /Applications/Tailscale.app/Contents/MacOS/Tailscale \
                /opt/homebrew/bin/tailscale /usr/local/bin/tailscale; do
        [[ -n "$path" && -x "$path" ]] && { printf '%s' "$path"; return 0; }
    done
    echo "no Tailscale command line found — install Tailscale, or set GALATA_TAILSCALE_BIN" >&2
    return 1
}

# tailnet_address: this machine's tailnet IPv4 address, when the tailnet is up.
#
# A service that others reach publishes its port on this address and on no
# other (measured 2026-11-24: a container reaches a port published here in
# 0.2 ms, and `tailscale serve` does not answer a container at all). So a
# tailnet that is down is a reason not to start, never a reason to publish
# somewhere else.
tailnet_address() {
    local ts state address
    ts="$(tailnet_tool)" || return 1
    state="$("$ts" status --json 2>/dev/null </dev/null | awk -F'"' '/"BackendState"/ { print $4; exit }')"
    if [[ "$state" != Running ]]; then
        echo "the tailnet is down (Tailscale reports '${state:-nothing}') — start Tailscale; nothing is published on another address in its place" >&2
        return 1
    fi
    address="$("$ts" ip -4 2>/dev/null </dev/null | head -1)"
    if [[ ! "$address" =~ ^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
        echo "the tailnet is down: Tailscale is running and gave no tailnet address" >&2
        return 1
    fi
    printf '%s' "$address"
}
