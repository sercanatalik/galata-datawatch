#!/bin/bash
#
# Hand a container its secrets over standard input, and become `container run`.
#
#   hand-secrets.sh NAME [NAME ...] -- <container> run -i ... <image> with-secrets NAME ... -- <program>
#
# Run by `gv run`, which has read each NAME with this service's own token and
# put it in this process's environment (scripts/run-service.sh, from_vault).
# This writes one `NAME=value` line per name to the standard input of the
# command after `--`, and is replaced by that command with every NAME removed
# from its environment. In the image, `with-secrets` receives the lines.
#
# **Why not `container run -e NAME`.** Measured 2026-11-24, Apple container
# 1.7.1: a value passed with `-e` is written to two files under the container
# tool's state, mode 0644, while the container exists, and `container inspect`
# prints it. Written to standard input, it was in neither, and in no host
# process's arguments or environment once this had exec'd.
#
# **`exec`, so a stop reaches the service.** launchd's SIGTERM goes to the
# process this becomes, `container run`, which passes it to the container's
# process (measured the same day). The lines are written by a child that
# exits as soon as it has written them. `container run` never collects it, so
# it stays as one defunct entry beneath the service until the service stops:
# a defunct process holds no memory and no environment.
#
# **A value with a line break is refused, by name.** The hand-over is one line
# per name; half a value would arrive as a line that names nothing.
#
# macOS's bash is 3.2, so no associative arrays and no namerefs.

set -euo pipefail

refuse() { echo "hand-secrets: REFUSED — $1" >&2; exit 2; }

names=()
while [[ $# -gt 0 && "$1" != -- ]]; do
    [[ "$1" =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]] || refuse "'$1' cannot be the name of a variable"
    names+=("$1")
    shift
done
[[ "${1:-}" == -- && ${#names[@]} -gt 0 && $# -ge 2 ]] \
    || refuse "usage: hand-secrets.sh NAME [NAME ...] -- <command>"
shift

without=()
for name in "${names[@]}"; do
    value="${!name-}"
    [[ -n "$value" ]] || refuse "$name is not set, so the service would start without it"
    [[ "$value" != *$'\n'* ]] \
        || refuse "the value of $name holds a line break, and a container is handed its secrets one line each"
    without+=(-u "$name")
done
unset value

# The child inherits the values, writes them and exits; `env -u` starts the
# command without them. Nothing that stays running holds a value.
exec env "${without[@]}" "$@" < <(
    for name in "${names[@]}"; do
        printf '%s=%s\n' "$name" "${!name}"
    done
)
