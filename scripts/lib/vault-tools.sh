# Where the vault's own programs are, and which `gv` is new enough to trust
# with a service's token. Sourced, never run; it sets nothing and starts
# nothing.
#
#   gv="$(gv_tool 2>&1)" || refuse "$gv"
#
# Each function prints the answer on stdout, or the reason on stderr and
# returns 1 — so the caller's own `refuse` says it, under the caller's name.
#
# **The installed release, never a checkout.** These scripts used to run
# `$GALATA_VAULT_ROOT/target/release/gv`, which made a deployment depend on a
# sibling source tree and on whatever was last built in it. galata-vault ships
# installers; `gv`'s puts it in `$CARGO_HOME/bin`.
#
# **Only `gv`.** These scripts once ran the vault's server too. The
# deployment's vault is now the hosted one, which galata-vault runs itself,
# so the only vault program a service's start needs is the client.
#
# **One directory, not PATH.** launchd's PATH has no `~/.cargo/bin`, and a
# name resolved through PATH at install time and again at start time can be
# two different programs. `GALATA_VAULT_BIN` names the directory; nothing else
# is searched.
#
# **The floor is what makes `gv run` safe here.** Before 0.5.0, `gv` ignored
# `GV_TOKEN_FILE` and answered with the owner key, which this machine holds:
# measured on 2026-09-25, the tower's token "read" capture's password through
# `gv run`. 0.5.0 uses the token or refuses, and refuses a name outside the
# token's allow-list instead of starting the command without it. An older
# `gv` is therefore not a slower path but the leak itself, and is refused by
# version before it is handed a token.

GV_FLOOR="0.5.0"

vault_bin_dir() {
    printf '%s' "${GALATA_VAULT_BIN:-${CARGO_HOME:-$HOME/.cargo}/bin}"
}

# How to get them, said wherever one is missing or too old.
vault_install_hint() {
    printf '%s' "install the galata-vault $GV_FLOOR tools (README, Configuration and secrets): the gv-installer.sh of its latest release — or set GALATA_VAULT_BIN to the directory that holds it"
}

# vault_tool <name>: the path of a vault program, which today is only `gv`.
vault_tool() {
    local path
    path="$(vault_bin_dir)/$1"
    if [[ ! -x "$path" ]]; then
        echo "no $1 at $path — $(vault_install_hint)" >&2
        return 1
    fi
    printf '%s' "$path"
}

# version_at_least <have> <want>: both MAJOR.MINOR.PATCH; a pre-release or
# build suffix on <have> is ignored, so 0.5.0-rc.1 counts as 0.5.0.
version_at_least() {
    local have="${1%%[-+]*}" want="$2" i h w
    local IFS=.
    # shellcheck disable=SC2206  # split on the dots, on purpose
    local hs=($have) ws=($want)
    for i in 0 1 2; do
        h="${hs[$i]:-0}"; w="${ws[$i]:-0}"
        [[ "$h" =~ ^[0-9]+$ ]] || return 1
        (( h > w )) && return 0
        (( h < w )) && return 1
    done
    return 0
}

# gv_tool: the path of a `gv` at or above the floor.
gv_tool() {
    local path have
    path="$(vault_tool gv)" || return 1
    # `gv --version` prints "gv 0.5.0".
    have="$("$path" --version 2>/dev/null </dev/null | awk 'NR == 1 { print $NF }')"
    if ! version_at_least "${have:-0}" "$GV_FLOOR"; then
        echo "$path is gv ${have:-of an unreadable version}, and $GV_FLOOR is required: an older gv ignores a token file and reads through the owner key, handing a service every secret — $(vault_install_hint)" >&2
        return 1
    fi
    printf '%s' "$path"
}

# The vault environment this deployment's services read: one section of the
# project per network. `mainnet` unless GALATA_VAULT_ENV names another, such
# as galata-datawatch/testnet for a deployment that captures the testnet.
# Provisioning makes both sections; a deployment reads one.
VAULT_PROJECT="galata-datawatch"
VAULT_SECTIONS="mainnet testnet"
vault_env() {
    printf '%s' "${GALATA_VAULT_ENV:-$VAULT_PROJECT/mainnet}"
}

# The broker's own naming: GALATA_BROKER_PASSWORD_<IDENTITY>, dashes to
# underscores, upper case (galata_broker::password_var). `gv run` skips a
# name that cannot be an environment variable, so the dash must not survive.
password_var() {
    local id="${1//-/_}"
    printf 'GALATA_BROKER_PASSWORD_%s' "$(printf '%s' "$id" | tr '[:lower:]' '[:upper:]')"
}

# markets_secrets: the names galata-markets reads, one per line: its bus
# password, then each client's token (scripts/lib/markets-clients).
markets_secrets() {
    local root client
    root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
    printf '%s\n' "$(password_var markets)"
    while read -r client; do
        [[ -n "$client" && "$client" != \#* ]] || continue
        printf 'GALATA_MARKETS_TOKEN_%s\n' "$(printf '%s' "$client" | tr 'a-z-' 'A-Z_')"
    done < "$root/scripts/lib/markets-clients"
}

# is_env_name <name>: could a process be handed this as a variable?
is_env_name() {
    [[ "$1" =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]]
}
