#!/usr/bin/env bash
#
# Make a Derive session key for the ledger, put its private half in the vault,
# and print only its address.
#
#   make-derive-session-key.sh <NAME>
#
#   NAME   the variable the ledger reads the key from: its account's
#          `session_key_var`, e.g. GALATA_LEDGER_DERIVE_SESSION_KEY
#
# **The private half lives only in a pipe.** `galata-session-key` writes it to
# standard output, which this script hands straight to `gv set`: no argument,
# no file and no terminal holds it, and if the vault does not take it, it is
# gone and nothing is stored. The vault section is the deployment's
# (GALATA_VAULT_ENV, else galata-datawatch/mainnet).
#
# Then the owner registers the printed address on the wallet, with the wallet:
# NO protocol scope and NO off-chain scope (a key with no scopes can only
# read), the wallet's own subaccounts, and an expiry. The ledger refuses a key
# that holds any scope (add-derive-account-ledger, design D3 and D4).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=scripts/lib/vault-tools.sh
source "$ROOT/scripts/lib/vault-tools.sh"

refuse() { echo "make-derive-session-key: $*" >&2; exit 1; }

[[ $# -eq 1 ]] || refuse "usage: make-derive-session-key.sh <NAME>"
name="$1"
is_env_name "$name" || refuse "$name cannot be a variable, so the ledger could not be handed it"

gv="$(gv_tool)" || exit 1
env="$(vault_env)"

# The key's maker: built here, from this tree, unless a test names one.
bin="${GALATA_SESSION_KEY_BIN:-}"
if [[ -z "$bin" ]]; then
    (cd "$ROOT" && cargo build --release --offline -q -p galata-datawatch \
        --features derive-xyz --bin galata-session-key) ||
        refuse "galata-session-key would not build"
    bin="$ROOT/target/release/galata-session-key"
fi

# Its address reaches the terminal on standard error; its private half reaches
# nothing but gv's standard input.
if ! "$bin" | "$gv" set "$name" --env "$env" >/dev/null; then
    refuse "the vault did not take the key; nothing was stored, and the key is gone"
fi
echo "stored as $name in $env. Register the address above on the wallet with no scope."
