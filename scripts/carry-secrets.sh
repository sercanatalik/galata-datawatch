#!/usr/bin/env bash
#
# Carry named secrets from one vault to another, as their owner, value by value.
#
#   carry-secrets.sh --from <project/env> [--from-home DIR]
#                    --to <project/env>   [--to-home DIR]
#                    [--pause] NAME [NAME ...]
#
# For the secrets that live nowhere but a vault. The broker's passwords are
# not among them: they are in var/broker.env, and provision-vault.sh imports
# them from there. A ledger's are: its account addresses and its fingerprint
# key were set by hand, and the fingerprint key cannot be made again — the
# ledger's record is keyed by it.
#
#   # the vault this deployment ran itself, to the hosted one; the two cannot
#   # both hold loopback 8750, so the values wait in memory in between
#   carry-secrets.sh --pause \
#       --from galata-datawatch/mainnet \
#       --to galata-datawatch/mainnet --to-home ~/.config/gv-hosted \
#       GALATA_LEDGER_ADDRESS_MAIN GALATA_LEDGER_FINGERPRINT_KEY
#
# **A value is never an argument and never a file**, and nothing printed holds
# one: scripts/lib/carry_secrets.py says how, and why it is not a shell loop.
#
# **Every read before any write, and every write read back.** A name that
# cannot be read sets nothing anywhere. A name that arrives is compared with
# what left, byte for byte, before it is called carried.
#
# **Two homes, for two projects of one name.** `gv` refuses a second project
# of the same name under one home (GV_HOME), so the destination is
# provisioned under a home of its own first:
#
#   GV_HOME=~/.config/gv-hosted scripts/provision-vault.sh <vault address>
#
# **The source keeps everything.** Nothing is removed from it, here or
# anywhere: retiring a vault is a separate act.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=scripts/lib/vault-tools.sh
source "$ROOT/scripts/lib/vault-tools.sh"

refuse() { echo "carry-secrets: REFUSED — $1" >&2; exit 1; }

# The same floor as a service's start: an older gv ignores a token file and
# answers as the owner, which is the wrong surprise in either direction.
GV="$(gv_tool 2>&1)" || refuse "$GV"
command -v python3 >/dev/null || refuse "no python3 on PATH"
exec python3 "$ROOT/scripts/lib/carry_secrets.py" "$GV" "$@"
