#!/usr/bin/env bash
#
# No address reaches a name: a partition path, a subject, a status field, a log.
#
# The ledger knows an account by its alias; only the vault knows the address.
# An address is not a credential on Hyperliquid, but it names its owner on a
# public chain, and a leak cannot be taken back (openspec change
# `ledger-accounts-and-snapshots`, D1).
#
# The type already holds most of this. An address is a `Secret` from the moment
# it is read — from the vault, or from a venue's sub-account listing — and a
# `Secret` has no `Display` and a `Debug` that withholds. **The only road from a
# `Secret` to a string is `.expose()`.** So this guard holds the road: every
# `.expose()` in shipped code sits in a file named below, with the one place its
# value goes. A new call site anywhere else fails, and has to be argued into the
# list rather than slipped past it.
#
# What this cannot see: bytes archived verbatim by design. A sub-account listing
# carries addresses in its raw answer, and the ledger root's mode is the
# boundary for that (`ledger::check_root`), not this guard.
#
# --- the protocol: check (default) | plant ---------------------------------
set -euo pipefail

VERB=check
if [[ $# -gt 0 ]]; then
    case "$1" in check|plant) VERB="$1"; shift ;; esac
fi
ROOT="${1:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
CRATES="$ROOT/crates"

# A guard handed a root it cannot scan reports success forever.
if [[ ! -d "$CRATES/galata-datawatch/src" ]]; then
    echo "$(basename "$0"): $ROOT has no crates/galata-datawatch/src — refusing to scan nothing and call it ok" >&2
    exit 2
fi

# The plant: the leak D1 exists to prevent — an address written into a
# partition path. At the TOP of a file the check reads, above any test module.
if [[ "$VERB" == "plant" ]]; then
    f="$CRATES/galata-datawatch/src/record/mod.rs"
    printf 'pub fn planted(a: &crate::config::Secret) -> std::path::PathBuf { std::path::PathBuf::from(format!("account={}", a.expose())) }\n' \
        | cat - "$f" > "$f.planted" && mv "$f.planted" "$f"
    exit 0
fi

# Where a secret may be exposed, and where its value goes. Paths relative to
# crates/.
allowed() {
    case "$1" in
        galata-datawatch/src/ledger/accounts.rs)              ;; # into the HMAC, never out of it
        galata-datawatch/src/adapters/hyperliquid/client.rs)  ;; # into an info request's body, to the venue
        galata-datawatch/src/adapters/rh_crypto/sign.rs)      ;; # into the signature
        galata-datawatch/src/adapters/rh_chain/client.rs)     ;; # the keyed RPC endpoint, to the client
        galata-datawatch/src/source/stream.rs)                ;; # the websocket endpoint, to the connector
        galata-datawatch/src/source/poll.rs)                  ;; # the poll endpoint, to the client
        galata-datawatch/src/venue/transport.rs)              ;; # a held endpoint, to the transport
        galata-datawatch/src/boot.rs)                         ;; # the broker password, to the connection
        galata-datawatch-vault/src/lib.rs)                    ;; # the vault's answer, into the loader
        *) return 1 ;;
    esac
}

non_test_lines() {
    awk '/^[[:space:]]*#\[cfg\(.*test.*\)\]/{exit} {print FILENAME ":" FNR ": " $0}' "$1"
}

failures=()
while IFS= read -r file; do
    relative="${file#"$CRATES"/}"
    hits=$(non_test_lines "$file" | grep -F '.expose()' || true)
    [[ -z "$hits" ]] && continue
    allowed "$relative" || failures+=("$hits")
done < <(find "$CRATES" -path '*/target' -prune -o -name '*.rs' -print)

if (( ${#failures[@]} > 0 )); then
    echo "no address in names: a secret is exposed somewhere it may become a name" >&2
    printf '%s\n' "${failures[@]}" >&2
    echo "  an address may go to the venue or into a fingerprint, and nowhere else;" >&2
    echo "  a new place needs its reason added to this script's list" >&2
    exit 1
fi
