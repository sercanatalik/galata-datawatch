#!/usr/bin/env bash
#
# THE FOURTH WALL. No published crate links the vault.
#
# `config/source.rs` states that galata-datawatch takes no vault dependency in
# order to publish without one. That sentence is load-bearing and, until this
# guard, was held by nothing: the vault reaches the tree through
# `galata-datawatch-vault`, which is `publish = false`, and one line in a
# manifest is all that stands between the two.
#
# A cargo feature could not have held it. Cargo unifies a dependency's features
# across one build, so a feature decides what is COMPILED, not what a binary can
# reach — which is why the vault lives in its own member rather than behind
# `--features vault`.
#
# What comes with it, measured under Cargo 1.98.1 on 2026-09-23: galata-vault
# 0.4 resolves 268 packages alone and adds 151 to this tree (335 -> 478), much
# of it `age`'s localisation stack — fluent, i18n-embed, unic-langid, rust-embed,
# intl-memoizer — which no `age` feature set drops. A tape reader must not
# compile a localisation framework.
#
# Asked of cargo rather than of the manifest, because a manifest is what
# somebody edits and a resolved tree is what a consumer gets.
#
# Usage: check-vault-reach.sh [check|plant] [root]

set -euo pipefail

VERB=check
if [[ $# -gt 0 ]]; then
    case "$1" in check|plant) VERB="$1"; shift ;; esac
fi
ROOT="${1:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
PLANT="$ROOT/crates/galata-datawatch/Cargo.toml"

if [[ "$VERB" == plant ]]; then
    # The ordinary mistake: a published crate reaching for the vault directly,
    # which is exactly how the wall would come down in practice.
    python3 - "$PLANT" <<'PLANTPY'
import sys, pathlib
path = pathlib.Path(sys.argv[1])
text = path.read_text()
marker = "[dependencies]\n"
assert marker in text, "the plant's target moved — the PLANT is wrong, not the guard"
at = text.index(marker) + len(marker)
path.write_text(text[:at] + "galata-vault = \"0.4\"  # planted by check-vault-reach.sh\n" + text[at:])
PLANTPY
    echo "planted in $PLANT" >&2
    exit 0
fi

cd "$ROOT"

VAULT_FAMILY=$(cargo tree -p galata-datawatch-vault --prefix none 2>/dev/null \
    | grep -E '^galata-vault(-client|-keys|-proto|-seal)? v[0-9]' | sort -u || true)
SUPPORTED_VAULT=$(printf '%s\n' "$VAULT_FAMILY" | grep -cE '^galata-vault v0\.4\.[0-9]+$' || true)
VAULT_COUNT=$(printf '%s\n' "$VAULT_FAMILY" | grep -c . || true)
if [[ "$SUPPORTED_VAULT" -ne 1 || "$VAULT_COUNT" -ne 1 ]]; then
    echo "check-vault-reach: expected one galata-vault 0.4.x package and no split family" >&2
    printf '%s\n' "$VAULT_FAMILY" >&2
    exit 1
fi

# The four that publish. Named explicitly: a fifth appearing here by accident is
# the failure this list exists to make loud.
PUBLISHED=(galata-wire galata-broker galata-segments galata-datawatch)

# `age` and its localisation stack enter only through the vault, so they are
# named too — the wall should hold even if the vault is ever vendored under
# another name.
FORBIDDEN='galata-vault|galata-vault-.*|age|age-core|i18n-embed|i18n-embed-fl|fluent|fluent-bundle|unic-langid|intl-memoizer'

# **The set itself, before its contents.** The wall below checks four crates; if
# a fifth ever publishes, the wall would still pass while no longer covering the
# tree. So the publishable set is asserted to be exactly these four, computed
# from the manifests rather than trusted from the list above.
ACTUAL=$(python3 scripts/lib/publishable.py)
EXPECTED=$(printf '%s\n' "${PUBLISHED[@]}" | sort | tr '\n' ' ' | sed 's/ $//')
if [[ "$ACTUAL" != "$EXPECTED" ]]; then
    echo "check-vault-reach: the publishable set is not the set this guard covers." >&2
    echo "  expected: $EXPECTED" >&2
    echo "  actual:   $ACTUAL" >&2
    echo "  A crate that publishes without being checked here may reach the vault" >&2
    echo "  unobserved. Add it above, or do not publish it." >&2
    exit 1
fi

failed=0
for crate in "${PUBLISHED[@]}"; do
    # --all-features: a crate publishes every feature it declares, and docs.rs
    # builds them all. A wall that only holds on the default set is not a wall.
    FOUND=$(cargo tree -p "$crate" --all-features --prefix none 2>/dev/null \
        | awk '{print $1}' | sort -u \
        | grep -xE "$FORBIDDEN" || true)
    if [[ -n "$FOUND" ]]; then
        echo "check-vault-reach: $crate links the vault, or what comes with it:" >&2
        echo "$FOUND" | sed 's/^/    /' >&2
        failed=1
    fi
done

if [[ $failed -ne 0 ]]; then
    echo "  A published crate must reach none of this. The vault belongs to" >&2
    echo "  galata-datawatch-vault, which does not publish." >&2
    exit 1
fi

echo "vault reach: ok. the four published crates link no vault crate and no age, at any depth"
