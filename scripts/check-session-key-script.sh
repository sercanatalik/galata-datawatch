#!/usr/bin/env bash
#
# make-derive-session-key.sh hands a session key's private half to the vault
# and to nothing else.
#
# Against a stub gv, with the real galata-session-key: the key arrives on gv's
# standard input, and is in no argument, none of the script's output and no
# file; the address is shown for the owner to register; and a vault that
# refuses the key leaves it nowhere, and says nothing was stored.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

cargo build --release --offline -q -p galata-datawatch --features derive-xyz \
    --bin galata-session-key
BIN="$ROOT/target/release/galata-session-key"

T="$(mktemp -d)"
trap 'rm -rf "$T"' EXIT
mkdir "$T/bin" "$T/work"
cat > "$T/bin/gv" <<'STUB'
#!/usr/bin/env bash
if [[ "${1:-}" == "--version" ]]; then echo "gv 0.5.1"; exit 0; fi
printf '%s\n' "$@" > "$GV_STUB_DIR/args"
cat > "$GV_STUB_DIR/stdin"
exit "$GV_STUB_EXIT"
STUB
chmod +x "$T/bin/gv"

# run <gv's exit status>
run() {
    (cd "$T/work" && env GALATA_VAULT_BIN="$T/bin" GV_STUB_DIR="$T" GV_STUB_EXIT="$1" \
        GALATA_SESSION_KEY_BIN="$BIN" GALATA_VAULT_ENV=test-project/mainnet \
        "$ROOT/scripts/make-derive-session-key.sh" TEST_SESSION_KEY > "$T/out" 2>&1)
}

# in_a_file <value>: any file under the scratch folder but gv's own record.
in_a_file() {
    grep -r -l -F -- "$1" "$T" --exclude=stdin 2>/dev/null | grep -q .
}

problems=()

# A vault that takes the key.
run 0 || problems+=("a vault that took the key was reported as refusing it: $(cat "$T/out")")
key="$(cat "$T/stdin" 2>/dev/null || true)"
if [[ ! "$key" =~ ^0x[0-9a-f]{64}$ ]]; then
    problems+=("gv's standard input did not hold one key of 32 bytes")
else
    asked="$(tr '\n' ' ' < "$T/args")"
    [[ "$asked" == "set TEST_SESSION_KEY --env test-project/mainnet " ]] ||
        problems+=("gv was asked something else: $asked")
    if grep -q -F -- "${key#0x}" "$T/args"; then problems+=("the key is in gv's arguments"); fi
    if grep -q -F -- "${key#0x}" "$T/out"; then problems+=("the key is in the script's output"); fi
    if in_a_file "${key#0x}"; then problems+=("the key is in a file"); fi
fi
grep -q -E 'address: 0x[0-9a-f]{40}' "$T/out" || problems+=("the address was not shown")

# A vault that refuses it.
rm -f "$T/stdin" "$T/args"
if run 1; then problems+=("a vault that refused the key was reported as storing it"); fi
grep -q 'nothing was stored' "$T/out" || problems+=("a refusal did not say nothing was stored")
refused="$(cat "$T/stdin" 2>/dev/null || true)"
if [[ -n "$refused" ]] && in_a_file "${refused#0x}"; then
    problems+=("a refused key is in a file")
fi

if (( ${#problems[@]} )); then
    printf 'check-session-key-script.sh: %s\n' "${problems[@]}" >&2
    exit 1
fi
echo "check-session-key-script.sh: ok — the key reaches gv's standard input only, the address is shown, and a refused key is kept nowhere"
