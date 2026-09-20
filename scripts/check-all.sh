#!/usr/bin/env bash
#
# Everything CI runs, in the order that fails cheapest first.
#
# The test suite must pass WITH NO NETWORK ACCESS. This tier's whole claim is
# that both crates are provable offline; an accidental network dependency would
# make that claim false silently on the machine where it was written and loudly
# on every other. `--offline` holds the Cargo half of it.
#
# Usage: check-all.sh [root]

set -euo pipefail
ROOT="${1:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
cd "$ROOT"

step() { printf '\n=== %s\n' "$1"; }

step "format"
cargo fmt --all -- --check

step "lints"
cargo clippy --all-targets --all-features --offline -- -D warnings

step "guards"
for guard in scripts/check-*.sh; do
    [[ "$guard" == "scripts/check-all.sh" ]] && continue
    printf '  %s\n' "$guard"
    "$guard"
done

step "the guards can fail"
./scripts/test-guards.sh

step "tests, offline"
cargo test --all-features --offline

printf '\nall green\n'
