#!/usr/bin/env bash
#
# **What a publish would actually ship.**
#
# `check-release-hygiene.sh` checks the manifest says the right things.
# This checks the tarball would CONTAIN the right things, which is a different
# question and the one that cannot be taken back: a crates.io version is
# permanent, and a file that should not have shipped ships for ever.
#
# Three rules:
#
#   1. EVERY PUBLISHABLE CRATE DECLARES `include`. Without it cargo ships the
#      whole directory minus `.gitignore`d paths — and this tree's default
#      archive root is `var/`, which holds captured market data. A crate that
#      lost its `include` would publish somebody's order flow.
#
#   2. THE TARBALL CARRIES ITS LICENCE, ITS README AND ITS SOURCE. `include`
#      is a whitelist, so a typo in it silently drops a file rather than
#      failing — and the crate still publishes, just without its licence.
#
#   3. NOTHING FROM `var/`, `target/` OR A DOTFILE SHIPS. The deny side of
#      rule 1, checked against the real file list rather than against the
#      whitelist's intent.
#
# `cargo package --list` is used rather than `cargo package`, because it does
# not resolve dependencies — so all four crates are checkable even though two
# of them cannot be packaged until the other two are on crates.io. See the
# publish order in README.md.
#
# Usage: check-package.sh [check|plant] [root]

set -euo pipefail

VERB=check
if [[ $# -gt 0 ]]; then
    case "$1" in check|plant) VERB="$1"; shift ;; esac
fi
ROOT="${1:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
PLANT="$ROOT/crates/galata-wire/Cargo.toml"

if [[ ! -f "$ROOT/Cargo.toml" ]]; then
    echo "$(basename "$0"): $ROOT is not a workspace — refusing to scan nothing and call it ok" >&2
    exit 2
fi

if [[ "$VERB" == plant ]]; then
    # Drop the licence from the whitelist. The crate still builds, still
    # publishes, and ships without the licence it claims — which is exactly
    # the failure this guard exists for.
    python3 - "$PLANT" <<'PLANTPY'
import sys, pathlib
path = pathlib.Path(sys.argv[1])
text = path.read_text()
marker = ', "/LICENSE-MIT"]'
assert marker in text, "the plant's target moved — the PLANT is wrong, not the guard"
path.write_text(text.replace(marker, "]", 1))
PLANTPY
    echo "planted in $PLANT" >&2
    exit 0
fi

cd "$ROOT"
problems=()

members=$(python3 - <<'PY'
import tomllib, pathlib
ws = tomllib.loads(pathlib.Path("Cargo.toml").read_text())
for member in ws.get("workspace", {}).get("members", []):
    manifest = pathlib.Path(member) / "Cargo.toml"
    if not manifest.exists():
        continue
    pkg = tomllib.loads(manifest.read_text()).get("package", {})
    if pkg.get("publish") is False:
        continue
    print(f"{pkg.get('name', member)}\t{member}")
PY
)

while IFS=$'\t' read -r name dir; do
    [[ -z "$name" ]] && continue

    if ! grep -q "^include" "$dir/Cargo.toml"; then
        problems+=("$name: declares no \`include\`, so cargo ships the whole directory — and this tree's default archive root is var/")
        continue
    fi

    if ! listing=$(cargo package -p "$name" --list --allow-dirty 2>&1); then
        problems+=("$name: cargo package --list failed: $(echo "$listing" | grep -E '^error' | head -1)")
        continue
    fi

    for required in LICENSE-MIT README.md src/lib.rs; do
        grep -qx "$required" <<<"$listing" ||
            problems+=("$name: would publish without $required — \`include\` is a whitelist, so a typo drops a file silently")
    done

    while IFS= read -r unwanted; do
        [[ -n "$unwanted" ]] &&
            problems+=("$name: would publish $unwanted, which is not source")
    done < <(grep -E "^(var/|target/|\.env)" <<<"$listing" || true)
done <<<"$members"

if (( ${#problems[@]} > 0 )); then
    for problem in "${problems[@]}"; do
        echo "check-package: $problem" >&2
    done
    exit 1
fi
