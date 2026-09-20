#!/usr/bin/env bash
#
# Licence and package metadata are stated once.
#
# No registry reads these fields until a crate is published, but licence
# scanners, `cargo deny` and anyone vendoring the tree read exactly them — and
# a second place to state a licence is a place for two to disagree.
#
# Every publishable member must also carry what crates.io requires, checked
# here rather than discovered by a failed `cargo publish`, and a copy of the
# licence text, because `include` ships only what it lists.
#
# Usage: check-release-hygiene.sh [root]

set -euo pipefail
ROOT="${1:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
[[ -f "$ROOT/Cargo.toml" ]] || { echo "$(basename "$0"): $ROOT has no Cargo.toml" >&2; exit 2; }

python3 - "$ROOT" <<'PY'
import sys, tomllib, pathlib

root = pathlib.Path(sys.argv[1])
ws = tomllib.loads((root / "Cargo.toml").read_text())
members = ws.get("workspace", {}).get("members", [])

INHERITED = ("license", "repository", "version", "edition", "rust-version")
REQUIRED = ("description", "readme", "keywords", "categories")

problems = []
for member in members:
    manifest = root / member / "Cargo.toml"
    if not manifest.exists():
        continue
    m = tomllib.loads(manifest.read_text())
    pkg = m.get("package", {})
    name = pkg.get("name", member)

    for field in INHERITED:
        value = pkg.get(field)
        if value is not None and not (isinstance(value, dict) and value.get("workspace")):
            problems.append(
                f"{name}: states its own `{field}` instead of inheriting it — "
                f"say `{field}.workspace = true`"
            )

    if pkg.get("publish") is False:
        continue

    for field in REQUIRED:
        if field not in pkg:
            problems.append(f"{name}: publishable and missing `{field}`")

    if not (root / member / "LICENSE-MIT").exists():
        problems.append(
            f"{name}: publishable and carries no LICENSE-MIT — `include` ships only what it lists"
        )

if problems:
    for p in problems:
        print(f"check-release-hygiene: {p}", file=sys.stderr)
    sys.exit(1)
PY
