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

    # **How docs.rs builds it**, which is invisible until the crate is
    # published and the page is wrong.
    #
    # Venues here are FEATURES, and docs.rs documents only the default set
    # unless told otherwise — so without this the page for the crate whose
    # README says "venues are features" would be missing one of them, and
    # would give no sign that any item is gated at all.
    docs = pkg.get("metadata", {}).get("docs", {}).get("rs", {})
    if not docs.get("all-features"):
        problems.append(
            f"{name}: publishable and declares no `[package.metadata.docs.rs] all-features` — "
            f"docs.rs would document the default features only"
        )
    elif "--cfg" not in docs.get("rustdoc-args", []):
        problems.append(
            f"{name}: docs.rs metadata sets no `--cfg docsrs`, so `doc_auto_cfg` stays off and "
            f"no item says which feature gates it"
        )

    if not (root / member / "LICENSE-MIT").exists():
        problems.append(
            f"{name}: publishable and carries no LICENSE-MIT — `include` ships only what it lists"
        )

if problems:
    for p in problems:
        print(f"check-release-hygiene: {p}", file=sys.stderr)
    sys.exit(1)
PY
