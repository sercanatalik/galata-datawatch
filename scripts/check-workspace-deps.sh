#!/usr/bin/env bash
#
# A workspace dependency is declared once and used once.
#
# Two rules, and the second is the one people forget:
#   1. A member may not re-declare a version for anything the workspace names.
#      Two places to pin a version is a place for two to disagree, and the pin
#      that loses is the one nobody reads.
#   2. The workspace may not name something no member uses. A table nobody
#      references reads as though versions are pinned in one place when they
#      are not.
#
# And one wall, which is why this guard exists at all:
#   3. galata-wire links serde, serde_json, rust_decimal and thiserror, and
#      NOTHING else. Every future consumer of the vocabulary links it, and a
#      process that only needs to NAME an event must not inherit a columnar
#      format to do it. In the predecessor tree eighteen crates reached the
#      broker through the crate that also owned the archive writer, so the one
#      crate designed to link no history linked parquet transitively.
#
# Usage: check-workspace-deps.sh [root]

set -euo pipefail
ROOT="${1:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
[[ -f "$ROOT/Cargo.toml" ]] || { echo "$(basename "$0"): $ROOT has no Cargo.toml" >&2; exit 2; }

python3 - "$ROOT" <<'PY'
import sys, tomllib, pathlib

root = pathlib.Path(sys.argv[1])
ws = tomllib.loads((root / "Cargo.toml").read_text())
declared = set(ws.get("workspace", {}).get("dependencies", {}))
members = ws.get("workspace", {}).get("members", [])

WIRE_ALLOWED = {"rust_decimal", "serde", "serde_json", "thiserror"}

problems, used = [], set()

for member in members:
    manifest = root / member / "Cargo.toml"
    if not manifest.exists():
        problems.append(f"{member}: declared as a member and has no Cargo.toml")
        continue
    m = tomllib.loads(manifest.read_text())
    name = m.get("package", {}).get("name", member)
    for table in ("dependencies", "dev-dependencies", "build-dependencies"):
        for dep, spec in m.get(table, {}).items():
            if dep in declared:
                used.add(dep)
                if isinstance(spec, dict) and not spec.get("workspace"):
                    problems.append(
                        f"{name}: {table}.{dep} re-declares a version the workspace already "
                        f"declares — say `workspace = true`"
                    )
                elif isinstance(spec, str):
                    problems.append(
                        f"{name}: {table}.{dep} re-declares version {spec!r} the workspace "
                        f"already declares — say `workspace = true`"
                    )
            # Rule 3: the vocabulary crate's wall.
            if name == "galata-wire" and table == "dependencies" and dep not in WIRE_ALLOWED:
                problems.append(
                    f"galata-wire: {dep} is outside its permitted set "
                    f"({', '.join(sorted(WIRE_ALLOWED))}). Every consumer of the vocabulary "
                    f"links this crate; it must not carry a columnar format, a runtime, a "
                    f"broker or an HTTP client."
                )

for dep in sorted(declared - used):
    problems.append(f"[workspace.dependencies] names {dep}, which no member uses")

if problems:
    for p in problems:
        print(f"check-workspace-deps: {p}", file=sys.stderr)
    sys.exit(1)
PY
