#!/usr/bin/env bash
#
# **Money never becomes a float**, held by the build rather than by memory.
#
# `Num` is `rust_decimal` with `serde-str`, so an amount round-trips as text and
# never through a double. Three ways to break that silently:
#
#   1. AN f32/f64 FIELD IN THE VOCABULARY. `galata-wire` is what crosses the
#      bus, the parquet schema and the HTTP contract. One `price: f64` there is
#      wrong in all three at once.
#
#   2. rust_decimal's `serde-float` FEATURE. It makes every `Num` serialise
#      through a double — no type changes, no call site changes, no warning.
#      The library's own documentation says not to enable it for
#      precision-critical data.
#
#   3. A Float COLUMN IN AN ARROW SCHEMA. The tape asserts this in a test,
#      which covers the tape and not `galata-segments`, and a test in a crate
#      nobody ran is not a build failure.
#
# None of these is broken today. **That is when a guard is worth writing**:
# afterwards the rows are already wrong and unrecoverable, because a double
# that has lost digits cannot say which ones.
#
# Each file is read only as far as its first test module: a test may name a
# float to assert about it.
#
# Usage: check-no-float-money.sh [check|plant] [root]

set -euo pipefail

VERB=check
if [[ $# -gt 0 ]]; then
    case "$1" in check|plant) VERB="$1"; shift ;; esac
fi
ROOT="${1:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
PLANT="$ROOT/crates/galata-wire/src/event.rs"

if [[ "$VERB" == plant ]]; then
    python3 - "$PLANT" <<'PLANTPY'
import re, sys, pathlib
path = pathlib.Path(sys.argv[1])
text = path.read_text()
violation = (
    "\n// planted by check-no-float-money.sh\n"
    "pub struct PlantedPrice {\n"
    "    /// A price that has already lost digits.\n"
    "    pub price: f64,\n"
    "}\n"
)
found = re.search(r"#\[cfg\((?:test\)|all\(\s*test\b)", text)
at = found.start() if found else len(text)
path.write_text(text[:at] + violation + text[at:])
PLANTPY
    echo "planted in $PLANT" >&2
    exit 0
fi

python3 - "$ROOT" <<'PY'
import pathlib, re, sys, tomllib

root = pathlib.Path(sys.argv[1])
problems = []

# **Where the tests begin, however the attribute is spelled.** A literal
# `#[cfg(test)]` split misses `#[cfg(all(test, feature = "x"))]`, and the miss
# is backwards: tests get scanned as shipped code.
TESTS = re.compile(r"#\[cfg\((?:test\)|all\(\s*test\b)")


def shipped_only(text):
    found = TESTS.search(text)
    return text[: found.start()] if found else text


# 1. No float field in the vocabulary.
#
# The ONE documented exception: a venue that sends a bare JSON number gives
# serde a double, and `visit_f64` renders it straight back to text. That is the
# boundary where a float arrives from outside, not one this tree creates.
FIELD = re.compile(r"^\s*(?:pub\s+)?[A-Za-z_][A-Za-z0-9_]*\s*:\s*(?:Option<)?f(?:32|64)\b")
for path in sorted((root / "crates" / "galata-wire" / "src").rglob("*.rs")):
    shipped = shipped_only(path.read_text())
    for number, line in enumerate(shipped.split("\n"), 1):
        if FIELD.match(line):
            problems.append(
                f"{path.relative_to(root)}:{number}: a float field in the vocabulary — "
                f"{line.strip()}. This type crosses the bus, the parquet schema and the "
                f"HTTP contract, and a double is wrong in all three."
            )
    for number, line in enumerate(shipped.split("\n"), 1):
        if re.search(r"\bfn\s+visit_f64\b", line):
            continue
        if re.search(r"\bas\s+f(?:32|64)\b", line):
            problems.append(
                f"{path.relative_to(root)}:{number}: casts to a float — {line.strip()}"
            )

# 2. The decimal must not serialise through a double.
manifest = tomllib.loads((root / "Cargo.toml").read_text())
deps = manifest.get("workspace", {}).get("dependencies", {})
decimal = deps.get("rust_decimal")
features = decimal.get("features", []) if isinstance(decimal, dict) else []
if "serde-float" in features:
    problems.append(
        "Cargo.toml: rust_decimal has `serde-float`, which routes every Num through a "
        "double — no type changes and no warning. The library documents it as wrong for "
        "precision-critical data."
    )
if "serde-str" not in features:
    problems.append(
        "Cargo.toml: rust_decimal is missing `serde-str`. Without it a Num serialises as "
        "a JSON number, which a consumer's parser reads back as a double."
    )

# 3. No float column in a columnar schema.
for path in sorted((root / "crates").rglob("*.rs")):
    if "tape/schema.rs" in path.as_posix() or "galata-segments" in path.as_posix():
        shipped = shipped_only(path.read_text())
        for number, line in enumerate(shipped.split("\n"), 1):
            if re.search(r"DataType::Float(?:16|32|64)\b", line):
                problems.append(
                    f"{path.relative_to(root)}:{number}: a float column — {line.strip()}"
                )

if problems:
    for problem in problems:
        print(f"check-no-float-money: {problem}", file=sys.stderr)
    sys.exit(1)
PY
