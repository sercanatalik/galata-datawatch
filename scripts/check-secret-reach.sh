#!/usr/bin/env bash
#
# Three rules about secrets, none of which the compiler can hold.
#
#   1. A SECRET IS READ IN ONE PLACE. `SecretSource::secret` is the door, and
#      `config/source.rs` is the only module that reads the environment for
#      one. One call site is one place to get the logging wrong; three is
#      three, and the third will be added by somebody who did not read the
#      design.
#
#   2. NO VAULT AUTHENTICATION VARIABLE IS NAMED HERE. How a vault client
#      authenticates itself is the vault's business, stated once in its own
#      documentation. A copy of that rule here would be a second
#      implementation — and a second implementation of a naming rule does not
#      fail when it drifts, it disagrees.
#
#   3. A TOOL THAT DOES NOT CONNECT NAMES NO SECRET SOURCE. The rebuild, the
#      compactor, the retention sweep and the watch run from a scheduler that
#      hands them no credential; one that reached for a secret source would
#      fail nightly, and the fix on offer would be a key in the scheduler's
#      environment. They build adapters with `AdapterConfig::for_replay`.
#
# Each file is read only as far as its first `#[cfg(test)]`: a test may name
# whatever it needs to assert about, and a violation appended after the tests
# is not a violation of the shipped code.
#
# Usage: check-secret-reach.sh [check|plant] [root]

set -euo pipefail

VERB=check
if [[ $# -gt 0 ]]; then
    case "$1" in check|plant) VERB="$1"; shift ;; esac
fi
ROOT="${1:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
PLANT="$ROOT/crates/galata-datawatch/src/capture/clock.rs"

if [[ "$VERB" == plant ]]; then
    # **Before the first `#[cfg(test)]`, not at the end of the file.**
    #
    # The check reads each file only as far as its tests, so a violation
    # appended after them lands in the region the check deliberately ignores —
    # and the planted run would PASS while reporting the guard as broken. That
    # is a real failure mode: this guard was written with an appending plant
    # and it silently proved nothing.
    python3 - "$PLANT" <<'PLANTPY'
import sys, pathlib
path = pathlib.Path(sys.argv[1])
text = path.read_text()
violation = '\n// planted by check-secret-reach.sh\nfn _planted() { let _ = std::env::var("GV_TOKEN"); }\n'
marker = "#[cfg(test)]"
if marker in text:
    at = text.index(marker)
    path.write_text(text[:at] + violation + text[at:])
else:
    path.write_text(text + violation)
PLANTPY
    echo "planted in $PLANT" >&2
    exit 0
fi

python3 - "$ROOT" <<'PY'
import pathlib, sys, re

# **Where the tests begin, however the attribute is spelled.**
#
# A literal `#[cfg(test)]` split misses `#[cfg(all(test, feature = "x"))]`,
# which a feature-gated test module needs — and the miss is silent and
# BACKWARDS: the tests get scanned as shipped code, so the guard goes red on an
# assertion written to prove its own rule.
TESTS = re.compile(r"#\[cfg\((?:test\)|all\(\s*test\b)")


def shipped_only(text):
    found = TESTS.search(text)
    return text[: found.start()] if found else text


root = pathlib.Path(sys.argv[1])
allowed = "crates/galata-datawatch/src/config/source.rs"
# The vault's own door, which is the vault's to document.
vault_vars = ("GV_TOKEN", "GV_TOKEN_FILE")

problems = []
for path in sorted(root.glob("crates/**/*.rs")):
    relative = path.relative_to(root).as_posix()
    text = path.read_text()
    # Shipped code only.
    shipped = shipped_only(text)

    for var in vault_vars:
        if var in shipped:
            problems.append(
                f"{relative}: names {var}. How a vault client authenticates is the vault's "
                f"rule, stated once in its own documentation; a copy here disagrees rather "
                f"than fails."
            )

    if relative == allowed or "/examples/" in relative:
        continue
    # A secret read outside the one door. `GALATA_CONFIG` is a path, not a
    # secret, so a binary reading it is fine.
    for match in re.finditer(r'env::var\(\s*([^)]*)\)', shipped):
        argument = match.group(1)
        if "GALATA_CONFIG" in argument:
            continue
        problems.append(
            f"{relative}: reads the environment for {argument.strip()} outside "
            f"{allowed}. A secret is read in one place, so there is one place to get the "
            f"logging wrong."
        )

# 3. A TOOL THAT DOES NOT CONNECT NAMES NO SECRET SOURCE. The scheduled lane
#    passes these four no credential, so one that reached for a secret source
#    would fail every night rather than run — and the remedy on offer would be
#    to put a provider key into the scheduler's environment. By list, not by
#    inference: a tool that starts connecting is a diff that moves itself off
#    this list and says why.
NON_CONNECTING = [
    "crates/galata-datawatch/src/bin/galata-tape-rebuild.rs",
    "crates/galata-datawatch/src/bin/galata-compact.rs",
    "crates/galata-datawatch/src/bin/galata-retain.rs",
    "crates/galata-datawatch/src/bin/galata-watch.rs",
]
for relative in NON_CONNECTING:
    path = root / relative
    if not path.exists():
        problems.append(f"{relative}: listed as a tool that does not connect, and not there — the LIST is wrong")
        continue
    shipped = shipped_only(path.read_text())
    for name in ("EnvSecrets", "VaultSecrets", "SecretSource"):
        if name in shipped:
            problems.append(
                f"{relative}: names {name}, and this tool does not connect. Build its adapter "
                f"with AdapterConfig::for_replay, which withholds what connecting needs."
            )

if problems:
    for p in problems:
        print(f"check-secret-reach: {p}", file=sys.stderr)
    sys.exit(1)
PY
