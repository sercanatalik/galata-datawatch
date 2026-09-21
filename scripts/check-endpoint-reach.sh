#!/usr/bin/env bash
#
# Three rules about endpoints, none of which the compiler can hold.
#
#   1. NO ERROR MESSAGE CARRIES A URL. A keyed provider puts its key in the
#      URL PATH — `https://arb-mainnet.g.alchemy.com/v2/<KEY>` — so the URL is
#      the credential, and the usual remedy of stripping a query string
#      protects nothing. Every `#[error(...)]` message in this tree is checked
#      for a `{url}`-shaped field.
#
#      **The rule is absolute on purpose.** Most of these endpoints are
#      compiled-in public addresses that could safely print. A rule with an
#      exception is a rule nobody can check, and the exception is where the
#      next keyed URL will be added.
#
#   2. A `reqwest::Error` IS REDACTED WHERE IT IS BUILT. Measured on reqwest
#      0.13.5, its `Display` carries the whole URL, path and query.
#      `without_url()` removes it and costs nothing diagnostic. Applied at
#      construction, never at printing: a redaction that has to be applied at
#      every print site is one the missed site does not apply. So an error
#      variant holding a `reqwest::Error` is built by a constructor, and a
#      struct literal for one is refused.
#
#   3. `Endpoint::expose` IS CALLED ONLY WHERE SOMETHING CONNECTS. The whole
#      point of the type is that `Display` is the only other way out.
#
# Each file is read only as far as its first `#[cfg(test)]`: a test may name
# whatever it needs to assert about, and a violation appended after the tests
# is not a violation of the shipped code.
#
# Usage: check-endpoint-reach.sh [check|plant] [root]

set -euo pipefail

VERB=check
if [[ $# -gt 0 ]]; then
    case "$1" in check|plant) VERB="$1"; shift ;; esac
fi
ROOT="${1:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
PLANT="$ROOT/crates/galata-datawatch/src/source/mod.rs"

if [[ "$VERB" == plant ]]; then
    # **Before the first `#[cfg(test)]`.** The check stops at the tests, so a
    # violation appended after them lands in the region it deliberately
    # ignores — and the planted run would pass while reporting the guard as
    # broken. That has happened in this tree.
    python3 - "$PLANT" <<'PLANTPY'
import sys, pathlib
path = pathlib.Path(sys.argv[1])
text = path.read_text()
violation = (
    "\n// planted by check-endpoint-reach.sh\n"
    "#[derive(Debug, thiserror::Error)]\n"
    "#[error(\"reaching {url}\")]\n"
    "pub struct Planted {\n"
    "    /// Where.\n"
    "    pub url: String,\n"
    "}\n"
)
marker = "#[cfg(test)]"
at = text.index(marker) if marker in text else len(text)
path.write_text(text[:at] + violation + text[at:])
PLANTPY
    echo "planted in $PLANT" >&2
    exit 0
fi

python3 - "$ROOT" <<'PY'
import pathlib, sys, re

root = pathlib.Path(sys.argv[1])

# Where a URL is allowed to leave the type: a client is built, or a socket is
# dialled. Nowhere else.
EXPOSE_ALLOWED = {
    "crates/galata-datawatch/src/source/stream.rs",
    "crates/galata-datawatch/src/adapters/rh_chain/client.rs",
    "crates/galata-datawatch/src/venue/transport.rs",   # the accessor itself
    "crates/galata-datawatch/src/config/source.rs",     # Secret::expose lives here
    "crates/galata-datawatch/src/bin/galata-datawatch.rs",  # hands the broker its password
}
# A field name in an error message that would carry an endpoint.
ENDPOINT_FIELD = re.compile(r"\{\s*(url|rpc_url|rest_url|ws_url|uri|endpoint_url)\b")

problems = []
for path in sorted(root.glob("crates/**/*.rs")):
    relative = path.relative_to(root).as_posix()
    if "/examples/" in relative or "/tests/" in relative:
        continue
    text = path.read_text()
    shipped = text.split("#[cfg(test)]", 1)[0]

    # 1. No error message carries a URL.
    for match in re.finditer(r"#\[error\((.*?)\)\]", shipped, re.S):
        message = match.group(1)
        found = ENDPOINT_FIELD.search(message)
        if found:
            line = shipped[: match.start()].count("\n") + 1
            problems.append(
                f"{relative}:{line}: an error message interpolates {{{found.group(1)}}}. "
                f"A keyed provider carries its key in the URL PATH, so an error that "
                f"prints a URL prints the credential. Carry the endpoint's safe label."
            )

    # 2. A reqwest::Error is redacted where it is built.
    if "reqwest::Error" in shipped:
        if "without_url()" not in shipped:
            problems.append(
                f"{relative}: holds a reqwest::Error and never calls without_url(). "
                f"Its Display carries the whole URL, path and query."
            )
        # A struct literal for the variant that holds one, outside the
        # constructor that redacts.
        for match in re.finditer(r"(\w+)::Http\s*\{", shipped):
            line = shipped[: match.start()].count("\n") + 1
            context = shipped[match.start() : match.end() + 260]
            if "without_url()" not in context:
                problems.append(
                    f"{relative}:{line}: builds {match.group(1)}::Http as a struct literal "
                    f"without redacting. Use the constructor, so a leaking value never "
                    f"enters the type."
                )

    # 3. expose() only where something connects.
    if relative not in EXPOSE_ALLOWED and ".expose()" in shipped:
        line = shipped[: shipped.index(".expose()")].count("\n") + 1
        problems.append(
            f"{relative}:{line}: calls expose() outside the places that connect. "
            f"Display is the only other way out of an Endpoint, and it is the safe one."
        )

if problems:
    for p in problems:
        print(f"check-endpoint-reach: {p}", file=sys.stderr)
    sys.exit(1)
PY
