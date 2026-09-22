#!/usr/bin/env bash
#
# EVERY PUBLISHABLE CRATE BUILDS FROM WHAT IT WOULD SHIP.
#
# `check-package.sh` answers *what would the tarball contain*. This answers the
# other half: *does that tarball compile* — against its siblings' TARBALLS,
# not against the path dependencies this workspace supplies.
#
# The distinction is the whole reason this exists. Inside a workspace cargo
# prefers the path dependency, so a crate can compile perfectly here while
# using a sibling change its own manifest does not require. It publishes, and
# the first person to find out is a stranger who ran `cargo add`. A crates.io
# version is permanent by then.
#
# `cargo package --workspace` builds a temporary registry under
# `target/package`, publishes each crate into it, unpacks every tarball and
# compiles each one against the PACKAGED versions of the rest. Until that
# existed, two of this workspace's four crates could not be verified before
# publishing at all, and check-package.sh's header said so.
#
# **This passes today**, which is the moment to hold it: a guard added while
# it is green is a guard that was never a bug report.
#
# WHAT IT DOES NOT CHECK: verification builds DEFAULT FEATURES. A tarball that
# compiles with defaults and fails with `default-features = false` passes here
# — and that is the combination galata-tower takes.
# `check-feature-matrix.sh` holds feature combinations; this holds packaging.
# A guard a reader could take for more than it is would be worse than none.
#
# `--allow-dirty` because the gate runs mid-change. A packaging check that
# refused uncommitted work would be unavailable during exactly the edit that
# breaks packaging; it checks what is on disk, which is what is about to be
# committed.
#
# Measured on this workspace: about 19s warm, about 95s on a cold tree.
#
# Usage: check-tarball-builds.sh [check|plant] [root]

set -euo pipefail

VERB=check
if [[ $# -gt 0 ]]; then
    case "$1" in check|plant) VERB="$1"; shift ;; esac
fi
ROOT="${1:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
PLANT="$ROOT/crates/galata-broker/Cargo.toml"

if [[ ! -f "$ROOT/Cargo.toml" ]]; then
    echo "$(basename "$0"): $ROOT is not a workspace — refusing to scan nothing and call it ok" >&2
    exit 2
fi

if [[ "$VERB" == plant ]]; then
    # **The exact failure this guard is for, and only this guard.** Narrowing
    # `include` from `/src/**` to the one file `include` is usually checked
    # for leaves a crate that compiles perfectly in the workspace — every
    # source file is on disk — and cannot compile from its tarball, where six
    # of its seven modules are simply absent.
    #
    # check-package.sh cannot see it: its whitelist rule asks whether the
    # licence, the README and SOME source ship, and they all do. Only building
    # the tarball finds it, which is the point.
    python3 - "$PLANT" <<'PLANTPY'
import sys, pathlib
path = pathlib.Path(sys.argv[1])
text = path.read_text()
marker = 'include = ["/src/**", "/Cargo.toml", "/README.md", "/LICENSE-MIT"]'
assert marker in text, "the plant's target moved — the PLANT is wrong, not the guard"
replacement = 'include = ["/src/lib.rs", "/Cargo.toml", "/README.md", "/LICENSE-MIT"]'
path.write_text(text.replace(marker, replacement, 1))
PLANTPY
    echo "planted in $PLANT" >&2
    exit 0
fi

cd "$ROOT"

if ! output=$(cargo package --workspace --allow-dirty 2>&1); then
    echo "tarball builds: a crate does not build from what it would ship" >&2
    # The compiler's own words, and the crate it was compiling. A guard that
    # reported only "packaging failed" would send somebody to run the command
    # themselves to find out what this already knows.
    echo "$output" | grep -E "^(error|   Verifying|error\[)" | head -12 >&2
    exit 1
fi

built=$(echo "$output" | grep -cE "^   Verifying " || true)
echo "tarball builds: ok. $built crate(s) unpacked and compiled from their tarballs, siblings resolved from the packaged registry (default features; check-feature-matrix.sh holds the rest)"
