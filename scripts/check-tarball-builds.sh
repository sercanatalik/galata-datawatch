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
# **This passed today**, which was the moment to hold it: a guard added while
# it is green is a guard that was never a bug report. It also passed for a
# reason it did not have — see the cache note below, found 2026-09-23 when it
# failed on source that was no longer in the tree.
#
# `cargo package` has no feature flags — verification builds DEFAULT FEATURES
# and there is no way to ask it for another set. So the configurations anybody
# else actually takes are built separately, from the same unpacked tarballs:
#
#   --no-default-features        what galata-tower takes, called "the whole
#                                dependency argument" in its own manifest. It
#                                takes these crates by PATH today and says they
#                                "become registry dependencies when its Tier 10
#                                lands" — so this shape had never been compiled
#                                from a tarball at all.
#   --features <venue>           the line this crate's README tells a reader to
#                                type: `cargo add galata-datawatch --features
#                                rh-chain`.
#
# **What that adds over `check-feature-matrix.sh`, precisely.** The matrix
# builds fourteen combinations in the WORKSPACE, against path dependencies: it
# proves the CODE compiles. This proves the PACKAGE does. The difference is the
# manifest, which cargo rewrites when it packages — path dependencies stripped,
# features kept. What it catches is a feature that resolves through the
# workspace and not through a registry.
#
# WHAT IT STILL DOES NOT CHECK, and the previous header overstated: *which
# files ship* cannot vary by feature in this tree. Every publishable crate's
# `include` is `/src/**`, and there is no `include_str!` or `include_bytes!`
# anywhere — both greped for, 2026-09-23 — so no file is reachable under one
# feature and absent under another. A guard a reader could take for more than
# it is would be worse than none.
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

# **THIS GUARD WAS VERIFYING SOURCE THAT NO LONGER EXISTED.**
#
# Cargo identifies a registry crate by name and version, and every crate here
# is 0.1.0 and stays 0.1.0. Two caches key on that pair and neither notices
# when the content behind it changes:
#
#   1. the EXTRACTED SOURCE, `$CARGO_HOME/registry/src/<hash>/<name>-<ver>/`,
#      reused whenever the directory already exists;
#   2. the COMPILED ARTIFACT in the target directory. Cargo normalises an
#      extracted registry file's mtime to a fixed date — Jul 24 2006, which is
#      what `ls` shows — so the fingerprint is identical for old and new
#      content and the cached `.rlib` is reused. This one survives deleting
#      the first.
#
# Observed 2026-09-23. A variant added to `galata-datawatch` was in the
# freshly built tarball, in the freshly extracted source, AND STILL the
# dependent crate compiled against a stale rlib and failed on code that was
# nowhere in the tree. `--verbose` named it:
# `--extern galata_datawatch=target/debug/deps/libgalata_datawatch-*.rlib`.
#
# **The direction that matters is the other one.** A sibling change that
# BREAKS a dependent would have been verified against last week's artifact and
# reported green — which is precisely the failure this guard exists to catch,
# so it was a guard that could not have caught it.
#
# Both caches are purged for THIS WORKSPACE's own crates, and only those: they
# are unpublished, nothing else can supply them, and the very command below
# reproduces them. The names come from cargo rather than a list here that would
# go stale.
# The version a member carries, asked of cargo rather than written here — this
# tree's crates are all 0.1.0 and a literal would be right until the day it
# silently was not.
version_of() {
    local want="$1" spec
    for spec in $members; do
        [[ "${spec%-*}" == "$want" ]] && { echo "${spec##*-}"; return 0; }
    done
    return 1
}

members=$(cargo metadata --no-deps --format-version 1 | python3 -c 'import json, sys
for pkg in json.load(sys.stdin)["packages"]:
    print(pkg["name"] + "-" + pkg["version"])')
if [[ -z "$members" ]]; then
    echo "$(basename "$0"): cargo metadata named no workspace member — refusing to skip the purge and call the result a check" >&2
    exit 2
fi

target=$(cargo metadata --no-deps --format-version 1 | python3 -c 'import json, sys
print(json.load(sys.stdin)["target_directory"])')

for spec in $members; do
    name="${spec%-*}"
    underscored="${name//-/_}"
    for dir in "${CARGO_HOME:-$HOME/.cargo}"/registry/src/*/"$spec"; do
        [[ -d "$dir" ]] && rm -rf "$dir"
    done
    # The compiled halves, and the fingerprints that would let cargo skip
    # rebuilding them.
    rm -f "$target"/debug/deps/lib"$underscored"-*.rlib \
          "$target"/debug/deps/lib"$underscored"-*.rmeta 2>/dev/null || true
    rm -rf "$target"/debug/.fingerprint/"$name"-* 2>/dev/null || true
done

if ! output=$(cargo package --workspace --allow-dirty 2>&1); then
    echo "tarball builds: a crate does not build from what it would ship" >&2
    # The compiler's own words, and the crate it was compiling. A guard that
    # reported only "packaging failed" would send somebody to run the command
    # themselves to find out what this already knows.
    echo "$output" | grep -E "^(error|   Verifying|error\[)" | head -12 >&2
    exit 1
fi

built=$(echo "$output" | grep -cE "^   Verifying " || true)

# **Patched to the PACKAGED directories, never to the workspace.**
#
# `cargo package` leaves each crate unpacked beside its tarball, so a `patch`
# pointing there compiles the SHIPPED files with whatever features are asked
# for. The obvious alternative was tried first and is wrong:
# `source.crates-io.replace-with` aimed at `tmp-registry` replaces crates.io
# ENTIRELY, so `arrow` is not found either. A patch redirects only the crates
# under test and leaves third-party resolution alone.
patches=()
for spec in $members; do
    dir="$ROOT/target/package/$spec"
    [[ -d "$dir" ]] || continue
    patches+=(--config "patch.crates-io.${spec%-*}.path='$dir'")
done

# The configurations somebody else takes. `under` is the crate being built;
# the rest are patched in around it.
shapes=(
    "galata-datawatch|--no-default-features|the shape galata-tower takes"
    "galata-datawatch|--no-default-features --features hyperliquid|cargo add --features hyperliquid"
    "galata-datawatch|--no-default-features --features rh-chain|cargo add --features rh-chain, the README's own line"
)

for shape in "${shapes[@]}"; do
    IFS='|' read -r crate flags why <<< "$shape"
    manifest="$ROOT/target/package/$crate-$(version_of "$crate")/Cargo.toml"
    if [[ ! -f "$manifest" ]]; then
        echo "tarball builds: $crate was not unpacked — the packaging step changed shape" >&2
        exit 2
    fi
    # shellcheck disable=SC2086
    if ! shaped=$(cargo check --manifest-path "$manifest" $flags "${patches[@]}" 2>&1); then
        echo "tarball builds: what ships does not build as \`$flags\` — $why" >&2
        echo "$shaped" | grep -E "^(error|error\[)" | head -8 >&2
        exit 1
    fi
done

echo "tarball builds: ok. $built crate(s) compiled from their tarballs with default features, and ${#shapes[@]} configuration(s) a stranger takes — no-default-features (galata-tower's) and the venue features the README advertises — built from the same unpacked sources"
