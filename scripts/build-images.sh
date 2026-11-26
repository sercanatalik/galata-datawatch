#!/usr/bin/env bash
#
# Build the images the containerised services run.
#
#   build-images.sh [--dirty] [image ...]
#
#   images: datawatch  nats  flows  markets     (default: all four)
#
#   datawatch   capture, the ledger and the maintenance binaries
#   nats        the broker, with the door its passwords enter by
#   flows       the scheduling lane and the binaries its flows spawn
#   markets     galata-markets, the one door for market data
#
# **An image is built from a tree someone can name.** Each is labelled with
# `git describe` (org.galata.source-tree), which `install-services.sh
# --status` reports beside a running container. So a tree with uncommitted
# work is refused: its label would name a commit the image was not built
# from. `--dirty` builds it anyway and says so in the label; that is for
# trying a change, not for a deployment.
#
# **The builder's size is stated, never left to its default.** Measured
# 2026-11-24, Apple container 1.7.1: at the default 2 GB the linker is killed
# linking galata-datawatch (the release profile keeps debug information); at
# 12 GB the same build takes 51 s. The builder also returns to its default
# whenever a build names no size, so every build here names one.
# GALATA_BUILDER_CPUS and GALATA_BUILDER_MEMORY move it.
#
# Building is not deploying: a running service keeps the image it started
# with until it is restarted (scripts/install-services.sh).

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=scripts/lib/container-tools.sh
source "$ROOT/scripts/lib/container-tools.sh"
cd "$ROOT"

refuse() { echo "build-images: REFUSED — $1" >&2; exit 1; }

DIRTY=0
if [[ "${1:-}" == --dirty ]]; then DIRTY=1; shift; fi
images=("$@")
(( ${#images[@]} )) || images=(datawatch nats flows markets)

tool="$(container_tool 2>&1)" || refuse "$tool"
said="$(container_running "$tool" 2>&1)" || refuse "$said"

# Untracked files count: a build is sent every file .dockerignore does not
# name, tracked or not.
tree="$(git describe --always 2>/dev/null)" || refuse "this is not a git checkout, so an image built here could not say what it was built from"
if [[ -n "$(git status --porcelain 2>/dev/null)" ]]; then
    (( DIRTY )) || refuse "the tree has uncommitted work, so its images would be labelled $tree and not be $tree — commit, or pass --dirty to build it for a trial"
    tree="$tree-dirty"
fi

cpus="${GALATA_BUILDER_CPUS:-8}"
memory="${GALATA_BUILDER_MEMORY:-12G}"

for image in "${images[@]}"; do
    case "$image" in
        datawatch) file=deploy/container/Containerfile ;;
        nats) file=deploy/container/Containerfile.nats ;;
        flows) file=deploy/container/Containerfile.flows ;;
        markets) file=deploy/container/Containerfile.markets ;;
        *) refuse "unknown image '$image' (datawatch, nats, flows, markets)" ;;
    esac
    [[ -f "$file" ]] || refuse "no $file"
    echo "building galata-$image:latest from $tree"
    "$tool" build -t "galata-$image:latest" -f "$file" \
        --build-arg "SOURCE_TREE=$tree" --cpus "$cpus" --memory "$memory" . \
        || refuse "galata-$image did not build"
    # Read back, not assumed: the label is what --status will report.
    labelled="$(image_source_tree "$tool" "galata-$image:latest" 2>&1)" \
        || refuse "galata-$image:latest was built and carries no source tree: $labelled"
    [[ "$labelled" == "$tree" ]] \
        || refuse "galata-$image:latest says it was built from '$labelled', and this tree is '$tree'"
    echo "built galata-$image:latest, labelled $labelled"
done
