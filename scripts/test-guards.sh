#!/usr/bin/env bash
#
# Every guard is proved able to fail.
#
# A guard nobody has seen fail is a guard nobody knows works. A green check
# that cannot go red is worse than no check, because it is believed.
#
# For each guard: plant the violation it exists to catch, assert it goes red,
# revert, assert it goes green. The tree is restored on success AND on failure,
# by a trap — a harness that can leave a plant behind is a harness that will.
#
# A plant may APPEND a violation or REPLACE a line. Both exist because
# appending to a TOML file lands inside whatever table came last, which is the
# wrong table for anything stated in [package] — and a plant that does not
# actually violate the rule reports the guard as broken when the plant is.
# That distinction cost one iteration of this script to learn.
#
# It also fails when a guard exists with no entry here, so a guard cannot ship
# unproved.
#
# Usage: test-guards.sh [root]

set -euo pipefail
ROOT="${1:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
cd "$ROOT"

PLANTED=""
restore() {
    if [[ -n "$PLANTED" && -f "$PLANTED.guard-backup" ]]; then
        mv "$PLANTED.guard-backup" "$PLANTED"
    fi
}
trap restore EXIT

fails=0

# prove <name> <guard> <file> append <text>
# prove <name> <guard> <file> replace <find> <with>
# prove <name> <guard> <file> own                — the guard plants for itself
#
# `own` exists because a plant must obey the same scanning rule the check does.
# A guard that reads each file only as far as its first `#[cfg(test)]` cannot be
# planted by appending: the violation lands in the region the check deliberately
# ignores, the planted run passes, and the harness reports the GUARD as broken
# when the PLANT is. Such a guard carries its own plant beside its own rule.
# **Each plant reports what it cost**, because the gate's own timing named this
# harness as its largest part and the next question was immediately "which
# plant". The answer is worth having in the output rather than re-derived: a
# plant that recompiles the tree is tens of seconds, and one that edits a
# manifest is nothing.
prove() {
    local name="$1" guard="$2" file="$3" mode="$4"; shift 4
    local began=$SECONDS

    if ! "$guard" >/dev/null 2>&1; then
        echo "  $name: expected green before planting, and the guard is already red" >&2
        fails=$((fails + 1)); return
    fi

    PLANTED="$file"
    cp "$file" "$file.guard-backup"
    if [[ "$mode" == "own" ]]; then
        "$guard" plant
    else
        python3 - "$file" "$mode" "$@" <<'PY'
import sys, pathlib
path, mode = pathlib.Path(sys.argv[1]), sys.argv[2]
text = path.read_text()
if mode == "append":
    text += sys.argv[3]
else:
    find, repl = sys.argv[3], sys.argv[4]
    if find not in text:
        sys.exit(f"the plant's target {find!r} is not in {path} — the PLANT is wrong, not the guard")
    text = text.replace(find, repl, 1)
path.write_text(text)
PY
    fi

    if "$guard" >/dev/null 2>&1; then
        echo "  $name: PLANTED ITS VIOLATION AND STAYED GREEN — the guard does not work" >&2
        fails=$((fails + 1))
    else
        echo "  $name: red on its violation, green without it ($(( SECONDS - began ))s)"
    fi

    restore; PLANTED=""

    if ! "$guard" >/dev/null 2>&1; then
        echo "  $name: still red after the plant was reverted" >&2
        fails=$((fails + 1))
    fi
}

echo "proving each guard can fail:"

# A member re-declaring a version the workspace already declares.
prove "check-workspace-deps (re-declared version)" \
      ./scripts/check-workspace-deps.sh \
      crates/galata-segments/Cargo.toml append \
      $'\n[dev-dependencies.thiserror]\nversion = "2"\n'

# The vocabulary crate's dependency wall — the rule this guard exists for.
prove "check-workspace-deps (galata-wire links arrow)" \
      ./scripts/check-workspace-deps.sh \
      crates/galata-wire/Cargo.toml replace \
      'rust_decimal.workspace = true' \
      $'arrow.workspace = true\nrust_decimal.workspace = true'

# A workspace dependency no member uses.
prove "check-workspace-deps (unused workspace dependency)" \
      ./scripts/check-workspace-deps.sh \
      Cargo.toml append \
      $'\n[workspace.dependencies.hex]\nversion = "0.4"\n'

# A member stating its own licence rather than inheriting it.
prove "check-release-hygiene (own licence)" \
      ./scripts/check-release-hygiene.sh \
      crates/galata-wire/Cargo.toml replace \
      'license.workspace = true' \
      'license = "MIT"'

# A publishable member missing metadata the registry requires.
prove "check-release-hygiene (missing description)" \
      ./scripts/check-release-hygiene.sh \
      crates/galata-segments/Cargo.toml replace \
      'description = "Durable parquet segments' \
      'not-description = "Durable parquet segments'

# The one path cannot be bypassed. This guard plants for itself, for the reason
# `own` exists at all.
prove "check-grant-coverage (a subject root granted to nobody)" \
    "$ROOT/scripts/check-grant-coverage.sh" \
    "$ROOT/crates/galata-broker/src/grants.rs" \
    own

prove "check-no-transport (the pure half reaches for the runtime)" \
    "$ROOT/scripts/check-no-transport.sh" \
    "$ROOT/crates/galata-datawatch/src/calendar.rs" \
    own

# galata-markets given the record writable: it serves it and must not be able
# to change it.
prove "check-container-launch (galata-markets mounts the record writable)" \
    "$ROOT/scripts/check-container-launch.sh" \
    "$ROOT/scripts/run-service.sh" \
    replace '        root_mounts_readonly paths.archive paths.tape' \
    '        root_mounts paths.archive paths.tape'

# galata-markets taking galata-datawatch with its capture loop.
prove "check-markets-no-transport (the service links a venue transport)" \
    "$ROOT/scripts/check-markets-no-transport.sh" \
    "$ROOT/crates/galata-markets/Cargo.toml" \
    replace 'features = ["hyperliquid", "derive-xyz"] }' \
    'features = ["hyperliquid", "derive-xyz", "capture"] }'

# The fourth wall. `own` because the guard plants a dependency, and where a
# dependency is planted matters: [dev-dependencies] would not break the wall,
# so the guard is the only thing that knows the right place to put it.
prove "check-vault-reach (a published crate reaches for the vault)" \
    "$ROOT/scripts/check-vault-reach.sh" \
    "$ROOT/crates/galata-datawatch/Cargo.toml" \
    own

prove "check-secret-reach (a secret read outside the one door)" \
    "$ROOT/scripts/check-secret-reach.sh" \
    "$ROOT/crates/galata-datawatch/src/capture/clock.rs" \
    own

# REPLACE, not append: appending lands in [dev-dependencies], and a dev
# dependency does NOT break this wall — it is not propagated to a consumer. The
# rule is right to check only [dependencies], and the plant has to obey it.
prove "check-workspace-deps (galata-broker links a store)" \
    "$ROOT/scripts/check-workspace-deps.sh" \
    "$ROOT/crates/galata-broker/Cargo.toml" \
    replace 'async-nats.workspace = true' \
    'async-nats.workspace = true
galata-segments = { version = "0.1.0", path = "../galata-segments" }'

prove "check-ingest-callers (a second caller appends)" \
      ./scripts/check-ingest-callers.sh \
      crates/galata-datawatch/src/calendar.rs own

# Exactly one module names a venue. Plants for itself, same reason.
prove "check-venue-boundary (a venue named outside the adapters tree)" \
      ./scripts/check-venue-boundary.sh \
      crates/galata-datawatch/src/sink.rs own

# Every deletion is listed with why it is safe. Plants for itself: an unlisted
# remove_file before the first #[cfg(test)].
prove "check-deletion-paths (an unlisted deletion)" \
      ./scripts/check-deletion-paths.sh \
      crates/galata-datawatch/src/calendar.rs own

# Nothing below the loop reads a clock. Plants for itself.
prove "check-clock-discipline (a clock reading below the loop)" \
      ./scripts/check-clock-discipline.sh \
      crates/galata-datawatch/src/record/mod.rs own

# What a publish would actually ship. Plants for itself: it drops the licence
# from the include whitelist, which still builds and still publishes.
# The changelog's newest release is the version that will publish. Plants for
# itself: the fault is a DISAGREEMENT between two files, which an append
# cannot produce.
prove "check-changelog (a version the workspace does not have)" \
      ./scripts/check-changelog.sh \
      CHANGELOG.md own

# The release document names exactly the crates that publish. Two plants, for
# the two directions: a crate that publishes and is not named, and a name that
# does not publish. One direction would rot in the direction nobody notices.

# 1. A publishable crate the document forgets. Plants for itself: the fault is
#    an ABSENCE, which an append cannot produce.
prove "check-release-doc (a crate that publishes and is not named)" \
      ./scripts/check-release-doc.sh \
      RELEASING.md own

# 2. The private crate claimed as published — the wall that keeps the four
#    vault-free, described away.
prove "check-release-doc (a name that does not publish)" \
      ./scripts/check-release-doc.sh \
      RELEASING.md \
      replace '| `galata-datawatch-vault` | **no** | `publish = false` |' \
      '| `galata-datawatch-vault` | yes | `publish = false` |'

prove "check-package (a crate that would ship without its licence)" \
      ./scripts/check-package.sh \
      crates/galata-wire/Cargo.toml own

# Every publishable crate builds from what it would ship. Plants for itself:
# it narrows `include` to one source file, which still COMPILES in the
# workspace — every file is on disk — and cannot compile from the tarball,
# where six of seven modules are absent. check-package.sh cannot see that: the
# licence, the README and some source all still ship.
prove "check-tarball-builds (a crate whose tarball is missing its modules)" \
      ./scripts/check-tarball-builds.sh \
      crates/galata-broker/Cargo.toml own

# The second half: `cargo package` verifies DEFAULT FEATURES and offers no way
# to ask for another set, so the guard builds the shapes somebody else takes
# from the same unpacked tarballs. This plant compiles perfectly with defaults
# and not without them — which is the configuration galata-tower takes, and the
# one nobody develops in.
prove "check-tarball-builds (what ships fails without default features)" \
      ./scripts/check-tarball-builds.sh \
      crates/galata-datawatch/src/lib.rs \
      replace 'pub mod config;' \
      'pub mod config;
#[cfg(not(feature = "capture"))]
const _PLANTED: () = this_does_not_exist();'

# The documentation builds clean. Plants for itself: it must insert before the
# tests, and an appended broken link would land in a region cargo doc still
# reads — but the plant belongs beside the rule either way.
prove "check-docs (a link to something that does not exist)" \
      ./scripts/check-docs.sh \
      crates/galata-wire/src/lib.rs own

# A binary that sweeps a declared store must check it can be read. Plants for
# itself: it must REPLACE the call, which an append cannot do.
prove "check-scannable-roots (a binary that sweeps without checking)" \
      ./scripts/check-scannable-roots.sh \
      crates/galata-datawatch/src/bin/galata-watch.rs own

# Money never becomes a float. Three rules, three plants.

# 1. A float field in the vocabulary. Plants for itself, before the tests.
prove "check-no-float-money (a float field in the vocabulary)" \
      ./scripts/check-no-float-money.sh \
      crates/galata-wire/src/event.rs own

# 2. The serde feature that routes every Num through a double.
prove "check-no-float-money (rust_decimal serialising as a float)" \
      ./scripts/check-no-float-money.sh \
      Cargo.toml \
      replace 'features = ["serde-str"]' \
      'features = ["serde-str", "serde-float"]'

# 3. A float column in a columnar schema.
prove "check-no-float-money (a float column)" \
      ./scripts/check-no-float-money.sh \
      crates/galata-datawatch/src/tape/schema.rs \
      replace 'Field::new("tick_size", PRICE, false),' \
      'Field::new("tick_size", DataType::Float64, false),'

# Every feature combination builds. Plants for itself: it must remove a cfg,
# which an append cannot do.
prove "check-feature-matrix (a venue-less build stops compiling)" \
      ./scripts/check-feature-matrix.sh \
      crates/galata-datawatch/src/adapters/mod.rs own

# The SECOND plant, and it is the defect this guard was widened for: the cursor
# branch called a method behind `rh-chain` with no gate, and `cargo build` with
# no flags did not compile while this guard was green. `cfg(all())` is always
# true, so the branch compiles unconditionally while BLOCKS_PER_DAY beside it
# stays gated — which is exactly how it failed.
#
# `replace` takes the FIRST occurrence, which is the cursor branch. If that ever
# stops being first the plant lands on the constant instead, the guard stays
# green, and `prove` says so loudly rather than passing.
prove "check-feature-matrix (the cursor branch loses its feature gate)" \
      ./scripts/check-feature-matrix.sh \
      crates/galata-datawatch/src/boot.rs \
      replace '#[cfg(feature = "rh-chain")]' \
      '#[cfg(all())]'

# A publishable crate must say how docs.rs builds it. REPLACE, not append:
# appending lands in whatever table came last, which is not [package].
prove "check-release-hygiene (no docs.rs metadata)" \
    "$ROOT/scripts/check-release-hygiene.sh" \
    "$ROOT/crates/galata-wire/Cargo.toml" \
    replace 'all-features = true' \
    'all-features = false'

# Three rules, three plants. A guard with three rules and one proof is a guard
# two thirds of which nobody has seen fail.

# 1. No error message carries a URL. Plants for itself, before the tests.
prove "check-endpoint-reach (an error message carries a URL)" \
      ./scripts/check-endpoint-reach.sh \
      crates/galata-datawatch/src/source/mod.rs own

# 2. A reqwest::Error built as a struct literal, skipping the redaction.
prove "check-endpoint-reach (a reqwest::Error stored unredacted)" \
      ./scripts/check-endpoint-reach.sh \
      crates/galata-datawatch/src/adapters/rh_chain/client.rs \
      replace '.map_err(|source| ChainError::http(method, source))?;' \
      '.map_err(|source| ChainError::Http { venue: VENUE, method, source })?;'

# 3. expose() somewhere that does not connect.
prove "check-endpoint-reach (expose outside a connect site)" \
      ./scripts/check-endpoint-reach.sh \
      crates/galata-datawatch/src/capture/cursor.rs \
      replace '        let client = ChainClient::new(rpc_url);' \
      '        let _planted = rpc_url.expose().to_string();
        let client = ChainClient::new(rpc_url);'

# 4. The ledger's loop reaches for a secret source.
prove "check-secret-reach (the ledger's loop holds a secret source)" \
      ./scripts/check-secret-reach.sh \
      crates/galata-datawatch/src/ledger/run.rs \
      replace 'use crate::config::Secret;' \
      'use crate::config::{Secret, SecretSource as _Planted};'

# An address exposed where it may become a name. Plants for itself: an address
# written into a partition path, the leak `ledger-accounts-and-snapshots` D1
# exists to prevent.
prove "check-no-address-in-names (an address written into a path)" \
      ./scripts/check-no-address-in-names.sh \
      crates/galata-datawatch/src/record/mod.rs own

# The scheduling lane: five rules, five plants.

# 1. An import outside the list — `os`, the door an inherited environment
#    comes through.
prove "check-python-flows (a flow imports os)" \
      ./scripts/check-python-flows.sh \
      py/flows/watch.py replace \
      'from __future__ import annotations' \
      $'from __future__ import annotations\n\nimport os'

# 2. A second module that spawns.
prove "check-python-flows (subprocess outside the runner)" \
      ./scripts/check-python-flows.sh \
      py/flows/compact.py replace \
      'from __future__ import annotations' \
      $'from __future__ import annotations\n\nimport subprocess'

# 3. A spawn that inherits the scheduler's environment.
prove "check-python-flows (a spawn without env=)" \
      ./scripts/check-python-flows.sh \
      py/flows/_runner.py replace \
      $'        env=job_env(config) | {name: str(Path(path).resolve()) for name, path in (paths or {}).items()},\n' \
      ''

# 4. The retention flow asked to delete.
prove "check-python-flows (a flow passes --delete)" \
      ./scripts/check-python-flows.sh \
      py/flows/retain.py replace \
      '_runner.run("galata-retain", [], config)' \
      '_runner.run("galata-retain", ["--delete"], config)'

# 5. A credential handed to a job.
prove "check-python-flows (a job is handed GV_TOKEN)" \
      ./scripts/check-python-flows.sh \
      py/flows/_runner.py replace \
      '"NO_COLOR": NO_COLOR}' \
      '"NO_COLOR": NO_COLOR, "GV_TOKEN": ""}'

# A tool that does not connect reaching for a secret source — the rebuild, which
# the scheduled lane runs with no credential in its environment.
prove "check-secret-reach (a tool that does not connect names a secret source)" \
      ./scripts/check-secret-reach.sh \
      crates/galata-datawatch/src/bin/galata-tape-rebuild.rs replace \
      'use galata_datawatch::config::{Adapters, Config, FileSource};' \
      'use galata_datawatch::config::{Adapters, Config, EnvSecrets, FileSource};'

# A session key's private half shown on the way to the vault.
prove "check-session-key-script (the key is shown as it is stored)" \
      ./scripts/check-session-key-script.sh \
      scripts/make-derive-session-key.sh replace \
      'if ! "$bin" | "$gv" set "$name" --env "$env" >/dev/null; then' \
      'if ! "$bin" | tee /dev/stderr | "$gv" set "$name" --env "$env" >/dev/null; then'

# The floor that keeps an older gv, which reads through the owner key, away
# from a service's token.
prove "check-service-secrets (the version floor is lowered)" \
      ./scripts/check-service-secrets.sh \
      scripts/lib/vault-tools.sh replace \
      'GV_FLOOR="0.5.0"' \
      'GV_FLOOR="0.4.0"'

# A floor compared as text, under which 0.10.0 sorts below 0.5.0.
prove "check-service-secrets (versions compared as text)" \
      ./scripts/check-service-secrets.sh \
      scripts/lib/vault-tools.sh replace \
      'if ! version_at_least "${have:-0}" "$GV_FLOOR"; then' \
      'if [[ "${have:-0}" < "$GV_FLOOR" ]]; then'

# A broker identity's dash surviving into a secret name, which gv run skips.
prove "check-service-secrets (a secret name that cannot be a variable)" \
      ./scripts/check-service-secrets.sh \
      scripts/lib/vault-tools.sh replace \
      'local id="${1//-/_}"' \
      'local id="$1"'

# A script reaching back into a galata-vault checkout.
prove "check-service-secrets (a script runs gv from a checkout)" \
      ./scripts/check-service-secrets.sh \
      scripts/provision-vault.sh append \
      $'\n# GV="$GALATA_VAULT_ROOT/target/release/gv"\n'

# A value left in the template a fresh vault project is filled from.
prove "check-service-secrets (the template holds a value)" \
      ./scripts/check-service-secrets.sh \
      config/secrets.env.example replace \
      'GALATA_BROKER_PASSWORD_READER=' \
      'GALATA_BROKER_PASSWORD_READER=hunter2'

# A secret a service reads, missing from the template.
prove "check-service-secrets (the template lacks a name)" \
      ./scripts/check-service-secrets.sh \
      config/secrets.env.example replace \
      $'\n# read by: nats, tower\nGALATA_BROKER_PASSWORD_READER=\n' \
      $'\n'

# A secret handed to a container with -e, which Apple's container writes into
# its own state, where `container inspect` prints it.
prove "check-container-launch (a secret passed with -e)" \
      ./scripts/check-container-launch.sh \
      scripts/run-service.sh replace \
      'in_container "capture:$venue" "$DATAWATCH_IMAGE" -i "${ROOT_MOUNTS[@]}" "${CONFIG_MOUNT[@]}"' \
      'in_container "capture:$venue" "$DATAWATCH_IMAGE" -i -e "$secret" "${ROOT_MOUNTS[@]}" "${CONFIG_MOUNT[@]}"'

# The folder that holds every service's token, mounted into one container.
prove "check-container-launch (the token folder is mounted)" \
      ./scripts/check-container-launch.sh \
      scripts/run-service.sh replace \
      'CONFIG_MOUNT=(--mount "type=bind,source=$dir,target=/app/config,readonly"' \
      'CONFIG_MOUNT=(-v "$ROOT/var/tokens:/app/tokens" --mount "type=bind,source=$dir,target=/app/config,readonly"'

# A port published with no address, which binds every interface of the Mac.
# After the `esac`, where nothing runs it: the guard reads the launcher's text.
prove "check-container-launch (a port published off the tailnet)" \
      ./scripts/check-container-launch.sh \
      scripts/run-service.sh append \
      $'\nin_container nats "$DATAWATCH_IMAGE" --publish 4222:4222\n'

# A limit left to the default: a line with CPUs and no memory, accepted.
prove "check-container-launch (half a limit is accepted)" \
      ./scripts/check-container-launch.sh \
      scripts/lib/container-tools.sh replace \
      'if [[ -z "$memory" ]]; then' \
      'if false; then'

# A service with no line started anyway, at the default the limits exist to
# avoid.
prove "check-container-launch (a service with no line starts at the default)" \
      ./scripts/check-container-launch.sh \
      scripts/lib/container-tools.sh replace \
      $'\n        echo "$conf states no CPU or memory limit for $spec, so it does not start' \
      $'\n        printf \'4 1G\'; return 0\n        echo "$conf states no CPU or memory limit for $spec, so it does not start'

# The broker handed a fixed list of identities that the generated
# authorization file has outgrown: the server refuses a password variable it
# is not given, so the broker would not restart (Derive's, 2026-11-25).
prove "check-container-launch (the broker is handed fewer identities than its file names)" \
      ./scripts/check-container-launch.sh \
      scripts/run-service.sh replace \
      $'done < <(awk \'$1 == "user:" { print $2 }\' "$ROOT/config/nats-authorization.conf")' \
      $'done < <(printf \'%s\\n\' datawatch-hyperliquid datawatch-rh-chain reader)'

# The broker's store put in var/, beside the record and the tokens, instead of
# on a volume of its own.
prove "check-container-launch (the broker's store is a folder of var/)" \
      ./scripts/check-container-launch.sh \
      scripts/run-service.sh replace \
      '-v "$NATS_STORE:/data"' \
      '-v "$ROOT/var/nats-store:/data"'

# A stopped tailnet giving its last address, to publish on.
prove "check-container-launch (a stopped tailnet gives an address)" \
      ./scripts/check-container-launch.sh \
      scripts/lib/container-tools.sh replace \
      'if [[ "$state" != Running ]]; then' \
      'if false; then'

# var/ sent to an image build, with every service's token in it.
prove "check-container-launch (var/ in the build context)" \
      ./scripts/check-container-launch.sh \
      .dockerignore replace \
      $'\nvar/\n' \
      $'\n'

# Every guard must have an entry above.
listed=$(grep -c '^prove "' "$0" || true)
present=$(find scripts -maxdepth 1 -name 'check-*.sh' ! -name 'check-all.sh' | wc -l | tr -d ' ')
if (( listed < present )); then
    echo "  $present guards exist and only $listed proofs are listed — a guard cannot ship unproved" >&2
    fails=$((fails + 1))
fi

if (( fails > 0 )); then
    echo "$fails guard proof(s) failed" >&2
    exit 1
fi
echo "every guard was observed failing on its own violation"
