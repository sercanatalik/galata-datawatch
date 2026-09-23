# Releasing galata-datawatch

Four crates go to crates.io together. One command sends them:

```sh
cargo publish --workspace
```

**Nothing here has been published yet.** The names do not exist on crates.io,
which is why the first run needs a token (below) and every run after it should
not.

## What goes, and what does not

| Crate | Publishes | Why |
|---|---|---|
| `galata-wire` | yes | the vocabulary |
| `galata-segments` | yes | the record's listing, standalone and useful alone |
| `galata-broker` | yes | the bus |
| `galata-datawatch` | yes | the capture, the tape and the venue seam |
| `galata-datawatch-vault` | **no** | `publish = false` |

`galata-datawatch-vault` exists so that the four above take no vault
dependency, and `cargo publish --workspace` skips it without being told —
the same wall `check-vault-reach.sh` holds at build time, holding at
publication time for free.

`scripts/check-release-doc.sh` holds this table to the manifests in both
directions: a crate that publishes and is missing here fails, and so does a
name here that does not publish.

## Cargo does the ordering

crates.io will not take a crate whose dependencies are not already there, and
these depend on each other:

```
  galata-wire        no sibling
  galata-segments    no sibling
  galata-broker      wire
  galata-datawatch   wire, segments, broker
```

`cargo publish --workspace` resolves that itself. Observed on cargo 1.98.1 with
`--dry-run`, which packages and verifies everything and uploads nothing:

```
  Uploading galata-segments -> galata-wire -> galata-broker -> galata-datawatch
  warning: aborting upload due to dry run
```

That is a topological order: the two crates with no sibling dependency first,
then the one needing `wire`, then the one needing all three.

**The advice you will find is older than this cargo.** Publishing crate by
crate in a shell loop, temporarily breaking a dependency to get the first one
out, or installing `cargo-workspaces` — all of it addresses a `cargo publish`
that could not take a workspace. This one can. Run the dry run and read the
order rather than imposing one:

```sh
cargo publish --workspace --dry-run
```

## Before the tag

```sh
./scripts/check-all.sh
```

`check-tarball-builds.sh` is the one that matters most here: it compiles every
crate **from its own tarball against its siblings' tarballs**, in default
features and in the configurations a stranger takes — `--no-default-features`,
which is what galata-tower uses, and the `--features <venue>` lines the README
advertises. Inside the workspace cargo prefers the path dependency, so a crate
can compile here while using a sibling change its published manifest does not
require. A crates.io version is permanent; that guard is the last chance to
find it.

## The first publish needs a token

Trusted publishing cannot create a crate that does not exist yet. All four
names are new, so the first `cargo publish --workspace` authenticates with
`CARGO_REGISTRY_TOKEN`, and a trusted publisher takes over once the names
exist — which is exactly the path galata-vault took for its own 0.1.0, recorded
in its `RELEASING.md`.

## What the dry run does not prove

It builds and verifies; it does not ask crates.io anything that matters. Name
availability, ownership, rate limits and whether a version already exists are
decided by the registry at upload, and no local check stands in for them.

## Version

All four are `0.1.0` and move together, from `[workspace.package]`. They are
one project with one record format between them: a `galata-wire` a reader can
pair with the wrong `galata-segments` is a bug this versioning exists to make
impossible.
