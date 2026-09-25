<picture>
  <source media="(prefers-color-scheme: dark)" srcset="assets/logo-on-dark.svg">
  <img src="assets/logo.svg" alt="" width="72" align="right">
</picture>

# galata-datawatch

[![check](https://github.com/sercanatalik/galata-datawatch/actions/workflows/check.yml/badge.svg)](https://github.com/sercanatalik/galata-datawatch/actions/workflows/check.yml)
[![MIT](https://img.shields.io/badge/licence-MIT-blue.svg)](LICENSE-MIT)
[![Rust 1.98+](https://img.shields.io/badge/rust-1.98%2B-b7410e.svg)](rust-toolchain.toml)

Market data capture and parquet archival, in Rust.

One process per venue. Bytes are made durable **before** anything tries to
parse them, gaps are published as events rather than inferred from silence, and
the store answers *how far am I durable* from a directory listing without
opening a file.

| | |
|---|---|
| **Archive first** | every payload lands verbatim before a parser sees it, so a parse bug costs a re-run and never the data |
| **A gap is an event** | an absence is written down with its cause and its bounds, never inferred from missing rows |
| **The frontier is a listing** | *how far am I durable* is answered from directory entries, with no parquet decode |
| **Venues are features** | a venue that is not compiled in cannot be reached, and a guard holds it |
| **The tape is a cache** | delete it and `galata-tape-rebuild` writes it again from the archive |

> **Pre-0.1.0.** Nothing is published yet. The roadmap is
> [`design/roadmap.md`](./design/roadmap.md).
>
> The screen for this record is
> [galata-tower](https://github.com/sercanatalik/galata-tower).

## The crates

| crate | holds | links |
|---|---|---|
| `galata-wire` | the vocabulary: `Envelope`, `Event`, `Kind`, `Series`, `Ticker`, `Num` | `serde` only |
| `galata-broker` | `Publisher`/`Subscriber` and the NATS implementation | `galata-wire` |
| `galata-segments` | durable parquet segments: write, sync, rename, compact | `arrow`, `parquet` |
| `galata-datawatch` | the record, the venue seam, the capture loop, the tape | all three |

A downstream process that only wants to *hear* about market data takes
`galata-wire` and `galata-broker` and links no columnar format:

```toml
galata-wire   = "0.1"
galata-broker = "0.1"
```

## Venues are features, not crates

```sh
cargo add galata-datawatch --features rh-chain
```

`hyperliquid` (WebSocket), `rh-chain` (block cursor over `eth_getLogs`) and
`rh-crypto` (signed REST poll) ship in-tree.

**A venue can also live in your own crate.** `Adapter` carries a worked example
that compiles, and the claim itself is checked by
[`tests/out_of_tree_venue.rs`](./crates/galata-datawatch/tests/out_of_tree_venue.rs)
— cargo builds that file as its own crate, so it sees exactly what a stranger
sees. If a venue needs something private, the compiler says which thing there
rather than in somebody's repository. The venue it implements is fictional on
purpose: one resembling an in-tree venue would tempt reuse of its helpers, and
reuse is what makes a test pass for the wrong reason.

docs.rs is told `all-features = true`, so every venue appears and every gated
item carries a badge naming the feature it needs.

## Vault-backed capture

The unpublished `galata-datawatch-vault` binary fetches its configuration once
at boot and hands the same text to the same validator as the file binary. When
configuration names a broker password, it fetches that secret once too. The
capture loop holds neither the vault nor the credential.

Install the published `galata-vault 0.4` tools, then provision a project and
environment. Keep the recovery kit produced by `gv init` somewhere safe.

```sh
curl --proto '=https' --tlsv1.2 -LsSf \
  https://github.com/sercanatalik/galata-vault/releases/latest/download/gv-installer.sh | sh
curl --proto '=https' --tlsv1.2 -LsSf \
  https://github.com/sercanatalik/galata-vault/releases/latest/download/gv-server-installer.sh | sh

gv-server local
gv init galata-datawatch --server http://127.0.0.1:8750
gv env add galata-datawatch/prod
gv config set datawatch --format toml --env galata-datawatch/prod < config/datawatch.toml
```

The binary opens one vault through the SDK's environment contract: set
`GV_SERVER`, then exactly one of `GV_TOKEN` or `GV_TOKEN_FILE`. A token file
must be mode `0600` or `0400`; setting both token variables is refused. The
Datawatch source does not duplicate the SDK's authentication rules.

Use the smallest credential the document needs:

```sh
# No [broker] block: configuration documents only.
gv token mint --scope config --env galata-datawatch/prod

# With [broker]: read the document and only the named broker secret.
printf %s "$BROKER_PASSWORD" | \
  gv set GALATA_DATAWATCH_PASSWORD --env galata-datawatch/prod
gv token mint --scope read --only GALATA_DATAWATCH_PASSWORD \
  --env galata-datawatch/prod
```

`password_var` names an environment variable for the file binary's
`EnvSecrets`, and a vault secret name for `VaultSecrets`. Use a child vault when
credentials must be cryptographically isolated between Datawatch instances or
venues; an allow-list is server policy, not a second encryption boundary.

The integration removes the broker secret from the Datawatch capture process's
environment. The current generated NATS authorization file still reads that
password from the NATS server's environment.

## Two stores

```
  var/archive/                    THE RECORD — one row per payload, verbatim
   venue=hyperliquid/               before any parse was attempted
     kind=quotes/
       date=2026-09-20/
         1758326400000000-1758326460000000-4711-3.parquet
         failures/                  same seq, no payload column

  var/tape/                       THE CACHE — one row per event, rebuildable
   kind=quotes/                     from the record at any time
     venue=hyperliquid/
       date=2026-09-20/
         part-000000123456-000000234567.parquet
```

`venue` sits above `kind` in the archive and below it in the tape. The
archive's unit is the capture — a venue's bytes are retained, replayed or
dropped as a subtree. The tape's unit is the dataset, so *this dataset across
every venue* is one prefix.

## Reading the tape

```sql
SELECT * FROM read_parquet('var/tape/kind=quotes/**/*.parquet');
```

Two things to know before filtering it.

**`at_micros` is null where the venue did not timestamp the event**, which is
not rare: measured over a 24-minute run, 100% of `marks` and 89% of `funding`
carry no venue time, against 0% of `quotes` and `trades`. Giving those rows our
receipt time would turn an absence of information into a latency of zero, so
the column is left null — and a `WHERE at_micros BETWEEN …` drops all of
`marks` without saying so. Filter on `recv_micros`, or use the bounded reader,
which keeps such a row once the partition holding it is in range.

**An execution can arrive twice.** A venue that sends recent history on
subscribe redelivers it on every reconnection — measured at 1.55% of a
24-minute run, once per session rotation. Both receipts are recorded because
both arrived; group on `trade_id` to count each execution once.

## Scheduling the maintenance

Capture writes the archive; four one-shot tools keep it. **The tape exists only
when `galata-tape-rebuild` runs**, so without a schedule the queryable half
stops at whatever day somebody last rebuilt by hand. `py/` is the schedule: a
[cereyan](https://github.com/sercanatalik/cereyan) lane whose flows are
subprocess calls to the release binaries, and nothing else.

| Flow | Runs | When (UTC) |
|---|---|---|
| `compact-the-archive` | `galata-compact` | daily 00:10 |
| `project-the-closed-days` | `galata-tape-rebuild --replace`, the last 3 closed days, per declared venue | daily 00:40 |
| `report-what-retention-would-expire` | `galata-retain`, the report only | Sundays 01:30 |
| `judge-the-record` | `galata-watch` | hourly at :05 |
| `rebuild-one-day` | `galata-tape-rebuild --replace <venue> <date>` | never; a backfill over history |

```sh
cargo build --release              # add --features rh-chain if it is declared
uv sync --project py
cd py && CEREYAN_HOME=~/.cereyan-galata uv run cereyan serve . --no-open   # from py/: runs import `flows`
```

**Its own cereyan home.** `~/.cereyan` is shared with every other cereyan
project on the machine, and a server started on it imports and schedules
their flows too. `scripts/install-services.sh flows` sets this for you.

**One configuration per deployment.** Capture and the lane both run from
`var/datawatch.local.toml` when it exists — the committed file plus this
machine's `[broker]` and `[watch]` — and from `config/datawatch.toml`
otherwise. Whole-file, never merged.

**Build with the features your declared venues need.** `rh-chain` is not a
default feature, and a binary that does not speak a declared venue refuses the
whole configuration at load — every scheduled job fails, by name, until the
build matches the file.

Under launchd, that last line is the `ProgramArguments` of a `KeepAlive` agent
(cereyan's [run-as-a-service guide](https://github.com/sercanatalik/cereyan/blob/main/docs/guides/run-as-a-service.md)
has the plist). Loading it is left to you; nothing here installs a service.

What the lane will not do, by construction rather than by care:

- **Pass a credential.** A job's environment is built — `GALATA_CONFIG`,
  `PATH`, `RUST_LOG`, `NO_COLOR` — and nothing is inherited from the scheduler.
  None needs one: the rebuild builds adapters with
  `AdapterConfig::for_replay`, which withholds a keyed provider rather than
  reading it, so a chain venue behind a provider key projects nightly with no
  key in reach.
- **Delete.** `galata-retain --delete` is not a flow, because cereyan's MCP
  `run_flow` starts any registered one.
- **Hold truth.** Deleting cereyan's store changes nothing about what a flow
  does next: the projection takes a fixed window, not a cursor.
- **Retry what the next night repairs.** Only `rebuild-one-day` retries, only
  on exit `1`, never on a bad argument.

`scripts/check-python-flows.sh` holds those rules over the lane's import graph,
and `check-all.sh` runs the lane's tests offline, so the gate needs
[uv](https://docs.astral.sh/uv/).

## Running as services

On macOS, the deployment's four long-running processes are launchd agents,
rendered from one tracked template and restarted whenever they exit:

```sh
cargo build --release                  # capture and the flows run release binaries
(cd ../galata-tower && cargo build --release)
scripts/install-services.sh            # nats, capture:hyperliquid, tower, flows
scripts/install-services.sh capture:rh-chain      # one more venue
scripts/install-services.sh --uninstall tower     # or remove one
```

Every agent runs `scripts/run-service.sh <service>`. Five services, `vault`
first: `gv-server local` (loopback, data in `~/.local/share/galata-vault`) is
the deployment's secret store. **Each service reads only its own secrets,
through a token minted for it alone** — NATS all three broker passwords,
capture its venue's, the tower the `reader`'s, the flows none — via
`galata-vault-exec --only NAME -- <command>`. Not `gv run`: `gv` reads through
the owner key, which this machine holds, so it would hand any service every
secret. No secret goes in a plist, which launchd leaves readable by every
user. Logs are `var/logs/<service>.log`.

Once, on a new machine, after `scripts/install-services.sh vault`:

```sh
scripts/provision-vault.sh        # project galata-datawatch, env .../prod, secrets imported
scripts/mint-service-tokens.sh    # var/tokens/<service>.gvt, 0600, 365 days
```

**The recovery kit** lands in `var/galata-datawatch-recovery.gvkit`. It is
the only way to recover the project — move it to offline storage and delete
that copy. **The tokens expire** after 365 days, the server's maximum:
re-run `mint-service-tokens.sh` before then and reinstall the services.

A stop is SIGTERM, which capture treats as a clean shutdown — it flushes,
marks, and the next start records the outage as `downtime` rather than a
crash. Installing twice replaces the agent, and a hand-started instance of
the same service is stopped first: two captures of one venue would be two
writers of one archive scope.

## Building

```sh
cargo test                 # no network is touched
scripts/check-all.sh       # format, lints, guards, the guard harness, tests, the lane
scripts/test-guards.sh     # proves every guard can fail
```

**CI runs `check-all.sh` and nothing else**, so the badge above and the command
above cannot disagree. Everything after the dependency fetch runs `--offline`:
the workspace is provable without a network, and an accidental network
dependency should fail rather than succeed quietly.

## Publishing

Not published yet. When it is, **the order is forced by the dependency graph**
and getting it wrong fails partway through a sequence that cannot be undone —
a crates.io version is permanent.

```text
  galata-wire        no internal dependencies   ─┐
  galata-segments    no internal dependencies   ─┴─ either order
  galata-broker      needs wire
  galata-datawatch   needs wire, segments, broker
```

`cargo package` on `galata-broker` or `galata-datawatch` **fails today**, and
correctly so — it cannot resolve a dependency that is not on the registry:

```text
  error: failed to prepare local package for uploading
  Caused by: no matching package named `galata-wire` found
```

All four are verified before any publish happens, and by the gate rather than
by remembering:

```sh
cargo package --workspace     # every crate, built from its own tarball
```

`cargo package --workspace` builds a temporary registry under `target/package`,
publishes each crate into it, and compiles every unpacked tarball against the
*packaged* versions of the rest — not against the path dependencies this
workspace supplies. That distinction is the point: inside a workspace cargo
prefers the path dependency, so a crate can compile perfectly here while using
a sibling change its own manifest does not require.

Two guards, two questions. `scripts/check-package.sh` asks what a tarball would
*contain*, using `cargo package --list`, which resolves nothing — so it still
answers for a crate that will not build. `scripts/check-tarball-builds.sh` asks
whether it *compiles*, which is the half that could not be checked at all
before `--workspace` existed. About 19s warm, 95s on a cold tree, measured.

Verification builds default features; combinations are
`scripts/check-feature-matrix.sh`'s.

Allow a moment between publishes: the registry index needs to carry a crate
before the next one can resolve it.

## Licence

MIT. See [LICENSE-MIT](./LICENSE-MIT).
