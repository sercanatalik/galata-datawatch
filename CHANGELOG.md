# Changelog

Every change to these crates that a user would notice, newest first. The format
is [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project
follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html), with the
pre-1.0 rule that a `0.y` release may break the API and a `0.y.z` release does
not.

**One changelog, because the crates publish in lockstep.** `galata-wire`,
`galata-segments`, `galata-broker` and `galata-datawatch` share one version
from `[workspace.package]` and go to crates.io together
([RELEASING.md](RELEASING.md)). A changelog per crate is the right shape when
crates publish independently; these do not.

`galata-datawatch-vault` is `publish = false` and is not covered here. It
exists so the four above take no vault dependency.

## [Unreleased]

## [0.1.0] - unreleased

First release. Nothing is on crates.io yet, so everything is new.

### Added

**`galata-wire` — the vocabulary, and serde only.**

- `Envelope`, `Event`, `Address`, `Origin`, `Kind`, `Series`, `Ticker`,
  `Venue`, `Market`, `Num`/`Token`, `SCHEMA_VERSION`.
- Token validation (`a-zA-Z0-9_-`, 64 max). **A subject is constructed, never
  interpolated.**
- No float may cross the wire: no float field in the vocabulary, no
  `serde-float` on `rust_decimal`, no float arrow column — three rules, each
  watched failing on its own plant.

**`galata-segments` — durable parquet segments, and the listing over them.**

- Partitions, the frontier, `overdue_closed`, and a cursor API over segments.
- Useful on its own: it is the crate a reader takes to ask what a record holds
  without taking a capture loop.
- Holds on a root, exclusive for a writer and shared for a reader, on one
  advisory lock released by process exit — `hold`, `hold_shared`, and a
  bounded `wait`. Checked across processes, not only within one.
- Writer-stated footer labels (`write_segment_labelled`, `label`), and
  `overlapping_ranges_by_label` for a store whose partitions several
  independently numbered streams share.

**`galata-broker` — the bus, over NATS.**

- `Publisher`/`Subscriber`, subject roots separated so a dashboard can take
  `status.>` without the market firehose.
- Grants are generated, and `check-grant-coverage.sh` refuses a root granted to
  nobody — the predecessor's inverted-table bug (`allow: []` meaning *allow
  everything*) was reproduced rather than taken on trust.

**`galata-datawatch` — the record, the venue seam, the capture loop, the tape.**

- `Adapter`/`Source` as the out-of-tree extension point: `normalise`,
  `classify` and `wire` are pure, and the loop owns the clock.
- **`capture` is a feature, and the wall is measured, not claimed.** A tree
  built without it links no venue transport at all — 541 crates down to 279 —
  which `check-no-transport.sh` asks cargo about rather than reading from a
  manifest. `galata-tower` takes exactly that build.
- The hyperliquid adapter, including HIP-3 dexes: a per-instrument `dex`, and a
  duplicate ticker across dexes refused at load, naming both.
- The rh-chain adapter: capture at the head, readers bounded at finalized,
  reorgs recorded as rows and joined to what they contradict. A provider URL is
  a secret — `Endpoint` prints a public URL and withholds a held one.
- The rh-crypto adapter: declarable (`poll_secs` required, keys named by
  variable), booted as a poll, every request signed over its path and query.
  **The live endpoint has not been called**: no credentials were obtained and
  none should be; the request is proved against a local server.
- The tape: parquet a `SELECT` can read with no flags, rebuilt deterministically
  — twice over a frozen archive gives identical segment names and identical
  bytes. Every tape segment holds one venue and says so in its footer
  (`galata.venue`); `--replace` replaces only the rebuilt venue's segments,
  since a partition is shared by every venue that supplies its dataset.
- The bounded reader's `Bound` is a position **per venue** (`positions`,
  `of_venue`): each venue numbers its stream from its own process, so one
  position cannot bound two. A tape written before labelling refuses to open,
  replace or pass `check_layout`, naming the remedy — it is a cache: remove it
  and rebuild.
- Configuration from a file or from a vault document, through one validator, so
  a document refuses exactly as a file does. The broker password can come from
  either the environment or the vault.
- `AdapterConfig::for_replay` and `Endpoint::Withheld`: an adapter built for a
  tool that does not connect reads no secret, and a keyed provider it declares
  is named and withheld — never replaced by the public node.
- `galata-watch` judges record age per declared venue, and reports a declared
  venue that has captured nothing — one venue stopping no longer hides behind
  another still writing.
- Binaries: `galata-datawatch <venue>`, `galata-tape-rebuild`, `galata-retain`,
  `galata-watch`, `measure`.

### The two clocks, which every user meets

`recv_micros` is ours and `at_micros` is the venue's. Coverage, gaps and
latency are measured in the first; the tape is sorted by the second. They are
not interchangeable and nothing in these crates pretends they are.
