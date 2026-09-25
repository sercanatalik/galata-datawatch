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
- An account's words: the `Account` token (an alias, never an address),
  `Address::Account` and `Envelope::for_account`, `Series::Margin`, and the
  `margin`, `positions` and `accounts` kinds with their `Margin`, `Position`,
  `AccountSeen` and `AccountMode` events. **Additive**; `Address` gains a
  variant.
- An account's history: `fills`, `funding_payments` and `ledger_updates`
  kinds and series, the `Fill`, `FundingPayment`, `LedgerUpdate` and
  `EventsReach` events, and `GapCause::BeyondReach`. `Kind` and `Series`
  serialise `snake_case`, identical for every one-word name and the only
  spelling that agrees with their partition names.

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
- History is walked at boot from the record's latest receipt, and a gap
  published **while running** (a lost session) is filled too: once the bar of
  the loss has closed, capture asks the venue for that ticker and series from
  the gap's start, as its own task so the live loop never waits for it, paced
  and capped like the walk (`Capture::fill_with`, `Capture::fill_step`).
- `walk_candles` per venue: bar widths the walk fetches beside the live
  `candle`, never subscribed live, each for the venue's whole reach on every
  boot, because the venue serves a rolling window of each width. A width the
  venue cannot name, the live width repeated, or a duplicate is refused before
  any request (`Adapter::interval_micros`, `capture::walk_items`).
- `galata-datawatch <venue> --import <dir>`: a rescue of saved venue pages,
  verified against its manifest's sha256 in full before any page is taken,
  then taken through the one path inside the boot, after the restart gap and
  before the walk (`capture::verified_pages`, `History::candle_page`).
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
  bytes. Every tape segment holds one venue's rows from one receipt day and
  says so in its footer (`galata.venue`, `galata.source_day`); `--replace`
  (`Replace::SourceDays`) removes only the rebuilt venue's segments from the
  receipt days it reads, since a partition is shared by every venue that
  supplies its dataset **and** by every receipt day whose walk reached back
  into its date. It refuses a range that splits a day and a segment missing
  either label, removing nothing.
- `LabelCache`, `Bound::of_cached`, `Reader::open_cached`: a caller asking
  repeatedly reads each segment's label once, not once a call.
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
- **The ledger** (`ledger` feature, on by default): perp margin and
  positions per account and dex, snapshotted on a declared cadence into its
  own owner-only root. Accounts are declared by alias under
  `[ledger.account.*]`, their addresses held as secrets and fingerprinted
  with a keyed HMAC in each segment's footer; Hyperliquid sub-accounts are
  discovered and bound by ordinal. It refuses an address in configuration, an
  alias whose address changed, a history it cannot verify, a sub-account
  declared as a master, a dex the venue does not know and a root others can
  read. Hyperliquid only; its shapes were measured on 2026-09-25.
- **The ledger's history** (`ledger-events`): fills, funding payments and
  ledger updates per account, asked forward from the newest recorded on a
  declared `events_secs`, one row per event on read however often a page
  was archived, and the reach judged by evidence. A hole the venue no
  longer holds is a gap, cause `beyond_reach`. A transfer's effect on perp
  margin is stated per dex, a counterparty is named by alias or by
  fingerprint, and no row carries an address or a transaction hash.
  `[ledger] events_secs` is required once a `[ledger]` block is declared.
- **The fold** (`ledger-fold`): books per account, dex and ticker from an
  account's fills, by weighted average, opened at the venue's stated
  `startPosition` with an unknown basis until flat; continuity breaks,
  realised skews against `closedPnl` and snapshot differences reported
  with both figures at declared tolerances (`[ledger]
  fold_position_tolerance`, `fold_relative_tolerance`); funding and fees
  per book, cash flows per dex, and no equity without a mark. The ledger
  writes `ledger-fold-<venue>.json` after each events pass.
- Binaries: `galata-datawatch <venue>`, `galata-ledger <venue>`,
  `galata-tape-rebuild`, `galata-retain`, `galata-watch`, `measure`.

### The two clocks, which every user meets

`recv_micros` is ours and `at_micros` is the venue's. Coverage, gaps and
latency are measured in the first; the tape is sorted by the second. They are
not interchangeable and nothing in these crates pretends they are.
