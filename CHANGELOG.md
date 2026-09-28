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

### Added

- **A correlation regime flag** (`flag-the-correlation-regime`).
  `galata-watch` reports a fitted horizon whose two newest `constancy` windows
  both have an Engle–Sheppard p-value below the new, optional
  `[watch] max_constancy_p` (the config suggests 0.001: a test repeated every
  half hour on overlapping windows alarms far more often than its nominal
  level). `watch::Thresholds` and `config::Watch` gain the field.

### Changed

- **`galata-segments`: a segment already present byte for byte is left
  alone** (`leave-an-identical-segment`). `SegmentWriter::finish` compares
  the new segment with the file at its final path, by size and then by bytes.
  When they are identical it discards its temporary and returns the existing
  path, with no sync and no rename, so the file keeps its mtime and inode. The
  hourly `--replace` projection re-derives about 10,000 identical segments: on
  a clone of production it went from 162 s to 41 s, and from 10,047 segments
  touched to none. `write_file` is unchanged.

### Added

- **Whether the correlation stayed constant** (`constancy`, Tier 16). Each
  fitted horizon writes Engle and Sheppard's (2001) test of constant
  correlation: `engle_sheppard_stat` and `engle_sheppard_p` over the last 30
  days of its standardised returns, against the declared R (galata-research
  `corr.constancy`, White-robust, 5 lags). A small p is CCC failing.

### Changed

- **Constant correlation at 5m, 1h and 4h** (`declare-constant-correlation`,
  the operator's choice). `varcov` accepts `corr = "ccc"` (Bollerslev 1990: R
  is the refit window's correlation of the standardised returns, a = b = 0),
  and `signals.toml` declares it for the fitted horizons, with the registered
  and the six-instrument evidence in the comment. Rows are labelled
  `garch-t/ccc` and `gjr-t/ccc`. Stored DCC rows are unchanged.

### Added

- **The fitted matrix is reconciled against `derive`** (Tier 16's
  cross-check). `galata-signals varcov` stores `correlation_target`, the R̄ a
  fitted horizon reverts to (from galata-research's `walk_forward`), and
  `galata-watch` compares each pair's newest R̄ with `derive`'s equal-weight ρ
  over the same window. A gap in Fisher z above the new, optional
  `[watch] max_correlation_target_gap` is a finding naming the pair, both
  figures and the gap. `watch::Thresholds` and `config::Watch` gain the field
  and lose `Eq`. `derive::tape::bars_from_tape` reads a venue's candles once
  for several horizons.
- **Liquidity every hour** (`galata-signals liquidity`, Tier 16): the
  time-weighted quoted spread and touch depth, the effective spread (volume-
  and equal-weighted), 5 s price impact, 24-hour Amihud, and a robust z of the
  spread against the signal's own stored week. `derive-the-signals` runs
  `varcov`, `carry`, `jumps` and `liquidity`.
- **Jump flags at every 5m close** (`galata-signals jumps`, Tier 16): the
  last bar's Lee–Mykland statistic and flag, over 24 hours the flagged bars by
  sign, Huang and Tauchen's jump share and its ratio z, the signed jump
  variance shares, and a decaying jump intensity. `derive-the-signals` runs
  `varcov`, `carry` and `jumps`.
- **Funding carry, hourly** (`galata-signals carry`, Tier 16). It writes the
  settled carry over 24h, 7d and 30d, the excess over each dex's declared
  interest-only baseline, a 7-day z-score, positive and at-baseline shares,
  carry over volatility, and the live rate as a nowcast. A trailing figure
  under 90% settled coverage is absent, naming the last settled hour.
  `derive-the-signals` now runs `varcov` then `carry`; one refusal does not
  stop the other, and the run fails after both, naming it.
- **The tape holds market-data signals, `kind=signals`** (Tier 16). A signal
  is computed, not projected, so it is a record that lives only on the tape.
  - `Kind::Signals` in `galata-wire`, addressed `Addressing::Market`, is the
    first dataset under that level. **API:** `Kind` gains a variant, and
    `Kind::ALL` is 20 long.
  - `galata_datawatch::signals` owns the dataset: its schema (one row per
    value, with `value` and `n_eff` its only floats), its labels
    (`galata.writer`, `galata.run_id`, `galata.code`), and `write`, one
    time-cursor segment per asof date at the run's `computed_micros`.
  - `--replace` plans over the projected datasets only, so it never lists,
    refuses on, or removes a signal.
  - `check_layout` compares a signal's ranges per writer, not per venue.
  - Retention recognises `signals` as its own family, with no horizon, so the
    tape's horizon never expires one.
  - **Rebuild every binary that lists the tape before the first signal is
    written.** An older `galata-watch` reports the directory as an unknown
    dataset every hour.
- **Signals are computed and committed on a schedule** (Tier 16,
  `derive-the-signals`).
  - `py/signals` is a uv project of its own, with galata-research pinned by
    commit. `galata-signals varcov` computes Σ per declared horizon (5m, 30m,
    1h, 4h, 1d and 1w) with `gr.models.corr`, from the last closed bar. It
    appends only when a bar of that width has closed, and writes a refusal as
    `absent` rows.
  - `galata-signals-commit` (a new binary) refuses a hand-off that is not the
    signals schema exactly (a LargeUtf8 is a refusal, not a cast), then
    writes it through `signals::commit`.
  - The lane's flow `derive-the-signals` runs both at :15 and :45, holding
    `galata-record`. `run-service.sh flows` syncs `py/signals` from its lock
    before serving.
  - Measured on the record on 2026-09-28: 216 rows (6 horizons × 21
    covariances and 15 correlations) in about 100 s, most of it reading 1m
    candles.
- **Signals derived from the covariance matrix**, in the same run and at the
  same asof: `beta` to BTC and its idiosyncratic share, `absorption` (n = 1,
  on Σ and on R), `surprise` (the last bar against the Σ forecast before it:
  Mahalanobis, its χ² percentile, magnitude and correlation surprise), and
  historical `turbulence`. A second one-origin walk at the previous close
  supplies Σ_{t|t−1}, and the stored `varcov` rows are unchanged by it
  (pinned by a test).

### Fixed

- **The time between a restart and the live subscription is a gap.** The
  restart gap is published before the boot walk, and the walk runs before
  the stream is subscribed. On 2026-09-28 that left ~58 s with no quotes or
  trades and no gap row, after 2.7 s of `downtime`. Every pair whose last
  accounting is a published gap now gets a second gap, from its end to the
  moment subscriptions are sent, with the same cause: `downtime` after a
  restart, `session_lost` after a reconnect's backoff. Walked candles and
  funding are covered by the walk and get none. The end is the send, never a
  first frame, so a quiet market is still never a gap. Gaps no longer
  overlap: a gap starts at the later of covered and already gapped.
- **A walk request that failed is no longer reported as covered.** On
  2026-09-28 the funding walk met 429 on every instrument and its outcome read
  `covered 1300d`, and the refused 1h, 4h and 1d candle pages read their
  whole reach. `WalkOutcome` now carries `failed` (`FailedFetch`: ticker,
  range, the venue's error). `reached` stops at the earliest failed start. A
  forward walk stops the failing instrument at that page. The report says
  `came back short` and is logged at error. The exit code stays zero: the
  venue refused it, and under `KeepAlive` a restart would ask again into the
  same refusal. **API:** `WalkOutcome` gains a public field, and
  `FailedFetch` is new.
- **`install-services.sh` waits for launchd to unload the old job** before
  loading the new one. `bootout` can return while the process is still
  exiting, and the `bootstrap` that followed failed with `5: Input/output
  error`, leaving the service unloaded (twice on 2026-09-27, both on the lane).
- **`galata-watch` holds both stores shared while it judges**, waiting up to
  50 minutes for a writer. Without the hold, it could judge a compaction or a
  replacing rebuild mid-run and report an interrupted compaction that was
  only an unfinished one.
- **A replacing tape rebuild no longer removes before it writes.** The hourly
  projection's `--replace` deleted four receipt days' segments and wrote their
  replacements after, so for 67 s of each run the tape listed 16 segments
  instead of 6,729 (measured on a copy of the real record). Readers take no
  hold, so they saw the hole: the tower logged it hourly as its bound going
  backwards. A failed write would have left the days missing until the next
  run. The run now commits first and removes the planned segments after,
  never one it just wrote.
- **Compaction no longer deletes fetched pages it took for leftovers.** It
  removed every segment another contained by range, unmerged, on the premise
  that only an interrupted compaction nests ranges. A page fetched while a
  live segment spans its time (the settle's `candleSnapshot`, a walk's
  `fundingHistory`) sits inside that range and holds rows of its own. A copy
  of one day's candles compacted from 402 fetched pages to 202. A contained
  segment is now removed only when every row is proven to be in its
  container, and merged otherwise. `galata-watch` stops reporting those pages
  as an interrupted compaction. **Rebuild every binary before the next
  nightly run** (`cargo build --release --workspace --bins`): `galata-watch`
  carries the same rule, and the one left behind failed each hourly judge on
  the pages the fix keeps. Pages already lost can be walked again where the venue still
  serves them.

### Added

- **`galata-compact --closed-hours`**, which the lane now runs hourly at :20:
  today's hours that ended at least 5 minutes ago are compacted as closed days
  are. On a copy of the live archive, today's partitions went from 130,906
  segments to 2,299 with every row kept, and a frontier listing from 416 ms to
  9 ms. The open hour is never read.
- **A BUILD section in `install-services.sh --status`**: each workspace
  binary, and the tower's installed copy, is listed as current or as STALE,
  naming the first commit after its build that touched its sources. A commit
  to docs, tests or scripts makes nothing stale. The installer now copies the
  tower with `cp -p`, so the copy keeps its build time.
- **`--check-config [path]` on every binary that reads the document**, and
  `scripts/vet-config.sh` to ask them all (named `check-config.sh` until
  the gate took it for a guard and failed on its usage line). Each binary judges the document
  with its own rules and prints `ok` or `refused: <reason>` before doing
  anything else. The document is parsed strictly, so a key a newer build added
  is refused by every older one. The script asks every deployed binary about a
  candidate before it is written. A build that predates the flag is reported
  as unable to tell, never as accepting. The tower is asked only when its
  `--version` lists the flag, because an older tower ignores arguments and
  serves.

- **`ledger.tape`: the ledger, projected into typed datasets.** When
  declared, the ledger process writes each polled account's fills and funding
  payments on every fold pass, from the rows the fold already reads
  (normalised, one per identity, our own counterparties aliased), to
  `<tape>/venue=<v>/account=<alias>/kind=<kind>/rows.parquet`. Each file is
  rewritten whole and committed by rename (`galata_segments::write_file`,
  new), and an empty kind is still a file, with zero rows. The root is held
  owner-only (`0700`, refused otherwise), never under the market tape, and no
  address names any path. Optional, and nothing is written when absent.
  **Rebuild every binary that reads the document before writing the key**: `[ledger]` is parsed by capture, the tower and the lane's tools as well as the ledger, and each refuses a key it does not know. It also projects
  margin (per dex), positions and ledger updates. Ledger updates are written
  long, one row per (update, dex it moved): an update that moved nothing is
  one row with a null dex, one this build cannot read says
  `effect_known = false`, and a counterparty is an alias or a fingerprint,
  never an address.

- **`walk_funding_days`: settled funding walked to a stated depth.** Declared
  per venue, it makes the funding walk ask that many days back on every boot,
  paged forward to now, instead of resuming from the record's receipt clock.
  That clock reached back only to when capture began, while Hyperliquid holds
  BTC's settlements from 2023-05-12. Refused by name outside 1..=3,650 days,
  or on a venue whose `funding` is not declared. Measured cost at 1,300 days:
  about 194 pages across six instruments, 44.8 KB each, about 40 s. Optional,
  and off when absent. **Rebuild every binary that reads the document before
  writing it**, since each refuses a key it does not know.

- **`galata_segments::ListingCache`: a quiet directory is listed once.**
  Each directory's entries are cached by its own mtime, which POSIX `rename`
  moves. A cached listing is trusted only when it is more than `RACY_MARGIN`
  (2 s) older than the newest directory in the same walk. That is Git's
  "racily clean" rule, on the filesystem's own times, with no clock read.
  `LabelCache` now holds one, so `Bound::of_cached` reads no quiet partition.
  Over 2,230 candle partitions its warm cost went from 108 ms to 19 ms. The
  cache also answers `unwritten`, instead of a second walk, and
  `unwritten_cached` is new.

- **`capture.settle_secs`: the bars closed while running are settled.** The
  stream never sends a candle final and the walk runs at boot, so a running
  capture held forming rows and no closes (11 BTC finals in 15 live hours).
  Every `settle_secs`, each instrument and candle width whose bar has closed
  since its last settle is asked for through the fill queue, paced, retried
  and taken through the one path as a gap fill is. Optional and off when
  absent; **rebuild every binary that reads the document before writing it**,
  since each refuses a key it does not know.

### Changed

- **`Funding.premium`: settled funding carries the premium it was computed
  from.** `galata-wire`'s `Funding` gains `premium: Option<Num>`
  (`#[serde(default)]`, so an envelope serialised before it still reads), and
  the tape's funding dataset appends a `premium` column after `next_micros`.
  Hyperliquid carries it as printed on `fundingHistory` rows and on the live
  asset context. It cannot be recovered from the rate: the rate is the premium
  plus a clamped interest term, so while the clamp does not bind the rate is
  the floor (13 distinct rates over 48 distinct premiums in BTC's last 48 hours
  on 2026-09-26). **A struct literal of `Funding` must add the field.** Tape
  files written before this lack the column until a rebuild. A polars scan
  whose first file lacks it refuses, which galata-research now tolerates.

- **rh-crypto's `best_bid_ask` decoder accepts each price as a JSON string or
  a JSON number**, keeping the digits exactly (never through a float).
  Robinhood's published OpenAPI document types them as numbers, and the
  decoder took strings only, so a live answer spelled that way would have been
  archived and never normalised. Anything else is refused as a shape error
  naming the field. The same document settles that v1, the path kept, has
  `price` (the midpoint) and no `quantity`, and that its spreads are
  percentages of the mid. `Quote::{bid,ask}_spread` are documented as being in
  the venue's own unit.

- A queued fill is identified by its ticker, series **and width**, so a
  settle's 1h fill does not widen its 1m fill.

- **A Hyperliquid candle is final when its receipt is at or past the close
  the venue states (`T`), whichever path carried it.** A walked page's newest
  bar was filed final while still forming; it is now forming. A candle
  without `T` is refused by name.
- **Derived statistics judge backfill by whether capture was heard at the
  close**: a forming row of the same ticker and width within one bar width of
  it. The stream never sends a bar final, so a receipt test marked every
  final bar backfilled. A row flagged final before its own close no longer
  fills a slot, so rows already on the tape under the old flag are safe
  without a rebuild.

- **Hyperliquid's `midPx` is no longer filed as the index.** `activeAssetCtx`
  prints a book midpoint and no index, and `Mark.index` had carried the one as
  the other, so the tape's `marks.index` was the mid and every mark-to-index
  basis read from it was a mark-to-mid basis. `index` is now `None` on this
  venue. **A tape rebuild corrects every captured day**; the archive holds the
  bytes.

### Added

- `Mark.mid` and `Mark.premium` in `galata-wire` (`#[serde(default)]`, so an
  envelope serialised before them still reads), carried by the Hyperliquid
  adapter from `midPx` and `premium`, and two tape columns, `marks.mid` and
  `marks.premium`, appended after `open_interest`.

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
- **Derived statistics** (`derive-statistics`): a returns grid of each
  bucket's closing bar, close-to-close volatility annualised by
  √(365 × 86,400 ÷ bucket), correlation per pair with a Fisher interval,
  beta on a reference, and absent cells below a declared floor naming the
  thinner instrument. Every figure carries its n, its backfilled share and
  the tape bound it read to. `galata-derive` prints them as JSON.
- Binaries: `galata-datawatch <venue>`, `galata-ledger <venue>`, `galata-derive`,
  `galata-tape-rebuild`, `galata-retain`, `galata-watch`, `measure`.

### The two clocks, which every user meets

`recv_micros` is ours and `at_micros` is the venue's. Coverage, gaps and
latency are measured in the first; the tape is sorted by the second. They are
not interchangeable and nothing in these crates pretends they are.
