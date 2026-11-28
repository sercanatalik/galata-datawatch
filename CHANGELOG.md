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

**The next release is 0.2.0, not 0.1.1.** `galata_wire::Instrument` and
`galata_datawatch::venue::Declaration` each gain public fields, which breaks a
struct literal written against 0.1.0, `VenueConfig::candle` changes type, and
both `ConnectionPolicy` variants gain `reply_secs`.

### Added

**`galata-signals`** (new, not published)

- **The EWMA covariance in Rust** (`add-galata-quant`): `galata-quant-signals
  varcov` computes `varcov`'s EWMA horizons (30m, 1d, 1w) and their beta,
  absorption, surprise and turbulence from the tape's candles with
  galata-quant (by git tag), and commits them through `signals::write` — the
  same rows `py/signals` writes, held to it on the record to 2.2e-15. A run
  already on the tape is refused, and the cursor is read again under the
  tape's exclusive hold, so two runs at once store a horizon once: the cause
  of 2026-11-18's overlapping segments, one run committed three times. The
  lane's image registers it as `derive-the-covariance`. `deny.toml` allows
  galata-quant's repository as the one git source.

**`galata-markets`** (new, not published)

- **The one door for market data** (`add-galata-markets`): history from the
  tape as Arrow IPC (`/v1/tape/{kind}`), candles at a period (`/v1/candles`),
  venues and instruments in one format (`/v1/venues`, `/v1/instruments`), and
  the live window as server-sent events (`/v1/live`), resumable by
  `Last-Event-ID`, joinable to a history read by its tape bound, with every
  hole the bus declared filled in place from the record. A token per client;
  it reads the tape and the archive read-only, reads the bus as `markets`,
  publishes nothing, and links no venue transport.

**`galata-broker`**

- **galata-markets' identity** (`add-galata-markets`): `grants::table` grants
  `markets` the stream info of MARKETS and STATUS and consumers of its own on
  them, replies on `_INBOX_MARKETS` alone, and nothing else — no publish, no
  stream administration, no account data. `GALATA_MARKETS`,
  `GALATA_MARKETS_INBOX`, `streams::MARKET_STREAM` and `streams::STATUS_STREAM`.
- **The broker keeps a window of the bus** (`add-bus-replay-window`):
  `streams::table` declares MARKETS (`markets.>`, two hours within a byte cap),
  ACCOUNTS (`accounts.>`, seven days) and STATUS (`status.>`, the last message
  per subject); `streams::apply` creates or updates them, reporting each one;
  `streams::connect_admin` and `streams::held` connect as the new `bus-admin`
  login and read what a stream holds. `bus-admin` may create, update and read
  the declared streams, on an inbox prefix of its own, and nothing else; no
  other login gains any stream right.
- `Subject::account` and `Subject::of` for an account envelope:
  `accounts.<venue>.<alias>.<kind>`, refusing an alias shaped like an address.
  `accounts.` joins `ROOTS` with the first login granted it, in the ledger's
  change.

**`galata-wire`**

- **A subaccount's margin, positions and collateral, as stated**
  (`add-derive-account-ledger`), additive: `Event::MarginState`,
  `PositionState` (options and perpetuals, with the venue's greeks) and
  `Collateral`, with `Kind::MarginState`, `PositionState` and `Collateral`
  addressed by account and `Series::MarginState` for the read a gap is about.
  Every figure optional.
- **An account's spot and borrowing, as stated**
  (`add-hyperliquid-portfolio-margin-state`), additive: `Event::Balance`,
  `BorrowLend`, `LendHealth`, `PortfolioMargin` and `TokenMargin`, with
  `Kind::Balances`, `BorrowLend`, `LendHealth`, `PortfolioMargin` and
  `TokenMargin` addressed by account, and `Series::Balances` and `BorrowLend`
  for the reads a gap can be about. Every figure a `Num` as the venue printed
  it, absent where it stated none; the portfolio-margin ratio is the venue's,
  never computed.
- `GapCause::Unpublished` (`unpublished`), additive (`add-bus-replay-window`):
  the bus did not carry a span the record holds, so a consumer fetches it from
  history. Sent on the bus only and never archived, so the tape never holds
  one. Every other cause says the data exists nowhere.
- `Kind::OptionPricing` (`option_pricing`) and `Event::OptionPricing`: an
  option's implied volatility, bid and ask volatility, delta, gamma, vega,
  theta, rho, forward and discount factor, each as the venue printed it. Not
  separately declarable: it rides an option's ticker.
- `Instrument::strike`, `Instrument::expiry_micros` and `Instrument::right`,
  for an option. Each is defaulted on deserialisation, so an envelope written
  before them still reads. **Breaking** for a struct literal.
- **BREAKING: one reference format for every venue**
  (`normalise-instrument-reference`). `Instrument` gains `venue_symbol`, `dex`,
  `price_figures`, `min_notional`, `max_leverage` and `isolated_only`, so a
  venue whose prices have no fixed tick, or whose minimum is a value, is
  stated without loss; `Instrument::price_is_valid` applies the rule. Each is
  defaulted on deserialisation, so a row written before them still reads. An
  out-of-tree venue building an `Instrument` by struct literal adds the six as
  `None`, which states nothing it did not state before.
- `Fill::liquidated` (`move-watcher-onto-markets`, design D9): whether the
  fill liquidated the account, where the venue says; `None` where it does not
  (Derive) or could not be told. Defaulted on deserialisation, so an envelope
  published before it still reads. **Breaking** for a struct literal. The
  projection's `fills` gain the `liquidated` column, null where unsaid.

**`galata-datawatch`**

- **A Derive session key, made and signed with** (`add-derive-account-ledger`,
  behind `derive-xyz`): `adapters::derive_xyz::sign` signs a request's time
  with EIP-191 and says a key's address; `galata-session-key` makes a key and
  writes its private half to a pipe only, which
  `scripts/make-derive-session-key.sh` hands to `gv set`, printing only the
  address. `k256` and `sha3` come with `derive-xyz` and nothing else.
- **A ledger account names its session key's variable** where the venue signs
  its reads, and no dexes where it reads a wallet whole
  (`session_key_var`, `session_key_warn_days`); `LedgerCost` says which
  (`has_dexes`, `reads_with_key`). A secret written where its variable
  belongs is refused without being repeated.
- **The ledger reads an account's spot and borrowing**
  (`add-hyperliquid-portfolio-margin-state`): each snapshot asks every polled
  Hyperliquid account's `spotClearinghouseState` and `borrowLendUserState`
  once, whatever its mode, each archived with the token names `spotMeta` gave
  at discovery, each on its own cadence and a miss a gap in its own series.
  Under portfolio margin the venue's own ratio and per-token figures are
  recorded as stated. Their weight is declared and checked against
  `ledger_share` at load. `ledger::project` projects the five new kinds, and
  `ledger::project::newest` returns an account's latest rows of a kind with
  the time they were read.
- **Candles at a period** (`add-galata-markets`): `tape::bars` builds bars at
  1m to 1d from the `candles` dataset exactly, from the finest held interval
  that divides the period and covers the bar with final bars, never mixing
  intervals; each bar states its source interval, how complete it is, whether
  a recorded gap touches it and whether it is final; a span no source covers
  is named, not filled. `Forming` keeps the live bar with the same function.
- `ingest::normalise_replayed`: a payload read back out of the record,
  normalised and emitted with no archive handle, for a reader whose view of
  the record is read-only. `ingest_replayed` is now that, and touches its
  archive parameter no more than it did.
- **Hyperliquid records reference data** (`normalise-instrument-reference`):
  each captured dex's `meta`, at capture's start and every five minutes,
  becomes a row per captured coin in the one format — the grid and five
  significant figures, the lot and a minimum value of 10, the leverage, the
  venue symbol and dex. The universe check now keeps the answers it was
  decided on, through the one path. A coin the venue has delisted is recorded
  inactive and leaves the set. `hyperliquid::meta_page` and
  `hyperliquid::listed`; `Client::meta` replaces `Client::universe`.
- Derive's reference rows state the venue symbol; rh-chain's, the contract.
- The tape's `instruments` dataset gains the six columns, appended and
  nullable; segments written before still read.
- **Capture declares what the bus did not carry** (`add-bus-replay-window`).
  An event dropped from the bus — the queue to the broker full, or a publish
  refused — is noted by its receipt time; capture sends for every pair it
  covers a gap with the cause `unpublished` over the dropped span, on the bus
  only, just before the first event received after the span (or once a
  publish succeeds with the queue empty), so it is stored where the hole is.
  The record is unaffected and holds no declaration. A declaration that cannot
  be sent stays pending, merged with the next span. `NatsSink::unpublished`
  hands the pending span to the publish task; `Unpublished::ends_before` says
  whether an event is the first after it.
- `galata-bus`, the broker's process (`add-bus-replay-window`): it starts a
  helper of itself that waits for the server, applies the stream table as
  `bus-admin` and logs what each stream holds, then becomes `nats-server`
  (`exec`), so launchd's stop reaches the server directly. A table it cannot
  apply is logged by name, and the server serves on without it.
- The `derive-xyz` venue, behind a feature of that name and off by default:
  options on Derive (derive.xyz), from public data. One ticker payload becomes
  a `quotes` row, a `marks` row and an `option_pricing` row; the index of each
  underlying in the set is captured beside its options. No credential, no
  order method, and no public trades.
- The **perpetual of each underlying** in a `derive-xyz` option set, captured
  beside its options and declared by nobody: its top of book in `quotes`, its
  mark in `marks` and its funding rate in `funding`. Not counted toward the
  cap. One the venue does not list is retired before the first subscribe and
  the options start without it. No book depth.
- An instrument can **leave a running set**. When a venue's reference data
  says an instrument is inactive, or the loop's clock passes its stated
  expiry, its subscriptions stop being declared, a reconnect does not ask for
  them, a lost connection records no gap for them, and the status snapshot
  lists it under `left` with its reason and time. The mechanism reads the
  vocabulary and names no venue. `Capture::retire`, `Capture::retire_step`,
  `Held::retire`, `Retirement`, `Coverage::forget`.
- A reference refresh on the stream path: `Capture::refresh_reference_with`
  and `Capture::reference_step`, with `adapters::ReferenceSource`.
- `Adapter::implied_subscriptions`: subscriptions that follow from the
  declared set and are declared by nobody.
- `Declaration::max_instruments`, and `Adapters::max_instruments` and
  `Adapters::instrument_problem`: a venue may state a cap on its declared set
  and a form its names take, and a configuration over the cap, or naming an
  instrument twice, or naming one the venue's names cannot be, is refused when
  it is loaded (`ConfigError::Instruments`). **Breaking** for a `Declaration`
  struct literal.
- The tape projects `option_pricing`, and the `instruments` dataset gains
  `strike`, `expiry_micros` and `right`, appended and nullable.
- `Status::left`, absent from the snapshot while it is empty.

### Changed

- **The deployment's configuration document is composed, not kept by hand**
  (`compose-deployment-config`). `var/datawatch.local.toml` is now generated at
  every service start (`run-service.sh`'s `use_config`) and by
  `scripts/compose-config.sh`: the committed `config/datawatch.toml` followed by
  this machine's `var/datawatch.overlay.toml` (`[broker]`, `[ledger]`), under a
  header carrying the body's digest. A table in both, a hand edit to the
  generated document, and a hand-kept one are refused by name, never
  overwritten; `--migrate` turns a hand-kept document into an overlay once,
  only if the composition reads exactly as it did. `required-secrets.sh` and
  `mint-service-tokens.sh` read the composed document. On 2026-11-28 four
  committed instruments did not reach capture until copied by hand.

- A reference refresh that fails is logged once for a run of failures, and
  once when it reads again, rather than at every refresh.
- **Breaking:** `VenueConfig::candle` is `Option<String>`. A venue that pushes
  no bars states none; Hyperliquid still requires it and refuses its absence
  by name.
- `adapters::check_universe` takes the caller's clock reading and returns the
  reference answers it read, which the boot takes through the one path.
- A stream venue that serves no history boots without a walk.
- **BREAKING: a venue declares how soon it answers a keepalive**
  (`capture-recovers-dead-connections`). Both `ConnectionPolicy` variants gain
  `reply_secs: Option<u64>`, read through `ConnectionPolicy::reply_secs`. An
  out-of-tree venue adds `reply_secs: None` to keep today's behaviour, in which
  nothing is inferred from a connection's silence. Hyperliquid declares 10 s.
- **BREAKING: a ledger venue answers account-wide reads**
  (`add-hyperliquid-portfolio-margin-state`). `ledger::run::AccountVenue`
  gains `account_wide_asks`, `account_wide`, `reference` and `compose_wide`;
  an out-of-tree venue with none returns `&[]` from the first and is never
  asked the others. `config::LedgerCost` gains `balances`, `borrow_lend` and
  `reference`, which a struct literal must name.
- **BREAKING:** `config::LedgerCost` gains `has_dexes` and `reads_with_key`,
  and `config::LedgerAccount` gains `session_key_var` and
  `session_key_warn_days`, which a struct literal must name; `dexes` may be
  absent (`add-derive-account-ledger`).
- `AccountMode::equity_not_held` says a unified or portfolio-margin account's
  collateral is "recorded as balances", where it said "out of scope".

### Removed

- **BREAKING: galata-tower is retired** (`retire-galata-tower`). The operator
  UI is no longer a service of the deployment: `install-services.sh` and
  `run-service.sh` know no `tower`, and `vet-config.sh`,
  `required-secrets.sh` and `mint-service-tokens.sh` no longer serve it.
  `scripts/lib/broker_addr.py`, which pointed it at the broker, is gone. Every
  service now runs in a container of its own.
- **BREAKING (`galata-broker`): the `reader` identity is removed.**
  `grants::table` no longer grants `reader` `status.>`, and
  `config/nats-authorization.conf` no longer declares it or reads
  `GALATA_BROKER_PASSWORD_READER`. Its only user was the tower; status is read
  through galata-markets' STATUS consumer.

### Fixed

- **A quiet market's candles are no longer judged a subscription shortfall**
  (`judge-quiet-candles-as-held`). A candle is pushed on a trade, so a thin
  market has no bar for minutes on a working connection: `xyz:BABA`, about
  $0.77M a day, first delivered candles 140 s into a connection, against the
  judge's 180 s grace. A `candles` pair not yet delivering whose instrument's
  `quotes` pair is held (`live`, `stale` or `closed`) is no longer counted as
  missing; quotes, funding, a refusal, and candles whose quotes are missing
  too still are, and the finding names capture's own counts. galata-markets
  passes the judge each pair's series and state (`status::PairReport`, not
  shown by `/v1/venues`).
- **A handover is no longer judged a subscription shortfall**
  (`judge-subscriptions-per-connection`). Capture hands over to a fresh socket
  every eight minutes, and its held count starts again at zero on each; the
  health judge's three-minute grace ran from capture's start only, so a capture
  hours old read short in the seconds after every handover. Between 2026-11-27
  and 2026-11-28 that put "holds 7 subscriptions of 27 declared" on the
  watcher's Telegram 86 times, while the archive shows all 27 delivering. The
  grace now runs from the later of capture's start and its current connection's
  opening (`session_age_secs`); with no connection open, the disconnected
  finding speaks instead; a status without the field is judged by its start, as
  before. **galata-markets now passes both openings to the judge**: the copy of
  capture's status it judges kept neither `started_at_micros` nor
  `session_age_secs`, so the restart grace added on 2026-11-27 never applied on
  the deployment either. `/v1/venues` shows what it showed.
- **A connection that dies without closing is found, and its silence is a gap**
  (`capture-recovers-dead-connections`). Measured on 2026-11-25: a change in the
  host's network killed capture's connection inside its container, capture said
  "connected" for minutes, and its 8-minute handover then claimed the dead time
  as covered — 2 min 18 s of nine instruments recorded as watched. Now, for a
  venue that declares how soon it answers its keepalive, an unanswered one
  means the connection is dead: a `session_lost` gap for every pair from its
  own last receipt, and a new connection at once. A handover claims continuity
  only once the replaced connection has shown it is alive after its replacement
  was subscribed; otherwise its silence is a gap up to the subscription. A
  quiet market on a live connection is still not a gap. A protocol ping or pong
  now reaches the loop as `Frame::Control` (`source::Frame` gains a variant; it
  is `#[non_exhaustive]`).
- **A broker that does not answer at capture's start is asked for again**
  (`capture-recovers-dead-connections`). It used to mean a sink that published
  nothing for the rest of the process's life — since the broker moved to the
  host's tailnet name, every capture started while the tailnet was down. Now
  capture archives at once, says once that the broker did not answer, and
  keeps asking after 1, 2, 4 … seconds, never more than a minute apart,
  publishing once it answers with no restart. A broker that answers and refuses
  the identity still stops capture, whether at the start or on a later attempt,
  with the same words.


## [0.1.0] - 2026-09-29

First release, published from `4ad9350`. Nothing was on crates.io before it, so
everything is new.

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

**Since this entry was first written** — newest first, each its own change.

- **The lead-lag's one-block peak explained and removed** (`resolve-the-block`).
  With every instrument stamped on the venue's shared block times,
  Hayashi–Yoshida at a shift of exactly 0 understates the correlation (ETH
  0.24 against 0.40 at −25 ms), so the peak fell to −100 ms everywhere.
  `galata-signals leadlag` scans ±1 ms, stores a peak within one block or
  indistinct from it as `lead_ms` 0, and adds `rho_btc_first`,
  `rho_other_first` and `block_asymmetry`.
- **Volatility forecast each day** (`galata-signals realvol`, `forecast-the-volatility`).
  HARQ (Bollerslev, Patton and Quaedvlieg 2016) on each instrument's daily
  realized variance from hourly bars: `sigma_1d`, `sigma_7d`, the day's
  realized `rv_sigma_1d`, `vol_term` (the day's RV over its 30-day mean), and
  the last 60 days' out-of-sample QLIKE of HARQ, yesterday's RV and the 30-day
  mean. galata-research's six-year study found HARQ beat GARCH at 1d on BTC
  and ETH. `derive-the-signals` runs it after `backtest`.
- **The xyz perps' moments keep to their session** (`keep-to-the-session`).
  `galata-signals moments` keeps an `xyz` perp's 5-minute return only when
  both its bars lie wholly in the CME Globex session, as Amaya et al. leave
  out the overnight return: the jump at Sunday's reopen held 93% of XYZ100's
  Σr⁴ on 27 September and made its kurtosis 45. Such rows' params say
  `"session": "external"`.
- **The xyz perps' sessions** (`tag-the-sessions`). `galata-signals basis`
  stores `external_share` for each `xyz` hour, the share of its minutes in the
  CME Globex session that trade.xyz's oracle follows (Sunday 18:00 to Friday
  17:00 New York, less the daily 17:00–18:00 break and the futures holidays,
  which come from the CMES calendar). The premium's `premium_z_30d`, refused
  for `xyz` until now, is taken for an hour wholly in session, against the
  stored hours that were too.

- **`galata-signals --asof <period end> [--redo]`**: compute a given whole
  period, and with `--redo` though it is stored, to repair one stored short.
  The repair is appended with its true computed time, and readers take the
  newest computation of an asof.

- **The stored tails backtested daily** (`galata-signals backtest`,
  Tier 16). For each fitted horizon and instrument, the last 90 days of `tail`
  rows against the bars they forecast: at 97.5%, the hit rate, Kupiec,
  Christoffersen's conditional coverage and DQ (`gr.models.evaluate`), and
  Acerbi and Székely's Z2 for the ES; at 99%, the hit rate, Kupiec and
  conditional coverage. It needs 250 matched forecasts.

- **Each instrument's next-bar tail** (`tail`, Tier 16). Every fitted horizon
  of `varcov` writes VaR 99%, VaR 97.5% and ES 97.5% (Basel FRTB) by filtered
  historical simulation: the walk's σ̂ₜ₊₁ and the margins' standardised
  residuals, a year of them and at least 1,000, so the fat tail is the data's.
  The ES/VaR ratio says how heavy it is.

- **Abnormal activity every hour** (`galata-signals activity`, Tier 16). Per
  instrument: the hour's notional, trade count and average trade size; their
  robust z and the notional's rank against the same hour of day on the same
  kind of day (weekday or weekend) over the signal's stored 28 days; and the
  share of notional in trades above the previous day's 95th-percentile size.
  `derive-the-signals` runs it after `leadlag`.

- **BTC's lead over each instrument, every hour** (`galata-signals leadlag`,
  Tier 16). The shifted Hayashi–Yoshida correlation of mid prices
  (galata-research `gr.leadlag.lead_lag`) to ±30 s: the lead in ms, ρ at the
  lead and at zero, and the lead/lag ratio, with a flag when the lead is within
  one block. `derive-the-signals` runs it after `cascade`.

- **Forced closures inferred every hour** (`galata-signals cascade`,
  Tier 16). Per instrument, from the marks' 1-minute open interest and price:
  long liquidations (price and open interest both falling, robust z ≤ −4)
  and short squeezes (price up, open interest down), clustered into events
  that each close at least 1% of open interest, sized as an upper bound.
  `derive-the-signals` runs it after `moments`.
- **Realized skewness and kurtosis every day** (`galata-signals moments`,
  Tier 16). Per instrument, from 5-minute log returns once a UTC day has
  closed: Amaya, Christoffersen, Jacobs and Vasquez's daily realized
  volatility, skewness and kurtosis, and their 7-day means. galata-research
  gains `timeseries.realized_moments`. `derive-the-signals` runs it after
  `flow`.
- **The order flow every hour** (`galata-signals flow`, Tier 16). Per
  instrument: Cont, Kukanov and Stoikov's order-flow imbalance over 10-s
  buckets, normalised by depth, with its price-impact slope and R²; the
  trade imbalance by aggressor and its R²; and the time-weighted queue
  imbalance with its share beyond ±0.5. galata-research gains
  `liquidity.ofi` and `liquidity.impact`. `derive-the-signals` runs it after
  `basis`.
- **The basis every hour** (`galata-signals basis`, Tier 16). From the tape's
  marks, per instrument: the venue's premium, time-weighted and winsorised,
  with its median; mark against oracle in bps; the hour's open-interest log
  change and its dollars; and a robust z of the premium against the signal's
  own 30 days, on the main dex only. `derive-the-signals` runs it after
  `liquidity`.
- **The monitor's alarm reaches the operator** (`surface-the-monitor`). When
  `[watch] monitor_alarms = true`, `galata-watch` reports a horizon whose
  newest `monitor` run alarmed, naming the pair and the bar its correlation
  changed at: Wied and Galeano's change-point estimate, now stored in the
  monitor's params as `change` and `change_at`. The tower's Signals panel
  shows the monitor per horizon. `watch::Thresholds` and `config::Watch` gain
  the switch.
- **A sequential correlation monitor** (`monitor`, Tier 16). Each fitted horizon
  writes Wied and Galeano's (2013) monitor of every pair's correlation over
  calendar epochs (5m 7 days, 1h 30, 4h 90), with the epoch before as the
  baseline. It uses galata-research `corr.monitor` (Bonferroni over the 15
  pairs, α 0.05, γ 0.25). The measures are `wied_galeano_ratio` (≥ 1 is an
  alarm) and `wied_galeano_alarm`, which bound the chance of any false alarm
  per epoch where the half-hourly `constancy` test cannot.

- **A correlation regime flag** (`flag-the-correlation-regime`).
  `galata-watch` reports a fitted horizon whose two newest `constancy` windows
  both have an Engle–Sheppard p-value below the new, optional
  `[watch] max_constancy_p` (the config suggests 0.001: a test repeated every
  half hour on overlapping windows alarms far more often than its nominal
  level). `watch::Thresholds` and `config::Watch` gain the field.

- **Whether the correlation stayed constant** (`constancy`, Tier 16). Each
  fitted horizon writes Engle and Sheppard's (2001) test of constant
  correlation: `engle_sheppard_stat` and `engle_sheppard_p` over the last 30
  days of its standardised returns, against the declared R (galata-research
  `corr.constancy`, White-robust, 5 lags). A small p is CCC failing.

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

- `Mark.mid` and `Mark.premium` in `galata-wire` (`#[serde(default)]`, so an
  envelope serialised before them still reads), carried by the Hyperliquid
  adapter from `midPx` and `premium`, and two tape columns, `marks.mid` and
  `marks.premium`, appended after `open_interest`.

### Changed

Made during development, against what this entry describes above. Nothing was
published before them, so "breaking" is relative to that, not to a release.

- **BREAKING: the walk paces by the venue's weight** (`pace-by-the-venue-weight`).
  `Budget.requests_per_minute` is now `Budget.weight_per_minute`, and each
  `Paging` states a `RequestCost { base, per_rows }` (`Paging::costing`); a
  venue that counts requests keeps the default weight of one. Hyperliquid
  declares candles 20 + 1 per 60 bars and funding 20 + 1 per 20 rows, from its
  docs; it had been paced 20–45× over its 1,200 a minute. Each call waits its
  own expected weight at the share. A 429 is a wait (`Retry-After`, else a
  minute), then the same page, at most three times at boot; a throttled fill
  holds every fill. `FetchResult`'s error is now `FetchFailure`.
- **Only the live width is walked before the subscription.** `walk_candles`
  and `walk_funding_days` are asked as history fills inside the live loop
  (`Capture::fill_history`), capped by `walk_cap` pages each and logged on
  completion, so a restart's hole no longer grows with the declared depth.
  A history fill that reaches the cap logs an error and capture continues.

- **`galata-segments`: a segment already present byte for byte is left
  alone** (`leave-an-identical-segment`). `SegmentWriter::finish` compares
  the new segment with the file at its final path, by size and then by bytes.
  When they are identical it discards its temporary and returns the existing
  path, with no sync and no rename, so the file keeps its mtime and inode. The
  hourly `--replace` projection re-derives about 10,000 identical segments: on
  a clone of production it went from 162 s to 41 s, and from 10,047 segments
  touched to none. `write_file` is unchanged.

- **Constant correlation at 5m, 1h and 4h** (`declare-constant-correlation`,
  the operator's choice). `varcov` accepts `corr = "ccc"` (Bollerslev 1990: R
  is the refit window's correlation of the standardised returns, a = b = 0),
  and `signals.toml` declares it for the fitted horizons, with the registered
  and the six-instrument evidence in the comment. Rows are labelled
  `garch-t/ccc` and `gjr-t/ccc`. Stored DCC rows are unchanged.

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

### Fixed

- **The signal calculators read the tape while the projection rewrote it**
  (`hold-the-tape`). `galata-signals` now holds galata-segments'
  `.compact.lock` shared around every calculator, so a projection in progress
  is waited out (up to 50 minutes). Without it the :45 run saw today's
  partition projected and yesterday's not yet, and read a 40-minute hour as
  whole.

- **The hourly and daily signals computed periods the tape did not yet hold
  whole** (`wait-for-the-tape`). The tape is projected at :40, so a run at
  :15 saw the hour to :00 without its last 20 minutes, stored it, and never
  recomputed it. `basis` was absent every hour, and `flow`, `liquidity`,
  `carry` and `moments` were computed short. Each now takes the latest period
  whole on the tape as well as by the clock (`galata_signals.frontier`). Rows
  already stored are not rewritten.

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

### The two clocks, which every user meets

`recv_micros` is ours and `at_micros` is the venue's. Coverage, gaps and
latency are measured in the first; the tape is sorted by the second. They are
not interchangeable and nothing in these crates pretends they are.
