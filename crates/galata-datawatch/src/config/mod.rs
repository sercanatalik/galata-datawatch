//! One file, one type, one load.
//!
//! Anything absent, unparseable, unknown or out of bounds **refuses here**, and
//! the process exits non-zero. A configuration validated in pieces at the point
//! of use fails halfway through a run, having already done something.
//!
//! # The source is a seam
//!
//! A source yields `(text, Origin)` and everything below is unchanged. That is
//! what lets a second source — a vault document rather than a file — drop in
//! later without changing a caller, and it is why `Origin` appears in every
//! refusal where a path would.

pub mod source;

pub use source::{ConfigSource, EnvSecrets, FileSource, Secret, SecretSource};

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use galata_wire::Series;
use serde::Deserialize;

/// Where a configuration's text came from.
///
/// It appears in every refusal, because *unknown key `walk_shre`* is not
/// actionable without it.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Origin {
    /// A file on disk.
    File(PathBuf),
    /// A versioned document from a vault. **Not implemented yet** — named so
    /// the shape of every refusal is settled before the second source exists.
    Document {
        /// Its name.
        name: String,
        /// The version read.
        version: u64,
    },
}

impl std::fmt::Display for Origin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Origin::File(path) => write!(f, "{}", path.display()),
            Origin::Document { name, version } => write!(f, "document {name} v{version}"),
        }
    }
}

/// Why a configuration was refused.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ConfigError {
    /// Two configuration sources were named at once.
    #[error(
        "{path_var} names {path} and {document_var} names {document}. Two sources is a question \
         about which one won, and the answer must never be whichever the code checked first — \
         unset one"
    )]
    TwoSources {
        /// The path variable.
        path_var: &'static str,
        /// What it said.
        path: String,
        /// The document variable.
        document_var: &'static str,
        /// What it said.
        document: String,
    },
    /// A document was named and nothing here can fetch one.
    #[error(
        "{document_var} names {document}, and this binary reads files. Fetching a document is a \
         vault client's job: read it and call `Config::load_from_str` with \
         `Origin::Document`, which takes no dependency on a vault from this crate"
    )]
    NoDocumentReader {
        /// The document variable.
        document_var: &'static str,
        /// What it said.
        document: String,
    },
    /// A reader that fetches documents was given no document to fetch.
    ///
    /// The mirror of [`ConfigError::NoDocumentReader`]: that one is a file
    /// reader handed a document, this is a document reader handed nothing.
    #[error(
        "{document_var} is not set, and it names the document to capture from. A file is the \
         other source, and {path_var} names one"
    )]
    NoDocumentNamed {
        /// The document variable.
        document_var: &'static str,
        /// The path variable, named so the other source is discoverable.
        path_var: &'static str,
    },
    /// A secret was not where it was said to be.
    #[error("{name} is not set. Nothing connects anonymously, and no default is invented")]
    SecretAbsent {
        /// The name it should have been under. **Never the value.**
        name: String,
    },
    /// A secret source answered, and declined.
    ///
    /// **Not [`ConfigError::SecretAbsent`].** *"is not set"* is an environment
    /// variable's sentence: it is what an operator reads and then goes and
    /// exports something. A vault that refuses has said something more useful
    /// and more specific — a token that is revoked, a secret that is not
    /// there, or a scope that **cannot decrypt a secret at all**, which is the
    /// answer a `config`-scoped token gets and is cryptographic rather than a
    /// permission check. Flattening that into *"is not set"* would send the
    /// reader to the wrong fix.
    #[error("{name} could not be read: {detail}")]
    SecretRefused {
        /// The name it was asked for. **Never the value.**
        name: String,
        /// What the source said. Never a credential, and never a vault
        /// authentication variable — `check-secret-reach.sh` holds the second,
        /// because a copy of the vault's naming rule disagrees rather than
        /// fails.
        detail: String,
    },
    /// The text could not be read.
    #[error("{origin}: {source}")]
    Read {
        /// Where it came from.
        origin: Origin,
        /// Why.
        #[source]
        source: std::io::Error,
    },
    /// The text is not the shape this type expects.
    #[error("{origin}: {detail}")]
    Parse {
        /// Where it came from.
        origin: Origin,
        /// What was wrong, as the parser saw it — including the key, for an
        /// unknown one.
        detail: String,
    },
    /// A value is outside its permitted range.
    #[error("{origin}: {field} is {value}, outside {bound}")]
    OutOfBounds {
        /// Where it came from.
        origin: Origin,
        /// Which field.
        field: &'static str,
        /// What it was.
        value: String,
        /// What it must be.
        bound: &'static str,
    },
    /// A venue declares a series its adapter cannot supply.
    #[error(
        "{origin}: [venue.{venue}] declares series `{series}`, which this venue neither streams \
         nor serves historically — it would subscribe to a channel that does not exist"
    )]
    UnsupportedSeries {
        /// Where it came from.
        origin: Origin,
        /// Which venue.
        venue: String,
        /// Which series.
        series: String,
    },
    /// A ledger account names a venue this build keeps no ledger for.
    #[error(
        "{origin}: [ledger.account.{alias}] names venue `{venue}`, which this build keeps no \
         ledger for. Ledgers compiled in: {known}"
    )]
    UnknownLedgerVenue {
        /// Where it came from.
        origin: Origin,
        /// Which account.
        alias: String,
        /// Which venue.
        venue: String,
        /// What is available.
        known: String,
    },
    /// An account's address was written into the configuration.
    #[error(
        "{origin}: [ledger.account.{alias}] {field} holds an address. An address is never written \
         in configuration: it identifies its owner on a public chain, and this document is read \
         by every service. Put it in the vault and name the variable with `address_var`"
    )]
    AddressInConfiguration {
        /// Where it came from.
        origin: Origin,
        /// Which account.
        alias: String,
        /// Which key held it. **Never the value.**
        field: &'static str,
    },
    /// An alias that cannot name an account.
    #[error("{origin}: [ledger.account.{alias}] is not a usable alias: {why}")]
    BadAlias {
        /// Where it came from.
        origin: Origin,
        /// The alias as written.
        alias: String,
        /// Why.
        why: String,
    },
    /// The ledger and the walk together claim more than the venue's budget.
    #[error(
        "{origin}: capture.walk_share {walk} and ledger.ledger_share {ledger} sum to more than \
         the whole budget. The venue counts both against one allowance per IP"
    )]
    SharesExceedBudget {
        /// Where it came from.
        origin: Origin,
        /// The walk's share.
        walk: f64,
        /// The ledger's share.
        ledger: f64,
    },
    /// The declared accounts, dexes and cadences cost more than the share allows.
    #[error(
        "{origin}: the ledger's declared polling for `{venue}` costs {cost:.1} weight a minute, \
         and ledger_share allows {allowed:.1}. Poll less often, declare fewer dexes, or raise \
         the share"
    )]
    LedgerOverBudget {
        /// Where it came from.
        origin: Origin,
        /// Which venue.
        venue: String,
        /// What the declared set costs, per minute.
        cost: f64,
        /// What the share allows, per minute.
        allowed: f64,
    },
    /// No adapter answers to this venue's name.
    #[error(
        "{origin}: [venue.{venue}] names a venue this build does not implement. Compiled in: {known}"
    )]
    UnknownVenue {
        /// Where it came from.
        origin: Origin,
        /// Which venue.
        venue: String,
        /// What is available.
        known: String,
    },
}

/// Where the stores live.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Paths {
    /// The record. Never rewritten except by compaction.
    pub archive: PathBuf,
    /// The local surface, which works when the sink does not.
    pub status: PathBuf,
    /// The tape — the projection that makes the record queryable.
    ///
    /// **A cache.** Deleting it loses nothing the archive does not hold, and a
    /// rebuild writes it again.
    pub tape: PathBuf,
}

/// The capture process's own cadences.
// Not `Eq`: `walk_share` is a share of a budget, and a float has no total
// equality. Nothing compares two of these for identity — `Config::hash` is what
// answers "is this the same configuration".
#[derive(Debug, Clone, Copy, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Capture {
    /// Seconds between commits. **A crash converts this window into a gap**,
    /// which is why it is declared rather than defaulted.
    pub flush_secs: u64,
    /// Seconds between status snapshots.
    pub status_secs: u64,
    /// How far back a cold start of the walk goes.
    pub cold_start_days: u32,
    /// The share of the venue's **stated** budget the walk may take.
    ///
    /// A share rather than a rate: the rate is the venue's declaration, and a
    /// number here would be one measured against a venue this file may not be
    /// describing.
    pub walk_share: f64,
    /// The most requests one series' walk will make.
    ///
    /// A bound so a run **says** it covered less rather than spending a budget
    /// nobody watched. Exceeding it exits non-zero, because this one is ours to
    /// raise.
    pub walk_cap: u32,
}

/// What the operator considers worth telling somebody about.
///
/// **Optional, and with no defaults.** Same argument as retention: a bound
/// right for one venue's cadence is wrong for the next, and a threshold nobody
/// chose is one nobody will believe when it fires. With no block, only
/// structural problems are checked — a ticker that became a directory is not a
/// matter of degree.
#[derive(Debug, Clone, Copy, Default, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Watch {
    /// The most segments a **closed** partition may hold before compaction is
    /// overdue. A partition still being written to is exempt: it is supposed to
    /// hold many small segments, which is what a two-second flush buys.
    pub max_segments_in_closed_partition: Option<usize>,
    /// How old the newest segment may be before the record is stale.
    pub max_record_age_secs: Option<u64>,
}

/// Where events go, if anywhere.
///
/// **Optional.** The record does not depend on the broker, so a configuration
/// with no `[broker]` block captures to disk and publishes nothing — a
/// supported state rather than a degraded one.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Broker {
    /// Where the broker is.
    pub url: String,
    /// The identity this process presents. **Nothing connects anonymously.**
    pub user: String,
    /// The name used to resolve the broker secret.
    ///
    /// `EnvSecrets` reads this as an environment-variable name;
    /// `VaultSecrets` reads it as a vault secret name. The field carries the
    /// name, never the secret. A password in a configuration file is a password
    /// in version control, and this file is committed.
    pub password_var: String,
    /// How many events may be outstanding towards the broker.
    ///
    /// **Exactly the number a broker stall can swallow before events start
    /// being dropped**, which is an operator's trade rather than ours — so it
    /// is declared, with no default.
    pub queue: usize,
}

/// What the operator declared should be kept, if anything.
///
/// **Optional, and with no defaults.** Numbers are the measurer's and horizons
/// are the operator's; a default here would be the builder answering *how long
/// should this be kept?* for someone who knows what the data is and does not.
/// A configuration with no `[retention]` block expires nothing.
#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Retention {
    /// Days to keep each venue's raw capture, by venue name.
    ///
    /// **A record.** Losing it is unrecoverable, so a venue absent from here is
    /// kept forever rather than assigned a number nobody chose.
    #[serde(default)]
    pub venue: BTreeMap<String, u32>,
    /// Days to keep the tape.
    ///
    /// **A cache.** Anything dropped comes back from `galata-tape-rebuild`, so
    /// this may be far shorter than any venue's without much thought.
    pub tape_days: Option<u32>,
}

impl Retention {
    /// The policy this declares.
    pub fn policy(&self) -> crate::retain::Policy {
        crate::retain::Policy {
            venues: self
                .venue
                .iter()
                .map(|(name, days)| (name.clone(), crate::retain::Horizon { days: *days }))
                .collect(),
            tape: self.tape_days.map(|days| crate::retain::Horizon { days }),
        }
    }
}

/// One instrument to capture.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct InstrumentDecl {
    /// The ticker, which is also the venue's bare symbol.
    pub ticker: String,
    /// The builder-deployed dex it lives on, where it is not on the main one.
    #[serde(default)]
    pub dex: Option<String>,
    /// The contract that emits its events, **on a chain venue**.
    ///
    /// A separate field rather than reusing `dex`, which is a different thing
    /// that happens to be a string. Two meanings in one field is a field whose
    /// validation cannot say which one is wrong.
    #[serde(default)]
    pub contract: Option<String>,
    /// How that contract counts, **on a chain venue**.
    ///
    /// No default here, and none downstream: stock tokens carry 18 and USDG
    /// carries 6, so a default is right for one and wrong for the other by a
    /// factor of a trillion.
    #[serde(default)]
    pub decimals: Option<u32>,
}

/// One venue to capture from.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct VenueConfig {
    /// The venue's own network selector.
    pub market: String,
    /// What to capture.
    pub series: Vec<Series>,
    /// The bar width subscribed live.
    pub candle: String,
    /// Bar widths the walk **fetches** beside `candle`, never subscribed live.
    ///
    /// The venue serves a rolling window of each width (Hyperliquid's most
    /// recent ~5,000 bars, measured 2026-09-25: `1h` back to March, `4h` to the
    /// June before last), so a width nobody declares is history leaving the
    /// venue uncaptured. Each is asked for its whole reach on every boot,
    /// because the record, dated by receipt, cannot say how far a width is
    /// covered. Named by the adapter and refused before anything connects if
    /// it cannot be. Absent means none.
    #[serde(default)]
    pub walk_candles: Vec<String>,
    /// The instruments.
    pub instruments: Vec<InstrumentDecl>,
    /// The variable holding this venue's endpoint, **on a chain venue**.
    ///
    /// **The name of a variable, never a URL.** This file is committed, and a
    /// keyed provider carries its key in the URL path — so a field that
    /// accepted a URL would be a field one `_var` suffix away from committing
    /// a credential to git.
    ///
    /// Absent means the venue's public default, which needs no key and which
    /// every reader can reach.
    #[serde(default)]
    pub rpc_url_var: Option<String>,
    /// Seconds between polls, **on a poll venue**, and required there.
    ///
    /// Also the width of the gap a single failed poll produces, which is why
    /// it is declared rather than defaulted — and why no default is invented
    /// for a venue whose limits are undocumented and explicitly variable.
    #[serde(default)]
    pub poll_secs: Option<u32>,
    /// The variable naming this venue's API key, **on a signing venue**.
    ///
    /// A name, never the key — resolved through the `SecretSource`, like every
    /// other secret, and never read by a tool that does not connect.
    #[serde(default)]
    pub api_key_var: Option<String>,
    /// The variable naming this venue's signing key (a base64 Ed25519 seed),
    /// **on a signing venue**. A name, never the key.
    #[serde(default)]
    pub private_key_var: Option<String>,
}

/// The ledger: account state, polled per venue. **Optional**; capture ignores it.
///
/// Its cadences and its share have **no defaults**, for the reason capture's
/// flush window has none: a snapshot cadence is also the width of the gap one
/// failed poll leaves, and a share is a claim on a budget the walk spends from
/// too.
// Not `Eq`: `ledger_share` is a float, as `walk_share` is.
#[derive(Debug, Clone, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Ledger {
    /// Its own root, beside the archive and the tape, **readable by its owner
    /// only**: its raw answers carry sub-account addresses.
    pub root: PathBuf,
    /// Seconds between snapshots of each account and dex.
    pub snapshot_secs: u64,
    /// Seconds between discovery runs.
    pub discover_secs: u64,
    /// Seconds between asking each account for its new events: fills,
    /// funding payments and ledger updates. **No default**, like the others:
    /// it is the latency of the ledger's history and a claim on the budget.
    ///
    /// Optional in the *parse*, and refused by the ledger when absent
    /// (`keeps_ledgers`). Capture and the maintenance tools read this document
    /// too: a key they required would stop capture restarting on the new
    /// binary before the operator had written it, and the old binary refuses
    /// a key it does not know, so no order of deploying the two would be safe.
    #[serde(default)]
    pub events_secs: Option<u64>,
    /// The fold's position tolerance, an absolute size: positions within it
    /// agree. **Undeclared is refused**, not zero by default: a reconciler
    /// whose tolerance nobody chose is one nobody will believe. Optional in the
    /// parse for the reason `events_secs` is (`keeps_ledgers`).
    #[serde(default)]
    pub fold_position_tolerance: Option<f64>,
    /// The fold's relative tolerance: realised P&L within it times a fill's
    /// closed notional agrees, and a basis within it times the price.
    /// Measured 2026-09-25: the venue's rounding stayed within 1.31×10⁻⁵ of
    /// notional on 779 closing fills; 2×10⁻⁵ is the suggested value.
    #[serde(default)]
    pub fold_relative_tolerance: Option<f64>,
    /// The share of the venue's stated budget the ledger may take, **beside**
    /// capture's `walk_share`: the venue counts both against one allowance.
    pub ledger_share: f64,
    /// The variable naming the per-deployment key that fingerprints addresses.
    /// A name, never the key.
    pub fingerprint_key_var: String,
    /// The accounts, by alias.
    #[serde(default)]
    pub account: BTreeMap<String, LedgerAccount>,
}

/// One declared account: a master, on a venue.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct LedgerAccount {
    /// The venue holding it.
    pub venue: String,
    /// The variable naming its address. **A name, never the address.**
    pub address_var: String,
    /// The dexes to snapshot, where the venue has several. `""` is the main
    /// one. Each keeps its own margin (`design/measured.md`, 2026-09-25).
    pub dexes: Vec<String>,
    /// **Present only to be refused by name.** Without it, `address = "0x…"`
    /// would be an unknown key, refused with a parser's sentence that does not
    /// say where the address should go instead.
    #[serde(default)]
    pub address: Option<String>,
}

/// What one venue's ledger costs, **as the venue states it**, in request
/// weight.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct LedgerCost {
    /// The venue's allowance, weight per minute.
    pub budget_per_minute: f64,
    /// One snapshot of one account on one dex.
    pub snapshot: f64,
    /// One listing of a master's sub-accounts.
    pub discovery: f64,
    /// One read of an account's mode.
    pub mode: f64,
    /// One events request (fills, funding or ledger updates), before any
    /// per-item weight the answer adds.
    pub events: f64,
}

impl Ledger {
    /// The weight a minute the **declared** set costs on one venue.
    ///
    /// Declared only: a sub-account is discovered at run time, and its cost is
    /// paced within the share by the ledger rather than guessed at load.
    pub fn declared_cost(&self, venue: &str, cost: &LedgerCost) -> f64 {
        let per_snapshot = 60.0 / self.snapshot_secs.max(1) as f64;
        let per_discovery = 60.0 / self.discover_secs.max(1) as f64;
        let per_events = match self.events_secs {
            Some(secs) => 60.0 / secs.max(1) as f64,
            None => 0.0,
        };
        self.account
            .values()
            .filter(|a| a.venue == venue)
            .map(|a| {
                a.dexes.len().max(1) as f64 * cost.snapshot * per_snapshot
                    + (cost.discovery + cost.mode) * per_discovery
                    // Three kinds a poll: fills, funding, ledger updates.
                    // Their per-item weight is paid at run time and paced
                    // within the share, as discovered accounts are.
                    + 3.0 * cost.events * per_events
            })
            .sum()
    }
}

/// The longest a declared alias may be: a token's 64, less the room a
/// discovered sub-account's `_s<n>` needs for any `u32` ordinal (12).
pub const MAX_DECLARED_ALIAS: usize = galata_wire::MAX_TOKEN - 12;

/// Whether a string is an EVM address: `0x` and forty hex digits.
fn looks_like_an_address(value: &str) -> bool {
    let value = value.trim();
    value.len() == 42
        && (value.starts_with("0x") || value.starts_with("0X"))
        && value[2..].chars().all(|c| c.is_ascii_hexdigit())
}

/// Everything the process was told.
#[derive(Debug, Clone, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// Where the stores live.
    pub paths: Paths,
    /// The process's own cadences.
    pub capture: Capture,
    /// What is kept, if anything. Absent means nothing expires.
    #[serde(default)]
    pub retention: Retention,
    /// Where events go. Absent means nowhere, and capture runs anyway.
    pub broker: Option<Broker>,
    /// What is worth reporting, if anything. Absent means structural only.
    #[serde(default)]
    pub watch: Watch,
    /// What to capture, per venue.
    pub venue: BTreeMap<String, VenueConfig>,
    /// The ledger, if one runs. Absent means no account is kept.
    #[serde(default)]
    pub ledger: Option<Ledger>,
}

/// What a caller must answer about an adapter, so this module can refuse a
/// configuration at load rather than at connect.
///
/// A trait rather than a direct call, so the loader itself names no venue —
/// `check-venue-boundary.sh` holds that.
pub trait Adapters {
    /// Whether a venue supplies a series, by either route.
    fn supplies(&self, venue: &str, series: Series) -> bool;
    /// Whether this build implements the venue at all.
    fn known(&self, venue: &str) -> bool;
    /// What it does implement, for a refusal that says what to do next.
    fn known_names(&self) -> Vec<&'static str>;
    /// What a venue's ledger costs, **where this build keeps a ledger for it**.
    fn ledger_cost(&self, venue: &str) -> Option<LedgerCost> {
        let _ = venue;
        None
    }
    /// Whether the loading tool **runs** the ledger, and so judges the
    /// `[ledger]` block's venues and cost.
    ///
    /// **Defaulted to no.** One deployment configuration is read by capture,
    /// the maintenance tools and the tower as well as the ledger, and a tool
    /// that runs no ledger has no business refusing one: before this, every
    /// one of them refused a document with a `[ledger]` block, because its
    /// resolver answered *no ledger here* for a venue it was never going to
    /// poll. The block's structure — an address in it, an alias, the cadence
    /// bounds, the two shares — is still checked by every tool.
    fn keeps_ledgers(&self) -> bool {
        false
    }
}

impl Config {
    /// Load from a source.
    ///
    /// **The one door.** A file, a document, or anything else that can say what
    /// the text is and where it came from — all of them cross the same
    /// validation, because a source that deserialised straight to this type
    /// would skip `Config::validate`, which is where the bounds and the
    /// unknown-key and unknown-venue refusals live.
    pub fn load(source: &dyn ConfigSource, adapters: &dyn Adapters) -> Result<Config, ConfigError> {
        let (text, origin) = source.read()?;
        Config::load_from_str(&text, origin, adapters)
    }

    /// Load from a file, which is [`Config::load`] over a [`FileSource`].
    ///
    /// Kept because a caller holding a path should not have to construct a
    /// source to use one.
    pub fn load_from(path: &Path, adapters: &dyn Adapters) -> Result<Config, ConfigError> {
        let origin = Origin::File(path.to_path_buf());
        let text = std::fs::read_to_string(path).map_err(|source| ConfigError::Read {
            origin: origin.clone(),
            source,
        })?;
        Config::load_from_str(&text, origin, adapters)
    }

    /// Load from text that came from somewhere.
    ///
    /// **Everything below this is source-agnostic**, which is what lets a vault
    /// document drop in later without changing a caller.
    pub fn load_from_str(
        text: &str,
        origin: Origin,
        adapters: &dyn Adapters,
    ) -> Result<Config, ConfigError> {
        let config: Config = toml::from_str(text).map_err(|e| ConfigError::Parse {
            origin: origin.clone(),
            detail: e.to_string(),
        })?;
        config.validate(&origin, adapters)?;
        Ok(config)
    }

    fn validate(&self, origin: &Origin, adapters: &dyn Adapters) -> Result<(), ConfigError> {
        if !(1..=60).contains(&self.capture.flush_secs) {
            return Err(ConfigError::OutOfBounds {
                origin: origin.clone(),
                field: "capture.flush_secs",
                value: self.capture.flush_secs.to_string(),
                bound: "1..=60 — a crash converts this window into a gap",
            });
        }
        if !(1..=300).contains(&self.capture.status_secs) {
            return Err(ConfigError::OutOfBounds {
                origin: origin.clone(),
                field: "capture.status_secs",
                value: self.capture.status_secs.to_string(),
                bound: "1..=300",
            });
        }

        if !(1..=3_650).contains(&self.capture.cold_start_days) {
            return Err(ConfigError::OutOfBounds {
                origin: origin.clone(),
                field: "capture.cold_start_days",
                value: self.capture.cold_start_days.to_string(),
                bound: "1..=3650",
            });
        }
        if !(0.001..=1.0).contains(&self.capture.walk_share) {
            return Err(ConfigError::OutOfBounds {
                origin: origin.clone(),
                field: "capture.walk_share",
                value: self.capture.walk_share.to_string(),
                bound: "0.001..=1.0 — a share of the venue's stated budget, never a rate",
            });
        }
        if !(1..=100_000).contains(&self.capture.walk_cap) {
            return Err(ConfigError::OutOfBounds {
                origin: origin.clone(),
                field: "capture.walk_cap",
                value: self.capture.walk_cap.to_string(),
                bound: "1..=100000",
            });
        }

        if let Some(broker) = &self.broker
            && !(1..=1_000_000).contains(&broker.queue)
        {
            return Err(ConfigError::OutOfBounds {
                origin: origin.clone(),
                field: "broker.queue",
                value: broker.queue.to_string(),
                bound: "1..=1000000 — the events a broker stall may swallow before they drop",
            });
        }

        // **A NATS URL may carry `nats://user:pass@host`, and this one must
        // not.** The password has a variable of its own, so userinfo here is
        // a credential in a committed file — and the connect line logs this
        // URL, which is only safe because of this refusal.
        if let Some(broker) = &self.broker
            && broker
                .url
                .split_once("://")
                .is_some_and(|(_, rest)| rest.split('/').next().is_some_and(|a| a.contains('@')))
        {
            return Err(ConfigError::OutOfBounds {
                origin: origin.clone(),
                field: "broker.url",
                // **Not the URL.** Echoing it back would put the credential in
                // the refusal that exists to keep it out.
                value: "<holds userinfo>".to_string(),
                bound: "no user:password in the URL — the password is named by password_var",
            });
        }

        for (name, venue) in &self.venue {
            if !adapters.known(name) {
                return Err(ConfigError::UnknownVenue {
                    origin: origin.clone(),
                    venue: name.clone(),
                    known: adapters.known_names().join(", "),
                });
            }
            for series in &venue.series {
                // Refused HERE, not at connect. A process that starts and then
                // cannot subscribe has already claimed it is capturing
                // something it is not.
                if !adapters.supplies(name, *series) {
                    return Err(ConfigError::UnsupportedSeries {
                        origin: origin.clone(),
                        venue: name.clone(),
                        series: series.as_str().to_string(),
                    });
                }
            }
        }
        if let Some(ledger) = &self.ledger {
            self.validate_ledger(ledger, origin, adapters)?;
        }
        Ok(())
    }

    fn validate_ledger(
        &self,
        ledger: &Ledger,
        origin: &Origin,
        adapters: &dyn Adapters,
    ) -> Result<(), ConfigError> {
        if !(1..=3_600).contains(&ledger.snapshot_secs) {
            return Err(ConfigError::OutOfBounds {
                origin: origin.clone(),
                field: "ledger.snapshot_secs",
                value: ledger.snapshot_secs.to_string(),
                bound: "1..=3600 — also the width of the gap one failed snapshot leaves",
            });
        }
        match ledger.events_secs {
            Some(secs) if !(1..=86_400).contains(&secs) => {
                return Err(ConfigError::OutOfBounds {
                    origin: origin.clone(),
                    field: "ledger.events_secs",
                    value: secs.to_string(),
                    bound: "1..=86400",
                });
            }
            None if adapters.keeps_ledgers() => {
                return Err(ConfigError::OutOfBounds {
                    origin: origin.clone(),
                    field: "ledger.events_secs",
                    value: "absent".to_string(),
                    bound: "1..=86400, declared: no default is invented for how often the \
                            ledger asks for an account's history",
                });
            }
            _ => {}
        }
        for (field, value, bound) in [
            (
                "ledger.fold_position_tolerance",
                ledger.fold_position_tolerance,
                1_000_000.0,
            ),
            (
                "ledger.fold_relative_tolerance",
                ledger.fold_relative_tolerance,
                0.01,
            ),
        ] {
            match value {
                Some(v) if !(0.0..=bound).contains(&v) => {
                    return Err(ConfigError::OutOfBounds {
                        origin: origin.clone(),
                        field,
                        value: v.to_string(),
                        bound: "a non-negative tolerance; the relative one at most 0.01",
                    });
                }
                None if adapters.keeps_ledgers() => {
                    return Err(ConfigError::OutOfBounds {
                        origin: origin.clone(),
                        field,
                        value: "absent".to_string(),
                        bound: "declared: a tolerance nobody declared is zero, and zero is a \
                                choice the operator makes, not the fold",
                    });
                }
                _ => {}
            }
        }
        if !(1..=86_400).contains(&ledger.discover_secs) {
            return Err(ConfigError::OutOfBounds {
                origin: origin.clone(),
                field: "ledger.discover_secs",
                value: ledger.discover_secs.to_string(),
                bound: "1..=86400",
            });
        }
        if !(0.001..=1.0).contains(&ledger.ledger_share) {
            return Err(ConfigError::OutOfBounds {
                origin: origin.clone(),
                field: "ledger.ledger_share",
                value: ledger.ledger_share.to_string(),
                bound: "0.001..=1.0 — a share of the venue's stated budget, never a rate",
            });
        }
        if self.capture.walk_share + ledger.ledger_share > 1.0 {
            return Err(ConfigError::SharesExceedBudget {
                origin: origin.clone(),
                walk: self.capture.walk_share,
                ledger: ledger.ledger_share,
            });
        }
        let mut venues = std::collections::BTreeSet::new();
        for (alias, account) in &ledger.account {
            // The alias is a partition level and a subject token, and `_` is
            // reserved: a discovered sub-account is `<master>_s<n>`, and a
            // declared `main_s1` would be indistinguishable from one.
            if let Err(e) = galata_wire::Account::new(alias.as_str()) {
                return Err(ConfigError::BadAlias {
                    origin: origin.clone(),
                    alias: alias.clone(),
                    why: e.to_string(),
                });
            }
            if alias.len() > MAX_DECLARED_ALIAS {
                return Err(ConfigError::BadAlias {
                    origin: origin.clone(),
                    alias: alias.clone(),
                    why: format!(
                        "longer than {MAX_DECLARED_ALIAS} characters, which leaves a discovered \
                         sub-account's `_s<n>` no room inside a token"
                    ),
                });
            }
            if alias.contains('_') {
                return Err(ConfigError::BadAlias {
                    origin: origin.clone(),
                    alias: alias.clone(),
                    why: "`_` is reserved for discovered sub-accounts, named `<master>_s<n>`"
                        .to_string(),
                });
            }
            if account.address.is_some() {
                return Err(ConfigError::AddressInConfiguration {
                    origin: origin.clone(),
                    alias: alias.clone(),
                    field: "address",
                });
            }
            if looks_like_an_address(&account.address_var) {
                return Err(ConfigError::AddressInConfiguration {
                    origin: origin.clone(),
                    alias: alias.clone(),
                    field: "address_var",
                });
            }
            if adapters.keeps_ledgers() && adapters.ledger_cost(&account.venue).is_none() {
                let known: Vec<&str> = adapters
                    .known_names()
                    .into_iter()
                    .filter(|v| adapters.ledger_cost(v).is_some())
                    .collect();
                return Err(ConfigError::UnknownLedgerVenue {
                    origin: origin.clone(),
                    alias: alias.clone(),
                    venue: account.venue.clone(),
                    known: if known.is_empty() {
                        "none".to_string()
                    } else {
                        known.join(", ")
                    },
                });
            }
            let mut seen = std::collections::BTreeSet::new();
            if account.dexes.is_empty() || !account.dexes.iter().all(|d| seen.insert(d)) {
                return Err(ConfigError::OutOfBounds {
                    origin: origin.clone(),
                    field: "ledger.account.dexes",
                    value: format!("{:?} for {alias}", account.dexes),
                    bound: "one or more distinct dexes; \"\" is the main one",
                });
            }
            venues.insert(account.venue.as_str());
        }
        for venue in venues.into_iter().filter(|_| adapters.keeps_ledgers()) {
            let Some(cost) = adapters.ledger_cost(venue) else {
                continue;
            };
            let spent = ledger.declared_cost(venue, &cost);
            let allowed = ledger.ledger_share * cost.budget_per_minute;
            if spent > allowed {
                return Err(ConfigError::LedgerOverBudget {
                    origin: origin.clone(),
                    venue: venue.to_string(),
                    cost: spent,
                    allowed,
                });
            }
        }
        Ok(())
    }

    /// An identifier that changes when any field does.
    ///
    /// What lets a consumer notice that two processes disagree about what they
    /// were told, which is otherwise invisible. FNV-1a: enough to tell two
    /// configurations apart, and it needs nothing.
    pub fn hash(&self) -> String {
        let rendered = format!("{self:?}");
        let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
        for byte in rendered.as_bytes() {
            hash ^= *byte as u64;
            hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
        }
        format!("{hash:016x}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Fake;
    impl Adapters for Fake {
        fn supplies(&self, venue: &str, series: Series) -> bool {
            venue == "hyperliquid"
                && matches!(
                    series,
                    Series::Trades | Series::Quotes | Series::Candles | Series::Funding
                )
        }
        fn known(&self, venue: &str) -> bool {
            venue == "hyperliquid"
        }
        fn known_names(&self) -> Vec<&'static str> {
            vec!["hyperliquid"]
        }
        fn ledger_cost(&self, venue: &str) -> Option<LedgerCost> {
            (venue == "hyperliquid").then_some(LedgerCost {
                budget_per_minute: 1_200.0,
                snapshot: 2.0,
                discovery: 20.0,
                mode: 20.0,
                events: 20.0,
            })
        }
        fn keeps_ledgers(&self) -> bool {
            true
        }
    }

    /// A tool that runs no ledger: capture, the maintenance tools, the tower.
    struct NoLedger;
    impl Adapters for NoLedger {
        fn supplies(&self, venue: &str, series: Series) -> bool {
            Fake.supplies(venue, series)
        }
        fn known(&self, venue: &str) -> bool {
            Fake.known(venue)
        }
        fn known_names(&self) -> Vec<&'static str> {
            Fake.known_names()
        }
    }

    const GOOD: &str = r#"
[paths]
archive = "var/archive"
status = "var/status"
tape = "var/tape"

[capture]
flush_secs = 2
status_secs = 1
cold_start_days = 7
walk_share = 0.25
walk_cap = 200

[venue.hyperliquid]
market = "mainnet"
series = ["trades", "quotes", "candles"]
candle = "1m"
instruments = [
  { ticker = "BTC" },
  { ticker = "XYZ100", dex = "xyz" },
]
"#;

    fn origin() -> Origin {
        Origin::File(PathBuf::from("config/datawatch.toml"))
    }

    fn load(text: &str) -> Result<Config, ConfigError> {
        Config::load_from_str(text, origin(), &Fake)
    }

    const LEDGER: &str = r#"
[ledger]
root = "var/ledger"
snapshot_secs = 10
discover_secs = 600
events_secs = 300
fold_position_tolerance = 0.0
fold_relative_tolerance = 0.00002
ledger_share = 0.25
fingerprint_key_var = "GALATA_LEDGER_FINGERPRINT_KEY"

[ledger.account.main]
venue = "hyperliquid"
address_var = "GALATA_LEDGER_HL_MAIN"
dexes = ["", "xyz"]
"#;

    fn with_ledger(ledger: &str) -> Result<Config, ConfigError> {
        load(&format!("{GOOD}{ledger}"))
    }

    #[test]
    fn a_declared_ledger_loads_and_capture_is_unchanged_without_one() {
        let config = with_ledger(LEDGER).unwrap();
        let ledger = config.ledger.unwrap();
        assert_eq!(ledger.account["main"].dexes, vec!["", "xyz"]);
        assert!(
            load(GOOD).unwrap().ledger.is_none(),
            "absent means no account is kept"
        );
    }

    #[test]
    fn a_tool_that_runs_no_ledger_loads_a_document_that_declares_one() {
        // The deployment's one document is read by capture and the tower as
        // well. Before this, both refused it: "keeps no ledger for".
        let text = format!("{GOOD}{LEDGER}");
        Config::load_from_str(&text, origin(), &NoLedger).expect("capture must still start");
        // An account on a venue nothing keeps a ledger for is the ledger's
        // refusal, not capture's.
        let elsewhere = text.replace(
            "venue = \"hyperliquid\"\naddress_var",
            "venue = \"rh-crypto\"\naddress_var",
        );
        assert!(Config::load_from_str(&elsewhere, origin(), &NoLedger).is_ok());
        assert!(Config::load_from_str(&elsewhere, origin(), &Fake).is_err());
        // Structure is still every tool's to refuse.
        let with_address = text.replace(
            "dexes = [\"\", \"xyz\"]",
            "dexes = [\"\"]\naddress = \"0x3f9aa0c1d2e3f4a5b6c7d8e9f0a1b2c3d4e5f6a7\"",
        );
        assert!(Config::load_from_str(&with_address, origin(), &NoLedger).is_err());
    }

    #[test]
    fn a_missing_events_cadence_is_refused() {
        let err = with_ledger(&LEDGER.replace("events_secs = 300\n", ""))
            .unwrap_err()
            .to_string();
        assert!(err.contains("events_secs"), "{err}");
    }

    #[test]
    fn the_fold_refuses_an_undeclared_tolerance() {
        let without = LEDGER.replace("fold_relative_tolerance = 0.00002\n", "");
        let err = with_ledger(&without).unwrap_err().to_string();
        assert!(err.contains("fold_relative_tolerance"), "{err}");
        // And capture loads the same document.
        assert!(Config::load_from_str(&format!("{GOOD}{without}"), origin(), &NoLedger).is_ok());
    }

    #[test]
    fn capture_starts_whether_or_not_the_events_cadence_is_written_yet() {
        // One document, many readers: the key the ledger needs must not stop
        // capture on either side of the operator writing it.
        let without = format!("{GOOD}{}", LEDGER.replace("events_secs = 300\n", ""));
        assert!(Config::load_from_str(&without, origin(), &NoLedger).is_ok());
        assert!(Config::load_from_str(&format!("{GOOD}{LEDGER}"), origin(), &NoLedger).is_ok());
    }

    #[test]
    fn events_polling_counts_against_the_share() {
        // Snapshots every 300 s and discovery every 600 s are cheap; asking
        // for events every second is 3 x 20 x 60 = 3,600 a minute alone.
        let err = with_ledger(
            &LEDGER
                .replace("snapshot_secs = 10", "snapshot_secs = 300")
                .replace("events_secs = 300", "events_secs = 1"),
        )
        .unwrap_err()
        .to_string();
        assert!(
            err.contains("events") || err.contains("weight a minute"),
            "{err}"
        );
        assert!(with_ledger(&LEDGER.replace("snapshot_secs = 10", "snapshot_secs = 300")).is_ok());
    }

    #[test]
    fn a_literal_address_is_refused_by_name() {
        let err = with_ledger(&LEDGER.replace(
            "dexes = [\"\", \"xyz\"]",
            "dexes = [\"\"]\naddress = \"0x3f9aa0c1d2e3f4a5b6c7d8e9f0a1b2c3d4e5f6a7\"",
        ))
        .unwrap_err()
        .to_string();
        assert!(err.contains("[ledger.account.main]"), "{err}");
        assert!(
            err.contains("address_var"),
            "says where it goes instead: {err}"
        );
        assert!(
            !err.contains("0x3f9a"),
            "the refusal must not repeat the address: {err}"
        );
    }

    #[test]
    fn an_address_written_where_its_variable_belongs_is_refused() {
        let err = with_ledger(&LEDGER.replace(
            "\"GALATA_LEDGER_HL_MAIN\"",
            "\"0x3f9aa0c1d2e3f4a5b6c7d8e9f0a1b2c3d4e5f6a7\"",
        ))
        .unwrap_err()
        .to_string();
        assert!(err.contains("address_var holds an address"), "{err}");
        assert!(!err.contains("0x3f9a"), "{err}");
    }

    #[test]
    fn a_missing_snapshot_cadence_is_refused() {
        let err = with_ledger(&LEDGER.replace("snapshot_secs = 10\n", ""))
            .unwrap_err()
            .to_string();
        assert!(err.contains("snapshot_secs"), "{err}");
    }

    #[test]
    fn two_shares_over_the_whole_are_refused() {
        let err = load(
            &format!("{GOOD}{LEDGER}")
                .replace("walk_share = 0.25", "walk_share = 0.8")
                .replace("ledger_share = 0.25", "ledger_share = 0.3"),
        )
        .unwrap_err()
        .to_string();
        assert!(
            err.contains("0.8") && err.contains("0.3"),
            "names both: {err}"
        );
    }

    #[test]
    fn a_cadence_the_share_cannot_afford_is_refused_with_both_figures() {
        // Two dexes every second at weight 2 is 240 a minute for snapshots;
        // one master's discovery and mode every 600 s is 4; three events
        // requests every 300 s is 12. 256 against the 1% share's 12.
        let err = with_ledger(
            &LEDGER
                .replace("snapshot_secs = 10", "snapshot_secs = 1")
                .replace("ledger_share = 0.25", "ledger_share = 0.01"),
        )
        .unwrap_err()
        .to_string();
        assert!(err.contains("256.0"), "the cost: {err}");
        assert!(err.contains("12.0"), "the allowance: {err}");
    }

    #[test]
    fn an_account_on_an_unimplemented_venue_is_refused() {
        let err = with_ledger(&LEDGER.replace("venue = \"hyperliquid\"", "venue = \"rh-crypto\""))
            .unwrap_err()
            .to_string();
        assert!(err.contains("rh-crypto"), "{err}");
        assert!(err.contains("Ledgers compiled in: hyperliquid"), "{err}");
    }

    #[test]
    fn an_alias_too_long_for_its_sub_accounts_is_refused() {
        let long = "a".repeat(MAX_DECLARED_ALIAS + 1);
        let err = with_ledger(
            &LEDGER.replace("[ledger.account.main]", &format!("[ledger.account.{long}]")),
        )
        .unwrap_err()
        .to_string();
        assert!(err.contains("`_s<n>` no room"), "{err}");
        let fits = "a".repeat(MAX_DECLARED_ALIAS);
        assert!(
            with_ledger(
                &LEDGER.replace("[ledger.account.main]", &format!("[ledger.account.{fits}]"))
            )
            .is_ok()
        );
    }

    #[test]
    fn an_alias_holding_the_reserved_separator_is_refused() {
        let err = with_ledger(&LEDGER.replace("[ledger.account.main]", "[ledger.account.main_s1]"))
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("reserved for discovered sub-accounts"),
            "{err}"
        );
    }

    #[test]
    fn a_ledger_cannot_be_declared_as_a_capture_series() {
        // `margin` is the ledger's series. A capture venue declaring it would
        // subscribe to a channel no market-data adapter serves.
        let err = load(&GOOD.replace(
            "series = [\"trades\", \"quotes\", \"candles\"]",
            "series = [\"trades\", \"margin\"]",
        ))
        .unwrap_err()
        .to_string();
        assert!(err.contains("margin"), "{err}");
    }

    #[test]
    fn the_shipped_shape_loads() {
        let c = load(GOOD).unwrap();
        assert_eq!(c.capture.flush_secs, 2);
        assert_eq!(c.venue["hyperliquid"].instruments.len(), 2);
        assert_eq!(
            c.venue["hyperliquid"].instruments[1].dex.as_deref(),
            Some("xyz")
        );
    }

    #[test]
    fn a_venue_with_no_walk_widths_walks_its_live_width_only() {
        assert!(
            load(GOOD).unwrap().venue["hyperliquid"]
                .walk_candles
                .is_empty()
        );
    }

    #[test]
    fn declared_walk_widths_load_in_order() {
        let text = GOOD.replace(
            "candle = \"1m\"",
            "candle = \"1m\"\nwalk_candles = [\"1h\", \"4h\", \"1d\"]",
        );
        assert_eq!(
            load(&text).unwrap().venue["hyperliquid"].walk_candles,
            ["1h", "4h", "1d"]
        );
    }

    #[test]
    fn an_unknown_key_is_refused_by_name() {
        let text = GOOD.replace("flush_secs = 2", "flush_secs = 2\nflush_secx = 3");
        let err = load(&text).unwrap_err().to_string();
        assert!(err.contains("flush_secx"), "{err}");
        assert!(
            err.contains("datawatch.toml"),
            "the refusal must say where: {err}"
        );
    }

    #[test]
    fn a_value_out_of_bounds_is_refused_with_its_bound() {
        let text = GOOD.replace("flush_secs = 2", "flush_secs = 0");
        let err = load(&text).unwrap_err().to_string();
        assert!(err.contains("flush_secs"), "{err}");
        assert!(err.contains("gap"), "the bound must say why: {err}");
    }

    #[test]
    fn a_series_the_venue_cannot_serve_is_refused_at_load() {
        // Not at connect. A process that starts and then cannot subscribe has
        // already claimed it is capturing something it is not.
        let text = GOOD.replace(
            r#"["trades", "quotes", "candles"]"#,
            r#"["trades", "book"]"#,
        );
        let err = load(&text).unwrap_err().to_string();
        assert!(err.contains("book"), "{err}");
        assert!(err.contains("hyperliquid"), "{err}");
    }

    #[test]
    fn an_unimplemented_venue_is_refused_by_listing_the_known() {
        let text = GOOD.replace("[venue.hyperliquid]", "[venue.kraken]");
        let err = load(&text).unwrap_err().to_string();
        assert!(err.contains("kraken"), "{err}");
        assert!(
            err.contains("hyperliquid"),
            "a refusal must say what IS available: {err}"
        );
    }

    #[test]
    fn one_changed_field_changes_the_hash() {
        let a = load(GOOD).unwrap();
        let b = load(&GOOD.replace("flush_secs = 2", "flush_secs = 3")).unwrap();
        assert_eq!(a.hash(), a.hash(), "stable");
        assert_ne!(a.hash(), b.hash());
    }

    #[test]
    fn a_dex_changes_the_hash() {
        // Two processes told different instruments must not report the same
        // configuration.
        let a = load(GOOD).unwrap();
        let b = load(&GOOD.replace(r#"dex = "xyz""#, r#"dex = "other""#)).unwrap();
        assert_ne!(a.hash(), b.hash());
    }

    #[test]
    fn a_refusal_from_a_document_names_the_document() {
        // The second source does not exist yet; the shape of its refusal does.
        let err = Config::load_from_str(
            "not toml at all {{{",
            Origin::Document {
                name: "datawatch".into(),
                version: 12,
            },
            &Fake,
        )
        .unwrap_err()
        .to_string();
        assert!(err.contains("document datawatch v12"), "{err}");
    }

    #[test]
    fn a_walk_share_above_the_whole_budget_is_refused() {
        // A share, never a rate. Above 1.0 is asking for more than the venue
        // said it would serve.
        let err = load(&GOOD.replace("walk_share = 0.25", "walk_share = 2.0"))
            .unwrap_err()
            .to_string();
        assert!(err.contains("walk_share"), "{err}");
        assert!(err.contains("share of the venue"), "{err}");
    }

    #[test]
    fn a_walk_cap_of_zero_is_refused() {
        let err = load(&GOOD.replace("walk_cap = 200", "walk_cap = 0"))
            .unwrap_err()
            .to_string();
        assert!(err.contains("walk_cap"), "{err}");
    }

    #[test]
    fn a_configuration_with_no_retention_expires_nothing() {
        // Numbers are the measurer's and horizons are the operator's. The
        // shipped shape declares none, and that must not mean "some default".
        let c = load(GOOD).unwrap();
        assert!(c.retention.policy().is_empty());
        assert!(c.retention.venue.is_empty());
        assert_eq!(c.retention.tape_days, None);
    }

    #[test]
    fn a_declared_retention_becomes_a_policy() {
        let text =
            format!("{GOOD}\n[retention]\ntape_days = 3\n\n[retention.venue]\nhyperliquid = 90\n");
        let policy = load(&text).unwrap().retention.policy();
        assert!(!policy.is_empty());
        assert_eq!(policy.tape, Some(crate::retain::Horizon { days: 3 }));
        assert_eq!(
            policy.venues.get("hyperliquid"),
            Some(&crate::retain::Horizon { days: 90 })
        );
        // A venue absent from the block is kept forever rather than assigned a
        // number nobody chose.
        assert_eq!(policy.venues.get("rh-crypto"), None);
    }

    #[test]
    fn a_configuration_with_no_broker_publishes_nowhere() {
        // The record does not depend on the broker, so this is a supported
        // state rather than a degraded one.
        assert_eq!(load(GOOD).unwrap().broker, None);
    }

    #[test]
    fn a_broker_names_a_variable_and_never_a_secret() {
        let text = format!(
            "{GOOD}\n[broker]\nurl = \"nats://localhost:4222\"\nuser = \"datawatch\"\n\
             password_var = \"GALATA_DATAWATCH_PASSWORD\"\nqueue = 8192\n"
        );
        let broker = load(&text).unwrap().broker.unwrap();
        assert_eq!(broker.password_var, "GALATA_DATAWATCH_PASSWORD");
        assert_eq!(broker.queue, 8192);
        // There is no field a secret could be written into.
        assert!(!format!("{broker:?}").to_lowercase().contains("password\":"));
    }

    #[test]
    fn a_broker_url_carrying_a_password_is_refused_without_echoing_it() {
        let err = load(&format!(
            "{GOOD}\n[broker]\nurl = \"nats://u:hunter2@host:4222\"\nuser = \"u\"\n\
             password_var = \"V\"\nqueue = 8\n"
        ))
        .unwrap_err()
        .to_string();
        assert!(err.contains("broker.url"), "{err}");
        assert!(err.contains("password_var"), "{err}");
        // The refusal that exists to keep a credential out does not print it.
        assert!(!err.contains("hunter2"), "{err}");
    }

    #[test]
    fn a_path_containing_an_at_sign_is_not_userinfo() {
        // The check looks at the AUTHORITY only. A `@` after the first `/` is
        // part of a path and refusing it would be a rule that is wrong.
        load(&format!(
            "{GOOD}\n[broker]\nurl = \"nats://host:4222/a@b\"\nuser = \"u\"\n\
             password_var = \"V\"\nqueue = 8\n"
        ))
        .unwrap();
    }

    #[test]
    fn a_queue_of_zero_is_refused() {
        let text = format!(
            "{GOOD}\n[broker]\nurl = \"nats://x\"\nuser = \"u\"\npassword_var = \"V\"\nqueue = 0\n"
        );
        let err = load(&text).unwrap_err().to_string();
        assert!(err.contains("broker.queue"), "{err}");
    }
}
