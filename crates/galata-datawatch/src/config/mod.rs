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
