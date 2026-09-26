//! Capture configured from a galata-vault document.
//!
//! **This crate exists so that four others do not have to.** `config/source.rs`
//! in `galata-datawatch` states that the crate takes no vault dependency in
//! order to publish without one. Putting the vault behind a cargo feature there
//! would have made that sentence a claim about a default rather than a fact:
//! Cargo unifies a dependency's features across one build, so a feature decides
//! what is compiled, not what a binary can reach. Here it is a fact about the
//! dependency graph, and `scripts/check-vault-reach.sh` asks cargo rather than
//! reading a manifest.
//!
//! # The fetch is eager, and that is the design
//!
//! [`ConfigSource::read`] returns a `ConfigError`, whose vocabulary is about
//! *text that did not parse* — it has no way to say *the vault refused this
//! token*. So the fetch happens first, in [`VaultConfig::fetch`], where the
//! three boot failures have their own type and their own messages; what reaches
//! the loader is a document already in hand, and `read` cannot fail.
//!
//! This is also what the rule *fetch at boot, never on the capture path* looks
//! like when it is structural rather than remembered: there is no vault in the
//! value the loop holds.
//!
//! # The text goes through the same door a file's does
//!
//! [`ConfigDocument::deserialize`](galata_vault::ConfigDocument::deserialize)
//! exists, is convenient, and would be a mistake: it reaches a typed value
//! without passing `Config::validate`, where the bounds, the unknown-key
//! refusal and the unknown-venue refusal live. This takes `text()` instead, and
//! `an_unknown_key_in_a_document_is_refused_by_name` is what fails if anybody
//! changes that.

use galata_datawatch::config::{ConfigError, ConfigSource, Origin, Secret, SecretSource};
use galata_vault::{ErrorKind, Vault};

/// A configuration document, fetched and in hand.
///
/// Construct it with [`VaultConfig::fetch`]; it implements [`ConfigSource`] so
/// the text crosses `Config::validate` exactly as a file's does.
#[derive(Debug, Clone)]
pub struct VaultConfig {
    text: String,
    origin: Origin,
}

/// Why a configuration document did not arrive.
///
/// Three causes, because the operator does something different for each. A
/// single "could not load configuration" would be one message standing in for
/// *start the server*, *mint a token* and *fix the document*.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum VaultConfigError {
    /// No vault answered.
    #[error(
        "the vault could not be reached within {timeout_secs}s, so {document} was never read: \
         {detail}. Capture will not start on a document it did not fetch — a process running \
         against last week's instruments reports success while capturing the wrong thing"
    )]
    Unreachable {
        /// The document it wanted.
        document: String,
        /// How long it waited, from the client's own default.
        timeout_secs: u64,
        /// What the vault said. **Never the server**: an endpoint in an error
        /// message is what `check-endpoint-reach.sh` exists to prevent, and the
        /// vault states its own address in its own documentation.
        detail: String,
    },

    /// The vault answered and declined.
    ///
    /// **The token's value appears nowhere in this**, and neither does a scope.
    /// An earlier version of this message said *a token whose scope includes
    /// `config`*, and a soak against a real vault showed it was wrong: a
    /// `read` token serves configuration documents perfectly well, and only
    /// `meta` is refused. Which credentials may read a config is the vault's
    /// rule, stated in its own message — restating it here is the same mistake
    /// `check-secret-reach.sh` refuses for the vault's authentication
    /// variables, and it disagreed rather than failed. (That guard refused
    /// this very comment for naming one of them, which is the rule working.)
    #[error("the vault refused to serve {document}: {detail}")]
    Refused {
        /// The document it wanted.
        document: String,
        /// The vault's own message. Never a credential.
        detail: String,
    },

    /// The document is there and is not text.
    #[error(
        "{document} v{version} is not UTF-8. A configuration is text; this was written by \
         something that did not think so"
    )]
    NotText {
        /// The document.
        document: String,
        /// The version read.
        version: u64,
    },
}

impl VaultConfig {
    /// Fetch a named document, once, at boot.
    ///
    /// The version in [`Origin::Document`] is the one the vault actually
    /// served, not the one that was asked for — so a refusal names the text
    /// that was refused rather than the request that preceded it.
    pub fn fetch(vault: &Vault, document: &str) -> Result<VaultConfig, VaultConfigError> {
        let fetched = vault.config(document).map_err(|error| {
            let detail = error.message().to_owned();
            match error.kind() {
                // Nothing answered *mid-fetch*. The ordinary unreachable case
                // never arrives here: `Vault::from_env` opens the vault, so a
                // refused connection is reported by the SDK before this is
                // called — measured at 40ms against a stopped server. This
                // covers a vault that goes away between opening and reading.
                ErrorKind::Transport => VaultConfigError::Unreachable {
                    document: document.to_owned(),
                    timeout_secs: FETCH_TIMEOUT_SECS,
                    detail,
                },
                // Answered, and said no: a bad, revoked or expired token, a
                // scope that does not reach configuration, or no such document.
                _ => VaultConfigError::Refused {
                    document: document.to_owned(),
                    detail,
                },
            }
        })?;

        let version = fetched.version();
        let text = fetched
            .text()
            .map_err(|_| VaultConfigError::NotText {
                document: document.to_owned(),
                version,
            })?
            .to_owned();

        Ok(VaultConfig {
            text,
            origin: Origin::Document {
                name: fetched.name().to_owned(),
                version,
            },
        })
    }

    /// A document already in hand.
    ///
    /// [`VaultConfig::fetch`] is for a [`galata_vault::Vault`]; this is for
    /// anyone whose client is a different one. `config/source.rs` says a
    /// vault-backed loader *lives wherever the vault client already is*, and
    /// that only holds if the type between the document and the loader can be
    /// built from a document's three facts rather than from one SDK.
    ///
    /// It takes the version the vault **served**, not the one asked for.
    pub fn from_document(
        name: impl Into<String>,
        version: u64,
        text: impl Into<String>,
    ) -> VaultConfig {
        VaultConfig {
            text: text.into(),
            origin: Origin::Document {
                name: name.into(),
                version,
            },
        }
    }

    /// Where this text came from, as every refusal will say it.
    pub fn origin(&self) -> &Origin {
        &self.origin
    }
}

impl ConfigSource for VaultConfig {
    fn read(&self) -> Result<(String, Origin), ConfigError> {
        // Infallible on purpose: the fetch already happened, and its failures
        // are `VaultConfigError` rather than being flattened into the loader's
        // vocabulary, which is about text that did not parse.
        Ok((self.text.clone(), self.origin.clone()))
    }
}

/// The wait a refusal reports, in seconds.
///
/// **Not a number chosen here.** It is `galata_vault_client::http::DEFAULT_TIMEOUT`,
/// documented as the whole-request timeout, restated so the refusal can say how
/// long it waited. Nothing in this tree has measured a better one, and inventing
/// a shorter one would be a figure with no entry in `design/measured.md`.
const FETCH_TIMEOUT_SECS: u64 = 120;

/// Secrets, from the vault the configuration came from.
///
/// **The gap this closes.** `galata-datawatch-vault` fetches its configuration
/// document from a vault and, until 2026-09-23, then took its broker password
/// from the process environment — because [`boot`](galata_datawatch::boot::boot)
/// named `EnvSecrets` itself and no caller could supply anything else. A
/// deployment that moved its configuration into a vault to stop holding it on
/// disk went on holding its password where `ps e`, a crash dump and every
/// child process can read it.
///
/// # What the name means
///
/// `password_var` names *where the password is*. `EnvSecrets` reads that name
/// as a variable; this reads it as a secret in the vault. The field is not
/// renamed, because the string is a reference and the source is what resolves
/// it — and because renaming it would break every configuration file in
/// existence to make one paragraph read better.
///
/// # The scope this cannot paper over
///
/// A `config`-scoped token **cannot read a secret**: the vault gives its
/// bundle no field for the vault key, so this is cryptography rather than a
/// permission check. A binary that fetches its document with one and then asks
/// for a password gets a refusal — a real one, carrying the vault's own
/// sentence, which is why [`galata_datawatch::config::ConfigError::SecretRefused`]
/// exists rather than the document's *"is not set"*.
///
/// Which token reaches both is the operator's decision — one `read` token, two
/// tokens, or a child vault holding one binary's credentials — and this type
/// deliberately makes all three askable without choosing.
#[derive(Debug, Clone, Copy)]
pub struct VaultSecrets<'a> {
    vault: &'a Vault,
}

impl<'a> VaultSecrets<'a> {
    /// Secrets from an already-open vault.
    ///
    /// **It takes an opened vault, never a token.** How a vault client
    /// authenticates is the vault's rule, stated once in its own
    /// documentation; `check-secret-reach.sh` refuses a copy of it here,
    /// because a second implementation of a naming rule disagrees rather than
    /// fails.
    pub fn new(vault: &'a Vault) -> VaultSecrets<'a> {
        VaultSecrets { vault }
    }
}

impl SecretSource for VaultSecrets<'_> {
    fn secret(&self, name: &str) -> Result<Secret, ConfigError> {
        let value = self
            .vault
            .secret(name)
            .map_err(|error| ConfigError::SecretRefused {
                name: name.to_owned(),
                // The vault's own message. It knows whether the token is
                // revoked, the secret absent, or the scope unable to decrypt
                // one at all; restating that here would be a second rule that
                // disagrees rather than fails.
                detail: error.message().to_owned(),
            })?;

        // **A password is text, and a vault holds bytes.** Anything that is
        // not UTF-8 is refused by name rather than lossily converted: a
        // password silently mangled into replacement characters authenticates
        // against nothing and the refusal would come from the broker, naming
        // the wrong thing.
        let text = std::str::from_utf8(value.expose()).map_err(|_| ConfigError::SecretRefused {
            name: name.to_owned(),
            detail: format!(
                "the vault holds {} bytes under this name and they are not UTF-8. A password is \
                 text; this was written by something that did not think so",
                value.len()
            ),
        })?;

        Ok(Secret::new(text))
    }
}

/// Named secrets, as `NAME=value` pairs for a child process's environment.
///
/// For `galata-vault-exec`, which starts a process that reads its secrets
/// from the environment — NATS's `$VAR`s, the tower's reader password, the
/// file binary's `EnvSecrets` — with each one read **through the vault's
/// token**. The vault decides which names that token may read, so a process
/// started with the tower's token cannot be handed capture's password
/// whatever the command line asks for.
///
/// The values leave the `Secret` type here, the one module of this member
/// permitted to (`check-endpoint-reach.sh`), because an environment variable
/// is a string. Every name must resolve; the first that does not is the
/// refusal, naming it.
pub fn environment_for(
    vault: &Vault,
    names: &[String],
) -> Result<Vec<(String, String)>, ConfigError> {
    let source = VaultSecrets::new(vault);
    names
        .iter()
        .map(|name| {
            source
                .secret(name)
                .map(|secret| (name.clone(), secret.expose().to_owned()))
        })
        .collect()
}

/// Days before a token's expiry that a service starts saying so.
///
/// **30, not the vault's own 14** (`galata_vault::EXPIRY_WARNING_SECS`, which
/// is about idle vault expiry and never fires here: no galata-vault server
/// expires an idle vault). The remedy is by hand — the owner at this machine,
/// `mint-service-tokens.sh`, then `install-services.sh` — and the warning is
/// printed only when a service starts, which under launchd's `KeepAlive` can
/// be weeks apart. The common manual-renewal practice warns at 30, 15 and 7
/// days; this takes the widest, once, because it is seen least often.
/// `install-services.sh --status` reads the verdict from `--expiry` rather
/// than restating the number.
pub const TOKEN_WARNING_DAYS: i64 = 30;

const DAY_SECS: i64 = 86_400;

/// What a token's expiry means now.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TokenNotice {
    /// More than [`TOKEN_WARNING_DAYS`] left.
    Ok {
        /// Whole days left.
        days: i64,
    },
    /// Inside the window, not yet lapsed.
    Soon {
        /// Whole days left; `0` on the last day.
        days: i64,
    },
    /// Lapsed. The vault refuses it; this only says so first.
    Expired,
}

impl TokenNotice {
    /// The state word `--expiry` prints: `ok`, `WARN` or `EXPIRED`.
    pub fn word(&self) -> &'static str {
        match self {
            TokenNotice::Ok { .. } => "ok",
            TokenNotice::Soon { .. } => "WARN",
            TokenNotice::Expired => "EXPIRED",
        }
    }

    /// Whole days left; `0` once lapsed, never negative.
    pub fn days(&self) -> i64 {
        match self {
            TokenNotice::Ok { days } | TokenNotice::Soon { days } => *days,
            TokenNotice::Expired => 0,
        }
    }
}

/// Judge a token expiring at `expires_at` against `now` (both Unix seconds).
///
/// **The clock is the caller's.** Only `galata-vault-exec`'s `main` reads
/// one, so this can be asked at any instant a test names.
pub fn token_notice(expires_at: i64, now: i64) -> TokenNotice {
    let left = expires_at - now;
    if left <= 0 {
        return TokenNotice::Expired;
    }
    let days = left / DAY_SECS;
    if days < TOKEN_WARNING_DAYS {
        TokenNotice::Soon { days }
    } else {
        TokenNotice::Ok { days }
    }
}

/// The UTC calendar day of a Unix instant, as `YYYY-MM-DD`.
///
/// Howard Hinnant's `civil_from_days`: one function, so no date crate.
pub fn utc_date(secs: i64) -> String {
    let z = secs.div_euclid(DAY_SECS) + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + i64::from(month <= 2);
    format!("{year:04}-{month:02}-{day:02}")
}
