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

use galata_datawatch::config::{ConfigError, ConfigSource, Origin};
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
