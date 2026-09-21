//! Where a configuration and a secret come from.
//!
//! **Two traits, because the two failures are different.**
//!
//! ```text
//!   ConfigSource   what to capture. Wrong → refuse at boot, loudly, with the
//!                  origin in the message. Not a secret, so it can be printed.
//!
//!   SecretSource   what to authenticate with. Wrong → refuse at boot, and the
//!                  value must NEVER appear in the refusal, the log, or a
//!                  Debug rendering.
//! ```
//!
//! A single trait returning strings would make the second rule a convention.
//! Separate ones let the secret side return a type that **cannot print itself**.
//!
//! # A vault needs nothing from this crate
//!
//! [`Config::load_from_str`](super::Config::load_from_str) takes the text and
//! its provenance rather than a path, so a vault-backed loader is:
//!
//! ```ignore
//! let (text, version) = vault.get("datawatch").await?;
//! Config::load_from_str(&text, Origin::Document { name, version }, &Resolver)
//! ```
//!
//! and lives wherever the vault client already is. **This crate takes no vault
//! dependency to be vault-backed**, which is what lets it publish without one.
//!
//! What must *not* happen is a vault SDK's convenient `deserialize` straight to
//! a typed value: it skips [`Config::validate`], where the bounds, the
//! unknown-key refusal and the unknown-venue refusal live. The text goes through
//! the same door a file's does. One door.

use std::path::PathBuf;

use super::{ConfigError, Origin};

/// The environment variable naming a configuration file.
pub const CONFIG_PATH_VAR: &str = "GALATA_CONFIG";

/// The environment variable naming a configuration **document**.
///
/// Read only to refuse it alongside [`CONFIG_PATH_VAR`]; nothing here fetches a
/// document, because fetching one is a vault client's job.
pub const CONFIG_DOCUMENT_VAR: &str = "GALATA_CONFIG_DOCUMENT";

/// A secret, held so it cannot be said.
///
/// **No `Display`, and a `Debug` that withholds.** A secret reaches a log
/// through the most ordinary line somebody writes — `tracing::info!(?thing)` —
/// and the only reliable defence is for the value to be unable to say itself.
#[derive(Clone, PartialEq, Eq)]
pub struct Secret(String);

impl Secret {
    /// Hold one.
    pub fn new(value: impl Into<String>) -> Secret {
        Secret(value.into())
    }

    /// Hand it to the one thing that needs it.
    ///
    /// Named `expose` rather than `as_str` so that every use of it reads, at
    /// the call site, like the decision it is.
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Debug for Secret {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Secret(<held>)")
    }
}

/// Where a configuration's text comes from.
pub trait ConfigSource {
    /// The text, and where it came from.
    ///
    /// **Both**, because a refusal that cannot say where the configuration came
    /// from is a refusal somebody has to guess at — and with two possible
    /// sources, guessing is how the wrong one gets edited.
    fn read(&self) -> Result<(String, Origin), ConfigError>;
}

/// A file on disk.
#[derive(Debug, Clone)]
pub struct FileSource {
    /// Where.
    pub path: PathBuf,
}

impl FileSource {
    /// A source for a path.
    pub fn new(path: impl Into<PathBuf>) -> FileSource {
        FileSource { path: path.into() }
    }

    /// The source the environment names, or the shipped default.
    ///
    /// **Refuses when both variables are set**, naming both. A precedence
    /// rule — document wins, or file wins — is a rule somebody has to know, and
    /// the failure of not knowing it is a process that ran against the wrong
    /// configuration and reported success.
    pub fn from_env(default: &str) -> Result<FileSource, ConfigError> {
        FileSource::reconcile(
            std::env::var(CONFIG_PATH_VAR).ok(),
            std::env::var(CONFIG_DOCUMENT_VAR).ok(),
            default,
        )
    }

    /// The rule itself, **taking the two values rather than reading them**.
    ///
    /// Separated from [`FileSource::from_env`] so it can be tested: this
    /// workspace forbids `unsafe`, and setting an environment variable is
    /// `unsafe` in edition 2024 — so a rule that read the environment for
    /// itself would be a rule no test could exercise. Which is the wrong way
    /// round, because the rule is the part with the judgement in it and the
    /// read is one line.
    pub fn reconcile(
        path: Option<String>,
        document: Option<String>,
        default: &str,
    ) -> Result<FileSource, ConfigError> {
        match (path, document) {
            (Some(path), Some(document)) => Err(ConfigError::TwoSources {
                path_var: CONFIG_PATH_VAR,
                path,
                document_var: CONFIG_DOCUMENT_VAR,
                document,
            }),
            (Some(path), None) => Ok(FileSource::new(path)),
            (None, Some(document)) => Err(ConfigError::NoDocumentReader {
                document_var: CONFIG_DOCUMENT_VAR,
                document,
            }),
            (None, None) => Ok(FileSource::new(default)),
        }
    }
}

impl ConfigSource for FileSource {
    fn read(&self) -> Result<(String, Origin), ConfigError> {
        let origin = Origin::File(self.path.clone());
        let text = std::fs::read_to_string(&self.path).map_err(|source| ConfigError::Read {
            origin: origin.clone(),
            source,
        })?;
        Ok((text, origin))
    }
}

/// Where a secret comes from.
pub trait SecretSource {
    /// The secret a name refers to.
    ///
    /// The **name** is in the error; the value never is.
    fn secret(&self, name: &str) -> Result<Secret, ConfigError>;
}

/// The process environment.
///
/// **This reads no vault token.** How a vault client authenticates itself is
/// the vault's business, stated once in its own documentation; a copy of that
/// rule here would be a second implementation that disagrees rather than fails.
#[derive(Debug, Default, Clone, Copy)]
pub struct EnvSecrets;

impl SecretSource for EnvSecrets {
    fn secret(&self, name: &str) -> Result<Secret, ConfigError> {
        std::env::var(name)
            .map(Secret::new)
            .map_err(|_| ConfigError::SecretAbsent {
                name: name.to_string(),
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_secret_cannot_be_printed() {
        // Not a convention — the type is unable to say it.
        let secret = Secret::new("hunter2");
        assert_eq!(format!("{secret:?}"), "Secret(<held>)");
        assert!(!format!("{secret:?}").contains("hunter2"));
        // And it is handed over only by a name that reads like a decision.
        assert_eq!(secret.expose(), "hunter2");
    }

    #[test]
    fn a_secret_has_no_display() {
        // Asserted by the compiler: uncommenting the line below fails to build,
        // which is the only way to assert the absence of a trait. Stated here
        // so the next person knows it is deliberate rather than forgotten.
        //
        //     format!("{}", Secret::new("x"));
        //
        // `Display` is what `{}` uses, and `{}` is what somebody writes when
        // they are in a hurry.
        let printed = format!("{:?}", Secret::new("x"));
        assert!(!printed.contains("\"x\""));
    }

    #[test]
    fn an_absent_secret_names_where_it_should_have_come_from() {
        let error = EnvSecrets
            .secret("GALATA_A_VARIABLE_NOBODY_SET")
            .unwrap_err()
            .to_string();
        assert!(error.contains("GALATA_A_VARIABLE_NOBODY_SET"), "{error}");
    }

    #[test]
    fn a_file_source_carries_its_origin_into_the_refusal() {
        let source = FileSource::new("/a/path/that/is/not/there.toml");
        let error = source.read().unwrap_err().to_string();
        assert!(error.contains("/a/path/that/is/not/there.toml"), "{error}");
    }

    #[test]
    fn both_sources_at_once_is_a_refusal_naming_both() {
        // A precedence rule is a rule somebody has to know, and the failure of
        // not knowing it is a process that ran against the wrong configuration
        // and reported success.
        let error = FileSource::reconcile(
            Some("/etc/a.toml".into()),
            Some("datawatch".into()),
            "default.toml",
        )
        .unwrap_err()
        .to_string();
        assert!(error.contains("/etc/a.toml"), "{error}");
        assert!(error.contains("datawatch"), "{error}");
        assert!(error.contains(CONFIG_PATH_VAR), "{error}");
        assert!(error.contains(CONFIG_DOCUMENT_VAR), "{error}");
        assert!(error.contains("unset one"), "{error}");
    }

    #[test]
    fn a_document_alone_says_who_should_fetch_it() {
        // This binary reads files. Fetching a document is a vault client's job,
        // and the refusal points at the door rather than inventing one — which
        // is also the sentence that says this crate takes no vault dependency.
        let error = FileSource::reconcile(None, Some("datawatch".into()), "default.toml")
            .unwrap_err()
            .to_string();
        assert!(error.contains("load_from_str"), "{error}");
        assert!(error.contains("Origin::Document"), "{error}");
    }

    #[test]
    fn neither_set_takes_the_shipped_default() {
        let source = FileSource::reconcile(None, None, "config/x.toml").unwrap();
        assert_eq!(source.path, PathBuf::from("config/x.toml"));
    }

    #[test]
    fn a_path_alone_is_taken() {
        let source = FileSource::reconcile(Some("/etc/b.toml".into()), None, "d.toml").unwrap();
        assert_eq!(source.path, PathBuf::from("/etc/b.toml"));
    }
}
