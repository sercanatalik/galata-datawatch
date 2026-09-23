//! The claims this crate exists to defend, seen from outside it.
//!
//! An integration test rather than a `#[cfg(test)]` module, for the reason
//! `out_of_tree_venue.rs` gives one level down: cargo builds this as its own
//! crate, so it reaches only what a stranger reaches.
//!
//! **None of these needs a vault**, which is the point. The document and the
//! text are separable, so the claims about what happens to the text are
//! testable without a server — and what needs a server is named in the change
//! rather than faked here.

use std::path::{Path, PathBuf};

use galata_datawatch::config::source::{CONFIG_DOCUMENT_VAR, CONFIG_PATH_VAR, document_named};
use galata_datawatch::config::{
    Adapters, Config, ConfigError, ConfigSource, FileSource, Origin, Secret, SecretSource,
};
use galata_datawatch_vault::VaultConfig;
use galata_wire::Series;

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

/// Write `text` where a `FileSource` can read it, and return both sources.
fn both_sources(dir: &Path, text: &str) -> (FileSource, VaultConfig) {
    let path = dir.join("datawatch.toml");
    std::fs::write(&path, text).expect("the fixture must be writable");
    (
        FileSource::new(path),
        VaultConfig::from_document("datawatch", 7, text),
    )
}

fn scratch(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("galata-datawatch-vault-{name}"));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("a scratch directory");
    dir
}

#[test]
fn a_document_and_a_file_configure_the_same_process() {
    let dir = scratch("same-process");
    let (file, document) = both_sources(&dir, GOOD);

    let from_file = Config::load(&file, &Fake).expect("the file loads");
    let from_document = Config::load(&document, &Fake).expect("the document loads");

    // Not a field-by-field comparison: the hash is what capture identifies a
    // configuration BY, so equal hashes is the claim that matters.
    assert_eq!(
        from_file.hash(),
        from_document.hash(),
        "the same text must configure the same process whichever source carried it"
    );
    assert_eq!(from_document.venue["hyperliquid"].instruments.len(), 2);
}

#[test]
fn an_unknown_key_in_a_document_is_refused_by_name() {
    // **The test that fails if anybody reaches for `ConfigDocument::deserialize`.**
    // That call reaches a typed value without `Config::validate`, where this
    // refusal lives — so an unknown key would simply be ignored and capture
    // would run against a configuration nobody wrote.
    let text = GOOD.replace("flush_secs = 2", "flush_secs = 2\nflush_secx = 3");
    let document = VaultConfig::from_document("datawatch", 7, text);

    let refusal = Config::load(&document, &Fake)
        .expect_err("an unknown key must be refused")
        .to_string();

    assert!(refusal.contains("flush_secx"), "name the key: {refusal}");
    assert!(
        refusal.contains("document datawatch v7"),
        "and name the document and the version actually read: {refusal}"
    );
}

#[test]
fn a_refusal_names_the_version_that_was_served() {
    // The version in the origin is the document's own, so a refusal describes
    // the text that was refused rather than the request that preceded it.
    let document = VaultConfig::from_document("datawatch", 41, "flush_secs = 2");
    assert_eq!(
        document.origin(),
        &Origin::Document {
            name: "datawatch".to_owned(),
            version: 41,
        }
    );
    assert_eq!(document.origin().to_string(), "document datawatch v41");
}

#[test]
fn both_variables_set_is_refused_from_the_document_side_too() {
    // The file side already refuses this. The claim here is that naming a
    // document does NOT become a precedence rule now that something can fetch
    // one — and that both binaries reach the same refusal, because it is the
    // same function.
    let refusal = document_named(
        Some("config/datawatch.toml".to_owned()),
        Some("datawatch".to_owned()),
    )
    .expect_err("two sources at once is a refusal")
    .to_string();

    assert!(refusal.contains(CONFIG_PATH_VAR), "{refusal}");
    assert!(refusal.contains(CONFIG_DOCUMENT_VAR), "{refusal}");
    assert!(
        refusal.contains("config/datawatch.toml") && refusal.contains("datawatch"),
        "both values, so the operator knows which to unset: {refusal}"
    );
}

#[test]
fn a_document_named_alone_is_the_document_to_fetch() {
    assert_eq!(
        document_named(None, Some("datawatch".to_owned())).expect("one source is enough"),
        "datawatch"
    );
}

#[test]
fn naming_no_document_is_refused_rather_than_defaulted() {
    // No invented default. A document name is not something to guess at: the
    // wrong guess is a process that captures the wrong instruments and reports
    // success.
    let refusal = document_named(None, None)
        .expect_err("a document reader with no document named must refuse")
        .to_string();
    assert!(refusal.contains(CONFIG_DOCUMENT_VAR), "{refusal}");
    assert!(
        refusal.contains(CONFIG_PATH_VAR),
        "and name the other source, so it is discoverable: {refusal}"
    );
}

#[test]
fn a_fetched_document_holds_no_vault() {
    // **The capture path cannot reach the vault**, and this is the structural
    // half of that claim rather than the remembered half: `VaultConfig` has no
    // lifetime parameter, so it borrows nothing from the `Vault` that served
    // it. If a future change stored the client to re-fetch, this stops
    // compiling — which is the point.
    fn only_owned_data<T: ConfigSource + Send + Sync + 'static>(
        source: T,
    ) -> Box<dyn ConfigSource> {
        Box::new(source)
    }

    let boxed = only_owned_data(VaultConfig::from_document("datawatch", 1, GOOD));
    let (text, origin) = boxed
        .read()
        .expect("a document in hand cannot fail to read");
    assert!(text.contains("hyperliquid"));
    assert!(matches!(origin, Origin::Document { .. }));
}

/// A refusal to hand over a secret says which secret, and never the secret.
///
/// **Why this is not `SecretAbsent`.** That variant says *"{name} is not set"*,
/// which is an environment variable's sentence: an operator reads it and goes
/// and exports something. Measured against a real `gv-server local` on
/// 2026-09-23, a vault says better things, and they point at different fixes:
///
/// ```text
///   config token   GALATA_DATAWATCH_PASSWORD could not be read: the token's
///                  vault: this credential can list names but cannot decrypt
///                  secrets
///   no such secret A_SECRET_NOBODY_SET could not be read: the token's vault:
///                  no secret named A_SECRET_NOBODY_SET
/// ```
///
/// The first is the `config` scope working as designed — the vault gives that
/// bundle no field for the vault key, so it is cryptography and not a
/// permission check — and *"is not set"* would have sent the reader to a
/// variable that was never involved.
///
/// **Neither names a credential or a vault authentication variable**, which is
/// `check-secret-reach.sh`'s second rule.
#[test]
fn a_refused_secret_names_the_secret_and_not_the_value() {
    let refusal = ConfigError::SecretRefused {
        name: "GALATA_DATAWATCH_PASSWORD".to_owned(),
        detail: "this credential can list names but cannot decrypt secrets".to_owned(),
    }
    .to_string();

    assert!(refusal.contains("GALATA_DATAWATCH_PASSWORD"), "{refusal}");
    assert!(refusal.contains("cannot decrypt secrets"), "{refusal}");
    // The sentence that would send an operator to the wrong fix.
    assert!(!refusal.contains("is not set"), "{refusal}");
}

/// The seam itself: `boot` takes whatever source its caller hands it.
///
/// Until 2026-09-23 `boot` named `EnvSecrets` in its own body, so a binary
/// that fetched its configuration from a vault still took its broker password
/// from the process environment and had no way to say otherwise. This is a
/// stranger's `SecretSource`, held as the trait object `boot` accepts — if the
/// seam closes again, this stops compiling.
#[test]
fn a_caller_supplies_its_own_secret_source() {
    struct FromNowhere;
    impl SecretSource for FromNowhere {
        fn secret(&self, name: &str) -> Result<Secret, ConfigError> {
            Err(ConfigError::SecretRefused {
                name: name.to_owned(),
                detail: "this source holds nothing, on purpose".to_owned(),
            })
        }
    }

    let source: &dyn SecretSource = &FromNowhere;
    let refusal = source.secret("GALATA_DATAWATCH_PASSWORD").unwrap_err();
    assert!(refusal.to_string().contains("GALATA_DATAWATCH_PASSWORD"));
}
