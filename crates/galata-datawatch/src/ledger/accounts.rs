//! Accounts: who they are in the record, and who they are in the vault.
//!
//! ```text
//!   [ledger.account.main]  ──resolve──▶  alias `main`, address (a Secret),
//!   address_var = "…"                    fingerprint = HMAC(key, address)
//!
//!   the record ──read back──▶  Bindings: main_s1 ↔ fingerprint, main_s2 ↔ …
//! ```
//!
//! **Three refusals live here**, each preferring a stopped ledger to a history
//! that silently means something else: an alias whose address changed, an
//! alias whose history cannot be verified, and a segment that carries no
//! fingerprint to verify against.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use galata_wire::{Account, AccountSeen, Event, Origin, TokenError, Venue};
use hmac::{Hmac, Mac};
use sha2::Sha256;

use crate::config::{ConfigError, Ledger, Secret, SecretSource};
use crate::record::{ACCOUNT_FP_LABEL, AccountAddress};

/// How many hex digits of the HMAC a fingerprint keeps.
///
/// 64 bits: enough that two of one deployment's addresses colliding is not a
/// case worth handling (a birthday bound near four billion addresses), and
/// short enough to read in a refusal.
pub const FINGERPRINT_HEX: usize = 16;

/// The per-deployment key fingerprints are made under. Held as a secret.
#[derive(Debug, Clone)]
pub struct FingerprintKey(Secret);

impl FingerprintKey {
    /// Hold one.
    pub fn new(secret: Secret) -> FingerprintKey {
        FingerprintKey(secret)
    }
}

/// The keyed fingerprint of an address: HMAC-SHA-256, lower-cased input,
/// truncated to [`FINGERPRINT_HEX`] digits.
///
/// **Keyed**, because an unkeyed hash of an address is reversed by hashing the
/// public list of active ones. With the key it proves two segments came from
/// one address, and says nothing else.
pub fn fingerprint(key: &FingerprintKey, address: &str) -> String {
    // HMAC accepts a key of any length, so this cannot fail; the fallback is
    // there so no panic sits on the path every account takes at boot.
    let Ok(mut mac) = Hmac::<Sha256>::new_from_slice(key.0.expose().as_bytes()) else {
        return String::new();
    };
    mac.update(address.trim().to_ascii_lowercase().as_bytes());
    mac.finalize()
        .into_bytes()
        .iter()
        .take(FINGERPRINT_HEX / 2)
        .map(|b| format!("{b:02x}"))
        .collect()
}

/// A declared account, resolved: its alias, its venue, and its address held so
/// that it cannot print itself.
#[derive(Debug, Clone)]
pub struct ResolvedAccount {
    /// Its alias. What the record, the bus and the status surface know.
    pub alias: Account,
    /// The venue holding it.
    pub venue: Venue,
    /// The dexes to snapshot.
    pub dexes: Vec<String>,
    /// The keyed fingerprint of its address.
    pub fingerprint: String,
    address: Secret,
}

impl ResolvedAccount {
    /// Hand the address to the one thing that needs it: the request.
    pub fn address(&self) -> &Secret {
        &self.address
    }

    /// The same account under the alias a binding gave it.
    pub fn with_alias(mut self, alias: Account) -> ResolvedAccount {
        self.alias = alias;
        self
    }

    /// Where this account's payloads land in the record.
    pub fn record_address(&self) -> AccountAddress {
        AccountAddress {
            venue: self.venue.as_str().to_string(),
            account: self.alias.as_str().to_string(),
            fingerprint: self.fingerprint.clone(),
        }
    }

    /// An account known by an address the ledger found rather than was told:
    /// a discovered sub-account.
    pub fn discovered(
        alias: Account,
        venue: Venue,
        dexes: Vec<String>,
        key: &FingerprintKey,
        address: Secret,
    ) -> ResolvedAccount {
        ResolvedAccount {
            alias,
            venue,
            dexes,
            fingerprint: fingerprint(key, address.expose()),
            address,
        }
    }
}

/// Why the ledger will not write.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum LedgerError {
    /// The configuration or a secret could not be read.
    #[error(transparent)]
    Config(#[from] ConfigError),
    /// An alias is not a token.
    #[error(transparent)]
    Token(#[from] TokenError),
    /// The record could not be read back.
    #[error(transparent)]
    Replay(#[from] crate::replay::ReplayError),
    /// A segment's footer could not be read.
    #[error(transparent)]
    Segment(#[from] galata_segments::SegmentError),
    /// An answer could not be recorded.
    #[error(transparent)]
    Record(crate::record::RecordError),
    /// The fingerprint key could not be read, and an account already has history.
    #[error(
        "{var} could not be read, and `{alias}` already has history under the ledger root. \
         Nothing is written under it until that history can be verified: a new key would \
         fingerprint the same address differently, and the refusal below would then fire on \
         every boot"
    )]
    Unverifiable {
        /// The account with history.
        alias: String,
        /// The key's variable. **Never the key.**
        var: String,
    },
    /// An alias now resolves to a different address from the one its history came from.
    #[error(
        "`{alias}` resolves to an address fingerprinted {current}, and its history was written \
         from {recorded} ({segment}). Repointing an alias merges two accounts' histories: give \
         the new address a new alias"
    )]
    Repointed {
        /// The alias.
        alias: String,
        /// The fingerprint its newest segment carries.
        recorded: String,
        /// The fingerprint of the address the vault resolves now.
        current: String,
        /// The segment it was read from.
        segment: PathBuf,
    },
    /// An account's newest segment carries no fingerprint to verify against.
    #[error(
        "`{alias}`'s newest segment carries no {ACCOUNT_FP_LABEL} label ({segment}), so whose \
         address its history came from cannot be checked. A tool that rewrote it dropped the \
         label"
    )]
    Unlabelled {
        /// The alias.
        alias: String,
        /// The segment.
        segment: PathBuf,
    },
    /// A declared dex the venue says does not exist.
    #[error(
        "`{alias}` declares dex `{dex}`, which the venue says does not exist. Every snapshot of \
         it would fail, and read as an outage forever"
    )]
    UnknownDex {
        /// The account.
        alias: String,
        /// The dex.
        dex: String,
    },
    /// A declared master is, by the venue's own account, a sub-account.
    #[error(
        "`{alias}` is declared as a master, and the venue says its address is a sub-account. It \
         would be polled twice, once declared and once discovered under its real master, and \
         its history would split in two. Declare the master instead"
    )]
    NotAMaster {
        /// The alias.
        alias: String,
    },
    /// The ledger root is readable by more than its owner.
    #[error(
        "the ledger root {root} has mode {mode:o}. Its raw answers carry sub-account \
         addresses, so it must be readable by its owner only: chmod 700"
    )]
    RootExposed {
        /// The root.
        root: PathBuf,
        /// Its permission bits.
        mode: u32,
    },
    /// The ledger root could not be inspected.
    #[error("the ledger root {root}: {source}")]
    Root {
        /// The root.
        root: PathBuf,
        /// Why.
        #[source]
        source: std::io::Error,
    },
}

/// **Refuse a ledger root anyone but its owner can read**, creating it 0700
/// where it does not exist yet.
///
/// Its raw answers carry sub-account addresses (`design/measured.md`,
/// 2026-09-25), so the root is the boundary D1's aliases cannot be.
pub fn check_root(root: &Path) -> Result<(), LedgerError> {
    let io = |source| LedgerError::Root {
        root: root.to_path_buf(),
        source,
    };
    if !root.exists() {
        std::fs::create_dir_all(root).map_err(io)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(root, std::fs::Permissions::from_mode(0o700)).map_err(io)?;
        }
        return Ok(());
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = std::fs::metadata(root).map_err(io)?.permissions().mode() & 0o777;
        if mode & 0o077 != 0 {
            return Err(LedgerError::RootExposed {
                root: root.to_path_buf(),
                mode,
            });
        }
    }
    Ok(())
}

/// Resolve every declared account on one venue, and the key that fingerprints them.
///
/// A key that cannot be read is refused as [`LedgerError::Unverifiable`] when
/// any of these accounts already has history, and as the secret source's own
/// refusal otherwise.
pub fn resolve(
    ledger: &Ledger,
    venue: &str,
    secrets: &dyn SecretSource,
) -> Result<(FingerprintKey, Vec<ResolvedAccount>), LedgerError> {
    let declared: Vec<(&String, &crate::config::LedgerAccount)> = ledger
        .account
        .iter()
        .filter(|(_, a)| a.venue == venue)
        .collect();

    let key = match secrets.secret(&ledger.fingerprint_key_var) {
        Ok(secret) => FingerprintKey::new(secret),
        Err(refusal) => {
            if let Some((alias, _)) = declared
                .iter()
                .find(|(alias, _)| has_history(&ledger.root, venue, alias))
            {
                return Err(LedgerError::Unverifiable {
                    alias: alias.to_string(),
                    var: ledger.fingerprint_key_var.clone(),
                });
            }
            return Err(refusal.into());
        }
    };

    let venue_token = Venue::new(venue)?;
    let mut out = Vec::with_capacity(declared.len());
    for (alias, account) in declared {
        let address = secrets.secret(&account.address_var)?;
        out.push(ResolvedAccount {
            alias: Account::new(alias.as_str())?,
            venue: venue_token.clone(),
            dexes: account.dexes.clone(),
            fingerprint: fingerprint(&key, address.expose()),
            address,
        });
    }
    Ok((key, out))
}

fn account_dir(root: &Path, venue: &str, alias: &str) -> PathBuf {
    root.join(format!("venue={venue}"))
        .join(format!("account={alias}"))
}

fn has_history(root: &Path, venue: &str, alias: &str) -> bool {
    newest_segment(&account_dir(root, venue, alias)).is_some()
}

/// The newest segment anywhere under a directory, by the position its name
/// carries. No file is opened.
fn newest_segment(dir: &Path) -> Option<PathBuf> {
    let mut best: Option<(i128, PathBuf)> = None;
    let mut stack = vec![dir.to_path_buf()];
    while let Some(here) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&here) else {
            continue;
        };
        for entry in entries.filter_map(|e| e.ok()) {
            if entry.file_type().is_ok_and(|t| t.is_dir()) {
                stack.push(entry.path());
            }
        }
        for (cursor, path) in galata_segments::list_segments(&here) {
            let position = cursor.last_position();
            if best.as_ref().is_none_or(|(p, _)| position > *p) {
                best = Some((position, path));
            }
        }
    }
    best.map(|(_, path)| path)
}

/// **The boot check**: each account's newest segment against the address the
/// vault resolves now. An account with no history passes, and its first
/// segment sets the fingerprint every later boot is checked against.
pub fn check_fingerprints(root: &Path, accounts: &[ResolvedAccount]) -> Result<(), LedgerError> {
    for account in accounts {
        let dir = account_dir(root, account.venue.as_str(), account.alias.as_str());
        let Some(segment) = newest_segment(&dir) else {
            continue;
        };
        match galata_segments::label(&segment, ACCOUNT_FP_LABEL)? {
            None => {
                return Err(LedgerError::Unlabelled {
                    alias: account.alias.to_string(),
                    segment,
                });
            }
            Some(recorded) if recorded != account.fingerprint => {
                return Err(LedgerError::Repointed {
                    alias: account.alias.to_string(),
                    recorded,
                    current: account.fingerprint.clone(),
                    segment,
                });
            }
            Some(_) => {}
        }
    }
    Ok(())
}

/// A discovered sub-account's alias: `<master>_s<ordinal>`.
///
/// `_` is the separator because it is the one character a token allows and a
/// declared alias does not (`Config::validate`), so a discovered alias can
/// never collide with a declared one.
pub fn sub_alias(master: &Account, ordinal: u32) -> Result<Account, TokenError> {
    Account::new(format!("{master}_s{ordinal}"))
}

/// What a discovery run made of one sub-account it saw.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Seen {
    /// Seen for the first time: bound to the next ordinal. The row is to be
    /// written.
    New {
        /// Its alias.
        alias: Account,
        /// The binding to record.
        seen: AccountSeen,
    },
    /// Already bound, and the venue now gives it a different name. The row is
    /// to be written; the ordinal does not change.
    Renamed {
        /// Its alias.
        alias: Account,
        /// The binding to record.
        seen: AccountSeen,
    },
    /// Already bound, unchanged. Nothing to write.
    Known {
        /// Its alias.
        alias: Account,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Bound {
    ordinal: u32,
    name: Option<String>,
}

/// Every sub-account binding, **as the record states it**.
///
/// Rebuilt from the record's `accounts` rows at every boot, never kept in a
/// side file: *a separate cursor is a second source of truth about the same
/// fact* (`capture/walk.rs`), and losing one is undetectable.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Bindings {
    /// master → fingerprint → binding.
    masters: BTreeMap<Account, BTreeMap<String, Bound>>,
}

impl Bindings {
    /// From binding rows in the order they were written: a later row for the
    /// same fingerprint updates its name and never its ordinal.
    pub fn from_seen(rows: impl IntoIterator<Item = AccountSeen>) -> Bindings {
        let mut bindings = Bindings::default();
        for row in rows {
            let under = bindings.masters.entry(row.master.clone()).or_default();
            match under.get_mut(&row.fingerprint) {
                Some(bound) => bound.name = row.name,
                None => {
                    under.insert(
                        row.fingerprint,
                        Bound {
                            ordinal: row.ordinal,
                            name: row.name,
                        },
                    );
                }
            }
        }
        bindings
    }

    /// Read one venue's bindings back out of the ledger root.
    pub fn read(root: &Path, venue: &str) -> Result<Bindings, LedgerError> {
        let scope = format!("venue={venue}");
        let rows = crate::replay::read_range(root, Some(&[scope.as_str()]), i64::MIN, i64::MAX)?
            .into_iter()
            .filter(|r| {
                r.payload().kind == galata_wire::Kind::Accounts.as_str()
                    && r.payload().origin == Origin::Generated
            })
            .filter_map(|r| crate::ingest::generated_envelope(&r.payload().payload).ok())
            .filter_map(|envelope| match envelope.event {
                Event::AccountSeen(seen) => Some(seen),
                _ => None,
            });
        Ok(Bindings::from_seen(rows))
    }

    /// The alias a fingerprint is bound to under a master, if it is.
    pub fn alias_of(&self, master: &Account, fingerprint: &str) -> Option<Account> {
        let bound = self.masters.get(master)?.get(fingerprint)?;
        sub_alias(master, bound.ordinal).ok()
    }

    /// Record that a discovery run saw a sub-account, and say what to write.
    ///
    /// A new one takes **the next unused ordinal** under its master: one more
    /// than the highest bound, so an ordinal is never reused, even for a
    /// sub-account that has since vanished.
    pub fn observe(
        &mut self,
        master: &Account,
        fingerprint: &str,
        name: Option<&str>,
    ) -> Result<Seen, TokenError> {
        let under = self.masters.entry(master.clone()).or_default();
        let name = name.map(str::to_string);
        if let Some(bound) = under.get_mut(fingerprint) {
            let alias = sub_alias(master, bound.ordinal)?;
            if bound.name == name {
                return Ok(Seen::Known { alias });
            }
            bound.name = name.clone();
            return Ok(Seen::Renamed {
                alias,
                seen: AccountSeen {
                    master: master.clone(),
                    ordinal: bound.ordinal,
                    fingerprint: fingerprint.to_string(),
                    name,
                },
            });
        }
        let ordinal = under.values().map(|b| b.ordinal).max().unwrap_or(0) + 1;
        let alias = sub_alias(master, ordinal)?;
        under.insert(
            fingerprint.to_string(),
            Bound {
                ordinal,
                name: name.clone(),
            },
        );
        Ok(Seen::New {
            alias,
            seen: AccountSeen {
                master: master.clone(),
                ordinal,
                fingerprint: fingerprint.to_string(),
                name,
            },
        })
    }

    /// The bound sub-accounts a discovery run did **not** see. Kept, never
    /// deleted, and no longer polled.
    pub fn missing(&self, master: &Account, present: &BTreeSet<String>) -> Vec<Account> {
        let Some(under) = self.masters.get(master) else {
            return Vec::new();
        };
        under
            .iter()
            .filter(|(fp, _)| !present.contains(*fp))
            .filter_map(|(_, bound)| sub_alias(master, bound.ordinal).ok())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Ledger;
    use crate::record::{Archive, Payload, PayloadAddress};

    const ADDRESS: &str = "0x3f9aa0c1d2e3f4a5b6c7d8e9f0a1b2c3d4e5f6a7";
    const OTHER: &str = "0x00000000000000000000000000000000000000aa";

    struct Secrets(BTreeMap<&'static str, &'static str>);
    impl SecretSource for Secrets {
        fn secret(&self, name: &str) -> Result<Secret, ConfigError> {
            self.0
                .get(name)
                .map(|v| Secret::new(*v))
                .ok_or_else(|| ConfigError::SecretAbsent {
                    name: name.to_string(),
                })
        }
    }

    fn ledger(root: &Path) -> Ledger {
        Ledger {
            root: root.to_path_buf(),
            snapshot_secs: 10,
            discover_secs: 600,
            events_secs: Some(300),
            ledger_share: 0.25,
            fingerprint_key_var: "KEY".into(),
            account: [(
                "main".to_string(),
                crate::config::LedgerAccount {
                    venue: "hyperliquid".into(),
                    address_var: "MAIN".into(),
                    dexes: vec![String::new(), "xyz".into()],
                    address: None,
                },
            )]
            .into(),
        }
    }

    fn secrets(address: &'static str) -> Secrets {
        Secrets([("KEY", "deployment-key"), ("MAIN", address)].into())
    }

    fn key(value: &str) -> FingerprintKey {
        FingerprintKey::new(Secret::new(value))
    }

    fn write_under(root: &Path, account: &ResolvedAccount) {
        let mut archive = Archive::open(root);
        archive
            .append(Payload {
                seq: 1,
                recv_micros: 1_758_326_400_000_000,
                address: PayloadAddress::Account(account.record_address()),
                channel: "clearinghouseState".into(),
                kind: "margin".into(),
                symbol: None,
                origin: Origin::Fetched,
                payload: b"{}".to_vec(),
            })
            .unwrap();
    }

    #[test]
    fn a_resolved_address_does_not_print() {
        let dir = tempfile::tempdir().unwrap();
        let (_, accounts) = resolve(&ledger(dir.path()), "hyperliquid", &secrets(ADDRESS)).unwrap();
        let printed = format!("{:?}", accounts[0]);
        assert!(printed.contains("main"), "{printed}");
        assert!(
            !printed.contains("3f9a"),
            "the address reached Debug: {printed}"
        );
        assert!(
            !printed.to_lowercase().contains(&ADDRESS[2..10]),
            "{printed}"
        );
    }

    #[cfg(unix)]
    #[test]
    fn a_group_readable_ledger_root_is_refused() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("ledger");
        check_root(&root).expect("a missing root is created");
        let mode = std::fs::metadata(&root).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o700, "created owner-only");

        std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o755)).unwrap();
        let err = check_root(&root).unwrap_err();
        assert!(
            matches!(err, LedgerError::RootExposed { mode: 0o755, .. }),
            "{err}"
        );
        assert!(err.to_string().contains("755"), "{err}");
    }

    #[test]
    fn one_address_under_two_keys_gives_two_fingerprints() {
        let a = fingerprint(&key("one"), ADDRESS);
        let b = fingerprint(&key("two"), ADDRESS);
        assert_ne!(a, b);
        assert_eq!(a.len(), FINGERPRINT_HEX);
        assert_eq!(
            a,
            fingerprint(&key("one"), &ADDRESS.to_uppercase().replace("0X", "0x")),
            "an address's case is not its identity"
        );
    }

    #[test]
    fn a_repointed_alias_writes_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let (_, before) = resolve(&ledger(dir.path()), "hyperliquid", &secrets(ADDRESS)).unwrap();
        write_under(dir.path(), &before[0]);
        check_fingerprints(dir.path(), &before).expect("the same address passes");

        let (_, after) = resolve(&ledger(dir.path()), "hyperliquid", &secrets(OTHER)).unwrap();
        let err = check_fingerprints(dir.path(), &after).unwrap_err();
        assert!(matches!(err, LedgerError::Repointed { .. }), "{err}");
        let message = err.to_string();
        assert!(message.contains(&before[0].fingerprint), "{message}");
        assert!(message.contains(&after[0].fingerprint), "{message}");
        assert!(
            !message.contains(&OTHER[2..12]),
            "no address in the refusal: {message}"
        );
    }

    #[test]
    fn a_missing_key_refuses_an_alias_with_history() {
        let dir = tempfile::tempdir().unwrap();
        let (_, accounts) = resolve(&ledger(dir.path()), "hyperliquid", &secrets(ADDRESS)).unwrap();
        write_under(dir.path(), &accounts[0]);

        let keyless = Secrets([("MAIN", ADDRESS)].into());
        let err = resolve(&ledger(dir.path()), "hyperliquid", &keyless).unwrap_err();
        assert!(matches!(err, LedgerError::Unverifiable { .. }), "{err}");
        assert!(
            err.to_string().contains("`main` already has history"),
            "{err}"
        );
    }

    #[test]
    fn a_missing_key_with_no_history_is_the_secret_sources_refusal() {
        let dir = tempfile::tempdir().unwrap();
        let keyless = Secrets([("MAIN", ADDRESS)].into());
        let err = resolve(&ledger(dir.path()), "hyperliquid", &keyless).unwrap_err();
        assert!(err.to_string().contains("KEY is not set"), "{err}");
    }

    fn master() -> Account {
        Account::new("main").unwrap()
    }

    #[test]
    fn a_new_sub_account_gets_the_next_ordinal() {
        let mut bindings = Bindings::from_seen([
            AccountSeen {
                master: master(),
                ordinal: 1,
                fingerprint: "aa".into(),
                name: None,
            },
            AccountSeen {
                master: master(),
                ordinal: 2,
                fingerprint: "bb".into(),
                name: None,
            },
        ]);
        let seen = bindings.observe(&master(), "cc", Some("arb")).unwrap();
        let Seen::New { alias, seen } = seen else {
            panic!("not new: {seen:?}")
        };
        assert_eq!(alias.as_str(), "main_s3");
        assert_eq!(seen.ordinal, 3);
    }

    #[test]
    fn an_ordinal_is_never_reused_after_a_sub_account_vanishes() {
        let mut bindings = Bindings::default();
        bindings.observe(&master(), "aa", None).unwrap();
        bindings.observe(&master(), "bb", None).unwrap();
        let missing = bindings.missing(&master(), &["bb".to_string()].into());
        assert_eq!(missing, vec![Account::new("main_s1").unwrap()]);
        let Seen::New { alias, .. } = bindings.observe(&master(), "cc", None).unwrap() else {
            panic!()
        };
        assert_eq!(alias.as_str(), "main_s3");
    }

    #[test]
    fn a_renamed_sub_account_keeps_its_ordinal() {
        let mut bindings = Bindings::default();
        bindings.observe(&master(), "aa", Some("arb")).unwrap();
        bindings.observe(&master(), "bb", Some("hedge")).unwrap();
        let seen = bindings.observe(&master(), "bb", Some("hedge-2")).unwrap();
        let Seen::Renamed { alias, seen } = seen else {
            panic!("not renamed: {seen:?}")
        };
        assert_eq!(alias.as_str(), "main_s2");
        assert_eq!(seen.name.as_deref(), Some("hedge-2"));
        assert!(matches!(
            bindings.observe(&master(), "bb", Some("hedge-2")).unwrap(),
            Seen::Known { .. }
        ));
    }

    #[test]
    fn ordinals_survive_a_restart_with_only_the_record() {
        let dir = tempfile::tempdir().unwrap();
        let (_, accounts) = resolve(&ledger(dir.path()), "hyperliquid", &secrets(ADDRESS)).unwrap();
        let mut bindings = Bindings::default();
        let mut archive = Archive::open(dir.path());
        for (fp, name) in [("aa", "arb"), ("bb", "hedge")] {
            let (Seen::New { alias, seen } | Seen::Renamed { alias, seen }) =
                bindings.observe(&master(), fp, Some(name)).unwrap()
            else {
                panic!()
            };
            let envelope = galata_wire::Envelope::for_account(
                accounts[0].venue.clone(),
                alias.clone(),
                None,
                1_758_326_400_000_000,
                Event::AccountSeen(seen),
            );
            let mut address = accounts[0].record_address();
            address.account = alias.to_string();
            crate::ingest::record_generated_at(
                &mut archive,
                &crate::sink::NullSink,
                PayloadAddress::Account(address),
                envelope,
            )
            .unwrap();
        }
        // Nothing survives but the directory.
        drop(bindings);
        let back = Bindings::read(dir.path(), "hyperliquid").unwrap();
        assert_eq!(back.alias_of(&master(), "aa").unwrap().as_str(), "main_s1");
        assert_eq!(back.alias_of(&master(), "bb").unwrap().as_str(), "main_s2");
    }
}
