//! Who may publish and subscribe to what, **enforced by the server**.
//!
//! ```text
//!   grants::table(venues, ledgers)   the declaration
//!   grants::to_nats_config()   the server's own fragment
//! ```
//!
//! A component holding the wrong identity is refused by `nats-server` — not
//! discouraged by review, and refused in whatever language it was written in.
//!
//! # Why this lives beside capture when a venue credential does not
//!
//! ```text
//!   A VENUE KEY        authority in itself. Held by anyone, anywhere, it moves
//!                      money at a venue that never heard of this system.
//!
//!   A BROKER PASSWORD  inert on its own. It selects a row in a table the
//!                      SERVER holds. Holding one granted
//!                      `markets.hyperliquid.>` is strictly LESS authority than
//!                      connecting anonymously — which is what every process
//!                      does without a table.
//! ```
//!
//! One widens what its holder can do; the other narrows it.

use std::fmt::Write as _;

/// The roots this system addresses. **Every one must be granted to somebody**,
/// which `scripts/check-grant-coverage.sh` holds — so adding one forces a
/// decision about who may read it, at the moment it is added.
///
/// `accounts.` arrived with the ledger's publication
/// (`add-realtime-account-ledger`): each venue's ledger publishes its own
/// accounts there, and nobody else publishes under it.
pub const ROOTS: [&str; 3] = ["markets.", "status.", "accounts."];

/// One identity's authority.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Grant {
    /// Who.
    pub identity: String,
    /// What it may publish. **Empty means nothing**, rendered as a denial.
    pub publish: Vec<String>,
    /// What it may subscribe to.
    ///
    /// **Enumerated, never `>`.** *"This component reads only market data"* is
    /// a subscribe restriction and nothing else; a table that enumerates
    /// publish and leaves subscribe open has written down the easy half.
    pub subscribe: Vec<String>,
}

/// The whole table.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Grants(Vec<Grant>);

impl Grants {
    /// Every grant, in declaration order.
    pub fn iter(&self) -> impl Iterator<Item = &Grant> {
        self.0.iter()
    }

    /// Whether any identity is granted a subject under this root.
    pub fn covers(&self, root: &str) -> bool {
        self.0.iter().any(|grant| {
            grant
                .publish
                .iter()
                .chain(grant.subscribe.iter())
                .any(|subject| subject.starts_with(root))
        })
    }
}

/// The table for a set of capturing venues and a set of venues whose ledger
/// publishes.
///
/// Two kinds of identity, because they are two kinds of component:
///
/// - **a capture process** publishes its own venue and its own status, and
///   subscribes to **nothing** — it never consumes, and a component that only
///   writes should not hold a handle that can read;
/// - **a reader** subscribes to status and publishes nothing. It is NOT
///   granted `markets.>`, and that is the point of the two roots being
///   separate: a dashboard takes `status.>` without also taking the firehose.
///   Market data for a screen comes from the record, which holds it bounded,
///   because *an identity that could read every venue's firehose is exactly
///   what a password on an operator's laptop should not be*. The grant is the
///   boundary; galata-tower's `check-no-market-reach.sh` holds the other side.
///
/// Beside them, two services: `bus-admin`, which administers the streams at
/// the broker's start, and galata-markets ([`GALATA_MARKETS`]), which consumes
/// the windows and serves clients over HTTP — so the firehose's one password
/// stays in a container and never reaches an operator.
///
/// And **a ledger per venue** ([`ledger_identity`]): it publishes its own
/// venue's accounts and nothing else, and hears only its own inbox, where the
/// broker's acknowledgements of what it published arrive. It subscribes to no
/// subject, as capture does not.
pub fn table(venues: &[&str], ledgers: &[&str]) -> Grants {
    let mut grants = Vec::new();
    for venue in venues {
        grants.push(Grant {
            identity: format!("datawatch-{venue}"),
            publish: vec![format!("markets.{venue}.>"), format!("status.{venue}")],
            // **Nothing.** Capture publishes and never consumes.
            subscribe: Vec::new(),
        });
    }
    for venue in ledgers {
        grants.push(Grant {
            identity: ledger_identity(venue),
            publish: vec![format!("accounts.{venue}.>")],
            subscribe: vec![format!("{}.>", ledger_inbox(venue))],
        });
    }
    grants.push(Grant {
        identity: "reader".into(),
        publish: Vec::new(),
        subscribe: vec!["status.>".into()],
    });
    // **The one identity that administers streams**, used only by the broker's
    // own start (`galata-bus`) to apply `streams::table`. It may create, update
    // and read the declared streams by name — not delete one, not publish or
    // read data — and it hears replies only on an inbox prefix of its own, so
    // it cannot read another client's.
    let mut admin = Vec::new();
    for stream in crate::streams::table() {
        for verb in ["CREATE", "UPDATE", "INFO"] {
            admin.push(format!("$JS.API.STREAM.{verb}.{}", stream.name));
        }
    }
    grants.push(Grant {
        identity: BUS_ADMIN.into(),
        publish: admin,
        subscribe: vec![format!("{BUS_ADMIN_INBOX}.>")],
    });
    // **galata-markets, the one consumer of the windows** (`add-galata-markets`).
    // It serves every client, so no client holds a bus password: this one
    // lives in the vault and galata-markets' container. It may read the market,
    // status and account streams and run consumers of its own on them —
    // create, read, page and delete — and it hears replies only on an inbox of
    // its own. It publishes nothing to any root, and creates, changes and
    // deletes no stream.
    //
    // **The account stream too** (`add-markets-accounts`, design D6), which it
    // serves to the clients granted account data, and on it alone the
    // stream's own message get: an account's newest set of each kind is the
    // last message on its subject.
    let mut markets = Vec::new();
    for stream in [
        crate::streams::MARKET_STREAM,
        crate::streams::STATUS_STREAM,
        crate::streams::ACCOUNT_STREAM,
    ] {
        markets.push(format!("$JS.API.STREAM.INFO.{stream}"));
        // An unnamed consumer — an ordered replay — is created on the stream's
        // own subject, which `.>` does not match (async-nats 0.50).
        markets.push(format!("$JS.API.CONSUMER.CREATE.{stream}"));
        for verb in ["CREATE", "DELETE", "INFO", "MSG.NEXT"] {
            markets.push(format!("$JS.API.CONSUMER.{verb}.{stream}.>"));
        }
    }
    markets.push(format!(
        "$JS.API.STREAM.MSG.GET.{}",
        crate::streams::ACCOUNT_STREAM
    ));
    grants.push(Grant {
        identity: GALATA_MARKETS.into(),
        publish: markets,
        subscribe: vec![format!("{GALATA_MARKETS_INBOX}.>")],
    });
    Grants(grants)
}

/// The identity one venue's ledger publishes its accounts as:
/// `ledger-hyperliquid`, `ledger-derive-xyz`.
pub fn ledger_identity(venue: &str) -> String {
    format!("ledger-{venue}")
}

/// The inbox prefix a venue's ledger connects with, and the only one it may
/// hear on: `_INBOX_LEDGER_DERIVE_XYZ`, as galata-markets has
/// [`GALATA_MARKETS_INBOX`].
pub fn ledger_inbox(venue: &str) -> String {
    format!("_INBOX_LEDGER_{}", venue.replace('-', "_").to_uppercase())
}

/// The identity galata-markets reads the bus as.
pub const GALATA_MARKETS: &str = "markets";

/// The inbox prefix [`GALATA_MARKETS`] connects with, and the only one it may
/// hear replies — and the messages its consumers deliver — on.
pub const GALATA_MARKETS_INBOX: &str = "_INBOX_MARKETS";

/// The identity the broker's start administers its streams as.
pub const BUS_ADMIN: &str = "bus-admin";

/// The inbox prefix [`BUS_ADMIN`] connects with, and the only one it may hear
/// replies on: requests are answered on the asker's inbox, and the default
/// `_INBOX.>` would let it read every client's replies.
pub const BUS_ADMIN_INBOX: &str = "_INBOX_BUS_ADMIN";

/// The environment variable holding one identity's password.
///
/// A reference, never a value: the generated file is committed and read by
/// anyone with the repository. Stated once here so a generator and any script
/// that sets the variables cannot disagree about the spelling.
pub fn password_var(identity: &str) -> String {
    format!(
        "GALATA_BROKER_PASSWORD_{}",
        identity.replace('-', "_").to_uppercase()
    )
}

/// The `authorization` block.
///
/// **No `no_auth_user` and no anonymous default.** A table granting three
/// identities their subjects changes nothing if the server still accepts a
/// connection with no credentials — the fourth, nameless client keeps the
/// unlimited authority every process has without a table.
pub fn to_nats_config(grants: &Grants) -> String {
    let mut out = String::new();
    out.push_str("# Generated. Do not edit.\n#\n");
    out.push_str("# Each identity may publish exactly the subjects it owns and subscribe to\n");
    out.push_str("# exactly the ones it consumes. Both are enumerated: subscribe is not `>`,\n");
    out.push_str("# because \"this component reads only market data\" is a subscribe\n");
    out.push_str("# restriction and nothing else.\n#\n");
    out.push_str("# Passwords are environment references. This file holds no secret.\n#\n");
    out.push_str("# There is no no_auth_user and no anonymous default: a table that only\n");
    out.push_str("# grants leaves the nameless client its unlimited authority.\n");
    out.push_str("authorization {\n  users: [\n");
    for grant in grants.iter() {
        let _ = writeln!(out, "    {{");
        let _ = writeln!(out, "      user: {}", grant.identity);
        let _ = writeln!(out, "      password: ${}", password_var(&grant.identity));
        let _ = writeln!(out, "      permissions: {{");
        let _ = writeln!(out, "        publish: {}", rule(&grant.publish));
        let _ = writeln!(out, "        subscribe: {}", rule(&grant.subscribe));
        let _ = writeln!(out, "      }}");
        let _ = writeln!(out, "    }},");
    }
    out.push_str("  ]\n}\n");
    out
}

/// One permission rule — and the empty case is **not** an empty allow-list.
///
/// **`allow: []` means ALLOW EVERYTHING in NATS**, not *allow nothing*. It
/// reads as an absent restriction rather than a total one, which is the exact
/// inverse of what a component granted no rights is supposed to have.
///
/// The predecessor shipped that inversion, and records what failed to catch it:
/// review, unit tests, and `nats-server -t`, which called the inverted table
/// **valid**. What caught it was loading the file into a running server and
/// watching a component granted nothing publish anyway.
///
/// So *nothing* is spelled `deny: [">"]`, which denies every subject there is —
/// and the test that proves it runs a real server, because **a generated
/// permission set that has never been loaded into one is a decoration.**
fn rule(subjects: &[String]) -> String {
    if subjects.is_empty() {
        return "{ deny: [\">\"] }".to_string();
    }
    let quoted: Vec<String> = subjects.iter().map(|s| format!("\"{s}\"")).collect();
    format!("{{ allow: [{}] }}", quoted.join(", "))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rendered() -> String {
        to_nats_config(&table(
            &["hyperliquid", "rh-chain"],
            &["hyperliquid", "derive-xyz"],
        ))
    }

    /// The lines the server acts on.
    fn directives(config: &str) -> impl Iterator<Item = &str> {
        config.lines().filter(|l| !l.trim_start().starts_with('#'))
    }

    #[test]
    fn nothing_is_a_denial_and_never_an_empty_allowance() {
        // `allow: []` means ALLOW EVERYTHING. The predecessor shipped that, and
        // review, unit tests and `nats-server -t` all called it valid.
        assert_eq!(rule(&[]), "{ deny: [\">\"] }");
        let cfg = rendered();
        assert!(!cfg.contains("allow: []"), "the inverted rule was rendered");
        assert!(
            cfg.contains("deny: [\">\"]"),
            "capture was granted a subscription"
        );
    }

    #[test]
    fn a_password_is_a_reference_and_never_a_value() {
        // The file is committed and read by anyone with the repository.
        for line in rendered()
            .lines()
            .filter(|l| l.trim_start().starts_with("password:"))
        {
            assert!(
                line.contains("$GALATA_BROKER_PASSWORD_"),
                "not an environment reference: {line}"
            );
        }
        assert_eq!(
            password_var("datawatch-rh-chain"),
            "GALATA_BROKER_PASSWORD_DATAWATCH_RH_CHAIN"
        );
    }

    #[test]
    fn there_is_no_anonymous_default() {
        // A table granting three identities changes nothing if a fourth,
        // nameless client keeps unlimited authority.
        let cfg = rendered();
        for forbidden in ["no_auth_user", "no_advertise", "allow_all"] {
            assert!(
                !directives(&cfg).any(|l| l.contains(forbidden)),
                "{forbidden} is in the generated table"
            );
        }
    }

    #[test]
    fn capture_publishes_and_never_consumes() {
        // A component that only writes should not hold a handle that can read.
        let grants = table(&["hyperliquid"], &["hyperliquid"]);
        let capture = grants
            .iter()
            .find(|g| g.identity == "datawatch-hyperliquid")
            .unwrap();
        assert!(capture.subscribe.is_empty());
        assert_eq!(
            capture.publish,
            vec!["markets.hyperliquid.>", "status.hyperliquid"]
        );
    }

    #[test]
    fn a_reader_subscribes_and_never_publishes() {
        let grants = table(&["hyperliquid"], &["hyperliquid"]);
        let reader = grants.iter().find(|g| g.identity == "reader").unwrap();
        assert!(reader.publish.is_empty());
        assert_eq!(reader.subscribe, vec!["status.>"]);
    }

    #[test]
    fn one_venue_cannot_publish_anothers_subjects() {
        // The whole point: the server refuses it, rather than a reviewer
        // noticing.
        let grants = table(&["hyperliquid", "rh-chain"], &["hyperliquid", "derive-xyz"]);
        let hl = grants
            .iter()
            .find(|g| g.identity == "datawatch-hyperliquid")
            .unwrap();
        assert!(
            !hl.publish.iter().any(|s| s.contains("rh-chain")),
            "{:?}",
            hl.publish
        );
    }

    /// **The assertion, not the absence of one.** A reader granted the
    /// firehose would pass every other test in this file; only naming the
    /// thing it must not have catches it coming back.
    #[test]
    fn a_reader_is_granted_no_market_subject() {
        let grants = table(&["hyperliquid", "rh-chain"], &["hyperliquid", "derive-xyz"]);
        let reader = grants.iter().find(|g| g.identity == "reader").unwrap();
        assert!(
            !reader
                .subscribe
                .iter()
                .any(|subject| subject.starts_with("markets.")),
            "the reader's password sits in a process that serves a web page: {:?}",
            reader.subscribe
        );
    }

    /// A root with one direction is still a granted root. `markets.` is
    /// published by every capture identity and read by none, which is the
    /// shape this table is for — not an oversight the coverage guard should
    /// start reporting.
    #[test]
    fn every_root_the_code_declares_is_granted_to_somebody() {
        // The guard's rule, asserted here too so a root added without a grant
        // fails a test as well as the build.
        let grants = table(&["hyperliquid"], &["hyperliquid"]);
        for root in ROOTS {
            assert!(grants.covers(root), "{root} is granted to nobody");
        }
    }

    #[test]
    fn a_root_nobody_is_granted_is_visible_as_such() {
        assert!(!table(&["hyperliquid"], &["hyperliquid"]).covers("views."));
    }

    #[test]
    fn the_bus_admin_may_administer_the_streams_and_nothing_else() {
        let grants = table(&["hyperliquid"], &["hyperliquid"]);
        let admin = grants.iter().find(|g| g.identity == BUS_ADMIN).unwrap();
        for stream in ["MARKETS", "ACCOUNTS", "STATUS"] {
            for verb in ["CREATE", "UPDATE", "INFO"] {
                let subject = format!("$JS.API.STREAM.{verb}.{stream}");
                assert!(admin.publish.contains(&subject), "missing {subject}");
            }
        }
        assert!(
            admin
                .publish
                .iter()
                .all(|s| s.starts_with("$JS.API.STREAM.")),
            "it publishes no data: {:?}",
            admin.publish
        );
        assert!(
            !admin
                .publish
                .iter()
                .any(|s| s.contains("DELETE") || s.contains("PURGE")),
            "it may not delete or purge a stream"
        );
        assert_eq!(admin.subscribe, vec![format!("{BUS_ADMIN_INBOX}.>")]);
    }

    #[test]
    fn galata_markets_may_consume_the_windows_and_nothing_else() {
        let grants = table(&["hyperliquid"], &["hyperliquid"]);
        let markets = grants
            .iter()
            .find(|g| g.identity == GALATA_MARKETS)
            .unwrap();
        for stream in ["MARKETS", "STATUS", "ACCOUNTS"] {
            let info = format!("$JS.API.STREAM.INFO.{stream}");
            assert!(markets.publish.contains(&info), "missing {info}");
            let unnamed = format!("$JS.API.CONSUMER.CREATE.{stream}");
            assert!(markets.publish.contains(&unnamed), "missing {unnamed}");
            for verb in ["CREATE", "DELETE", "INFO", "MSG.NEXT"] {
                let subject = format!("$JS.API.CONSUMER.{verb}.{stream}.>");
                assert!(markets.publish.contains(&subject), "missing {subject}");
            }
        }
        // An account's newest set: the account stream's own message get, and
        // no other stream's.
        let get = "$JS.API.STREAM.MSG.GET.ACCOUNTS".to_string();
        assert!(markets.publish.contains(&get), "missing {get}");
        for subject in &markets.publish {
            assert!(
                subject.starts_with("$JS.API.CONSUMER.")
                    || subject.starts_with("$JS.API.STREAM.INFO.")
                    || *subject == get,
                "it administers no stream and publishes no data: {subject}"
            );
            assert!(
                !subject.starts_with("accounts.") && !subject.starts_with("markets."),
                "it publishes no data: {subject}"
            );
        }
        assert_eq!(markets.subscribe, vec![format!("{GALATA_MARKETS_INBOX}.>")]);
    }

    #[test]
    fn each_ledger_publishes_its_own_venues_accounts_and_nothing_else() {
        let grants = table(&["hyperliquid"], &["hyperliquid", "derive-xyz"]);
        for (venue, other) in [("hyperliquid", "derive-xyz"), ("derive-xyz", "hyperliquid")] {
            let ledger = grants
                .iter()
                .find(|g| g.identity == format!("ledger-{venue}"))
                .unwrap();
            assert_eq!(ledger.publish, vec![format!("accounts.{venue}.>")]);
            assert!(
                !ledger.publish.iter().any(|s| s.contains(other)),
                "{:?}",
                ledger.publish
            );
            // Its own inbox only, where its acknowledgements arrive.
            assert_eq!(ledger.subscribe, vec![format!("{}.>", ledger_inbox(venue))]);
        }
        assert_eq!(ledger_inbox("derive-xyz"), "_INBOX_LEDGER_DERIVE_XYZ");
        assert_eq!(
            password_var(&ledger_identity("derive-xyz")),
            "GALATA_BROKER_PASSWORD_LEDGER_DERIVE_XYZ"
        );
    }

    #[test]
    fn nobody_but_a_ledger_publishes_an_account() {
        let grants = table(
            &["hyperliquid", "derive-xyz"],
            &["hyperliquid", "derive-xyz"],
        );
        for grant in grants.iter() {
            let publishes_accounts = grant.publish.iter().any(|s| s.starts_with("accounts."));
            assert_eq!(
                publishes_accounts,
                grant.identity.starts_with("ledger-"),
                "{}: {:?}",
                grant.identity,
                grant.publish
            );
            // And nobody subscribes to one yet: galata-markets' grant arrives
            // with `add-markets-accounts`.
            assert!(
                !grant.subscribe.iter().any(|s| s.starts_with("accounts.")),
                "{}",
                grant.identity
            );
        }
    }

    #[test]
    fn a_venue_without_a_ledger_has_no_ledger_identity() {
        let grants = table(&["hyperliquid", "rh-chain"], &["hyperliquid"]);
        assert!(!grants.iter().any(|g| g.identity == "ledger-rh-chain"));
    }

    #[test]
    fn no_other_identity_touches_the_stream_api() {
        for grant in table(&["hyperliquid", "rh-chain"], &["hyperliquid", "derive-xyz"]).iter() {
            // The two that do, each pinned by a test of its own above.
            if grant.identity == BUS_ADMIN || grant.identity == GALATA_MARKETS {
                continue;
            }
            assert!(
                !grant
                    .publish
                    .iter()
                    .chain(&grant.subscribe)
                    .any(|s| s.starts_with("$JS")),
                "{} is granted the stream API",
                grant.identity
            );
        }
    }
}
