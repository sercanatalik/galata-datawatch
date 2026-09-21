//! Who may publish and subscribe to what, **enforced by the server**.
//!
//! ```text
//!   grants::table(venues)      the declaration
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
/// which `scripts/check-grant-coverage.sh` holds — so adding a third forces a
/// decision about who may read it, at the moment it is added.
pub const ROOTS: [&str; 2] = ["markets.", "status."];

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

/// The table for a set of capturing venues.
///
/// Two kinds of identity, because they are two kinds of component:
///
/// - **a capture process** publishes its own venue and its own status, and
///   subscribes to **nothing** — it never consumes, and a component that only
///   writes should not hold a handle that can read;
/// - **a reader** subscribes to market data and status and publishes nothing.
pub fn table(venues: &[&str]) -> Grants {
    let mut grants = Vec::new();
    for venue in venues {
        grants.push(Grant {
            identity: format!("datawatch-{venue}"),
            publish: vec![format!("markets.{venue}.>"), format!("status.{venue}")],
            // **Nothing.** Capture publishes and never consumes.
            subscribe: Vec::new(),
        });
    }
    grants.push(Grant {
        identity: "reader".into(),
        publish: Vec::new(),
        subscribe: vec!["markets.>".into(), "status.>".into()],
    });
    Grants(grants)
}

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
        to_nats_config(&table(&["hyperliquid", "rh-chain"]))
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
        let grants = table(&["hyperliquid"]);
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
        let grants = table(&["hyperliquid"]);
        let reader = grants.iter().find(|g| g.identity == "reader").unwrap();
        assert!(reader.publish.is_empty());
        assert_eq!(reader.subscribe, vec!["markets.>", "status.>"]);
    }

    #[test]
    fn one_venue_cannot_publish_anothers_subjects() {
        // The whole point: the server refuses it, rather than a reviewer
        // noticing.
        let grants = table(&["hyperliquid", "rh-chain"]);
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

    #[test]
    fn every_root_the_code_declares_is_granted_to_somebody() {
        // The guard's rule, asserted here too so a root added without a grant
        // fails a test as well as the build.
        let grants = table(&["hyperliquid"]);
        for root in ROOTS {
            assert!(grants.covers(root), "{root} is granted to nobody");
        }
    }

    #[test]
    fn a_root_nobody_is_granted_is_visible_as_such() {
        assert!(!table(&["hyperliquid"]).covers("views."));
    }
}
