//! What the broker keeps, **declared beside who may publish it**.
//!
//! ```text
//!   grants::table()     who may publish and subscribe to what
//!   streams::table()    what the broker stores, and for how long
//! ```
//!
//! Three streams, cut by subject root, because the roots want different
//! things kept:
//!
//! ```text
//!   MARKETS   markets.>    two hours, within a byte cap    lossy; holes declared
//!   ACCOUNTS  accounts.>   seven days                      complete; acknowledged
//!   STATUS    status.>     the last message per subject    each venue's latest
//! ```
//!
//! **The market window only has to reach back to where the tape ends.** The
//! tape is projected hourly at :40, so it trails "now" by up to about 65
//! minutes; two hours leaves room for a late or failed projection. Its holes
//! are not prevented but declared: capture sends a gap with the cause
//! `unpublished` for every span it archived and could not publish, and a
//! consumer fetches that span from history.
//!
//! **Account data is complete**: a fill missed is a wrong position. The broker
//! acknowledges each message only once stored, and its publisher republishes
//! what was not acknowledged from its own record.
//!
//! **Applied by the broker's start, and by nothing else** (`galata-bus`), with
//! the one identity that may create, update and read streams
//! ([`crate::grants::BUS_ADMIN`]). A producer publishes into a stream and can
//! neither change nor delete one.

use std::time::Duration;

use async_nats::jetstream::stream::{Config, DiscardPolicy, RetentionPolicy, StorageType};

/// How long the market window keeps a message.
pub const MARKET_WINDOW: Duration = Duration::from_secs(2 * 60 * 60);

/// The market window's byte cap: age, not size, is what should remove a
/// message. **Twice the two hours expected at fifty instruments**, as measured
/// on the deployment with nine (`design/measured.md`, "The bus's window,
/// full"): a message stored is about 330 bytes and an instrument about nine
/// messages a second, so two hours of fifty is about 1.04 GB.
pub const MARKET_WINDOW_BYTES: i64 = 2 * 1024 * 1024 * 1024;

/// How long account data is kept.
pub const ACCOUNTS_KEPT: Duration = Duration::from_secs(7 * 24 * 60 * 60);

/// One stream the broker keeps.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Stream {
    /// Its name, which is also how its management subjects are spelled.
    pub name: &'static str,
    /// The subjects it stores.
    pub subjects: Vec<String>,
    /// How long a message is kept; zero is "no age limit".
    pub max_age: Duration,
    /// Its byte cap; `-1` is "no cap".
    pub max_bytes: i64,
    /// How many messages each subject keeps; `-1` is "no limit".
    pub max_messages_per_subject: i64,
}

impl Stream {
    /// The configuration the broker applies, on file storage, discarding the
    /// oldest when a limit is reached.
    pub fn config(&self) -> Config {
        Config {
            name: self.name.to_string(),
            subjects: self.subjects.clone(),
            max_age: self.max_age,
            max_bytes: self.max_bytes,
            max_messages_per_subject: self.max_messages_per_subject,
            storage: StorageType::File,
            retention: RetentionPolicy::Limits,
            discard: DiscardPolicy::Old,
            num_replicas: 1,
            ..Default::default()
        }
    }
}

/// What applying one stream came to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Applied {
    /// Created or updated to the table's settings; what it holds now.
    Held {
        /// The stream.
        name: &'static str,
        /// Messages it holds.
        messages: u64,
        /// Bytes it holds.
        bytes: u64,
    },
    /// Not applied, and why — said by name, because the broker serves on
    /// without it and nothing else would say so.
    Failed {
        /// The stream.
        name: &'static str,
        /// What the broker said.
        reason: String,
    },
}

/// A client connected as [`crate::grants::BUS_ADMIN`] ([`connect_admin`]).
pub type AdminClient = async_nats::Client;

/// What a stream holds — its messages and bytes — or `None` where the broker
/// has no such stream, or will not say.
pub async fn held(client: &AdminClient, name: &str) -> Option<(u64, u64)> {
    let jetstream = async_nats::jetstream::new(client.clone());
    let mut stream = jetstream.get_stream(name).await.ok()?;
    let state = &stream.info().await.ok()?.state;
    Some((state.messages, state.bytes))
}

/// Connect as [`crate::grants::BUS_ADMIN`], on its own inbox prefix — the only
/// one its grants let it hear replies on.
///
/// Classified as every connect here is: a refusal of the identity will not fix
/// itself, and a broker that does not answer yet may.
pub async fn connect_admin(
    addr: &str,
    password: &str,
) -> Result<AdminClient, crate::ConnectRefusal> {
    let identity = crate::BrokerIdentity::new(
        crate::grants::BUS_ADMIN,
        password,
        crate::password_var(crate::grants::BUS_ADMIN),
    );
    async_nats::ConnectOptions::with_user_and_password(
        crate::grants::BUS_ADMIN.to_string(),
        password.to_string(),
    )
    .custom_inbox_prefix(crate::grants::BUS_ADMIN_INBOX)
    .connection_timeout(std::time::Duration::from_secs(2))
    .connect(addr)
    .await
    .map_err(|e| crate::ConnectRefusal::classify(addr, &identity, &e))
}

/// Apply the table through a client connected as [`crate::grants::BUS_ADMIN`]:
/// each stream created where missing and updated where its settings differ,
/// leaving what it holds alone.
///
/// **Every stream is attempted**, and each one's outcome is reported: one that
/// fails does not stop the others, and none stops the broker serving.
pub async fn apply(client: &AdminClient) -> Vec<Applied> {
    let jetstream = async_nats::jetstream::new(client.clone());
    let mut outcomes = Vec::new();
    for stream in table() {
        outcomes.push(
            match jetstream.create_or_update_stream(stream.config()).await {
                Ok(info) => Applied::Held {
                    name: stream.name,
                    messages: info.state.messages,
                    bytes: info.state.bytes,
                },
                Err(error) => Applied::Failed {
                    name: stream.name,
                    reason: error.to_string(),
                },
            },
        );
    }
    outcomes
}

/// The streams, in the order the broker's start applies them.
pub fn table() -> Vec<Stream> {
    vec![
        Stream {
            name: "MARKETS",
            subjects: vec!["markets.>".into()],
            max_age: MARKET_WINDOW,
            max_bytes: MARKET_WINDOW_BYTES,
            max_messages_per_subject: -1,
        },
        Stream {
            name: "ACCOUNTS",
            subjects: vec!["accounts.>".into()],
            max_age: ACCOUNTS_KEPT,
            max_bytes: -1,
            max_messages_per_subject: -1,
        },
        Stream {
            name: "STATUS",
            subjects: vec!["status.>".into()],
            max_age: Duration::ZERO,
            max_bytes: -1,
            max_messages_per_subject: 1,
        },
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    fn named(name: &str) -> Stream {
        table().into_iter().find(|s| s.name == name).unwrap()
    }

    #[test]
    fn the_market_window_keeps_two_hours_within_a_cap() {
        let markets = named("MARKETS");
        assert_eq!(markets.subjects, vec!["markets.>"]);
        assert_eq!(markets.max_age, Duration::from_secs(7_200));
        assert!(
            markets.max_bytes > 0,
            "a window with no cap could fill the disk"
        );
        assert_eq!(markets.max_messages_per_subject, -1);
    }

    #[test]
    fn account_data_is_kept_seven_days() {
        let accounts = named("ACCOUNTS");
        assert_eq!(accounts.subjects, vec!["accounts.>"]);
        assert_eq!(accounts.max_age, Duration::from_secs(7 * 86_400));
    }

    #[test]
    fn status_keeps_each_venues_latest() {
        let status = named("STATUS");
        assert_eq!(status.subjects, vec!["status.>"]);
        assert_eq!(status.max_messages_per_subject, 1);
        assert_eq!(
            status.max_age,
            Duration::ZERO,
            "the latest is kept however old"
        );
    }

    #[test]
    fn no_two_streams_claim_a_subject() {
        // The broker refuses a second stream over a subject one already
        // stores; the roots keep them apart.
        let roots: Vec<String> = table().into_iter().flat_map(|s| s.subjects).collect();
        let mut sorted = roots.clone();
        sorted.sort();
        sorted.dedup();
        assert_eq!(sorted.len(), roots.len());
        for (i, a) in roots.iter().enumerate() {
            for b in roots.iter().skip(i + 1) {
                let (a, b) = (a.trim_end_matches('>'), b.trim_end_matches('>'));
                assert!(
                    !a.starts_with(b) && !b.starts_with(a),
                    "{a} and {b} overlap"
                );
            }
        }
    }

    #[test]
    fn every_stream_is_stored_on_file_and_discards_the_oldest() {
        for stream in table() {
            let config = stream.config();
            assert_eq!(config.name, stream.name);
            assert_eq!(config.storage, StorageType::File, "{}", stream.name);
            assert_eq!(config.retention, RetentionPolicy::Limits, "{}", stream.name);
            assert_eq!(config.discard, DiscardPolicy::Old, "{}", stream.name);
            assert_eq!(config.num_replicas, 1);
        }
    }
}
