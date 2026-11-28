//! The ledger's door to the account stream (`add-realtime-account-ledger`,
//! design D4): **every message with its id, and none counted until the stream
//! acknowledges it.**
//!
//! ```text
//!   capture   core publish, fire and forget: a market window is lossy by design
//!   a ledger  JetStream publish, acknowledged: an account's stream is complete
//! ```
//!
//! The id is what the broker drops a repeat by, inside its duplicate window:
//! a resend after a lost acknowledgement is one message, not two. Beyond the
//! window, the ledger's own publication cursor is what keeps a message from
//! going twice.

use std::future::Future;
use std::pin::Pin;

use galata_wire::Envelope;

use crate::encode::encode;
use crate::identity::BrokerIdentity;
use crate::publisher::{ConnectRefusal, PublishError};
use crate::subject::Subject;

/// What the stream said of a message it took.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Acknowledged {
    /// Its position in the stream.
    pub stream_seq: u64,
    /// Whether the stream already held a message with this id, inside its
    /// duplicate window, and kept that one instead.
    pub duplicate: bool,
}

/// Publishing to the account stream, each message acknowledged.
pub trait AccountPublisher: Send + Sync {
    /// One envelope, on its subject, with its id: returns once the stream has
    /// acknowledged it, or says why it did not.
    fn publish_acked<'a>(
        &'a self,
        subject: &'a Subject,
        id: &'a str,
        envelope: &'a Envelope,
    ) -> Pin<Box<dyn Future<Output = Result<Acknowledged, PublishError>> + Send + 'a>>;
}

/// The real broker's account stream.
#[derive(Debug, Clone)]
pub struct JetStreamPublisher {
    jetstream: async_nats::jetstream::Context,
}

impl JetStreamPublisher {
    /// Connect as a ledger's identity, hearing replies on its own inbox.
    ///
    /// **A broker that does not answer yet is not a refusal**: the client keeps
    /// connecting in the background, as the record keeps recording, and a
    /// publish meanwhile fails and is asked again from the cursor. A broker
    /// that answers and refuses the identity is refused here, at once.
    pub async fn connect(
        addr: &str,
        identity: &BrokerIdentity,
        inbox_prefix: &str,
    ) -> Result<JetStreamPublisher, ConnectRefusal> {
        let options = || {
            async_nats::ConnectOptions::with_user_and_password(
                identity.user.clone(),
                identity.password.clone(),
            )
            .custom_inbox_prefix(inbox_prefix)
        };
        // Asked once, plainly, so a refusal is said at once: retried in the
        // background, it would be retried for ever and said nowhere.
        let client = match options().connect(addr).await {
            Ok(client) => client,
            Err(e) => match ConnectRefusal::classify(addr, identity, &e) {
                refusal if refusal.is_fatal() => return Err(refusal),
                _ => options()
                    .retry_on_initial_connect()
                    .connect(addr)
                    .await
                    .map_err(|e| ConnectRefusal::classify(addr, identity, &e))?,
            },
        };
        Ok(JetStreamPublisher {
            jetstream: async_nats::jetstream::new(client),
        })
    }
}

impl AccountPublisher for JetStreamPublisher {
    fn publish_acked<'a>(
        &'a self,
        subject: &'a Subject,
        id: &'a str,
        envelope: &'a Envelope,
    ) -> Pin<Box<dyn Future<Output = Result<Acknowledged, PublishError>> + Send + 'a>> {
        Box::pin(async move {
            let failed = |reason: String| PublishError::Failed {
                subject: subject.as_str().to_string(),
                reason,
            };
            let body = encode(envelope).map_err(|e| failed(e.to_string()))?;
            let ack = self
                .jetstream
                .send_publish(
                    subject.to_nats(),
                    async_nats::jetstream::message::PublishMessage::build()
                        .payload(body.into())
                        .message_id(id),
                )
                .await
                .map_err(|e| failed(e.to_string()))?
                .await
                .map_err(|e| failed(e.to_string()))?;
            Ok(Acknowledged {
                stream_seq: ack.sequence,
                duplicate: ack.duplicate,
            })
        })
    }
}
