//! Reading the bus.
//!
//! **Separate from the publisher**, because publishing and consuming are
//! different authorities: datawatch publishes and never consumes, and a
//! component that only folds should not hold a handle that can write. The
//! server's grant table enforces it; these types make the intent legible on
//! this side.

use futures_util::StreamExt;
use galata_wire::Envelope;

use crate::encode::{DecodeError, decode};
use crate::identity::BrokerIdentity;
use crate::publisher::{ConnectRefusal, connect_as};

/// The reading half.
#[derive(Debug)]
pub struct NatsSubscriber {
    inner: async_nats::Subscriber,
}

impl NatsSubscriber {
    /// Subscribe to a subject, which may be a wildcard.
    ///
    /// The **subscription itself** can be refused when the identity's grant
    /// does not cover the subject — which is how *"this component reads only
    /// market data"* stops being a rule somebody reviews and becomes one the
    /// server enforces.
    pub async fn connect(
        addr: &str,
        identity: &BrokerIdentity,
        subject: &str,
    ) -> Result<NatsSubscriber, ConnectRefusal> {
        let client = connect_as(addr, identity).await?;
        let inner =
            client
                .subscribe(subject.to_string())
                .await
                .map_err(|e| ConnectRefusal::Rejected {
                    addr: addr.to_string(),
                    identity: identity.user.clone(),
                    var: identity.password_var.clone(),
                    reason: format!("subscribe to {subject}: {e}"),
                })?;
        Ok(NatsSubscriber { inner })
    }

    /// The next envelope, or `None` when the subscription ends.
    ///
    /// **A message that will not decode is an error, never a skip.** A consumer
    /// silently dropping what it could not read is the failure a named error
    /// exists to prevent.
    pub async fn next(&mut self) -> Option<Result<Envelope, DecodeError>> {
        let message = self.inner.next().await?;
        Some(decode(&message.payload))
    }

    /// The next payload with the subject it arrived on — for a wildcard
    /// subscriber fanning one subscription across many speakers, as a dashboard
    /// does over `status.>`.
    pub async fn next_addressed(&mut self) -> Option<(String, Vec<u8>)> {
        self.inner
            .next()
            .await
            .map(|m| (m.subject.to_string(), m.payload.to_vec()))
    }
}
