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
    /// Held so the transport's state can be reported. `async-nats` reconnects
    /// underneath a subscription, so the subscription alone cannot answer it.
    client: async_nats::Client,
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
        Ok(NatsSubscriber { client, inner })
    }

    /// Whether the transport is up, as the client itself reports it.
    ///
    /// **A subscription that has not ended is not evidence of a broker.**
    /// `async-nats` reconnects underneath one, so a consumer that reported
    /// only "subscribed" would go on claiming a connection right through an
    /// outage. A status surface that did that would be telling the most
    /// reassuring falsehood available to it, which is why this is here.
    ///
    /// Read-only, and deliberately so: it exposes what the client already
    /// knows and changes nothing about how a connection is made. The refusal
    /// semantics `connect_as` keeps — *did not answer* apart from *rejected
    /// the identity* — are what the capture depends on and are untouched.
    pub fn connected(&self) -> bool {
        self.client.connection_state() == async_nats::connection::State::Connected
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
