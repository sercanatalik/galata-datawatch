//! Connecting, and putting events on the bus.

use std::future::Future;
use std::pin::Pin;

use galata_wire::Envelope;

use crate::encode::encode;
use crate::identity::BrokerIdentity;
use crate::subject::Subject;

/// A publish that did not happen.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum PublishError {
    /// The broker refused it, or was not there.
    #[error("publish to {subject}: {reason}")]
    Failed {
        /// Where it was going.
        subject: String,
        /// What went wrong.
        reason: String,
    },
}

/// Why a connection did not happen — and these two are **opposite facts**.
///
/// ```text
///   Unreachable   the broker did not answer. An OUTAGE the record survives:
///                 archive-before-publish means capture keeps running, which a
///                 nine-hour soak measured with no broker at all.
///
///   Rejected      the broker answered and refused this identity. A
///                 MISCONFIGURATION. It will never fix itself, and the status
///                 surface that would report it is a publish too. Fatal at boot.
/// ```
///
/// Both used to arrive as one error in the predecessor, which is why running
/// for a day archiving everything and publishing nothing looked exactly like
/// running for a day with a broker outage. **This is the only place the two are
/// told apart.**
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ConnectRefusal {
    /// The broker answered, and said no.
    #[error(
        "broker at {addr} refused identity {identity}: {reason}.\n  \
         The grant table and this process disagree. Check that {var} is set, and that the \
         table loaded into the server grants this identity."
    )]
    Rejected {
        /// Where.
        addr: String,
        /// Who we said we were.
        identity: String,
        /// The variable the secret came from.
        var: String,
        /// What the broker said.
        reason: String,
    },
    /// The broker did not answer.
    #[error("broker at {addr} did not answer: {reason}")]
    Unreachable {
        /// Where.
        addr: String,
        /// What went wrong.
        reason: String,
    },
}

impl ConnectRefusal {
    /// Which of the two this is.
    ///
    /// **The classification is a guess made carefully, and it is made once.**
    /// `async-nats` reports an authentication failure and a dead host through
    /// the same error type, so the text is what distinguishes them. Anything
    /// not recognisably an authentication failure is treated as **unreachable**
    /// — the safe direction, because mistaking a refusal for an outage costs a
    /// process that runs without a broker and says so, while the reverse costs
    /// a process that refuses to start over a network blip.
    pub fn classify(addr: &str, identity: &BrokerIdentity, error: &impl std::fmt::Display) -> Self {
        let reason = error.to_string();
        let lowered = reason.to_lowercase();
        let authentication = lowered.contains("authorization")
            || lowered.contains("authorisation")
            || lowered.contains("authentication")
            || lowered.contains("unauthorized")
            || lowered.contains("invalid client protocol")
            || lowered.contains("user")
                && (lowered.contains("password") || lowered.contains("credential"));
        if authentication {
            ConnectRefusal::Rejected {
                addr: addr.to_string(),
                identity: identity.user.clone(),
                var: identity.password_var.clone(),
                reason,
            }
        } else {
            ConnectRefusal::Unreachable {
                addr: addr.to_string(),
                reason,
            }
        }
    }

    /// Whether the process should refuse to boot.
    ///
    /// A misconfiguration will never fix itself; an outage will.
    pub fn is_fatal(&self) -> bool {
        matches!(self, ConnectRefusal::Rejected { .. })
    }
}

/// Where normalised events go.
///
/// A trait, so that a **failure** to publish can be exercised in a test — which
/// is the behaviour that matters, because the record must not depend on the
/// broker.
pub trait Publisher: Send + Sync {
    /// One envelope, on its subject.
    fn publish<'a>(
        &'a self,
        subject: &'a Subject,
        envelope: &'a Envelope,
    ) -> Pin<Box<dyn Future<Output = Result<(), PublishError>> + Send + 'a>>;

    /// A status snapshot, on its own root.
    fn publish_status<'a>(
        &'a self,
        subject: &'a Subject,
        body: &'a [u8],
    ) -> Pin<Box<dyn Future<Output = Result<(), PublishError>> + Send + 'a>>;
}

/// Connect as a named identity.
///
/// **Nothing connects anonymously**, and this is the only door.
pub(crate) async fn connect_as(
    addr: &str,
    identity: &BrokerIdentity,
) -> Result<async_nats::Client, ConnectRefusal> {
    async_nats::ConnectOptions::with_user_and_password(
        identity.user.clone(),
        identity.password.clone(),
    )
    .connect(addr)
    .await
    .map_err(|e| ConnectRefusal::classify(addr, identity, &e))
}

/// The real broker, writing.
#[derive(Debug, Clone)]
pub struct NatsPublisher {
    client: async_nats::Client,
}

impl NatsPublisher {
    /// Connect, or say which kind of no this was.
    pub async fn connect(
        addr: &str,
        identity: &BrokerIdentity,
    ) -> Result<NatsPublisher, ConnectRefusal> {
        Ok(NatsPublisher {
            client: connect_as(addr, identity).await?,
        })
    }

    /// Push whatever is buffered to the wire.
    ///
    /// The client buffers, so a publish that returned is not yet a publish that
    /// arrived. A process shutting down calls this; the hot path does not,
    /// because flushing per message is what makes a bus slow.
    pub async fn flush(&self) -> Result<(), PublishError> {
        self.client.flush().await.map_err(|e| PublishError::Failed {
            subject: "<flush>".into(),
            reason: e.to_string(),
        })
    }
}

impl Publisher for NatsPublisher {
    fn publish<'a>(
        &'a self,
        subject: &'a Subject,
        envelope: &'a Envelope,
    ) -> Pin<Box<dyn Future<Output = Result<(), PublishError>> + Send + 'a>> {
        Box::pin(async move {
            self.client
                .publish(subject.as_str().to_string(), encode(envelope).into())
                .await
                .map_err(|e| PublishError::Failed {
                    subject: subject.as_str().to_string(),
                    reason: e.to_string(),
                })
        })
    }

    fn publish_status<'a>(
        &'a self,
        subject: &'a Subject,
        body: &'a [u8],
    ) -> Pin<Box<dyn Future<Output = Result<(), PublishError>> + Send + 'a>> {
        Box::pin(async move {
            self.client
                .publish(subject.as_str().to_string(), body.to_vec().into())
                .await
                .map_err(|e| PublishError::Failed {
                    subject: subject.as_str().to_string(),
                    reason: e.to_string(),
                })
        })
    }
}

/// A publisher that goes nowhere.
///
/// What capture runs on until a broker answers — **the record does not depend
/// on one**, so this is a supported state rather than a degraded one.
#[derive(Debug, Default, Clone, Copy)]
pub struct NullPublisher;

impl Publisher for NullPublisher {
    fn publish<'a>(
        &'a self,
        _subject: &'a Subject,
        _envelope: &'a Envelope,
    ) -> Pin<Box<dyn Future<Output = Result<(), PublishError>> + Send + 'a>> {
        Box::pin(async { Ok(()) })
    }

    fn publish_status<'a>(
        &'a self,
        _subject: &'a Subject,
        _body: &'a [u8],
    ) -> Pin<Box<dyn Future<Output = Result<(), PublishError>> + Send + 'a>> {
        Box::pin(async { Ok(()) })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn identity() -> BrokerIdentity {
        BrokerIdentity::new("datawatch", "secret", "GALATA_DATAWATCH_PASSWORD")
    }

    #[test]
    fn an_authorisation_failure_is_fatal() {
        // A misconfiguration that will never fix itself, and the surface that
        // would report it is a publish too.
        let refusal = ConnectRefusal::classify(
            "nats://localhost:4222",
            &identity(),
            &"Authorization Violation".to_string(),
        );
        assert!(refusal.is_fatal());
        let said = refusal.to_string();
        assert!(said.contains("datawatch"), "{said}");
        assert!(
            said.contains("GALATA_DATAWATCH_PASSWORD"),
            "a refusal must name the variable: {said}"
        );
        assert!(!said.contains("secret"), "the password leaked: {said}");
    }

    #[test]
    fn a_dead_host_is_not_fatal() {
        // An outage the record survives.
        let refusal = ConnectRefusal::classify(
            "nats://localhost:4222",
            &identity(),
            &"failed to resolve host".to_string(),
        );
        assert!(!refusal.is_fatal());
        assert!(refusal.to_string().contains("did not answer"));
    }

    #[test]
    fn an_unrecognised_failure_is_treated_as_an_outage() {
        // The safe direction. Mistaking a refusal for an outage costs a process
        // that runs without a broker and says so; the reverse costs a process
        // that refuses to start over a network blip.
        let refusal = ConnectRefusal::classify(
            "nats://localhost:4222",
            &identity(),
            &"something nobody has seen before".to_string(),
        );
        assert!(!refusal.is_fatal());
    }

    #[test]
    fn a_null_publisher_takes_everything() {
        // The record does not depend on the broker, so this is a supported
        // state rather than a degraded one.
        let subject = Subject::status(&galata_wire::Venue::new("hyperliquid").unwrap());
        // Polled once by hand rather than on a runtime: this future never
        // yields, and pulling in an executor to prove that would be a
        // dependency for a test.
        let mut future = NullPublisher.publish_status(&subject, b"{}");
        let waker = std::task::Waker::noop();
        let mut context = std::task::Context::from_waker(waker);
        assert!(matches!(
            future.as_mut().poll(&mut context),
            std::task::Poll::Ready(Ok(()))
        ));
    }
}
