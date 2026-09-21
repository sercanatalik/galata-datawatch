//! What a process presents to the broker.

/// A named identity, and its secret.
///
/// **Nothing connects anonymously.** A grant table that gives three identities
/// their subjects changes nothing if a nameless client keeps the unlimited
/// authority it had before the table existed.
#[derive(Clone)]
pub struct BrokerIdentity {
    /// The user the broker knows.
    pub user: String,
    /// The secret. Never printed.
    pub password: String,
    /// The environment variable the password came from, **carried rather than
    /// recomputed**, so a refusal can name it.
    ///
    /// The rule that maps an identity to a variable name belongs to whatever
    /// generates the grant table. A second implementation of it here would not
    /// fail when it drifted — it would disagree.
    pub password_var: String,
}

impl BrokerIdentity {
    /// An identity, and where its secret was read from.
    pub fn new(
        user: impl Into<String>,
        password: impl Into<String>,
        password_var: impl Into<String>,
    ) -> BrokerIdentity {
        BrokerIdentity {
            user: user.into(),
            password: password.into(),
            password_var: password_var.into(),
        }
    }
}

/// **No derived `Debug`**, because a derived one prints the password.
///
/// A secret reaches a log through the most ordinary line somebody writes —
/// `tracing::info!(?identity)` — and the only reliable defence is for the type
/// to be unable to say it.
impl std::fmt::Debug for BrokerIdentity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BrokerIdentity")
            .field("user", &self.user)
            .field("password", &"<held>")
            .field("password_var", &self.password_var)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_password_cannot_be_printed() {
        // Not a convention — the type is unable to say it.
        let identity = BrokerIdentity::new("datawatch", "hunter2", "GALATA_DATAWATCH_PASSWORD");
        let printed = format!("{identity:?}");
        assert!(!printed.contains("hunter2"), "{printed}");
        assert!(printed.contains("<held>"), "{printed}");
        assert!(printed.contains("datawatch"));
        // The VARIABLE name is not a secret, and a refusal needs to name it.
        assert!(printed.contains("GALATA_DATAWATCH_PASSWORD"));
    }
}
