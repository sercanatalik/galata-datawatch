//! **Who may ask** (`market-data-access`, design D9).
//!
//! Every client is named in [`CLIENTS`] and holds a token of its own, read
//! from the vault through the one door secrets are read by and delivered on
//! standard input, as a broker password is. A request carries it as
//! `Authorization: Bearer <token>`; a request without one, or with one not
//! issued, is refused alike, and the refusal names no client.
//!
//! **The token is the only identity.** A port published on the Mac's tailnet
//! address sees every caller as the container gateway, so no address tells
//! callers apart. `/health` alone is open, and says nothing but that the
//! service answers.

use std::sync::Arc;

use axum::extract::{Request, State};
use axum::http::{StatusCode, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use galata_datawatch::config::{ConfigError, Secret, SecretSource};

/// The clients galata-markets serves.
pub const CLIENTS: [&str; 3] = ["controlboard", "watcher", "operator"];

/// The variable holding one client's token: `GALATA_MARKETS_TOKEN_<CLIENT>`.
pub fn token_var(client: &str) -> String {
    format!(
        "GALATA_MARKETS_TOKEN_{}",
        client.replace('-', "_").to_uppercase()
    )
}

/// The client a request was served to, for the handlers that log or count by
/// it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Client(pub &'static str);

/// Every client's token.
#[derive(Clone)]
pub struct Tokens(Arc<Vec<(&'static str, Secret)>>);

impl std::fmt::Debug for Tokens {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let clients: Vec<&str> = self.0.iter().map(|(client, _)| *client).collect();
        f.debug_tuple("Tokens").field(&clients).finish()
    }
}

impl Tokens {
    /// Every client's token from the source, **an unset one refused by
    /// name**: a client the table names and the vault does not is a
    /// deployment that would turn a client away for no reason it could see.
    pub fn from_source(source: &dyn SecretSource) -> Result<Tokens, ConfigError> {
        let mut tokens = Vec::with_capacity(CLIENTS.len());
        for client in CLIENTS {
            tokens.push((client, source.secret(&token_var(client))?));
        }
        Ok(Tokens(Arc::new(tokens)))
    }

    /// The client a presented token was issued to.
    ///
    /// **In constant time**: every client's token is compared, every byte of
    /// each, whether or not one has already matched, so how long a refusal
    /// takes says nothing about how close a guess came.
    pub fn client_of(&self, presented: &[u8]) -> Option<&'static str> {
        let mut found = None;
        for (client, token) in self.0.iter() {
            if same(presented, token.expose().as_bytes()) {
                found = Some(*client);
            }
        }
        found
    }
}

/// Whether two byte strings are equal, looking at every byte of the longer.
fn same(a: &[u8], b: &[u8]) -> bool {
    let mut differ = u8::from(a.len() != b.len());
    for i in 0..a.len().max(b.len()) {
        differ |= a.get(i).copied().unwrap_or(0) ^ b.get(i).copied().unwrap_or(0);
    }
    differ == 0
}

/// **The refusal**, the same for no token and for one not issued.
pub fn refused() -> Response {
    (
        StatusCode::UNAUTHORIZED,
        [(header::WWW_AUTHENTICATE, "Bearer")],
        "a token issued to a client of galata-markets is required\n",
    )
        .into_response()
}

/// The middleware every route but `/health` is behind.
pub async fn require_token(
    State(tokens): State<Tokens>,
    mut request: Request,
    next: Next,
) -> Response {
    let presented = request
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "));
    match presented.and_then(|token| tokens.client_of(token.as_bytes())) {
        Some(client) => {
            request.extensions_mut().insert(Client(client));
            next.run(request).await
        }
        None => refused(),
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use std::collections::BTreeMap;

    /// A source of stand-in tokens made for the test, printed nowhere.
    struct Stand(BTreeMap<String, String>);

    impl SecretSource for Stand {
        fn secret(&self, name: &str) -> Result<Secret, ConfigError> {
            self.0
                .get(name)
                .map(|v| Secret::new(v.clone()))
                .ok_or_else(|| ConfigError::SecretAbsent {
                    name: name.to_string(),
                })
        }
    }

    pub(crate) fn stand_in() -> Tokens {
        let source = Stand(
            CLIENTS
                .iter()
                .map(|c| (token_var(c), format!("stand-in-token-for-{c}")))
                .collect(),
        );
        Tokens::from_source(&source).unwrap()
    }

    #[test]
    fn each_client_is_named_by_its_own_token_and_no_other() {
        let tokens = stand_in();
        assert_eq!(
            tokens.client_of(b"stand-in-token-for-watcher"),
            Some("watcher")
        );
        assert_eq!(tokens.client_of(b"stand-in-token-for-watche"), None);
        assert_eq!(tokens.client_of(b"stand-in-token-for-watcherr"), None);
        assert_eq!(tokens.client_of(b""), None);
    }

    #[test]
    fn a_client_with_no_token_in_the_vault_refuses_the_start_by_name() {
        let mut held: BTreeMap<String, String> = CLIENTS
            .iter()
            .map(|c| (token_var(c), "x".repeat(32)))
            .collect();
        held.remove("GALATA_MARKETS_TOKEN_WATCHER");
        let refusal = Tokens::from_source(&Stand(held)).unwrap_err().to_string();
        assert!(
            refusal.contains("GALATA_MARKETS_TOKEN_WATCHER"),
            "{refusal}"
        );
    }

    #[test]
    fn tokens_never_print() {
        let shown = format!("{:?}", stand_in());
        assert!(!shown.contains("stand-in-token"), "{shown}");
        assert!(shown.contains("controlboard"));
    }

    #[test]
    fn the_scripts_and_the_service_name_the_same_clients() {
        // The launcher, the secrets template and token minting read
        // scripts/lib/markets-clients; the service reads CLIENTS. A client in
        // one and not the other is a token minted for nobody, or a client
        // that cannot start the service.
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../scripts/lib/markets-clients");
        let listed: Vec<String> = std::fs::read_to_string(&path)
            .unwrap()
            .lines()
            .map(str::trim)
            .filter(|l| !l.is_empty() && !l.starts_with('#'))
            .map(str::to_string)
            .collect();
        assert_eq!(listed, CLIENTS);
    }

    #[test]
    fn the_comparison_looks_at_every_byte() {
        assert!(same(b"abc", b"abc"));
        assert!(!same(b"abc", b"abd"));
        assert!(!same(b"abc", b"abcd"));
        assert!(!same(b"", b"a"));
        assert!(same(b"", b""));
    }
}
