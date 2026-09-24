//! Signing every request, and the three traps that make it fail.
//!
//! ```text
//!   message = api_key + timestamp + path + method + body
//!   headers = x-api-key, x-timestamp, x-signature
//! ```
//!
//! Each trap below is documented as a thing people hit, and each is refused
//! here **by name** rather than discovered as a `401`:
//!
//! 1. **Milliseconds instead of seconds.** Fails every request. The kindest
//!    possible failure — loud and immediate — and still worth making
//!    impossible: this takes microseconds, like everything else in this tree,
//!    and converts once.
//!
//! 2. **A 64-byte expanded keypair, or PKCS#8, where a 32-byte seed is
//!    wanted.** *Signature invalid* tells nobody anything; *this is 64 bytes,
//!    which is an expanded keypair, and the seed is its first 32* tells them
//!    what to do.
//!
//! 3. **Clock skew past thirty seconds.** The signature expires, so a drifting
//!    clock stops being a latency figure and becomes a refusal. This is the
//!    first place in the system where a wrong clock does not merely mislabel
//!    data — it stops capture entirely.

use base64::Engine;
use ed25519_dalek::{Signer, SigningKey};

/// How long a signature is good for.
///
/// **Thirty seconds**, stated by the venue. Carried here so the clock check can
/// name it rather than a caller guessing.
pub const SIGNATURE_LIFETIME_SECS: i64 = 30;

/// What a private key must be.
pub const SEED_BYTES: usize = 32;

/// Why a credential or a signature could not be made.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
#[non_exhaustive]
pub enum SignError {
    /// The key is not base64.
    #[error("the private key is not base64: {detail}")]
    NotBase64 {
        /// What was wrong.
        detail: String,
    },
    /// The key decoded to an expanded keypair.
    #[error(
        "the private key is 64 bytes, which is an **expanded keypair**; this wants the 32-byte \
         seed, which is its first half"
    )]
    ExpandedKeypair,
    /// The key looks like a wrapped format.
    #[error(
        "the private key is {len} bytes and begins with an ASN.1 sequence, which is PKCS#8; this \
         wants the raw 32-byte seed"
    )]
    Pkcs8 {
        /// How long it was.
        len: usize,
    },
    /// Some other length entirely.
    #[error("the private key is {len} bytes; an Ed25519 seed is {SEED_BYTES}")]
    WrongLength {
        /// How long it was.
        len: usize,
    },
}

/// A signing credential.
///
/// **No `Debug` that can print the key.** The same rule as `BrokerIdentity`: a
/// secret reaches a log through the most ordinary line somebody writes, and the
/// only reliable defence is for the type to be unable to say it.
pub struct Credential {
    api_key: String,
    signing: SigningKey,
}

impl std::fmt::Debug for Credential {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Credential")
            .field("api_key", &self.api_key)
            .field("private_key", &"<held>")
            .finish()
    }
}

impl Credential {
    /// Build one from an API key and a **base64 32-byte seed**.
    ///
    /// Every wrong shape is refused here, before a request is attempted,
    /// because the alternative is learning about it from a `401` that looks
    /// exactly like a wrong key, a wrong clock or a revoked credential.
    pub fn new(
        api_key: impl Into<String>,
        private_key_base64: &str,
    ) -> Result<Credential, SignError> {
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(private_key_base64.trim())
            .map_err(|e| SignError::NotBase64 {
                detail: e.to_string(),
            })?;

        let seed: [u8; SEED_BYTES] = match bytes.len() {
            SEED_BYTES => bytes
                .as_slice()
                .try_into()
                .expect("the length was just checked"),
            64 => return Err(SignError::ExpandedKeypair),
            // 0x30 is an ASN.1 SEQUENCE, which is how every PKCS#8 wrapper
            // starts. Naming it is the difference between a minute and an
            // afternoon.
            len if bytes.first() == Some(&0x30) => return Err(SignError::Pkcs8 { len }),
            len => return Err(SignError::WrongLength { len }),
        };

        Ok(Credential {
            api_key: api_key.into(),
            signing: SigningKey::from_bytes(&seed),
        })
    }

    /// The same, from the two secrets a configuration names.
    ///
    /// **The one place their values leave the `Secret` type**, and they leave
    /// it to become a signing key, which prints as nothing either.
    pub fn from_secrets(
        api_key: &crate::config::Secret,
        private_key: &crate::config::Secret,
    ) -> Result<Credential, SignError> {
        Credential::new(api_key.expose(), private_key.expose())
    }

    /// The key, which is not a secret.
    pub fn api_key(&self) -> &str {
        &self.api_key
    }

    /// Sign a request.
    ///
    /// `at_micros` comes from the loop's clock, like every other time in this
    /// system — **and is divided to seconds here, once.** The venue expects
    /// whole seconds, and milliseconds fail every request.
    pub fn sign(&self, at_micros: i64, path: &str, method: &str, body: &str) -> Signed {
        let timestamp = at_micros.div_euclid(1_000_000);
        let message = format!("{}{timestamp}{path}{method}{body}", self.api_key);
        let signature = self.signing.sign(message.as_bytes());
        Signed {
            api_key: self.api_key.clone(),
            timestamp,
            signature: base64::engine::general_purpose::STANDARD.encode(signature.to_bytes()),
        }
    }
}

/// The three headers a signed request carries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Signed {
    /// `x-api-key`.
    pub api_key: String,
    /// `x-timestamp` — **whole seconds**.
    pub timestamp: i64,
    /// `x-signature`, base64.
    pub signature: String,
}

impl Signed {
    /// Whether this is still good at a moment.
    ///
    /// The venue expires a signature after thirty seconds, so this is also the
    /// clock-skew tolerance: sign at a drifted clock and the venue refuses.
    pub fn valid_at(&self, at_micros: i64) -> bool {
        let now = at_micros.div_euclid(1_000_000);
        let age = now - self.timestamp;
        (0..SIGNATURE_LIFETIME_SECS).contains(&age)
    }

    /// The headers, in the order a reader expects them.
    pub fn headers(&self) -> [(&'static str, String); 3] {
        [
            ("x-api-key", self.api_key.clone()),
            ("x-timestamp", self.timestamp.to_string()),
            ("x-signature", self.signature.clone()),
        ]
    }
}

impl crate::venue::RequestSigner for Credential {
    fn headers(
        &self,
        at_micros: i64,
        path_and_query: &str,
        method: &str,
        body: &str,
    ) -> Vec<(&'static str, String)> {
        self.sign(at_micros, path_and_query, method, body)
            .headers()
            .to_vec()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// **RFC 8032, section 7.1, TEST 1.** A published vector, so the
    /// implementation is checked against the standard rather than against its
    /// own output — which would be a test that a bug and its mirror image
    /// agree.
    const RFC8032_SEED_HEX: &str =
        "9d61b19deffd5a60ba844af492ec2cc44449c5697b326919703bac031cae7f60";
    const RFC8032_SIG_HEX: &str = concat!(
        "e5564300c360ac729086e2cc806e828a84877f1eb8e5d974d873e06522490155",
        "5fb8821590a33bacc61e39701cf9b46bd25bf5f0595bbe24655141438e7a100b"
    );

    fn hex(s: &str) -> Vec<u8> {
        (0..s.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
            .collect()
    }

    fn base64_of(bytes: &[u8]) -> String {
        base64::engine::general_purpose::STANDARD.encode(bytes)
    }

    #[test]
    fn the_signature_matches_a_published_vector() {
        // RFC 8032 TEST 1: the empty message.
        let seed = hex(RFC8032_SEED_HEX);
        let signing = SigningKey::from_bytes(&seed.as_slice().try_into().unwrap());
        let signature = signing.sign(b"");
        assert_eq!(
            signature.to_bytes().to_vec(),
            hex(RFC8032_SIG_HEX),
            "the implementation disagrees with RFC 8032"
        );
    }

    #[test]
    fn the_message_is_key_then_timestamp_then_path_then_method_then_body() {
        // The vectors cannot check this — it is the venue's own shape — so it
        // is asserted by signing the same bytes two ways and comparing.
        let seed = hex(RFC8032_SEED_HEX);
        let credential = Credential::new("API-KEY", &base64_of(&seed)).unwrap();

        let signed = credential.sign(
            1_700_000_000_000_000,
            "/api/v1/crypto/marketdata/best_bid_ask/",
            "GET",
            "",
        );

        let expected_message =
            "API-KEY1700000000/api/v1/crypto/marketdata/best_bid_ask/GET".to_string();
        let signing = SigningKey::from_bytes(&seed.as_slice().try_into().unwrap());
        assert_eq!(
            signed.signature,
            base64_of(&signing.sign(expected_message.as_bytes()).to_bytes()),
            "the message is not api_key + timestamp + path + method + body"
        );
    }

    #[test]
    fn the_timestamp_is_whole_seconds_and_not_milliseconds() {
        // **The trap that fails every request.** The type takes micros, like
        // everything else here, and divides once.
        let credential = Credential::new("K", &base64_of(&hex(RFC8032_SEED_HEX))).unwrap();
        let signed = credential.sign(1_700_000_000_123_456, "/p", "GET", "");
        assert_eq!(signed.timestamp, 1_700_000_000);
        // Ten digits, not thirteen. A millisecond timestamp is visibly longer.
        assert_eq!(signed.timestamp.to_string().len(), 10);
    }

    #[test]
    fn an_expanded_keypair_is_refused_and_says_where_the_seed_is() {
        // "Signature invalid" tells nobody anything.
        let expanded = vec![7u8; 64];
        let error = Credential::new("K", &base64_of(&expanded)).unwrap_err();
        assert_eq!(error, SignError::ExpandedKeypair);
        assert!(error.to_string().contains("first half"), "{error}");
    }

    #[test]
    fn a_pkcs8_key_is_refused_by_what_it_is() {
        // 0x30 is an ASN.1 SEQUENCE, which is how every PKCS#8 wrapper starts.
        let mut pkcs8 = vec![0x30u8, 0x2e, 0x02, 0x01, 0x00];
        pkcs8.extend_from_slice(&[0u8; 43]);
        let error = Credential::new("K", &base64_of(&pkcs8)).unwrap_err();
        assert!(matches!(error, SignError::Pkcs8 { .. }), "{error}");
        assert!(error.to_string().contains("PKCS#8"), "{error}");
    }

    #[test]
    fn a_key_that_is_not_base64_is_refused_before_any_request() {
        let error = Credential::new("K", "not base64 at all!!").unwrap_err();
        assert!(matches!(error, SignError::NotBase64 { .. }), "{error}");
    }

    #[test]
    fn a_wrong_length_names_the_length_and_the_one_wanted() {
        let error = Credential::new("K", &base64_of(&[1u8; 16])).unwrap_err();
        assert_eq!(error, SignError::WrongLength { len: 16 });
        assert!(error.to_string().contains("32"), "{error}");
    }

    #[test]
    fn the_credential_cannot_print_its_key() {
        let credential = Credential::new("PUBLIC-KEY", &base64_of(&hex(RFC8032_SEED_HEX))).unwrap();
        let printed = format!("{credential:?}");
        assert!(
            printed.contains("PUBLIC-KEY"),
            "the api key is not a secret"
        );
        assert!(printed.contains("<held>"), "{printed}");
        assert!(!printed.contains(RFC8032_SEED_HEX), "the seed leaked");
    }

    #[test]
    fn a_signature_expires_after_thirty_seconds() {
        // **Clock skew becomes a 401.** The first place in this system where a
        // wrong clock does not merely mislabel data — it stops capture.
        let credential = Credential::new("K", &base64_of(&hex(RFC8032_SEED_HEX))).unwrap();
        let signed = credential.sign(1_700_000_000_000_000, "/p", "GET", "");

        assert!(signed.valid_at(1_700_000_000_000_000), "valid immediately");
        assert!(signed.valid_at(1_700_000_029_000_000), "valid at 29 s");
        assert!(!signed.valid_at(1_700_000_030_000_000), "expired at 30 s");
        // A clock BEHIND the venue's is refused too, which is the case that
        // looks like a bad key rather than a bad clock.
        assert!(
            !signed.valid_at(1_699_999_999_000_000),
            "a future signature"
        );
    }

    #[test]
    fn the_headers_are_the_three_the_venue_reads() {
        let credential = Credential::new("K", &base64_of(&hex(RFC8032_SEED_HEX))).unwrap();
        let names: Vec<&str> = credential
            .sign(1_700_000_000_000_000, "/p", "GET", "")
            .headers()
            .iter()
            .map(|(name, _)| *name)
            .collect();
        assert_eq!(names, ["x-api-key", "x-timestamp", "x-signature"]);
    }
}
