//! Derive's request signature: **EIP-191 `personal_sign` by a session key.**
//!
//! A private request carries the time in milliseconds as a decimal string,
//! signed by the wallet or a session key registered to it, in its
//! `X-DeriveSignature` header (docs.derive.xyz, "Session login", read
//! 2026-11-26). Nothing else is signed here: an action — an order, a transfer,
//! a key's registration — is EIP-712 and is not this module's, since the
//! ledger performs none.
//!
//! ```text
//!   digest     keccak-256("\x19Ethereum Signed Message:\n" + len(message) + message)
//!   signature  r ‖ s ‖ v, 65 bytes, v = 27 + recovery id, as hex with 0x
//!   address    the last 20 bytes of keccak-256 of the uncompressed public key
//! ```
//!
//! **The key never leaves this type.** It is read once, from a [`Secret`],
//! and nothing here formats it: an error names what was wrong with it, never
//! its value.

use k256::ecdsa::SigningKey;
use sha3::{Digest, Keccak256};

use crate::config::Secret;

/// Why a session key could not be read. Never carries the key.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum KeyError {
    /// Not 32 bytes of hex, with or without `0x`.
    #[error("the session key is not 32 bytes of hex")]
    NotHex,
    /// 32 bytes, and not a valid secp256k1 private key.
    #[error("the session key is not a valid secp256k1 key")]
    NotAKey,
}

/// A session key, able to sign a request's time and to say its own address.
pub struct SessionKey {
    key: SigningKey,
}

impl std::fmt::Debug for SessionKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // The address is public; the key is not.
        f.debug_struct("SessionKey")
            .field("address", &self.address())
            .finish()
    }
}

/// keccak-256 of the EIP-191 prefixed message.
pub fn personal_digest(message: &[u8]) -> [u8; 32] {
    let mut hasher = Keccak256::new();
    hasher.update(format!("\x19Ethereum Signed Message:\n{}", message.len()).as_bytes());
    hasher.update(message);
    hasher.finalize().into()
}

fn address_of(key: &k256::ecdsa::VerifyingKey) -> String {
    let point = key.to_encoded_point(false);
    let hash = Keccak256::digest(&point.as_bytes()[1..]);
    format!("0x{}", hex_lower(&hash[12..]))
}

fn hex_lower(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// Make a new session key from the operating system's randomness, write its
/// private half to `out` — the one time it leaves a key, so that it goes
/// straight into the vault — and return its address. `galata-session-key`
/// hands it standard output, which `scripts/make-derive-session-key.sh` pipes
/// into `gv set`.
pub fn generate_into(out: &mut impl std::io::Write) -> std::io::Result<String> {
    use std::io::Read;
    let mut random = std::fs::File::open("/dev/urandom")?;
    loop {
        let mut bytes = [0u8; 32];
        random.read_exact(&mut bytes)?;
        // Zero, or at or above the curve's order, is not a key: about one
        // draw in 2^128. Drawn again rather than reduced, which would bias it.
        if let Ok(key) = SigningKey::from_bytes(&bytes.into()) {
            out.write_all(format!("0x{}", hex_lower(&bytes)).as_bytes())?;
            out.flush()?;
            return Ok(address_of(key.verifying_key()));
        }
    }
}

impl SessionKey {
    /// The key a secret holds: 32 bytes of hex, `0x` optional.
    pub fn from_secret(secret: &Secret) -> Result<SessionKey, KeyError> {
        let text = secret.expose().trim();
        let text = text.strip_prefix("0x").unwrap_or(text);
        if text.len() != 64 {
            return Err(KeyError::NotHex);
        }
        let mut bytes = [0u8; 32];
        for (i, pair) in text.as_bytes().chunks(2).enumerate() {
            let pair = std::str::from_utf8(pair).map_err(|_| KeyError::NotHex)?;
            bytes[i] = u8::from_str_radix(pair, 16).map_err(|_| KeyError::NotHex)?;
        }
        let key = SigningKey::from_bytes(&bytes.into()).map_err(|_| KeyError::NotAKey)?;
        Ok(SessionKey { key })
    }

    /// The key's address, lower-case hex with `0x`: what the venue lists the
    /// key as among the wallet's session keys.
    pub fn address(&self) -> String {
        address_of(self.key.verifying_key())
    }

    /// The EIP-191 signature of `message`, as the venue's header carries it.
    pub fn sign(&self, message: &str) -> String {
        let digest = personal_digest(message.as_bytes());
        let (signature, recovery) = self
            .key
            .sign_prehash_recoverable(&digest)
            .expect("a 32-byte digest is always signable");
        let mut out = signature.to_bytes().to_vec();
        out.push(27 + recovery.to_byte());
        format!("0x{}", hex_lower(&out))
    }
}

/// The address that signed `message`, recovered from its signature. What the
/// venue does with a request; here, what a test proves a signature against.
pub fn recover(message: &str, signature: &str) -> Option<String> {
    let text = signature.strip_prefix("0x")?;
    if text.len() != 130 {
        return None;
    }
    let bytes: Vec<u8> = (0..65)
        .map(|i| u8::from_str_radix(&text[2 * i..2 * i + 2], 16).ok())
        .collect::<Option<_>>()?;
    let signature = k256::ecdsa::Signature::from_slice(&bytes[..64]).ok()?;
    let recovery = k256::ecdsa::RecoveryId::from_byte(bytes[64].checked_sub(27)?)?;
    let digest = personal_digest(message.as_bytes());
    let key =
        k256::ecdsa::VerifyingKey::recover_from_prehash(&digest, &signature, recovery).ok()?;
    Some(address_of(&key))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The test key of Ethereum's account-signing examples, and its address.
    /// A published throwaway: it holds nothing on any chain worth holding.
    const KEY: &str = "0x4c0883a69102937d6231471b5dbb6204fe5129617082792ae468d01a3f362318";
    const ADDRESS: &str = "0x2c7536e3605d9c16a7a3d7b1898e529396a65c23";

    #[test]
    fn the_key_says_its_address() {
        let key = SessionKey::from_secret(&Secret::new(KEY)).unwrap();
        assert_eq!(key.address(), ADDRESS);
        // `0x` is optional, and whitespace around a secret is not part of it.
        let bare = SessionKey::from_secret(&Secret::new(format!(" {} ", &KEY[2..]))).unwrap();
        assert_eq!(bare.address(), ADDRESS);
    }

    #[test]
    fn the_digest_is_eip_191s() {
        // The published example: "Some data" under the personal prefix.
        assert_eq!(
            hex_lower(&personal_digest(b"Some data")),
            "1da44b586eb0729ff70a73c326926f6ed5a25f5b056e7f47fbc6e58d86871655"
        );
    }

    #[test]
    fn a_signed_time_recovers_to_the_key() {
        let key = SessionKey::from_secret(&Secret::new(KEY)).unwrap();
        // A request's time, in milliseconds, as the header carries it.
        let time = "1795705200000";
        let signature = key.sign(time);
        assert_eq!(signature.len(), 2 + 130, "r, s and v, as hex with 0x");
        assert!(
            signature.ends_with("1b") || signature.ends_with("1c"),
            "v is 27 or 28"
        );
        assert_eq!(recover(time, &signature).as_deref(), Some(ADDRESS));
        // Another message does not recover to it.
        assert_ne!(
            recover("1795705200001", &signature).as_deref(),
            Some(ADDRESS)
        );
    }

    #[test]
    fn a_new_key_is_a_key_and_says_its_own_address() {
        let (mut one, mut two) = (Vec::new(), Vec::new());
        let address = generate_into(&mut one).unwrap();
        generate_into(&mut two).unwrap();
        let written = String::from_utf8(one).unwrap();
        assert_eq!(
            written.len(),
            2 + 64,
            "32 bytes of hex, with 0x, and nothing else"
        );
        let key = SessionKey::from_secret(&Secret::new(written.clone())).unwrap();
        assert_eq!(key.address(), address);
        assert_ne!(
            String::from_utf8(two).unwrap(),
            written,
            "two draws, two keys"
        );
    }

    #[test]
    fn a_bad_key_is_refused_without_its_value() {
        for (value, want) in [
            ("0x1234", KeyError::NotHex),
            (&"zz".repeat(32)[..], KeyError::NotHex),
            (&"00".repeat(32)[..], KeyError::NotAKey),
        ] {
            let err = SessionKey::from_secret(&Secret::new(value)).unwrap_err();
            assert_eq!(err, want);
            assert!(!err.to_string().contains(value), "{err}");
        }
        // And a key's Debug says its address, never itself.
        let key = SessionKey::from_secret(&Secret::new(KEY)).unwrap();
        let said = format!("{key:?}");
        assert!(
            said.contains(ADDRESS) && !said.contains(&KEY[2..]),
            "{said}"
        );
    }
}
