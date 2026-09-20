//! Numbers as the venue spelled them, and the exact values they parse to.
//!
//! A venue documents a field as a number and sends a string, or the reverse,
//! and does it inconsistently across channels. [`Token`] accepts either, which
//! is what wire-quirk tolerance means in practice.
//!
//! The token itself does **not** ride alongside every parsed number onto the
//! tape. The predecessor did that and it doubled the width of every numeric
//! column in the largest table, to duplicate something the archive already
//! holds verbatim. The token lives in the archive; `recv_micros` is the road
//! back to it.

use std::fmt;

use rust_decimal::Decimal;

/// An exact number.
///
/// # A stated bound
///
/// This is a 96-bit value holding roughly **28 significant digits**, while the
/// on-disk type is `decimal(38,18)`, which holds **38**. The parquet type
/// over-promises what this type can carry.
///
/// It is kept anyway. The alternative is carrying an `i128` and a scale by
/// hand, and every arithmetic site then becomes a place to get the scale
/// wrong. This parses from a string exactly, compares exactly, and covers
/// every price, size and notional in scope by a wide margin.
///
/// It is written down because a store that silently truncated at the 29th
/// digit would be a defect discovered by a future venue rather than by a
/// reader of this line.
pub type Num = Decimal;

/// Why a numeric field could not be read.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum NumError {
    /// The venue sent something that does not spell a number.
    #[error("{field}: {value:?} is not a number the venue could have meant")]
    NotANumber {
        /// Which field, so a refusal says where rather than only what.
        field: &'static str,
        /// What arrived.
        value: String,
    },
    /// The field the caller required was not present.
    #[error("{field} is absent")]
    Absent {
        /// Which field.
        field: &'static str,
    },
}

/// A numeric field as it arrived, before it is a number.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct Token(String);

impl Token {
    /// Wrap raw text. No parsing happens here — a token that does not spell a
    /// number is still what the venue sent, and the archive already holds it.
    pub fn new(raw: impl Into<String>) -> Token {
        Token(raw.into())
    }

    /// The text, exactly as it arrived.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// The number this token spells.
    ///
    /// `field` is taken by name so the refusal says which field was wrong,
    /// which is the difference between a diagnosable normalisation defect and
    /// a log line reading `invalid number`.
    pub fn require(&self, field: &'static str) -> Result<Num, NumError> {
        self.0.parse::<Decimal>().map_err(|_| NumError::NotANumber {
            field,
            value: self.0.clone(),
        })
    }
}

impl fmt::Display for Token {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// A token that may be absent, required by name so the error says which field.
pub fn require(value: Option<&Token>, field: &'static str) -> Result<Num, NumError> {
    value.ok_or(NumError::Absent { field })?.require(field)
}

impl<'de> serde::Deserialize<'de> for Token {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct V;
        impl serde::de::Visitor<'_> for V {
            type Value = Token;
            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a number, as a string or as a number")
            }
            fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<Token, E> {
                Ok(Token::new(v))
            }
            fn visit_u64<E: serde::de::Error>(self, v: u64) -> Result<Token, E> {
                Ok(Token::new(v.to_string()))
            }
            fn visit_i64<E: serde::de::Error>(self, v: i64) -> Result<Token, E> {
                Ok(Token::new(v.to_string()))
            }
            // A venue that sends a float sends its DECIMAL TEXT, and serde
            // hands the parsed double. Rendering it back is the one place a
            // float touches this path, and it is why a venue's numbers should
            // arrive as strings wherever it offers them that way.
            fn visit_f64<E: serde::de::Error>(self, v: f64) -> Result<Token, E> {
                Ok(Token::new(v.to_string()))
            }
        }
        d.deserialize_any(V)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    #[test]
    fn a_string_and_a_number_parse_to_one_value() {
        let from_string: Token = serde_json::from_str("\"123.45\"").unwrap();
        let from_number: Token = serde_json::from_str("123.45").unwrap();
        assert_eq!(
            from_string.require("price").unwrap(),
            from_number.require("price").unwrap()
        );
        assert_eq!(
            from_string.require("price").unwrap(),
            Decimal::from_str("123.45").unwrap()
        );
    }

    #[test]
    fn a_non_numeric_token_names_its_field() {
        let err = Token::new("abc").require("price").unwrap_err();
        assert!(err.to_string().contains("price"));
        assert!(err.to_string().contains("abc"));
    }

    #[test]
    fn an_absent_field_is_refused_by_name() {
        let err = require(None, "funding rate").unwrap_err();
        assert!(matches!(err, NumError::Absent { .. }));
        assert!(err.to_string().contains("funding rate"));
    }

    #[test]
    fn the_text_survives_the_parse() {
        // Trailing zeros, a leading plus, an exponent: what the venue wrote is
        // recoverable, because a normalisation defect found in month four is
        // diagnosed by comparing the two.
        let token = Token::new("0.00010000");
        assert_eq!(token.as_str(), "0.00010000");
        assert_eq!(
            token.require("size").unwrap(),
            Decimal::from_str("0.0001").unwrap()
        );
    }

    #[test]
    fn a_big_and_a_tiny_number_are_exact() {
        // Within the 28 digits Num carries. A f64 would lose the last of these
        // and the loss would look like a price.
        let big = Token::new("123456789012345.678901234").require("px").unwrap();
        assert_eq!(big.to_string(), "123456789012345.678901234");
        let tiny = Token::new("0.000000000000000001").require("px").unwrap();
        assert_eq!(tiny.to_string(), "0.000000000000000001");
    }

    #[test]
    fn a_negative_rate_is_a_number_like_any_other() {
        // Funding goes negative routinely, and a parser that refused it would
        // turn a normal market into an unparsed payload.
        assert_eq!(
            Token::new("-0.0000125").require("rate").unwrap(),
            Decimal::from_str("-0.0000125").unwrap()
        );
    }
}
