//! How a thing is named: validated tokens, checked once, at construction.
//!
//! These values become partition directory names and broker subject tokens. A
//! ticker carrying a `.` builds a subject the broker reads as two addresses;
//! one carrying a `/` builds a path. Validating at every use is a check that
//! will be forgotten at one of them, so there is exactly one constructor and it
//! refuses by name.

use std::fmt;
use std::str::FromStr;

/// A venue, as a validated token. Also a partition directory name.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, serde::Serialize)]
pub struct Venue(String);

/// An instrument, as the adapter resolved it: **one name per venue**, whatever
/// the venue called it on each channel.
///
/// The venue's own symbol is not necessarily a legal ticker. Hyperliquid spells
/// a builder-deployed perp `xyz:XYZ100`, and the `:` is refused here — so the
/// dex prefix is composed at the venue seam and the ticker stays `XYZ100`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, serde::Serialize)]
pub struct Ticker(String);

/// A declared name for an ordered set of `(venue, ticker)` constituents — the
/// thing a cross-venue comparison is *about*.
///
/// A token rather than a `(venue, ticker)` pair because a market may span
/// venues, and one pair cannot name a thing that does.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, serde::Serialize)]
pub struct Market(String);

/// Why a token was refused. Every variant names the value and, where it
/// applies, the exact character that caused it.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum TokenError {
    /// The value held nothing.
    #[error("{what} {value:?} is empty")]
    Empty {
        /// Which kind of token was being built.
        what: &'static str,
        /// The offending value.
        value: String,
    },
    /// The value held a character a partition path or a subject cannot carry.
    #[error(
        "{what} {value:?} contains {ch:?}, which a token may not hold — permitted: a-z A-Z 0-9 _ -"
    )]
    Illegal {
        /// Which kind of token was being built.
        what: &'static str,
        /// The offending value.
        value: String,
        /// The first character that is not permitted.
        ch: char,
    },
    /// The value was longer than [`MAX_TOKEN`].
    #[error("{what} {value:?} is longer than {max} characters")]
    TooLong {
        /// Which kind of token was being built.
        what: &'static str,
        /// The offending value.
        value: String,
        /// The bound that was exceeded.
        max: usize,
    },
}

/// The longest a token may be. Generous for a ticker, and short of anything a
/// broker subject or a filesystem path component objects to.
pub const MAX_TOKEN: usize = 64;

pub(crate) fn validate(what: &'static str, value: &str) -> Result<(), TokenError> {
    if value.is_empty() {
        return Err(TokenError::Empty {
            what,
            value: value.to_string(),
        });
    }
    if value.len() > MAX_TOKEN {
        return Err(TokenError::TooLong {
            what,
            value: value.to_string(),
            max: MAX_TOKEN,
        });
    }
    if let Some(ch) = value
        .chars()
        .find(|c| !(c.is_ascii_alphanumeric() || *c == '_' || *c == '-'))
    {
        return Err(TokenError::Illegal {
            what,
            value: value.to_string(),
            ch,
        });
    }
    Ok(())
}

macro_rules! token_type {
    ($ty:ident, $what:literal) => {
        impl $ty {
            /// The only way to make one. A value that would change a path's or
            /// a subject's meaning is refused here rather than discovered at
            /// the filesystem or the broker.
            pub fn new(value: impl Into<String>) -> Result<Self, TokenError> {
                let value = value.into();
                validate($what, &value)?;
                Ok($ty(value))
            }

            /// The validated value.
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl fmt::Display for $ty {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(&self.0)
            }
        }

        impl FromStr for $ty {
            type Err = TokenError;
            fn from_str(s: &str) -> Result<Self, Self::Err> {
                $ty::new(s)
            }
        }

        impl<'de> serde::Deserialize<'de> for $ty {
            fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
                let raw = String::deserialize(d)?;
                $ty::new(raw).map_err(serde::de::Error::custom)
            }
        }
    };
}

token_type!(Venue, "venue");
token_type!(Ticker, "ticker");
token_type!(Market, "market");

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_legal_ticker_is_accepted() {
        assert_eq!(Ticker::new("XYZ100").unwrap().as_str(), "XYZ100");
        assert_eq!(Ticker::new("BTC-USD").unwrap().as_str(), "BTC-USD");
        assert_eq!(Ticker::new("PI_XBTUSD").unwrap().as_str(), "PI_XBTUSD");
    }

    #[test]
    fn a_dotted_ticker_cannot_be_constructed() {
        // A subject token permits a-z A-Z 0-9 _ and -. A ticker carrying a dot
        // would build a subject the broker reads as two tokens.
        let err = Ticker::new("BTC.PERP").unwrap_err();
        assert!(matches!(err, TokenError::Illegal { ch: '.', .. }));
        assert!(err.to_string().contains("BTC.PERP"));
    }

    #[test]
    fn a_dex_prefixed_symbol_cannot_become_a_ticker() {
        // Hyperliquid spells a HIP-3 perp `xyz:XYZ100`. That is a VENUE SYMBOL,
        // resolved to a ticker at the seam; it must never reach a path.
        let err = Ticker::new("xyz:XYZ100").unwrap_err();
        assert!(matches!(err, TokenError::Illegal { ch: ':', .. }));
    }

    #[test]
    fn a_slashed_ticker_cannot_be_constructed() {
        // Hyperliquid spells a spot pair `PURR/USDC`. A `/` in a partition
        // level would silently create a directory.
        assert!(matches!(
            Ticker::new("PURR/USDC").unwrap_err(),
            TokenError::Illegal { ch: '/', .. }
        ));
    }

    #[test]
    fn an_over_long_token_names_its_bound() {
        let err = Ticker::new("x".repeat(MAX_TOKEN + 1)).unwrap_err();
        assert!(matches!(err, TokenError::TooLong { max: MAX_TOKEN, .. }));
        assert!(err.to_string().contains("64"));
        // The bound itself is legal.
        assert!(Ticker::new("x".repeat(MAX_TOKEN)).is_ok());
    }

    #[test]
    fn an_empty_token_is_refused() {
        assert!(matches!(
            Venue::new("").unwrap_err(),
            TokenError::Empty { .. }
        ));
    }

    #[test]
    fn a_refusal_names_which_kind_of_token_it_was() {
        // Three types share one validator, so the message has to carry which
        // one was being built or a config refusal says nothing useful.
        assert!(Venue::new("a b").unwrap_err().to_string().contains("venue"));
        assert!(Ticker::new("a b").unwrap_err().to_string().contains("ticker"));
        assert!(Market::new("a b").unwrap_err().to_string().contains("market"));
    }

    #[test]
    fn there_is_no_constructor_from_a_whole_subject() {
        // Not a runtime assertion — a compile-time one, stated here so the
        // property is written down where somebody would go looking for it.
        // `Ticker` has `new` and `from_str`, both of which validate. Adding an
        // unchecked constructor would make every other check decorative.
        let parsed: Result<Ticker, _> = "BTC.hyperliquid.trades".parse();
        assert!(parsed.is_err());
    }
}
