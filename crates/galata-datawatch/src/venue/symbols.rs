//! Per-channel symbol resolution to **one ticker per venue**.
//!
//! A venue may call the same instrument one thing on its trade channel and
//! another on its reference channel. Both resolve here, once, and no consumer
//! sees either string.
//!
//! It is also what keeps a venue's spelling off the filesystem. Hyperliquid
//! spells a builder-deployed perp `xyz:XYZ100`, and a [`Ticker`] may not hold a
//! `:` — so the ticker is `XYZ100`, the venue symbol is `xyz:XYZ100`, and the
//! prefix is composed here rather than carried everywhere.

use std::collections::BTreeMap;

use galata_wire::{Ticker, TokenError};

/// The venue's own strings, per channel, and the one ticker they mean.
#[derive(Debug, Clone, Default)]
pub struct Symbols {
    /// `(channel, venue symbol) -> ticker`.
    by_channel: BTreeMap<(String, String), Ticker>,
    /// The fallback: a venue symbol that means the same on every channel.
    everywhere: BTreeMap<String, Ticker>,
}

impl Symbols {
    /// An empty resolver.
    pub fn new() -> Symbols {
        Symbols::default()
    }

    /// A venue symbol that means one ticker on every channel — the ordinary
    /// case.
    pub fn everywhere(&mut self, venue_symbol: &str, ticker: &str) -> Result<(), TokenError> {
        self.everywhere
            .insert(venue_symbol.to_string(), Ticker::new(ticker)?);
        Ok(())
    }

    /// A venue symbol that means one ticker on one channel only.
    pub fn on_channel(
        &mut self,
        channel: &str,
        venue_symbol: &str,
        ticker: &str,
    ) -> Result<(), TokenError> {
        self.by_channel.insert(
            (channel.to_string(), venue_symbol.to_string()),
            Ticker::new(ticker)?,
        );
        Ok(())
    }

    /// The one ticker a venue symbol means on a channel.
    ///
    /// A channel-specific mapping wins over the general one, because that is
    /// the case it exists for.
    pub fn resolve(&self, channel: &str, venue_symbol: &str) -> Option<&Ticker> {
        self.by_channel
            .get(&(channel.to_string(), venue_symbol.to_string()))
            .or_else(|| self.everywhere.get(venue_symbol))
    }

    /// The venue's own string for a ticker, which is what a subscribe frame
    /// carries.
    pub fn venue_symbol_for(&self, ticker: &Ticker) -> Option<&str> {
        self.everywhere
            .iter()
            .find(|(_, t)| *t == ticker)
            .map(|(s, _)| s.as_str())
    }

    /// Every venue symbol declared.
    pub fn venue_symbols(&self) -> Vec<&str> {
        self.everywhere.keys().map(String::as_str).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn two_venue_strings_resolve_to_one_ticker() {
        let mut symbols = Symbols::new();
        symbols.everywhere("PI_XBTUSD", "PI_XBTUSD").unwrap();
        symbols
            .on_channel("instrument", "pi_xbtusd", "PI_XBTUSD")
            .unwrap();

        assert_eq!(
            symbols.resolve("trade", "PI_XBTUSD"),
            symbols.resolve("instrument", "pi_xbtusd")
        );
    }

    #[test]
    fn a_dex_prefixed_symbol_yields_an_unprefixed_ticker() {
        // The `:` is refused by Ticker, which is right for an unrelated reason:
        // a `:` in a partition level is a path that means something else.
        let mut symbols = Symbols::new();
        symbols.everywhere("xyz:XYZ100", "XYZ100").unwrap();

        let ticker = symbols.resolve("l2Book", "xyz:XYZ100").unwrap();
        assert_eq!(ticker.as_str(), "XYZ100");
        assert_eq!(
            symbols.venue_symbol_for(ticker),
            Some("xyz:XYZ100"),
            "and the prefixed form is what goes back on the wire"
        );
    }

    #[test]
    fn an_unknown_symbol_resolves_to_nothing() {
        // Never a ticker invented from the string: an unrecognised symbol is a
        // configuration this venue was not told about, and inventing one would
        // start a partition nobody declared.
        assert!(Symbols::new().resolve("trades", "NOPE").is_none());
    }

    #[test]
    fn a_channel_specific_mapping_wins() {
        let mut symbols = Symbols::new();
        symbols.everywhere("BTC", "BTC").unwrap();
        symbols.on_channel("weird", "BTC", "BTC-ALT").unwrap();

        assert_eq!(symbols.resolve("trades", "BTC").unwrap().as_str(), "BTC");
        assert_eq!(symbols.resolve("weird", "BTC").unwrap().as_str(), "BTC-ALT");
    }

    #[test]
    fn a_ticker_a_path_cannot_hold_is_refused_here() {
        let mut symbols = Symbols::new();
        assert!(symbols.everywhere("PURR/USDC", "PURR/USDC").is_err());
    }
}
