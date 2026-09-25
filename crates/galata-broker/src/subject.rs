//! How an event is addressed on the bus.
//!
//! **Here rather than in the vocabulary.** A subject is a *bus* concept: the
//! record has none, the tape has none, and a consumer of the vocabulary that
//! never touches a broker should not carry one.
//!
//! **Built from validated types, never from strings.** A `.` in a token would
//! split one subject level into two, and the identity types already refuse
//! one — so an unaddressable subject is unconstructible rather than merely
//! unlikely.

use std::fmt;

use galata_wire::{Kind, Ticker, Venue};

/// A subject a message is published on.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Subject(String);

impl Subject {
    /// One instrument's one dataset: `markets.<venue>.<ticker>.<kind>`.
    pub fn market(venue: &Venue, ticker: &Ticker, kind: Kind) -> Subject {
        Subject(format!(
            "markets.{}.{}.{}",
            venue.as_str(),
            ticker.as_str(),
            kind.as_str()
        ))
    }

    /// What one capture process says about itself: `status.<venue>`.
    ///
    /// **Its own root**, so a subscriber of market data does not receive
    /// status, and a dashboard watching every process subscribes `status.>`
    /// without also receiving the firehose.
    pub fn status(venue: &Venue) -> Subject {
        Subject(format!("status.{}", venue.as_str()))
    }

    /// The subject an envelope belongs on, where it names an instrument.
    ///
    /// `None` for an envelope addressed to a market rather than a venue — it
    /// has no venue and no ticker, and inventing either would publish it
    /// somewhere nothing is listening.
    pub fn of(envelope: &galata_wire::Envelope) -> Option<Subject> {
        Some(Subject::market(
            envelope.venue()?,
            envelope.ticker()?,
            envelope.kind(),
        ))
    }

    /// As the wire wants it.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for Subject {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use galata_wire::{Clipped, Envelope, Event, Gap, GapCause, Series};

    fn venue() -> Venue {
        Venue::new("hyperliquid").unwrap()
    }

    #[test]
    fn a_market_subject_names_its_four_parts() {
        let subject = Subject::market(&venue(), &Ticker::new("BTC").unwrap(), Kind::Quotes);
        assert_eq!(subject.as_str(), "markets.hyperliquid.BTC.quotes");
    }

    #[test]
    fn status_is_its_own_root() {
        // So a market-data subscriber does not receive status, and a dashboard
        // takes `status.>` without also taking the firehose.
        assert_eq!(Subject::status(&venue()).as_str(), "status.hyperliquid");
        assert!(!Subject::status(&venue()).as_str().starts_with("markets."));
    }

    #[test]
    fn no_token_can_hold_a_dot() {
        // Which would split one level into two. The identity types refuse it,
        // so this is unconstructible rather than merely unlikely.
        assert!(Ticker::new("BTC.PERP").is_err());
        assert!(Venue::new("hyper.liquid").is_err());
    }

    #[test]
    fn every_subject_has_exactly_the_levels_it_should() {
        let market = Subject::market(&venue(), &Ticker::new("XYZ100").unwrap(), Kind::Trades);
        assert_eq!(market.as_str().split('.').count(), 4);
        assert_eq!(Subject::status(&venue()).as_str().split('.').count(), 2);
    }

    #[test]
    fn an_envelope_is_addressed_by_what_it_is_about() {
        let envelope = Envelope::new(
            venue(),
            Ticker::new("ETH").unwrap(),
            Some(1),
            2,
            Event::Gap(Gap {
                series: Series::Quotes,
                from_micros: 0,
                to_micros: 1,
                cause: GapCause::SessionLost,
                clipped: Clipped::Continuous,
                dex: None,
            }),
        );
        assert_eq!(
            Subject::of(&envelope).unwrap().as_str(),
            "markets.hyperliquid.ETH.gaps"
        );
    }
}
