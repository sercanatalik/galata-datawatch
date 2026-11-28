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

use galata_wire::{Account, Kind, Ticker, Venue};

/// A subject a message is published on.
///
/// **Held as the client's own subject type**, which is a shared byte buffer:
/// publishing hands the client a reference-counted clone, where a `String`
/// had to be copied into a fresh one for every message — the subject built
/// twice to publish once.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Subject(async_nats::Subject);

impl std::hash::Hash for Subject {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.as_str().hash(state);
    }
}

impl Subject {
    /// One instrument's one dataset: `markets.<venue>.<ticker>.<kind>`.
    pub fn market(venue: &Venue, ticker: &Ticker, kind: Kind) -> Subject {
        Subject(
            format!(
                "markets.{}.{}.{}",
                venue.as_str(),
                ticker.as_str(),
                kind.as_str()
            )
            .into(),
        )
    }

    /// What one capture process says about itself: `status.<venue>`.
    ///
    /// **Its own root**, so a subscriber of market data does not receive
    /// status, and a dashboard watching every process subscribes `status.>`
    /// without also receiving the firehose.
    pub fn status(venue: &Venue) -> Subject {
        Subject(format!("status.{}", venue.as_str()).into())
    }

    /// What a venue said about one account: `accounts.<venue>.<alias>.<kind>`.
    ///
    /// **Its own root**, kept apart from market data by the broker's streams:
    /// account data is complete and kept for days, market data a lossy window
    /// of hours. **By alias, never by address**: `None` for an alias shaped
    /// like an EVM address (`0x` and forty hex digits), because the token
    /// itself cannot refuse one — `0x…` is alphanumeric — and a subject is
    /// read by everything that can subscribe.
    pub fn account(venue: &Venue, account: &Account, kind: Kind) -> Option<Subject> {
        let alias = account.as_str();
        let address_shaped = alias.len() == 42
            && (alias.starts_with("0x") || alias.starts_with("0X"))
            && alias[2..].chars().all(|c| c.is_ascii_hexdigit());
        if address_shaped {
            return None;
        }
        Some(Subject(
            format!("accounts.{}.{}.{}", venue.as_str(), alias, kind.as_str()).into(),
        ))
    }

    /// The subject an envelope belongs on: an instrument's, or an account's.
    ///
    /// `None` for an envelope addressed to a market rather than a venue — it
    /// has no venue and no ticker, and inventing either would publish it
    /// somewhere nothing is listening — and for an account whose alias is
    /// shaped like an address ([`Subject::account`]).
    pub fn of(envelope: &galata_wire::Envelope) -> Option<Subject> {
        let venue = envelope.venue()?;
        if let Some(account) = envelope.account() {
            return Subject::account(venue, account, envelope.kind());
        }
        Some(Subject::market(venue, envelope.ticker()?, envelope.kind()))
    }

    /// As the wire wants it.
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }

    /// The client's subject, shared rather than copied.
    pub fn to_nats(&self) -> async_nats::Subject {
        self.0.clone()
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
    use galata_wire::{AccountMode, Clipped, Envelope, Event, Gap, GapCause, Margin, Series};

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

    fn margin() -> Event {
        Event::Margin(Margin {
            dex: None,
            mode: AccountMode::Disabled,
            equity_not_held: None,
            account_value: None,
            total_notional: None,
            total_raw_usd: None,
            margin_used: None,
            maintenance_margin_used: None,
            withdrawable: None,
        })
    }

    #[test]
    fn an_account_is_addressed_by_its_alias_under_its_own_root() {
        let envelope =
            Envelope::for_account(venue(), Account::new("main").unwrap(), None, 1, margin());
        let subject = Subject::of(&envelope).unwrap();
        assert_eq!(subject.as_str(), "accounts.hyperliquid.main.margin");
        assert!(!subject.as_str().starts_with("markets."));
        assert_eq!(subject.as_str().split('.').count(), 4);
    }

    #[test]
    fn the_order_and_pnl_kinds_are_addressed_by_alias() {
        // Each kind the order vocabulary adds is a subject of its account's,
        // under the accounts root, spelled as its dataset is.
        let derive = Venue::new("derive-xyz").unwrap();
        let alias = Account::new("owner_s3").unwrap();
        for (kind, last) in [
            (Kind::Orders, "orders"),
            (Kind::OrderUpdates, "order_updates"),
            (Kind::PnlHistory, "pnl_history"),
        ] {
            let subject = Subject::account(&derive, &alias, kind).unwrap();
            assert_eq!(
                subject.as_str(),
                format!("accounts.derive-xyz.owner_s3.{last}")
            );
        }
    }

    #[test]
    fn an_address_where_an_alias_belongs_forms_no_subject() {
        // The token accepts it — `0x…` is alphanumeric — so the subject is
        // the last place it can be stopped before every subscriber reads it.
        let address = "0x3f9aa0c1d2e3f4a5b6c7d8e9f0a1b2c3d4e5f6a7";
        let envelope =
            Envelope::for_account(venue(), Account::new(address).unwrap(), None, 1, margin());
        assert_eq!(Subject::of(&envelope), None);
        // A discovered sub-account's alias is still an alias.
        let sub =
            Envelope::for_account(venue(), Account::new("main_s1").unwrap(), None, 1, margin());
        assert_eq!(
            Subject::of(&sub).unwrap().as_str(),
            "accounts.hyperliquid.main_s1.margin"
        );
    }
}
