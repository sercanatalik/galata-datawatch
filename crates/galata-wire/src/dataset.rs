//! What a dataset is: the declarable vocabulary, the written vocabulary, and
//! how each is addressed.
//!
//! [`Series`] is deliberately smaller than [`Kind`]. A series is what a
//! configuration may name and what a gap may be *about*; some datasets arrive
//! as a consequence of subscribing to something else — `marks` rides an asset
//! context, `instruments` rides a universe fetch — and a gap in one is not a
//! thing a consumer reasons about.

use std::fmt;
use std::str::FromStr;

/// A series a configuration may declare and a gap may be about.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum Series {
    /// Executions the venue printed.
    Trades,
    /// Price levels, as snapshots or deltas.
    Book,
    /// Bars at a declared interval.
    Candles,
    /// The perpetual funding rate.
    Funding,
    /// Top of book: the best bid and ask, with sizes where the venue states
    /// them. Serves a push `bbo` channel and a polled best-bid-ask alike,
    /// which is what makes a cross-venue comparison one table.
    Quotes,
    /// Value moving between addresses on a chain. Not a trade: a transfer on
    /// its own proves custody moved, not that anything was bought.
    Transfers,
    /// Primary issuance and redemption — a transfer from or to the zero
    /// address. A supply event, which no centralised venue has.
    Mints,
    /// An account's perp margin, one snapshot per dex, **polled by the
    /// ledger**. Its open positions ride along, as marks ride an asset
    /// context: one answer, and a gap in it is a gap in both.
    ///
    /// Declarable by the ledger only. A capture venue refuses it through its
    /// own declaration, since no market-data adapter serves it.
    Margin,
    /// An account's fills, walked by the ledger. What a gap in them is about.
    Fills,
    /// An account's funding payments, walked by the ledger.
    FundingPayments,
    /// An account's ledger updates, walked by the ledger.
    LedgerUpdates,
    /// An account's spot balances, one read per account, **polled by the
    /// ledger**. The portfolio-margin figures stated in the same answer ride
    /// along, so a gap here is a gap in them too.
    Balances,
    /// An account's borrowed and supplied amounts, one read per account,
    /// polled by the ledger. Its health rides along.
    BorrowLend,
    /// A wallet's subaccounts, read whole each snapshot by the ledger: their
    /// margin, with their positions and collateral riding along.
    MarginState,
    /// An account's open orders, read whole at each snapshot by the ledger
    /// where the venue answers them apart (Hyperliquid). Where they ride
    /// another answer (Derive's portfolio), a gap there is a gap in them.
    Orders,
    /// An account's order history, read by the ledger at the events cadence.
    OrderUpdates,
    /// The account-value and PnL history the venue states, read by the ledger
    /// at the events cadence.
    PnlHistory,
}

/// A dataset a store writes, and the last token of a market-data subject.
///
/// `snake_case` rather than `lowercase`: identical for every one-word kind,
/// and the only spelling that agrees with [`Kind::as_str`] for
/// `funding_payments` and `ledger_updates`. Two spellings of one dataset do
/// not fail; they disagree (`serde_spelling_agrees_with_the_partition_name`).
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum Kind {
    /// [`Series::Trades`].
    Trades,
    /// [`Series::Book`].
    Book,
    /// [`Series::Candles`].
    Candles,
    /// [`Series::Funding`].
    Funding,
    /// [`Series::Quotes`].
    Quotes,
    /// [`Series::Transfers`].
    Transfers,
    /// [`Series::Mints`].
    Mints,
    /// Mark, index, oracle and open interest. Four prices, none derivable from
    /// another, each as the venue printed it.
    Marks,
    /// An absence, with the reason it happened.
    Gaps,
    /// A payload that would not normalise. A row here always has bytes behind
    /// it.
    Unparsed,
    /// Trading hours, captured rather than maintained.
    Sessions,
    /// Reference data: tick size, contract type, and the venue's own index.
    Instruments,
    /// A chain reorganisation: rows previously written that the chain no
    /// longer holds. The one absence that can be *proved* rather than
    /// inferred.
    Reorgs,
    /// [`Series::Margin`]: an account's margin, per dex, as the venue stated it.
    Margin,
    /// One open position per row, riding a margin snapshot.
    Positions,
    /// An account the ledger bound: a discovered sub-account's ordinal, its
    /// address fingerprint and the name the venue gave it.
    Accounts,
    /// An account's fills, as the venue recorded them.
    Fills,
    /// Funding the account paid or received, per position and hour.
    FundingPayments,
    /// Deposits, withdrawals, transfers, liquidations and vault moves: the
    /// venue's non-funding ledger updates.
    LedgerUpdates,
    /// A number computed from market data on a schedule: a volatility, a
    /// correlation, a covariance. **Computed, not captured**, so no archive
    /// row stands behind it and no rebuild writes it; it may span venues, so
    /// it names none of them.
    Signals,
    /// What a venue's own model says an option is worth: implied volatility
    /// and greeks, each as the venue printed it. **Captured, never computed**,
    /// and it rides the option's ticker the way marks ride an asset context,
    /// so it is not separately declarable.
    OptionPricing,
    /// [`Series::Balances`]: one coin an account holds in spot per row.
    Balances,
    /// [`Series::BorrowLend`]: what an account borrowed and supplied, per token.
    BorrowLend,
    /// An account's borrowing health, riding a borrow and lend read.
    LendHealth,
    /// Whether an account is portfolio-margined and its ratio, as the venue
    /// stated them, riding a balances read.
    PortfolioMargin,
    /// One token's margin figures for an account, riding a balances read.
    TokenMargin,
    /// [`Series::MarginState`]: a subaccount's margin figures and type.
    MarginState,
    /// One open position of a subaccount, riding a margin-state read.
    PositionState,
    /// One collateral asset of a subaccount, riding a margin-state read.
    Collateral,
    /// [`Series::Orders`]: an account's open orders, a complete set per read.
    Orders,
    /// [`Series::OrderUpdates`]: each change of an order's status, once.
    OrderUpdates,
    /// [`Series::PnlHistory`]: the venue's account-value and PnL points, once.
    PnlHistory,
}

/// The partition level a dataset sits under, above `date=`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, serde::Serialize)]
#[serde(rename_all = "lowercase")]
#[non_exhaustive]
pub enum Addressing {
    /// Bytes a venue sent, dropped whole by venue.
    Venue,
    /// Numbers this system computed about a market, which may span venues and
    /// therefore names none of them.
    Market,
    /// What a venue said about **one account**: under its venue, then its
    /// alias — `venue=<v>/account=<alias>/` — so one account's history is a
    /// subtree that can be retained or dropped whole.
    Account,
}

impl Addressing {
    /// The partition level's key, without its value.
    pub fn key(&self) -> &'static str {
        match self {
            Addressing::Venue => "venue",
            Addressing::Market => "market",
            Addressing::Account => "account",
        }
    }
}

impl Series {
    /// Every series, for a loader that must refuse an unknown one by listing
    /// the known ones.
    pub const ALL: [Series; 17] = [
        Series::Trades,
        Series::Book,
        Series::Candles,
        Series::Funding,
        Series::Quotes,
        Series::Transfers,
        Series::Mints,
        Series::Margin,
        Series::Fills,
        Series::FundingPayments,
        Series::LedgerUpdates,
        Series::Balances,
        Series::BorrowLend,
        Series::MarginState,
        Series::Orders,
        Series::OrderUpdates,
        Series::PnlHistory,
    ];

    /// The discriminator written to disk and to a subject.
    pub fn as_str(&self) -> &'static str {
        match self {
            Series::Trades => "trades",
            Series::Book => "book",
            Series::Candles => "candles",
            Series::Funding => "funding",
            Series::Quotes => "quotes",
            Series::Transfers => "transfers",
            Series::Mints => "mints",
            Series::Margin => "margin",
            Series::Fills => "fills",
            Series::FundingPayments => "funding_payments",
            Series::LedgerUpdates => "ledger_updates",
            Series::Balances => "balances",
            Series::BorrowLend => "borrow_lend",
            Series::MarginState => "margin_state",
            Series::Orders => "orders",
            Series::OrderUpdates => "order_updates",
            Series::PnlHistory => "pnl_history",
        }
    }

    /// The dataset this series' own events land in.
    pub fn kind(&self) -> Kind {
        match self {
            Series::Trades => Kind::Trades,
            Series::Book => Kind::Book,
            Series::Candles => Kind::Candles,
            Series::Funding => Kind::Funding,
            Series::Quotes => Kind::Quotes,
            Series::Transfers => Kind::Transfers,
            Series::Mints => Kind::Mints,
            Series::Margin => Kind::Margin,
            Series::Fills => Kind::Fills,
            Series::FundingPayments => Kind::FundingPayments,
            Series::LedgerUpdates => Kind::LedgerUpdates,
            Series::Balances => Kind::Balances,
            Series::BorrowLend => Kind::BorrowLend,
            Series::MarginState => Kind::MarginState,
            Series::Orders => Kind::Orders,
            Series::OrderUpdates => Kind::OrderUpdates,
            Series::PnlHistory => Kind::PnlHistory,
        }
    }
}

impl fmt::Display for Series {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// **The inverse of [`Series::as_str`], beside it on purpose.** A store that
/// writes the discriminator has to read it back, and a second mapping written
/// somewhere else does not fail when it drifts — it disagrees.
impl FromStr for Series {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Series::ALL
            .into_iter()
            .find(|series| series.as_str() == s)
            .ok_or_else(|| {
                let known: Vec<&str> = Series::ALL.iter().map(|s| s.as_str()).collect();
                format!("{s:?} is not a series. Known: {}", known.join(", "))
            })
    }
}

impl Kind {
    /// Every dataset a store may write.
    ///
    /// **Here rather than in a consumer**, because [`Kind`] is
    /// `#[non_exhaustive]`: outside this crate a `match` needs a catch-all, so
    /// no consumer can be held exhaustive by the compiler. Inside it, adding a
    /// variant without extending this array is a length mismatch and the build
    /// fails — which is the check a consumer cannot have, offered to it as a
    /// list it can iterate.
    pub const ALL: [Kind; 32] = [
        Kind::Trades,
        Kind::Book,
        Kind::Candles,
        Kind::Funding,
        Kind::Quotes,
        Kind::Transfers,
        Kind::Mints,
        Kind::Marks,
        Kind::Gaps,
        Kind::Unparsed,
        Kind::Sessions,
        Kind::Instruments,
        Kind::Reorgs,
        Kind::Margin,
        Kind::Positions,
        Kind::Accounts,
        Kind::Fills,
        Kind::FundingPayments,
        Kind::LedgerUpdates,
        Kind::Signals,
        Kind::OptionPricing,
        Kind::Balances,
        Kind::BorrowLend,
        Kind::LendHealth,
        Kind::PortfolioMargin,
        Kind::TokenMargin,
        Kind::MarginState,
        Kind::PositionState,
        Kind::Collateral,
        Kind::Orders,
        Kind::OrderUpdates,
        Kind::PnlHistory,
    ];

    /// How this dataset is addressed above `date=`.
    ///
    /// Selected here, exhaustively, so a dataset added later cannot inherit a
    /// partition shape nobody chose for it.
    pub fn addressing(&self) -> Addressing {
        match self {
            Kind::Trades
            | Kind::Book
            | Kind::Candles
            | Kind::Funding
            | Kind::Quotes
            | Kind::Transfers
            | Kind::Mints
            | Kind::Marks
            | Kind::Gaps
            | Kind::Unparsed
            | Kind::Sessions
            | Kind::Instruments
            | Kind::Reorgs
            | Kind::OptionPricing => Addressing::Venue,
            Kind::Margin
            | Kind::Positions
            | Kind::Accounts
            | Kind::Fills
            | Kind::FundingPayments
            | Kind::LedgerUpdates
            | Kind::Balances
            | Kind::BorrowLend
            | Kind::LendHealth
            | Kind::PortfolioMargin
            | Kind::TokenMargin
            | Kind::MarginState
            | Kind::PositionState
            | Kind::Collateral
            | Kind::Orders
            | Kind::OrderUpdates
            | Kind::PnlHistory => Addressing::Account,
            Kind::Signals => Addressing::Market,
        }
    }

    /// Whether an account's rows of this kind are **state**, read whole and
    /// published as one set per read, rather than events, each published once
    /// (`add-realtime-account-ledger`, design D3).
    ///
    /// Exhaustive, as [`Kind::addressing`] is, so a kind added later decides.
    pub fn is_state(&self) -> bool {
        match self {
            Kind::Margin
            | Kind::Positions
            | Kind::Balances
            | Kind::BorrowLend
            | Kind::LendHealth
            | Kind::PortfolioMargin
            | Kind::TokenMargin
            | Kind::MarginState
            | Kind::PositionState
            | Kind::Collateral
            | Kind::Orders => true,
            Kind::Fills
            | Kind::FundingPayments
            | Kind::LedgerUpdates
            | Kind::OrderUpdates
            | Kind::PnlHistory
            | Kind::Accounts
            | Kind::Trades
            | Kind::Book
            | Kind::Candles
            | Kind::Funding
            | Kind::Quotes
            | Kind::Transfers
            | Kind::Mints
            | Kind::Marks
            | Kind::Gaps
            | Kind::Unparsed
            | Kind::Sessions
            | Kind::Instruments
            | Kind::Reorgs
            | Kind::Signals
            | Kind::OptionPricing => false,
        }
    }

    /// The discriminator written to disk and to a subject.
    pub fn as_str(&self) -> &'static str {
        match self {
            Kind::Trades => "trades",
            Kind::Book => "book",
            Kind::Candles => "candles",
            Kind::Funding => "funding",
            Kind::Quotes => "quotes",
            Kind::Transfers => "transfers",
            Kind::Mints => "mints",
            Kind::Marks => "marks",
            Kind::Gaps => "gaps",
            Kind::Unparsed => "unparsed",
            Kind::Sessions => "sessions",
            Kind::Instruments => "instruments",
            Kind::Reorgs => "reorgs",
            Kind::Margin => "margin",
            Kind::Positions => "positions",
            Kind::Accounts => "accounts",
            Kind::Fills => "fills",
            Kind::FundingPayments => "funding_payments",
            Kind::LedgerUpdates => "ledger_updates",
            Kind::Signals => "signals",
            Kind::OptionPricing => "option_pricing",
            Kind::Balances => "balances",
            Kind::BorrowLend => "borrow_lend",
            Kind::LendHealth => "lend_health",
            Kind::PortfolioMargin => "portfolio_margin",
            Kind::TokenMargin => "token_margin",
            Kind::MarginState => "margin_state",
            Kind::PositionState => "position_state",
            Kind::Collateral => "collateral",
            Kind::Orders => "orders",
            Kind::OrderUpdates => "order_updates",
            Kind::PnlHistory => "pnl_history",
        }
    }
}

impl fmt::Display for Kind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for Kind {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Kind::ALL
            .into_iter()
            .find(|kind| kind.as_str() == s)
            .ok_or_else(|| {
                let known: Vec<&str> = Kind::ALL.iter().map(|k| k.as_str()).collect();
                format!("{s:?} is not a dataset. Known: {}", known.join(", "))
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_kind_states_its_addressing() {
        // Not a smoke test: `addressing()` is an exhaustive match, so this
        // fails to COMPILE if a variant is added without deciding where it
        // partitions. The assertion below only proves the match is total at
        // runtime too.
        for kind in Kind::ALL {
            let _ = kind.addressing().key();
        }
        assert_eq!(Kind::ALL.len(), 32);
    }

    #[test]
    fn a_state_kind_is_an_accounts() {
        // State is what one read of an account holds: never market data.
        for kind in Kind::ALL.into_iter().filter(Kind::is_state) {
            assert!(matches!(kind.addressing(), Addressing::Account), "{kind}");
        }
        for kind in [
            Kind::Fills,
            Kind::OrderUpdates,
            Kind::FundingPayments,
            Kind::PnlHistory,
        ] {
            assert!(!kind.is_state(), "{kind} is an event");
        }
    }

    #[test]
    fn every_series_has_a_dataset_to_land_in() {
        for series in Series::ALL {
            assert!(Kind::ALL.contains(&series.kind()), "{series} lands nowhere");
        }
    }

    #[test]
    fn series_is_smaller_than_kind() {
        // Deliberate. `marks`, `gaps`, `unparsed`, `sessions`, `instruments`
        // and `reorgs` are written and are not separately declarable, because
        // they arrive as a consequence of something else and a gap in one is
        // not a thing a consumer reasons about.
        assert!(Series::ALL.len() < Kind::ALL.len());
        let declarable: Vec<Kind> = Series::ALL.iter().map(|s| s.kind()).collect();
        for kind in [
            Kind::Marks,
            Kind::Gaps,
            Kind::Unparsed,
            Kind::Reorgs,
            Kind::Positions,
            Kind::Accounts,
            Kind::Signals,
            Kind::OptionPricing,
            Kind::LendHealth,
            Kind::PortfolioMargin,
            Kind::TokenMargin,
            Kind::PositionState,
            Kind::Collateral,
        ] {
            assert!(!declarable.contains(&kind), "{kind} must not be declarable");
        }
    }

    #[test]
    fn a_discriminator_round_trips_both_ways() {
        // The store writes `as_str` and reads `from_str`. Two mappings that
        // drift do not fail — they disagree, and the disagreement is a
        // partition nobody can find.
        for series in Series::ALL {
            assert_eq!(series.as_str().parse::<Series>().unwrap(), series);
        }
        for kind in Kind::ALL {
            assert_eq!(kind.as_str().parse::<Kind>().unwrap(), kind);
        }
    }

    #[test]
    fn an_unknown_series_is_refused_by_listing_the_known_ones() {
        let err = "orderbook".parse::<Series>().unwrap_err();
        assert!(err.contains("orderbook"));
        assert!(err.contains("book"), "the refusal must list what IS known");
    }

    #[test]
    fn the_new_venues_vocabulary_is_present() {
        // quotes serves Hyperliquid's bbo and Robinhood's best_bid_ask alike;
        // transfers and mints are Robinhood Chain's. Added now because a
        // dataset that arrives later cannot retro-name rows already written.
        assert!(Series::ALL.contains(&Series::Quotes));
        assert!(Series::ALL.contains(&Series::Transfers));
        assert!(Series::ALL.contains(&Series::Mints));
        assert!(Kind::ALL.contains(&Kind::Reorgs));
    }

    #[test]
    fn an_accounts_datasets_sit_under_the_account() {
        // An account's history must be one subtree, so that it can be retained
        // or dropped without touching another account's.
        for kind in [
            Kind::Margin,
            Kind::Positions,
            Kind::Accounts,
            Kind::Balances,
            Kind::BorrowLend,
            Kind::LendHealth,
            Kind::PortfolioMargin,
            Kind::TokenMargin,
            Kind::MarginState,
            Kind::PositionState,
            Kind::Collateral,
            Kind::Orders,
            Kind::OrderUpdates,
            Kind::PnlHistory,
        ] {
            assert!(matches!(kind.addressing(), Addressing::Account), "{kind}");
        }
        assert!(matches!(Kind::Quotes.addressing(), Addressing::Venue));
    }

    #[test]
    fn a_series_is_spelled_as_its_dataset_is() {
        // What a configuration declares is parsed by serde, and a gap names
        // its series by `as_str`; the two must agree for `funding_payments`.
        for series in Series::ALL {
            let json = serde_json::to_string(&series).unwrap();
            assert_eq!(json, format!("\"{}\"", series.as_str()), "{series:?}");
        }
    }

    #[test]
    fn serde_spelling_agrees_with_the_partition_name() {
        for kind in Kind::ALL {
            let json = serde_json::to_string(&kind).unwrap();
            assert_eq!(json, format!("\"{}\"", kind.as_str()), "{kind:?}");
            assert_eq!(serde_json::from_str::<Kind>(&json).unwrap(), kind);
        }
    }

    #[test]
    fn every_events_kind_sits_under_the_account() {
        for kind in [Kind::Fills, Kind::FundingPayments, Kind::LedgerUpdates] {
            assert!(matches!(kind.addressing(), Addressing::Account), "{kind}");
        }
    }

    #[test]
    fn every_kind_is_in_all_and_round_trips() {
        // The length is checked by the compiler; that each entry is DISTINCT
        // and parses back is checked here, because a copy-paste duplicate would
        // satisfy the length and silently drop a dataset.
        let mut seen = std::collections::BTreeSet::new();
        for kind in Kind::ALL {
            assert!(
                seen.insert(kind.as_str()),
                "{kind} appears twice in Kind::ALL"
            );
            assert_eq!(kind.as_str().parse::<Kind>(), Ok(kind));
        }
        assert_eq!(seen.len(), Kind::ALL.len());
    }

    #[test]
    fn the_signals_spelling_round_trips_and_is_addressed_by_market() {
        // Computed about a market, which may span venues: the one dataset
        // under the `market` level, reserved for it before it existed.
        assert_eq!("signals".parse::<Kind>(), Ok(Kind::Signals));
        assert_eq!(Kind::Signals.as_str(), "signals");
        assert_eq!(Kind::Signals.addressing().key(), "market");
        let market: Vec<Kind> = Kind::ALL
            .into_iter()
            .filter(|k| matches!(k.addressing(), Addressing::Market))
            .collect();
        assert_eq!(market, vec![Kind::Signals]);
    }
}
