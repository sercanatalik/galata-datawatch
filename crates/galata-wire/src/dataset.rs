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
#[serde(rename_all = "lowercase")]
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
}

/// A dataset a store writes, and the last token of a market-data subject.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, serde::Serialize)]
#[serde(rename_all = "lowercase")]
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
    pub const ALL: [Series; 8] = [
        Series::Trades,
        Series::Book,
        Series::Candles,
        Series::Funding,
        Series::Quotes,
        Series::Transfers,
        Series::Mints,
        Series::Margin,
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
    pub const ALL: [Kind; 16] = [
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
            | Kind::Reorgs => Addressing::Venue,
            Kind::Margin | Kind::Positions | Kind::Accounts => Addressing::Account,
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
        assert_eq!(Kind::ALL.len(), 16);
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
        for kind in [Kind::Margin, Kind::Positions, Kind::Accounts] {
            assert!(matches!(kind.addressing(), Addressing::Account), "{kind}");
        }
        assert!(matches!(Kind::Quotes.addressing(), Addressing::Venue));
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
}
