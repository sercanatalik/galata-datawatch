//! Which adapter a venue name means.
//!
//! **This is the only module in the crate that names a venue**, which is what
//! `scripts/check-venue-boundary.sh` keeps honest: everything above the seam
//! holds a `dyn Adapter` and cannot tell two venues apart.
//!
//! A venue name appearing elsewhere is a branch that will be wrong for the next
//! venue, and it will be wrong quietly.

#[cfg(feature = "hyperliquid")]
pub mod hyperliquid;

use crate::venue::{Adapter, ConstructError};

/// Every venue this build can speak to.
///
/// A name absent here is one whose feature is off — which is a configuration a
/// build refuses rather than a venue it silently skips.
pub fn known() -> Vec<&'static str> {
    // A cfg on each element rather than a conditional push: the list is a
    // literal, so what this build speaks is readable at a glance.
    [
        #[cfg(feature = "hyperliquid")]
        hyperliquid::VENUE,
    ]
    .to_vec()
}

/// Why a venue could not be resolved.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ResolveError {
    /// No adapter answers to this name in this build.
    #[error("{name:?} is not a venue this build implements. Compiled in: {known}")]
    Unknown {
        /// The name asked for.
        name: String,
        /// What is available, so a refusal says what to do next.
        known: String,
    },
    /// The adapter refused its configuration.
    #[error("{0}")]
    Construct(#[from] ConstructError),
    /// The configuration names something the venue does not list, or the
    /// listing could not be read.
    #[error("{detail}")]
    Universe {
        /// What was wrong.
        detail: String,
    },
}

/// What an adapter is built from, venue-neutrally.
///
/// One variant per venue. Adding a venue adds a variant here and nowhere else,
/// which is what makes the boundary checkable.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum AdapterConfig {
    /// Hyperliquid.
    #[cfg(feature = "hyperliquid")]
    Hyperliquid(hyperliquid::Config),
}

/// Whether a venue supplies a series, by either route.
///
/// Answered from the adapter's own declaration rather than from a second list
/// here: a second implementation of what a venue serves does not fail, it
/// disagrees.
pub fn supplies(venue: &str, series: galata_wire::Series) -> bool {
    match venue {
        #[cfg(feature = "hyperliquid")]
        hyperliquid::VENUE => declaration_of_hyperliquid()
            .map(|d| d.supplies(series))
            .unwrap_or(false),
        _ => false,
    }
}

#[cfg(feature = "hyperliquid")]
fn declaration_of_hyperliquid() -> Option<crate::venue::Declaration> {
    use crate::venue::{Adapter, Construct};
    // A placeholder instrument: what a venue serves and pages does not depend
    // on which instruments are asked for.
    hyperliquid::Hyperliquid::new(
        hyperliquid::Config {
            market: hyperliquid::Market::Mainnet,
            instruments: vec![hyperliquid::Instrument::main("BTC")],
            candle_interval: "1m".into(),
        },
        None,
    )
    .ok()
    .map(|a| a.declaration().clone())
}

impl AdapterConfig {
    /// The adapter configuration a declared venue block means.
    ///
    /// **The only place a venue's name becomes a variant**, which is what keeps
    /// the boundary checkable.
    pub fn from_declared(
        name: &str,
        venue: &crate::config::VenueConfig,
    ) -> Result<AdapterConfig, ResolveError> {
        match name {
            #[cfg(feature = "hyperliquid")]
            hyperliquid::VENUE => Ok(AdapterConfig::Hyperliquid(hyperliquid::Config {
                market: hyperliquid::Market::parse(&venue.market)
                    .map_err(ResolveError::Construct)?,
                instruments: venue
                    .instruments
                    .iter()
                    .map(|i| hyperliquid::Instrument {
                        ticker: i.ticker.clone(),
                        dex: i.dex.clone(),
                    })
                    .collect(),
                candle_interval: venue.candle.clone(),
            })),
            other => Err(ResolveError::Unknown {
                name: other.to_string(),
                known: known().join(", "),
            }),
        }
    }
}

/// Check a configuration's declared instruments against what the venue lists,
/// **before anything connects**.
///
/// This lives here rather than in the binary because it is the one place
/// permitted to name a venue — and it is not on the [`Adapter`] trait because
/// that seam's whole claim is that it has no network and needs no runtime.
///
/// **Measured 2026-09-20:** an unlisted coin is answered by a hang-up rather
/// than a refusal, and it takes every other subscription on the socket with
/// it. One request per dex removes the whole failure mode.
pub async fn check_universe(config: &AdapterConfig) -> Result<(), ResolveError> {
    match config {
        #[cfg(feature = "hyperliquid")]
        AdapterConfig::Hyperliquid(c) => {
            use crate::venue::{Construct, universe};
            let adapter = hyperliquid::Hyperliquid::new(c.clone(), None)?;
            let client = adapter.client();
            for dex in adapter.dexes() {
                let listed = client
                    .universe(&dex)
                    .await
                    .map_err(|e| ResolveError::Universe {
                        detail: e.to_string(),
                    })?;
                let declared: Vec<String> = adapter
                    .instruments()
                    .iter()
                    .filter(|i| i.dex.clone().unwrap_or_default() == dex)
                    .map(|i| i.venue_symbol())
                    .collect();
                universe::check(hyperliquid::VENUE, &dex, &declared, &listed).map_err(|e| {
                    ResolveError::Universe {
                        detail: e.to_string(),
                    }
                })?;
            }
            Ok(())
        }
    }
}

/// Build the adapter a configuration names.
pub fn build(config: AdapterConfig) -> Result<Box<dyn Adapter>, ResolveError> {
    use crate::venue::Construct;
    match config {
        #[cfg(feature = "hyperliquid")]
        AdapterConfig::Hyperliquid(c) => {
            Ok(Box::new(hyperliquid::Hyperliquid::new(c, None)?) as Box<dyn Adapter>)
        }
    }
}
