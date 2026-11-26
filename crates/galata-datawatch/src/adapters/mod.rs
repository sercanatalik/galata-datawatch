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

/// Robinhood Chain. The decoder is pure and unconditional; nothing here yet
/// makes a request.
#[cfg(feature = "rh-chain")]
pub mod rh_chain;

/// Robinhood Crypto. Behind the `rh-crypto` feature, which carries the
/// signing dependencies a tape reader has no use for.
#[cfg(feature = "rh-crypto")]
pub mod rh_crypto;

/// Derive: options, from public data. Behind the `derive-xyz` feature.
#[cfg(feature = "derive-xyz")]
pub mod derive_xyz;

#[cfg(feature = "capture")]
use galata_wire::Series;

#[cfg(feature = "capture")]
use crate::capture::Fetch;
#[cfg(feature = "capture")]
use crate::record::Payload;
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
        #[cfg(feature = "rh-chain")]
        rh_chain::VENUE,
        #[cfg(feature = "rh-crypto")]
        rh_crypto::VENUE,
        #[cfg(feature = "derive-xyz")]
        derive_xyz::VENUE,
    ]
    .to_vec()
}

/// What a boot check learned from a venue, for the boot to act on.
///
/// Empty for a venue checked against a listing, which is not a thing the
/// record keeps.
#[cfg(feature = "capture")]
#[derive(Debug, Default)]
pub struct Universe {
    /// The reference answers the check read: the venue's bytes, which the
    /// boot takes through the one path so the record holds what the check
    /// was decided on.
    pub pages: Vec<Payload>,
    /// Instruments the venue does not have, that capture would otherwise
    /// subscribe to because they follow from the declared set, each with the
    /// reason in plain words. The boot retires them before the first
    /// subscribe.
    pub absent: Vec<(galata_wire::Ticker, String)>,
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
    /// A named secret could not be read.
    ///
    /// **The variable's name is in the error; its value never is** — the whole
    /// reason `Secret` exists.
    #[error("{0}")]
    Secret(crate::config::ConfigError),
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
    /// Robinhood Chain.
    #[cfg(feature = "rh-chain")]
    RhChain(rh_chain::Config),
    /// Robinhood Crypto.
    #[cfg(feature = "rh-crypto")]
    RhCrypto(rh_crypto::Config),
    /// Derive options.
    #[cfg(feature = "derive-xyz")]
    DeriveXyz(derive_xyz::Config),
}

/// Whether a venue supplies a series, by either route.
///
/// Answered from the adapter's own declaration rather than from a second list
/// here: a second implementation of what a venue serves does not fail, it
/// disagrees.
// A build with no adapter feature has no arms here, so the parameters are
// genuinely unused. That build is legitimate — it is what a tape reader takes —
// and the alternative to naming it is a lint nobody can satisfy.
#[cfg_attr(not(feature = "hyperliquid"), allow(unused_variables))]
pub fn supplies(venue: &str, series: galata_wire::Series) -> bool {
    match venue {
        #[cfg(feature = "hyperliquid")]
        hyperliquid::VENUE => declaration_of_hyperliquid()
            .map(|d| d.supplies(series))
            .unwrap_or(false),
        #[cfg(feature = "rh-chain")]
        rh_chain::VENUE => matches!(
            series,
            galata_wire::Series::Transfers | galata_wire::Series::Mints
        ),
        // The venue serves one thing: the current top of book, when asked.
        #[cfg(feature = "rh-crypto")]
        rh_crypto::VENUE => series == galata_wire::Series::Quotes,
        // One channel carries an option's quote, marks and pricing, and
        // `quotes` is the series that subscribes it. **Not `trades`**: the
        // venue has the channel and this capture does not take it, so a
        // configuration naming it is refused rather than accepted and ignored.
        #[cfg(feature = "derive-xyz")]
        derive_xyz::VENUE => series == galata_wire::Series::Quotes,
        _ => false,
    }
}

/// The most instruments one capture of a venue may be declared, where the
/// venue states a cap. See [`Declaration::max_instruments`].
///
/// [`Declaration::max_instruments`]: crate::venue::Declaration::max_instruments
#[cfg_attr(not(feature = "derive-xyz"), allow(unused_variables))]
pub fn max_instruments(venue: &str) -> Option<u32> {
    match venue {
        #[cfg(feature = "derive-xyz")]
        derive_xyz::VENUE => Some(derive_xyz::MAX_INSTRUMENTS),
        _ => None,
    }
}

/// What is wrong with a declared instrument's name, where the venue's names
/// have a form a configuration can be checked against without asking it.
///
/// `None` for a name that fits, and for every venue whose names have no such
/// form: whether one of those exists is the venue's to say, at boot.
#[cfg_attr(not(feature = "derive-xyz"), allow(unused_variables))]
pub fn instrument_problem(venue: &str, ticker: &str) -> Option<String> {
    match venue {
        #[cfg(feature = "derive-xyz")]
        derive_xyz::VENUE => derive_xyz::OptionName::parse(ticker).err(),
        _ => None,
    }
}

/// What a venue's ledger costs, in the venue's own request weight — **where
/// this build keeps a ledger for it**. `None` is *no ledger here*, which the
/// configuration refuses by name.
///
/// Hyperliquid's figures were read from its documentation on 2026-09-25
/// (`design/measured.md`, *what Hyperliquid says about an account*): 1,200
/// weight a minute **per IP**, shared with capture's walk; `clearinghouseState`
/// 2; `subAccounts` and `userAbstraction` 20, as *"all other documented info
/// requests"* — the second is not documented at all, so 20 is the documented
/// default assumed, not a figure stated for it.
#[cfg_attr(
    not(all(feature = "hyperliquid", feature = "ledger")),
    allow(unused_variables)
)]
pub fn ledger_cost(venue: &str) -> Option<crate::config::LedgerCost> {
    match venue {
        #[cfg(all(feature = "hyperliquid", feature = "ledger"))]
        hyperliquid::VENUE => Some(crate::config::LedgerCost {
            budget_per_minute: 1_200.0,
            snapshot: 2.0,
            discovery: 20.0,
            mode: 20.0,
            // `userFillsByTime`, `userFunding`, `userNonFundingLedgerUpdates`:
            // 20 each, as "all other documented info requests", plus 1 per
            // 20 items for fills and funding (read 2026-09-25).
            events: 20.0,
            // `spotClearinghouseState` weighs 2; `borrowLendUserState` and
            // `spotMeta` 20, as other documented info requests (read
            // 2026-11-26).
            balances: 2.0,
            borrow_lend: 20.0,
            reference: 20.0,
        }),
        _ => None,
    }
}

/// What a ledger run is handed, whichever venue it is for.
#[cfg(all(feature = "capture", feature = "ledger"))]
pub struct LedgerParts {
    /// The ledger root's archive.
    pub archive: crate::record::Archive,
    /// Where events go.
    pub sink: Box<dyn crate::sink::Sink>,
    /// The fingerprint key.
    pub key: crate::ledger::FingerprintKey,
    /// How often.
    pub cadences: crate::ledger::run::Cadences,
    /// The declared masters on this venue, resolved and checked.
    pub masters: Vec<crate::ledger::ResolvedAccount>,
    /// The bindings, read back from the ledger root.
    pub bindings: crate::ledger::Bindings,
    /// Where the status surface is written.
    pub status: crate::capture::StatusFile,
    /// Where the fold's report is written, and its tolerances.
    pub fold: (crate::capture::StatusFile, crate::ledger::fold::Tolerances),
    /// Where the ledger is projected, if anywhere (`ledger.tape`).
    pub projection: Option<std::path::PathBuf>,
}

/// Run a venue's ledger until cancelled.
///
/// **Here because it names the venue**: the loop is generic over the venue's
/// [`AccountVenue`](crate::ledger::run::AccountVenue), and choosing which one
/// is the one decision `check-venue-boundary.sh` keeps in this module.
#[cfg(all(feature = "capture", feature = "ledger"))]
#[cfg_attr(not(feature = "hyperliquid"), allow(unused_variables))]
pub async fn run_ledger(
    venue: &str,
    market: &str,
    parts: LedgerParts,
    shutdown: tokio_util::sync::CancellationToken,
) -> Result<(), Box<dyn std::error::Error>> {
    match venue {
        #[cfg(feature = "hyperliquid")]
        hyperliquid::VENUE => {
            let accounts = hyperliquid::accounts::HyperliquidAccounts::new(
                hyperliquid::Market::parse(market)?.rest_url(),
            )?
            .with_key(parts.key.clone());
            let mut run = crate::ledger::run::LedgerRun::new(
                accounts,
                crate::capture::SystemClock,
                parts.archive,
                parts.sink,
                parts.key,
                parts.cadences,
                parts.masters,
                parts.bindings,
            )
            .with_status_file(parts.status)
            .with_fold(parts.fold.0, parts.fold.1)
            .with_projection(parts.projection);
            run.run(shutdown).await?;
            Ok(())
        }
        other => Err(format!("{other} keeps no ledger in this build").into()),
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

/// Whether resolution may read secrets, or must withhold what needs one.
///
/// Only a keyed venue reads the source: rh-chain's provider key and
/// rh-crypto's API keys. Without either compiled in — the tower's build, or
/// galata-markets' with only the public venues' normalisers — the field is
/// allowed to go unread, and nowhere else.
#[derive(Clone, Copy)]
#[cfg_attr(
    not(any(feature = "rh-chain", feature = "rh-crypto")),
    allow(dead_code)
)]
enum Secrets<'a> {
    /// Capture: every declared secret is read, and an unset one refuses.
    Resolve(&'a dyn crate::config::SecretSource),
    /// Replay: nothing is read. A held endpoint is withheld.
    Withhold,
}

impl AdapterConfig {
    /// The adapter configuration a declared venue block means, **for a tool
    /// that connects** — every declared secret resolved, an unset one refused.
    ///
    /// **The only place a venue's name becomes a variant**, which is what keeps
    /// the boundary checkable.
    pub fn from_declared(
        name: &str,
        venue: &crate::config::VenueConfig,
        secrets: &dyn crate::config::SecretSource,
    ) -> Result<AdapterConfig, ResolveError> {
        AdapterConfig::resolve(name, venue, Secrets::Resolve(secrets))
    }

    /// The same, **for a tool that does not connect**: a replay normalising
    /// archived bytes, where the adapter is built for `normalise` alone.
    ///
    /// Takes no secret source at all, so none can be reached. An endpoint the
    /// configuration declares as held is [`crate::venue::Endpoint::Withheld`]:
    /// named, not resolved, and never the public node in its place. Every
    /// refusal about instruments, contracts and decimals is the same one
    /// capture makes — they are written once, in `resolve`.
    pub fn for_replay(
        name: &str,
        venue: &crate::config::VenueConfig,
    ) -> Result<AdapterConfig, ResolveError> {
        AdapterConfig::resolve(name, venue, Secrets::Withhold)
    }

    #[cfg_attr(not(feature = "hyperliquid"), allow(unused_variables))]
    #[allow(unused_variables)]
    fn resolve(
        name: &str,
        venue: &crate::config::VenueConfig,
        secrets: Secrets<'_>,
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
                // **Required here and nowhere else.** A venue that pushes no
                // bars has no width to declare, so the field is optional in
                // the configuration and this venue, which does, refuses its
                // absence by name.
                candle_interval: venue.candle.clone().ok_or_else(|| ResolveError::Unknown {
                    name: format!(
                        "{name} declares no `candle`. This venue pushes bars, and the width it \
                         is subscribed at is declared rather than defaulted"
                    ),
                    known: known().join(", "),
                })?,
            })),
            #[cfg(feature = "derive-xyz")]
            derive_xyz::VENUE => Ok(AdapterConfig::DeriveXyz(derive_xyz::Config {
                market: derive_xyz::Market::parse(&venue.market)
                    .map_err(ResolveError::Construct)?,
                // The venue's own name is the ticker: it is already a legal
                // token, fractional strike and all.
                instruments: venue.instruments.iter().map(|i| i.ticker.clone()).collect(),
            })),
            #[cfg(feature = "rh-chain")]
            rh_chain::VENUE => Ok(AdapterConfig::RhChain(rh_chain::Config {
                // **Held where one is named, public where none is.** A
                // variable that is named and unset refuses here rather than
                // falling back to the public node — a silent fallback is how a
                // process runs for a week against the wrong provider.
                rpc_url: match (&venue.rpc_url_var, secrets) {
                    (Some(var), Secrets::Resolve(secrets)) => crate::venue::Endpoint::held(
                        var,
                        secrets.secret(var).map_err(ResolveError::Secret)?,
                    ),
                    (Some(var), Secrets::Withhold) => crate::venue::Endpoint::withheld(var),
                    (None, _) => crate::venue::Endpoint::public(rh_chain::PUBLIC_RPC),
                },
                instruments: venue
                    .instruments
                    .iter()
                    .map(|i| {
                        // **Both refused when absent, by name.** A chain
                        // instrument with no contract is unaddressable, and one
                        // with no decimals would be scaled by a guess that is
                        // wrong by a factor of a trillion for half the tokens
                        // on this chain.
                        let contract = i.contract.clone().ok_or_else(|| ResolveError::Unknown {
                            name: format!(
                                "{} declares no `contract`, and a chain instrument is \
                                     addressed by one",
                                i.ticker
                            ),
                            known: known().join(", "),
                        })?;
                        let decimals = i.decimals.ok_or_else(|| ResolveError::Unknown {
                            name: format!(
                                "{} declares no `decimals`. Stock tokens carry 18 and USDG \
                                 carries 6, so there is no default that is not wrong for one \
                                 of them",
                                i.ticker
                            ),
                            known: known().join(", "),
                        })?;
                        Ok(rh_chain::Instrument {
                            ticker: i.ticker.clone(),
                            contract,
                            decimals,
                        })
                    })
                    .collect::<Result<Vec<_>, ResolveError>>()?,
            })),
            #[cfg(feature = "rh-crypto")]
            rh_crypto::VENUE => {
                // **Required, and no default invented.** The venue documents no
                // limit — "undocumented and explicitly variable" — and the
                // interval is also the width of the gap one failure produces.
                let poll_secs = match venue.poll_secs {
                    Some(secs) if secs >= 1 => secs,
                    Some(_) => {
                        return Err(ResolveError::Unknown {
                            name: "rh-crypto declares poll_secs = 0; a poll needs at least a \
                                   second between asks"
                                .into(),
                            known: known().join(", "),
                        });
                    }
                    None => {
                        return Err(ResolveError::Unknown {
                            name: "rh-crypto declares no `poll_secs`. The venue's limits are \
                                   undocumented and variable, and the interval is also the \
                                   width of a gap one failed poll produces, so it is declared \
                                   rather than defaulted"
                                .into(),
                            known: known().join(", "),
                        });
                    }
                };
                let credential = match secrets {
                    // Replay normalises bytes and never asks, so it reads no
                    // key at all — the lane can rebuild this venue's tape.
                    Secrets::Withhold => None,
                    Secrets::Resolve(secrets) => {
                        let named = |field: &str, var: &Option<String>| {
                            var.clone().ok_or_else(|| ResolveError::Unknown {
                                name: format!(
                                    "rh-crypto declares no `{field}`. Every request to this \
                                     venue is signed, market data included, and an unsigned \
                                     one is a 401 that looks exactly like a revoked key"
                                ),
                                known: known().join(", "),
                            })
                        };
                        let api_key_var = named("api_key_var", &venue.api_key_var)?;
                        let private_key_var = named("private_key_var", &venue.private_key_var)?;
                        let api_key = secrets.secret(&api_key_var).map_err(ResolveError::Secret)?;
                        let private_key = secrets
                            .secret(&private_key_var)
                            .map_err(ResolveError::Secret)?;
                        let credential =
                            rh_crypto::sign::Credential::from_secrets(&api_key, &private_key)
                                .map_err(|error| ResolveError::Unknown {
                                    name: format!("{private_key_var}: {error}"),
                                    known: known().join(", "),
                                })?;
                        Some(std::sync::Arc::new(credential))
                    }
                };
                Ok(AdapterConfig::RhCrypto(rh_crypto::Config {
                    tickers: venue.instruments.iter().map(|i| i.ticker.clone()).collect(),
                    poll_secs,
                    credential,
                }))
            }
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
///
/// **Returns what it learned**, as a [`Universe`], where checking a venue means
/// reading its reference data: they are the venue's bytes, and the caller
/// takes them through the one path so the record holds what the check was
/// decided on. Empty for a venue checked against a listing, which is not a
/// thing the record keeps.
///
/// `now_micros` is the caller's clock reading, stamped on those answers as
/// their receipt time. Nothing here reads a clock.
#[cfg(feature = "capture")]
#[cfg_attr(not(feature = "derive-xyz"), allow(unused_variables))]
pub async fn check_universe(
    config: &AdapterConfig,
    now_micros: i64,
) -> Result<Universe, ResolveError> {
    // See `History::for_config`: an empty `AdapterConfig` still needs an arm.
    #[cfg(not(any(
        feature = "hyperliquid",
        feature = "rh-chain",
        feature = "rh-crypto",
        feature = "derive-xyz"
    )))]
    {
        let _ = config;
        return Ok(Universe::default());
    }
    #[cfg(any(
        feature = "hyperliquid",
        feature = "rh-chain",
        feature = "rh-crypto",
        feature = "derive-xyz"
    ))]
    match config {
        #[cfg(feature = "hyperliquid")]
        AdapterConfig::Hyperliquid(c) => {
            use crate::venue::{Construct, universe};
            let adapter = hyperliquid::Hyperliquid::new(c.clone(), None)?;
            let client = adapter.client();
            // **The answers are kept**: each dex's `meta` is the reference data
            // of every captured coin on it, so the boot takes them through the
            // one path (`normalise-instrument-reference`).
            let mut pages = Vec::new();
            for dex in adapter.dexes() {
                let page =
                    client
                        .meta(&dex, now_micros)
                        .await
                        .map_err(|e| ResolveError::Universe {
                            detail: e.to_string(),
                        })?;
                let listed =
                    hyperliquid::listed(&page.payload).map_err(|e| ResolveError::Universe {
                        detail: e.to_string(),
                    })?;
                pages.push(page);
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
            Ok(Universe {
                pages,
                absent: Vec::new(),
            })
        }
        // A chain has no instrument universe to check against: a contract
        // either emits logs or it does not, and asking produces an empty
        // answer rather than a refusal. The chain-id check at boot is the
        // equivalent guard, and it lives in the loop because it needs the
        // provider.
        #[cfg(feature = "rh-chain")]
        AdapterConfig::RhChain(_) => Ok(Universe::default()),
        // **Unchecked, and said so.** The listing endpoint is signed like every
        // other, and how the venue answers an unknown symbol has not been
        // observed — no credentials were obtained. The first poll is where an
        // unlisted symbol shows itself, and the archive keeps that answer.
        #[cfg(feature = "rh-crypto")]
        AdapterConfig::RhCrypto(_) => Ok(Universe::default()),
        // **An unknown name refuses the start; an expired one does not.** The
        // venue acknowledges a subscription to either with `ok` and then says
        // nothing (measured 2026-11-24), so this is the only place a mistyped
        // name is caught — and an option that expired yesterday is not a
        // mistake, it is Tuesday. The venue still answers for an expired
        // option, which is what lets the two be told apart.
        #[cfg(feature = "derive-xyz")]
        AdapterConfig::DeriveXyz(c) => {
            use crate::venue::Construct;
            let adapter = derive_xyz::DeriveXyz::new(c.clone(), None)?;
            let client = adapter.client();
            let pace =
                std::time::Duration::from_millis(adapter.declaration().budget.interval_ms(1, 1.0));
            let mut pages = Vec::with_capacity(adapter.options().len());
            let mut standings = Vec::with_capacity(adapter.options().len());
            for (asked, name) in adapter.options().iter().enumerate() {
                if asked > 0 {
                    tokio::time::sleep(pace).await;
                }
                let page = client.instrument(name, now_micros).await.map_err(|e| {
                    ResolveError::Universe {
                        detail: format!(
                            "{}: the venue's instruments could not be read, so nothing can be \
                             checked: {e}",
                            derive_xyz::VENUE
                        ),
                    }
                })?;
                let standing = derive_xyz::standing(name, &page.payload)
                    .map_err(|detail| ResolveError::Universe { detail })?;
                standings.push((name.clone(), standing));
                pages.push(page);
            }
            derive_xyz::judge_set(&standings)
                .map_err(|detail| ResolveError::Universe { detail })?;

            // **The perpetual of each underlying, after the options.** One
            // the venue does not list is no reason to refuse options the
            // operator declared: it is handed back as absent, and the boot
            // retires it before the first subscribe.
            let mut absent = Vec::new();
            for name in adapter.perpetuals() {
                tokio::time::sleep(pace).await;
                let page = client.instrument(&name, now_micros).await.map_err(|e| {
                    ResolveError::Universe {
                        detail: format!(
                            "{}: the venue's instruments could not be read, so nothing can be \
                             checked: {e}",
                            derive_xyz::VENUE
                        ),
                    }
                })?;
                match derive_xyz::perpetual_standing(&name, &page.payload)
                    .map_err(|detail| ResolveError::Universe { detail })?
                {
                    derive_xyz::PerpetualStanding::Absent => {
                        let ticker = galata_wire::Ticker::new(name)
                            .map_err(|e| ResolveError::Construct(e.into()))?;
                        absent.push((ticker, "not listed".to_string()));
                    }
                    // Live, or inactive: the answer says which, and the loop
                    // acts on it when it takes the answer.
                    derive_xyz::PerpetualStanding::Live
                    | derive_xyz::PerpetualStanding::Inactive => pages.push(page),
                }
            }
            Ok(Universe { pages, absent })
        }
    }
}

/// Build the adapter a configuration names.
pub fn build(config: AdapterConfig) -> Result<Box<dyn Adapter>, ResolveError> {
    #[cfg(feature = "hyperliquid")]
    use crate::venue::Construct;
    match config {
        #[cfg(feature = "hyperliquid")]
        AdapterConfig::Hyperliquid(c) => {
            Ok(Box::new(hyperliquid::Hyperliquid::new(c, None)?) as Box<dyn Adapter>)
        }
        #[cfg(feature = "rh-chain")]
        AdapterConfig::RhChain(c) => Ok(Box::new(rh_chain::RhChain::new(c)?) as Box<dyn Adapter>),
        #[cfg(feature = "rh-crypto")]
        AdapterConfig::RhCrypto(c) => {
            Ok(Box::new(rh_crypto::RhCrypto::new(c)?) as Box<dyn Adapter>)
        }
        #[cfg(feature = "derive-xyz")]
        AdapterConfig::DeriveXyz(c) => {
            use crate::venue::Construct;
            Ok(Box::new(derive_xyz::DeriveXyz::new(c, None)?) as Box<dyn Adapter>)
        }
    }
}

/// Where a venue's reference data is asked again while capture runs.
///
/// Beside [`History`] and for its reason: a thing that makes requests is
/// transport, and this is the last place permitted to name the venue it asks.
/// A venue with no reference data to refresh has none: [`for_config`] answers
/// `None` for it.
///
/// **A closure rather than an enum of venues.** An enum with one variant per
/// venue is empty in a build that compiles none of them in, and code that
/// holds a value of an empty type is unreachable — which the compiler says, as
/// warnings, in the build most people take.
///
/// [`for_config`]: ReferenceSource::for_config
#[cfg(feature = "capture")]
#[derive(Clone)]
pub struct ReferenceSource {
    ask: std::sync::Arc<ReferenceAsk>,
}

/// One refresh: the venue's symbols and the caller's clock in, an answer for
/// each symbol out.
#[cfg(feature = "capture")]
type ReferenceAsk = dyn Fn(Vec<String>, ReferenceClock) -> ReferenceAnswers + Send + Sync;

/// The answers of one refresh, still being fetched.
#[cfg(feature = "capture")]
type ReferenceAnswers =
    std::pin::Pin<Box<dyn std::future::Future<Output = Vec<crate::capture::FetchResult>> + Send>>;

/// The caller's clock, read as each reference answer arrives.
#[cfg(feature = "capture")]
pub type ReferenceClock = std::sync::Arc<dyn Fn() -> i64 + Send + Sync>;

#[cfg(feature = "capture")]
impl std::fmt::Debug for ReferenceSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ReferenceSource")
    }
}

#[cfg(feature = "capture")]
impl ReferenceSource {
    /// The source for a configuration, or `None` where the venue has no
    /// reference data it must be asked for again.
    #[allow(unreachable_patterns)]
    pub fn for_config(config: &AdapterConfig) -> Result<Option<ReferenceSource>, ResolveError> {
        match config {
            // One `meta` request per captured dex, each the reference data of
            // every captured coin on it. Two at most here: no pacing needed
            // within the venue's weight budget.
            #[cfg(feature = "hyperliquid")]
            AdapterConfig::Hyperliquid(c) => {
                use crate::venue::Construct;
                let client = hyperliquid::Hyperliquid::new(c.clone(), None)?.client();
                Ok(Some(ReferenceSource {
                    ask: std::sync::Arc::new(move |dexes, now| {
                        let client = client.clone();
                        Box::pin(async move {
                            let mut answers = Vec::with_capacity(dexes.len());
                            for dex in &dexes {
                                answers.push(client.meta(dex, now()).await.map_err(|e| {
                                    crate::capture::FetchFailure::from(e.to_string())
                                }));
                            }
                            answers
                        })
                    }),
                }))
            }
            // One request per declared option, paced by the venue's stated
            // budget.
            #[cfg(feature = "derive-xyz")]
            AdapterConfig::DeriveXyz(c) => {
                use crate::venue::Construct;
                let adapter = derive_xyz::DeriveXyz::new(c.clone(), None)?;
                let client = adapter.client();
                let pace = std::time::Duration::from_millis(
                    adapter.declaration().budget.interval_ms(1, 1.0),
                );
                Ok(Some(ReferenceSource {
                    ask: std::sync::Arc::new(move |symbols, now| {
                        let client = client.clone();
                        Box::pin(async move {
                            let mut answers = Vec::with_capacity(symbols.len());
                            for (asked, symbol) in symbols.iter().enumerate() {
                                if asked > 0 {
                                    tokio::time::sleep(pace).await;
                                }
                                answers.push(client.instrument(symbol, now()).await.map_err(|e| {
                                    crate::capture::FetchFailure::from(e.to_string())
                                }));
                            }
                            answers
                        })
                    }),
                }))
            }
            _ => Ok(None),
        }
    }

    /// Ask about each symbol, in turn, and hand back each answer or the reason
    /// there was none.
    ///
    /// `now` is read as each answer arrives, because a payload's receipt time
    /// is a fact about when its bytes did. It is the caller's clock: nothing
    /// here reads one.
    pub async fn fetch(
        &self,
        symbols: Vec<String>,
        now: ReferenceClock,
    ) -> Vec<crate::capture::FetchResult> {
        (self.ask)(symbols, now).await
    }
}

/// The historical endpoint for a venue.
///
/// **Behind the `capture` feature**: it makes requests, and a thing that makes
/// requests is transport whatever else it is near.
///
/// Here rather than on the [`Adapter`] trait for the reason that trait states
/// about itself: its methods are facts about a venue, and a fact that needs a
/// runtime to state cannot be asserted in a test. This one needs a network.
///
/// It is also the second and last place permitted to name a venue, which is
/// what keeps [`crate::capture::Capture::walk`] — and the binary — free of one.
#[cfg(feature = "capture")]
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum History {
    /// Hyperliquid's info endpoint.
    #[cfg(feature = "hyperliquid")]
    Hyperliquid(hyperliquid::client::Client),
}

#[cfg(feature = "capture")]
impl History {
    /// The fetcher a configuration names.
    pub fn for_config(config: &AdapterConfig) -> Result<History, ResolveError> {
        use crate::venue::Construct;
        match config {
            #[cfg(feature = "hyperliquid")]
            AdapterConfig::Hyperliquid(c) => {
                let adapter = hyperliquid::Hyperliquid::new(c.clone(), None)?;
                Ok(History::Hyperliquid(adapter.client()))
            }
            // A chain has no *historical walk* separate from its capture: the
            // cursor loop IS the backfill, because asking for old blocks and
            // asking for new ones is the same call at a different position.
            #[cfg(feature = "rh-chain")]
            AdapterConfig::RhChain(_) => Err(ResolveError::Unknown {
                name: "rh-chain has no separate history walk; its cursor loop backfills".into(),
                known: known().join(", "),
            }),
            // The venue serves the current state and nothing before it.
            #[cfg(feature = "rh-crypto")]
            AdapterConfig::RhCrypto(_) => Err(ResolveError::Unknown {
                name: "rh-crypto serves no history; it is polled for the current state".into(),
                known: known().join(", "),
            }),
            // The venue serves trade and settlement history, and this capture
            // walks none of it: its declaration lists no historical series, so
            // the boot never asks for this.
            #[cfg(feature = "derive-xyz")]
            AdapterConfig::DeriveXyz(_) => Err(ResolveError::Unknown {
                name: "derive-xyz has no history this capture walks".into(),
                known: known().join(", "),
            }),
            // **`AdapterConfig` is EMPTY with no venue feature on**, and a
            // match over a reference to an empty enum is not exhaustive on its
            // own — Rust will not infer unreachability through the reference.
            // This arm cannot run, because no value of the type can be built.
            #[cfg(not(any(
                feature = "hyperliquid",
                feature = "rh-chain",
                feature = "rh-crypto",
                feature = "derive-xyz"
            )))]
            _ => Err(ResolveError::Unknown {
                name: "this build compiles in no venue".into(),
                known: String::new(),
            }),
        }
    }

    /// Saved `candleSnapshot` bytes as the payload the walk's own fetch would
    /// have produced, taken at `recv_micros`: for an import of a rescue.
    ///
    /// Through the seam, because building a payload names a venue, and one
    /// constructor per venue so an import and a fetch cannot disagree.
    pub fn candle_page(&self, symbol: &str, bytes: Vec<u8>, recv_micros: i64) -> Payload {
        match self {
            #[cfg(feature = "hyperliquid")]
            History::Hyperliquid(_) => hyperliquid::client::candle_page(symbol, bytes, recv_micros),
            #[cfg(not(feature = "hyperliquid"))]
            _ => {
                let _ = (symbol, bytes, recv_micros);
                unreachable!("History has no variants without a venue feature")
            }
        }
    }

    /// One historical request, returning the bytes **and the moment they
    /// arrived** — the payload the one path then archives verbatim.
    ///
    /// The error says only what the walk acts on: whether the venue refused to
    /// pace, and how long it asked for. Anything else is logged, and the
    /// outcome reports what was reached.
    pub async fn fetch(
        &self,
        request: Fetch,
        now_micros: i64,
    ) -> Result<Payload, crate::capture::FetchFailure> {
        // See `for_config`: with no venue feature on, `History` has no
        // variants and a match through `&History` still needs an arm.
        #[cfg(not(feature = "hyperliquid"))]
        {
            let _ = (request, now_micros);
            return Err("this build compiles in no venue with a history walk".into());
        }
        #[cfg(feature = "hyperliquid")]
        match self {
            #[cfg(feature = "hyperliquid")]
            History::Hyperliquid(client) => match request.series {
                Series::Candles => {
                    let interval = request.interval_label.ok_or_else(|| {
                        crate::capture::FetchFailure::from(format!(
                            "the venue serves no bar of {} micros",
                            request.interval_micros
                        ))
                    })?;
                    client
                        .candles(
                            &request.symbol,
                            &interval,
                            request.from_micros,
                            request.to_micros,
                            now_micros,
                        )
                        .await
                        .map_err(|e| e.into_failure())
                }
                Series::Funding => client
                    .funding(
                        &request.symbol,
                        request.from_micros,
                        request.to_micros,
                        now_micros,
                    )
                    .await
                    .map_err(|e| e.into_failure()),
                // The walk never asks for one the declaration does not list as
                // historical, so reaching this is a defect rather than a venue
                // refusal — and it says so instead of returning empty bytes
                // that would read as "no rows in that range".
                other => Err(format!(
                    "{} is not served historically; the walk should not have asked",
                    other.as_str()
                )
                .into()),
            },
        }
    }
}

#[cfg(all(test, feature = "rh-chain"))]
mod tests {
    use super::*;
    use crate::config::{ConfigError, Secret, SecretSource, VenueConfig};

    /// A secret source that answers one name and refuses the rest.
    struct OneSecret(&'static str, &'static str);

    impl SecretSource for OneSecret {
        fn secret(&self, name: &str) -> Result<Secret, ConfigError> {
            if name == self.0 {
                Ok(Secret::new(self.1))
            } else {
                Err(ConfigError::SecretAbsent {
                    name: name.to_string(),
                })
            }
        }
    }

    fn chain_venue(rpc_url_var: Option<&str>) -> VenueConfig {
        VenueConfig {
            market: "mainnet".into(),
            series: vec![galata_wire::Series::Transfers],
            candle: Some("1m".into()),
            walk_candles: Vec::new(),
            walk_funding_days: None,
            instruments: vec![crate::config::InstrumentDecl {
                ticker: "NVDA".into(),
                dex: None,
                contract: Some("0xd0601ce157db5bdc3162bbac2a2c8af5320d9eec".into()),
                decimals: Some(18),
            }],
            rpc_url_var: rpc_url_var.map(str::to_string),
            poll_secs: None,
            api_key_var: None,
            private_key_var: None,
        }
    }

    #[test]
    fn no_variable_means_the_public_node() {
        let AdapterConfig::RhChain(config) = AdapterConfig::from_declared(
            rh_chain::VENUE,
            &chain_venue(None),
            &OneSecret("UNUSED", "x"),
        )
        .unwrap() else {
            panic!("a chain");
        };
        // A reader with no provider account runs exactly as before.
        assert_eq!(config.rpc_url.to_string(), rh_chain::PUBLIC_RPC);
        assert!(!config.rpc_url.is_held());
    }

    #[test]
    fn a_named_variable_is_held_and_never_printed() {
        let keyed = "https://arb-mainnet.g.alchemy.com/v2/SUPERSECRETKEY";
        let AdapterConfig::RhChain(config) = AdapterConfig::from_declared(
            rh_chain::VENUE,
            &chain_venue(Some("GALATA_RHCHAIN_RPC_URL")),
            &OneSecret("GALATA_RHCHAIN_RPC_URL", keyed),
        )
        .unwrap() else {
            panic!("a chain");
        };
        assert!(config.rpc_url.is_held());
        assert_eq!(config.rpc_url.expose(), keyed);
        assert!(!format!("{:?}", config.rpc_url).contains("SUPERSECRETKEY"));
    }

    #[test]
    fn a_named_variable_that_is_unset_refuses_by_name_and_does_not_fall_back() {
        // **Not the public node.** A silent fallback is how a process runs for
        // a week against a provider nobody chose.
        let error = AdapterConfig::from_declared(
            rh_chain::VENUE,
            &chain_venue(Some("GALATA_RHCHAIN_RPC_URL")),
            &OneSecret("SOMETHING_ELSE", "x"),
        )
        .unwrap_err();
        let said = error.to_string();
        assert!(said.contains("GALATA_RHCHAIN_RPC_URL"), "{said}");
    }

    #[test]
    fn replay_withholds_a_named_provider_without_reading_it() {
        // No secret source is passed because none CAN be: `for_replay` takes
        // none, so the variable's being unset cannot matter.
        let AdapterConfig::RhChain(config) = AdapterConfig::for_replay(
            rh_chain::VENUE,
            &chain_venue(Some("GALATA_RHCHAIN_RPC_URL")),
        )
        .unwrap() else {
            panic!("a chain");
        };
        assert!(config.rpc_url.is_withheld());
        assert!(
            config
                .rpc_url
                .to_string()
                .contains("GALATA_RHCHAIN_RPC_URL")
        );
        // And the adapter builds from it, which is all a replay needs.
        build(AdapterConfig::RhChain(config)).unwrap();
    }

    #[test]
    fn replay_still_refuses_an_instrument_with_no_contract() {
        let mut venue = chain_venue(Some("GALATA_RHCHAIN_RPC_URL"));
        venue.instruments[0].contract = None;
        let said = AdapterConfig::for_replay(rh_chain::VENUE, &venue)
            .unwrap_err()
            .to_string();
        assert!(said.contains("contract"), "{said}");
    }

    /// Two named secrets, for a venue that signs.
    #[cfg(feature = "rh-crypto")]
    struct Keys;

    #[cfg(feature = "rh-crypto")]
    impl SecretSource for Keys {
        fn secret(&self, name: &str) -> Result<Secret, ConfigError> {
            match name {
                "GALATA_RHCRYPTO_API_KEY" => Ok(Secret::new("API-KEY")),
                // A valid base64 32-byte seed.
                "GALATA_RHCRYPTO_PRIVATE_KEY" => {
                    Ok(Secret::new("BwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwc="))
                }
                other => Err(ConfigError::SecretAbsent {
                    name: other.to_string(),
                }),
            }
        }
    }

    #[cfg(feature = "rh-crypto")]
    fn crypto_venue(poll_secs: Option<u32>) -> VenueConfig {
        VenueConfig {
            market: "mainnet".into(),
            series: vec![galata_wire::Series::Quotes],
            candle: Some("1m".into()),
            walk_candles: Vec::new(),
            walk_funding_days: None,
            instruments: vec![crate::config::InstrumentDecl {
                ticker: "BTC".into(),
                dex: None,
                contract: None,
                decimals: None,
            }],
            rpc_url_var: None,
            poll_secs,
            api_key_var: Some("GALATA_RHCRYPTO_API_KEY".into()),
            private_key_var: Some("GALATA_RHCRYPTO_PRIVATE_KEY".into()),
        }
    }

    #[test]
    #[cfg(feature = "rh-crypto")]
    fn rh_crypto_is_declared_with_its_interval() {
        assert!(known().contains(&rh_crypto::VENUE));
        assert!(supplies(rh_crypto::VENUE, galata_wire::Series::Quotes));
        let AdapterConfig::RhCrypto(config) =
            AdapterConfig::from_declared(rh_crypto::VENUE, &crypto_venue(Some(5)), &Keys).unwrap()
        else {
            panic!("rh-crypto");
        };
        assert_eq!(config.poll_secs, 5);
        assert!(config.credential.is_some(), "capture signs");
        let adapter = build(AdapterConfig::RhCrypto(config)).unwrap();
        let crate::venue::Transport::Poll {
            interval_micros,
            signer,
            ..
        } = adapter.transport()
        else {
            panic!("a poll");
        };
        assert_eq!(interval_micros, 5_000_000);
        assert!(signer.is_some());
    }

    #[test]
    #[cfg(feature = "rh-crypto")]
    fn no_interval_is_refused_and_none_invented() {
        for poll_secs in [None, Some(0)] {
            let said =
                AdapterConfig::from_declared(rh_crypto::VENUE, &crypto_venue(poll_secs), &Keys)
                    .unwrap_err()
                    .to_string();
            assert!(said.contains("poll_secs"), "{said}");
        }
    }

    #[test]
    #[cfg(feature = "rh-crypto")]
    fn replay_builds_rh_crypto_with_no_credential() {
        // No secret source exists here to ask, and none is needed.
        let AdapterConfig::RhCrypto(config) =
            AdapterConfig::for_replay(rh_crypto::VENUE, &crypto_venue(Some(5))).unwrap()
        else {
            panic!("rh-crypto");
        };
        assert!(config.credential.is_none());
        build(AdapterConfig::RhCrypto(config)).unwrap();
    }
}

#[cfg(all(test, feature = "hyperliquid"))]
mod candle_tests {
    use super::*;
    use crate::config::{ConfigError, Secret, SecretSource, VenueConfig};

    struct NoSecrets;

    impl SecretSource for NoSecrets {
        fn secret(&self, name: &str) -> Result<Secret, ConfigError> {
            Err(ConfigError::SecretAbsent {
                name: name.to_string(),
            })
        }
    }

    fn venue(candle: Option<&str>) -> VenueConfig {
        VenueConfig {
            market: "mainnet".into(),
            series: vec![galata_wire::Series::Quotes],
            candle: candle.map(str::to_string),
            walk_candles: Vec::new(),
            walk_funding_days: None,
            instruments: vec![crate::config::InstrumentDecl {
                ticker: "BTC".into(),
                dex: None,
                contract: None,
                decimals: None,
            }],
            rpc_url_var: None,
            poll_secs: None,
            api_key_var: None,
            private_key_var: None,
        }
    }

    #[test]
    fn a_venue_that_pushes_bars_refuses_a_block_with_no_width() {
        // Optional in the configuration, because a venue of options has none.
        // This one subscribes to a width, and a default would be one nobody
        // chose.
        let refusal = AdapterConfig::from_declared(hyperliquid::VENUE, &venue(None), &NoSecrets)
            .unwrap_err()
            .to_string();
        assert!(refusal.contains("candle"), "{refusal}");
        assert!(refusal.contains(hyperliquid::VENUE), "{refusal}");
        assert!(
            AdapterConfig::from_declared(hyperliquid::VENUE, &venue(Some("1m")), &NoSecrets)
                .is_ok()
        );
    }

    #[test]
    fn this_venue_states_no_cap_and_serves_no_series_called_marks() {
        assert_eq!(max_instruments(hyperliquid::VENUE), None);
        assert_eq!(instrument_problem(hyperliquid::VENUE, "anything"), None);
    }
}

#[cfg(all(test, feature = "derive-xyz"))]
mod derive_xyz_tests {
    use super::*;
    use crate::config::{ConfigError, Secret, SecretSource, VenueConfig};

    struct NoSecrets;

    impl SecretSource for NoSecrets {
        fn secret(&self, name: &str) -> Result<Secret, ConfigError> {
            Err(ConfigError::SecretAbsent {
                name: name.to_string(),
            })
        }
    }

    fn venue(names: &[&str]) -> VenueConfig {
        VenueConfig {
            market: "mainnet".into(),
            series: vec![galata_wire::Series::Quotes],
            // A venue that pushes no bars states no width.
            candle: None,
            walk_candles: Vec::new(),
            walk_funding_days: None,
            instruments: names
                .iter()
                .map(|name| crate::config::InstrumentDecl {
                    ticker: name.to_string(),
                    dex: None,
                    contract: None,
                    decimals: None,
                })
                .collect(),
            rpc_url_var: None,
            poll_secs: None,
            api_key_var: None,
            private_key_var: None,
        }
    }

    #[test]
    fn the_venue_is_known_supplies_quotes_and_refuses_trades() {
        assert!(known().contains(&derive_xyz::VENUE));
        assert!(supplies(derive_xyz::VENUE, galata_wire::Series::Quotes));
        // The venue has a trades channel and this capture does not take it, so
        // a configuration naming it is refused at load, naming the series.
        for series in [
            galata_wire::Series::Trades,
            galata_wire::Series::Candles,
            galata_wire::Series::Funding,
            galata_wire::Series::Book,
        ] {
            assert!(!supplies(derive_xyz::VENUE, series), "{series}");
        }
    }

    #[test]
    fn the_cap_and_the_form_of_a_name_are_the_venues_own() {
        assert_eq!(max_instruments(derive_xyz::VENUE), Some(50));
        assert_eq!(
            instrument_problem(derive_xyz::VENUE, "BTC-20261225-60000-P"),
            None
        );
        let problem = instrument_problem(derive_xyz::VENUE, "BTC-PERP").unwrap();
        assert!(problem.contains("BTC-PERP"), "{problem}");
        assert!(
            problem.contains("<CCY>-<YYYYMMDD>-<STRIKE>-<C|P>"),
            "{problem}"
        );
    }

    #[test]
    fn a_declared_set_resolves_with_no_secret_and_no_bar_width() {
        let config = AdapterConfig::from_declared(
            derive_xyz::VENUE,
            &venue(&["BTC-20261225-60000-P", "ETH-20261225-2000-C"]),
            &NoSecrets,
        )
        .unwrap();
        let adapter = build(config).unwrap();
        assert_eq!(adapter.venue().as_str(), "derive-xyz");
        assert!(adapter.transport().is_stream());
        // Two options on two underlyings: two indexes and two perpetuals
        // beside them.
        assert_eq!(adapter.implied_subscriptions().len(), 4);
        assert!(adapter.declaration().historical.is_empty());
        assert_eq!(adapter.live_interval_micros(), None);
    }

    #[test]
    fn a_testnet_block_resolves_and_an_unknown_market_is_refused() {
        let mut block = venue(&["BTC-20261225-60000-P"]);
        block.market = "testnet".into();
        assert!(AdapterConfig::from_declared(derive_xyz::VENUE, &block, &NoSecrets).is_ok());
        block.market = "staging".into();
        let refusal = AdapterConfig::from_declared(derive_xyz::VENUE, &block, &NoSecrets)
            .unwrap_err()
            .to_string();
        assert!(refusal.contains("staging"), "{refusal}");
    }
}
