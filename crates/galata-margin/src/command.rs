//! **`galata-margin advise`**, end to end (task 7.1): load what the
//! configuration declares, check the key, read the accounts and the markets,
//! simulate each placement on Derive, compute each on Hyperliquid, and
//! compare.
//!
//! **Refused before anything is asked** when the request is malformed, the
//! configuration has no `[margin]` block or keeps no ledger, or a secret is
//! absent. **Refused before any account is read** when the session key holds
//! a scope that can move funds (design D11).
//!
//! Requests made, all reads: `private/session_keys`, `private/get_subaccounts`,
//! one `private/get_margin` per placement (a simulation), Derive's ticker and
//! book, and Hyperliquid's market contexts and book. Nothing else is asked.
//! No clock is read here: the binary passes one in.

use galata_datawatch::adapters::derive_xyz::Market as Network;
use galata_datawatch::adapters::derive_xyz::sign::SessionKey;
use galata_datawatch::config::{Config, SecretSource};
use galata_datawatch::ledger::{Bindings, FingerprintKey};
use galata_wire::Account;
use galata_wire::event::AccountMode;
use rust_decimal::Decimal;

use crate::advice::Advice;
use crate::agreement::Agreements;
use crate::config::MarginConfig;
use crate::derive::{MarginRequest, SimulatedPosition, Simulator};
use crate::estimate::{HedgeOnHyperliquid, derive_estimate, hyperliquid_estimate};
use crate::market::MarketReader;
use crate::placement::{self, Gathered, Market, RawRequest, Request};
use crate::state::{read_derive, read_hyperliquid};

/// How many times a rate-limited simulation is asked again.
const RETRIES: u32 = 2;

/// What `advise` produced.
#[derive(Debug, Clone)]
pub struct Advised {
    /// The request, as validated.
    pub request: Request,
    /// The advice.
    pub advice: Advice,
    /// When it was asked for.
    pub asked_at_micros: i64,
}

/// The ledger account a Derive subaccount alias belongs to: `owner_s3`'s is
/// `owner`, or the one Derive account the ledger keeps.
fn derive_master<'a>(
    config: &'a Config,
    alias: &str,
) -> Result<(&'a str, &'a galata_datawatch::config::LedgerAccount), String> {
    let ledger = config
        .ledger
        .as_ref()
        .ok_or("the configuration keeps no ledger, so no account is read")?;
    let derive: Vec<_> = ledger
        .account
        .iter()
        .filter(|(_, a)| a.venue == "derive-xyz")
        .collect();
    derive
        .iter()
        .find(|(name, _)| alias.starts_with(&format!("{name}_s")) || alias == name.as_str())
        .or_else(|| (derive.len() == 1).then(|| &derive[0]))
        .map(|(name, a)| (name.as_str(), *a))
        .ok_or_else(|| format!("no Derive account the ledger keeps owns `{alias}`"))
}

/// Produce the advice for a request.
pub async fn advise(
    config: &Config,
    raw: &RawRequest,
    secrets: &dyn SecretSource,
    clock: &(dyn Fn() -> i64 + Sync),
    reader: &MarketReader,
    simulator_at: Option<&str>,
) -> Result<Advised, String> {
    let margin = MarginConfig::from_config(config).map_err(|e| e.to_string())?;
    let request = placement::validate(raw, &margin.accounts)?;
    let ledger = config
        .ledger
        .as_ref()
        .ok_or("the configuration keeps no ledger, so no account is read")?;
    let root = ledger
        .tape
        .as_ref()
        .ok_or("the ledger projects nothing ([ledger] tape), so no account state can be read")?;
    let (master, account) = derive_master(config, &request.derive_account)?;
    let secret = |name: &str| secrets.secret(name).map_err(|e| e.to_string());
    let wallet = secret(&account.address_var)?;
    let key =
        SessionKey::from_secret(&secret(&margin.session_key_var)?).map_err(|e| e.to_string())?;
    let fingerprint_key = FingerprintKey::new(secret(&ledger.fingerprint_key_var)?);
    let network = Network::parse(account.market.as_deref().unwrap_or("mainnet"))
        .map_err(|e| e.to_string())?;
    let simulator = match simulator_at {
        Some(url) => Simulator::at(url),
        None => Simulator::for_market(network),
    };

    // The key first: nothing of the owner's is read with one that can move
    // funds.
    simulator
        .check_key(&key, &wallet, clock())
        .await
        .map_err(|e| e.to_string())?;

    let asked_at = clock();
    let bound = margin.snapshot_age_secs;
    let mut notes = Vec::new();
    let derive_state = read_derive(root, &request.derive_account).map_err(|e| e.to_string())?;
    let hyperliquid_state = read_hyperliquid(root, &request.hyperliquid_account);

    let collateral: Vec<String> = hyperliquid_state
        .as_ref()
        .map(|s| s.pm.ltv.keys().cloned().collect())
        .unwrap_or_default();
    let coin = request.underlying.clone();
    let perp = request.derive_perp();
    let (hl_market, hl_book, derive_funding, derive_book) = tokio::join!(
        reader.hyperliquid_market(&coin, &collateral, asked_at),
        reader.hyperliquid_book(&coin),
        reader.derive_funding(&perp),
        reader.derive_book(&perp),
    );
    let hyperliquid_lists = !matches!(hl_market, Ok(None));
    if !hyperliquid_lists {
        notes.push(format!(
            "Hyperliquid lists no perpetual for {coin}, so only the hedge beside its option on Derive is compared"
        ));
    }
    let hl_market = hl_market.map_err(|e| e.to_string());
    let derive_funding = derive_funding.map_err(|e| e.to_string());
    let derive_mark = derive_funding.as_ref().ok().map(|f| f.mark);

    // galata's Hyperliquid figure, checked against the venue's on the
    // account as it stands.
    let mut agreements = Agreements::default();
    let mut label = None;
    if let (Ok(state), Ok(Some(m))) = (&hyperliquid_state, &hl_market)
        && state.mode == AccountMode::PortfolioMargin
    {
        if let Some(stated) = state.stated_ratio {
            let mut pm = state.pm.clone();
            pm.prices = m.prices.clone();
            if let Err(e) = agreements.compare(&state.account, state.at_micros, &pm, stated) {
                notes.push(format!(
                    "galata's ratio for `{}` could not be compared with the venue's: {e}",
                    state.account
                ));
            }
        }
        label = agreements.label(&state.account, margin.agreement_tolerance);
    }

    let subaccount = simulator
        .subaccount_of(
            &key,
            &wallet,
            clock(),
            &Bindings::read(&ledger.root, "derive-xyz").map_err(|e| e.to_string())?,
            &Account::new(master).map_err(|e| e.to_string())?,
            &fingerprint_key,
            &request.derive_account,
        )
        .await?;

    // The hedge on Hyperliquid opens at the market's own position's
    // leverage, or at its maximum where the account holds none.
    let leverage_for_hedge = |state: &crate::state::HyperliquidState, max: u32| -> Decimal {
        state
            .positions
            .iter()
            .find(|p| p.dex.is_empty() && p.market == coin)
            .and_then(|p| p.leverage)
            .unwrap_or_else(|| Decimal::from(max))
    };
    if let (Ok(state), Ok(Some(m))) = (&hyperliquid_state, &hl_market)
        && !state
            .positions
            .iter()
            .any(|p| p.dex.is_empty() && p.market == coin)
    {
        notes.push(format!(
            "`{}` holds no {coin} on Hyperliquid, so its hedge is taken at the market's maximum leverage, {}x",
            state.account, m.max_leverage
        ));
    }

    let mut gathered = Vec::new();
    for split in placement::splits(request.hedge_size, &margin.split_pct, hyperliquid_lists) {
        let mut positions = vec![SimulatedPosition {
            instrument_name: request.option.clone(),
            amount: request.option_size,
            entry_price: None,
        }];
        if !split.on_derive.is_zero() {
            positions.push(SimulatedPosition {
                instrument_name: perp.clone(),
                amount: split.on_derive,
                entry_price: derive_mark,
            });
        }
        let simulation = simulator
            .get_margin_waiting(
                &key,
                &wallet,
                &MarginRequest {
                    subaccount_id: subaccount,
                    simulated_position_changes: positions,
                },
                clock,
                RETRIES,
            )
            .await
            .map_err(|e| e.to_string());
        let derive = derive_estimate(
            &derive_state,
            simulation.as_ref().map_err(String::clone),
            clock(),
            asked_at,
            bound,
        );
        let hyperliquid = match (&hyperliquid_state, &hl_market) {
            (Err(e), _) => crate::advice::VenueEstimate::Missing {
                venue: crate::advice::Venue::Hyperliquid,
                account: request.hyperliquid_account.clone(),
                reason: e.to_string(),
            },
            (_, Err(e)) => crate::advice::VenueEstimate::Missing {
                venue: crate::advice::Venue::Hyperliquid,
                account: request.hyperliquid_account.clone(),
                reason: e.clone(),
            },
            (Ok(state), Ok(m)) => {
                let hedge = m
                    .as_ref()
                    .filter(|_| !split.on_hyperliquid.is_zero())
                    .map(|m| HedgeOnHyperliquid {
                        dex: String::new(),
                        market: coin.clone(),
                        size: split.on_hyperliquid,
                        mark: m.funding.mark,
                        leverage: leverage_for_hedge(state, m.max_leverage),
                        max_leverage: m.max_leverage,
                    });
                let prices = m.as_ref().map(|m| m.prices.clone()).unwrap_or_default();
                hyperliquid_estimate(state, hedge.as_ref(), &prices, &label, asked_at, bound)
            }
        };
        gathered.push(Gathered {
            split,
            derive,
            hyperliquid,
        });
    }

    let market = Market {
        derive_book: derive_book.map_err(|e| e.to_string()),
        hyperliquid_book: hl_book.map_err(|e| e.to_string()),
        derive_funding,
        hyperliquid_funding: match hl_market {
            Ok(Some(m)) => Ok(m.funding),
            Ok(None) => Err(format!("Hyperliquid lists no perpetual for {coin}")),
            Err(e) => Err(e),
        },
    };
    let advice = placement::advise(
        &request,
        gathered,
        &market,
        margin.funding_hours,
        margin.venue_cap_pct,
        derive_state.margin_type.as_deref(),
        notes,
    );
    Ok(Advised {
        request,
        advice,
        asked_at_micros: asked_at,
    })
}
