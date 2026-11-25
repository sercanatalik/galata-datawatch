//! Derive (derive.xyz): options, from public data.
//!
//! Market data is public: no secret is loaded to construct this adapter, and a
//! credential is refused rather than ignored.
//!
//! **The token is `derive-xyz`, not `derive`.** This crate already has a
//! `derive` module, a `galata-derive` binary and a `[watch]` block that mean
//! *statistics derived from the tape*, and `status.derive` or `venue=derive`
//! would read as either.
//!
//! # What makes this venue different
//!
//! **Its instruments expire.** Every other venue here lists an instrument
//! until somebody delists it; an option stops existing on a date in its own
//! name, at an hour the venue states and the name does not. So the set is
//! named instrument by instrument, capped, and each instrument leaves it when
//! the venue's reference data says so — which is the capture loop's doing,
//! from the vocabulary, and nothing in this module's.
//!
//! **One payload is three datasets.** The venue's ticker carries an option's
//! top of book, its mark and index, and the venue's own implied volatility and
//! greeks. They land in `quotes`, `marks` and `option_pricing`, and the
//! declared series is `quotes`: a gap in it is a gap in all three, the way a
//! gap in Hyperliquid's `funding` is a gap in its marks.
//!
//! **The underlying's index rides beside its options.** One index
//! subscription per underlying in the set, added here and declared by nobody,
//! addressed to a ticker named for the venue's own index (`BTC-USD`).
//!
//! **Public trades are not captured.** The venue has a trades channel and this
//! adapter does not name it: nothing that runs reads public prints, and the
//! venue serves its trade history after the fact.
//!
//! # Measured, 2026-11-24
//!
//! - A subscription to an expired, unknown or malformed instrument is
//!   **acknowledged `ok` and then silent**. It does not close the connection,
//!   as Hyperliquid does for an unlisted coin, and it does not refuse — so the
//!   only place a mistyped name is caught is the reference check at boot.
//! - The ticker is sent **every second whether or not anything changed**:
//!   0.988 messages a second per instrument, 549 bytes each. Fifty options and
//!   four indexes were 27.5 KB a second, 2.26 GiB a day before compression.
//! - The server pings every **30 s**. Its documentation says about 180.
//! - A socket held 630 s was not closed by the venue. No lifetime is known.
//!
//! See `design/measured.md`.

/// **Behind the `capture` feature**: it makes HTTP requests.
#[cfg(feature = "capture")]
pub mod client;
pub mod normalise;
pub mod wire;

use std::collections::BTreeSet;

use galata_wire::{Num, Origin, Series, Ticker, Venue};

use crate::normalise::{Normalise, NormaliseError};
use crate::record::{Payload, PayloadAddress};
use crate::venue::{
    Adapter, Budget, ConnectionPolicy, Construct, ConstructError, Credential, Declaration,
    Endpoint, Keepalive, REFERENCE_CHANNEL, Reference, Streaming, Subscription, Symbols, Transport,
};

/// The venue's token: a partition directory name and a subject level.
pub const VENUE: &str = "derive-xyz";

/// The most options one capture follows.
///
/// The owner's number (2026-11-24), counted apart from every other venue's
/// instruments. The underlying indexes this adapter adds are not counted: they
/// follow from the set and nobody declares them.
pub const MAX_INSTRUMENTS: u32 = 50;

/// How often the venue's reference data is asked again while capture runs.
///
/// Five minutes. It bounds how long an instrument the venue marks inactive
/// *before* its stated expiry stays expected; an expiry itself is acted on by
/// the loop's own clock, at the moment, and does not wait for this.
pub const REFERENCE_INTERVAL_MICROS: i64 = 5 * 60 * 1_000_000;

/// How often this side says something to the venue.
///
/// The venue pings every 30 s (measured) and counts any frame from us as
/// liveness, so this is a second keepalive rather than the only one.
pub const KEEPALIVE_SECS: u64 = 60;

/// Which network.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Market {
    /// Real markets.
    Mainnet,
    /// The venue's test network.
    Testnet,
}

impl Market {
    /// The market a configuration names.
    pub fn parse(name: &str) -> Result<Market, ConstructError> {
        match name {
            "mainnet" => Ok(Market::Mainnet),
            "testnet" => Ok(Market::Testnet),
            other => Err(ConstructError::Rejected {
                venue: VENUE.into(),
                detail: format!("market {other:?} is neither mainnet nor testnet"),
            }),
        }
    }

    fn ws_url(&self) -> &'static str {
        match self {
            Market::Mainnet => "wss://api.derive.xyz/v3/ws",
            Market::Testnet => "wss://testnet.api.derive.xyz/v3/ws",
        }
    }

    /// The root every REST method hangs from.
    pub fn rest_url(&self) -> &'static str {
        match self {
            Market::Mainnet => "https://api.derive.xyz/v3",
            Market::Testnet => "https://testnet.api.derive.xyz/v3",
        }
    }
}

/// `call` or `put`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Right {
    /// The right to buy.
    Call,
    /// The right to sell.
    Put,
}

/// An option's name, read: `<CCY>-<YYYYMMDD>-<STRIKE>-<C|P>`.
///
/// **What a name says, and no more.** The currency, the day, the strike and
/// the right are in it. The *hour* of expiry is not — crypto options settle at
/// 08:00 UTC, equity ones at 21:00 and crude at 19:30, all under names of this
/// one shape — so [`OptionName`] carries a date and never a time, and the
/// expiry this tree acts on is the one the venue's reference data states.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OptionName {
    /// The underlying: `BTC`.
    pub currency: String,
    /// The day it expires, as the name spells it: `20261225`.
    pub date: String,
    /// The strike. A fractional one is spelled with `_`: `0_15` is 0.15.
    pub strike: Num,
    /// Call or put.
    pub right: Right,
    /// The venue's risk universe, where the name carries one. None of the
    /// options listed on 2026-11-24 did.
    pub universe: Option<u32>,
}

/// The form a name takes, for a refusal that says what was expected.
pub const NAME_FORM: &str =
    "<CCY>-<YYYYMMDD>-<STRIKE>-<C|P>, for example BTC-20261225-60000-P or ADA-20261127-0_15-C";

impl OptionName {
    /// Read a name, or say what is wrong with it.
    pub fn parse(name: &str) -> Result<OptionName, String> {
        let refuse =
            |why: &str| format!("{name:?} is not an option name: {why}. Expected {NAME_FORM}");
        let parts: Vec<&str> = name.split('-').collect();
        let (currency, date, strike, right, universe) = match parts.as_slice() {
            [c, d, s, r] => (*c, *d, *s, *r, None),
            [c, d, s, r, u] => (*c, *d, *s, *r, Some(*u)),
            _ => return Err(refuse("it does not have four parts")),
        };
        if currency.is_empty()
            || !currency
                .chars()
                .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit())
        {
            return Err(refuse("the currency is not upper-case letters and digits"));
        }
        if date.len() != 8 || !date.chars().all(|c| c.is_ascii_digit()) {
            return Err(refuse("the date is not eight digits"));
        }
        let digits = |s: &str| !s.is_empty() && s.chars().all(|c| c.is_ascii_digit());
        let strike_ok = match strike.split_once('_') {
            Some((whole, fraction)) => digits(whole) && digits(fraction),
            None => digits(strike),
        };
        if !strike_ok {
            return Err(refuse("the strike is not a number"));
        }
        let strike: Num = strike
            .replace('_', ".")
            .parse()
            .map_err(|_| refuse("the strike is not a number"))?;
        let right = match right {
            "C" => Right::Call,
            "P" => Right::Put,
            _ => return Err(refuse("the last part is neither C nor P")),
        };
        let universe = match universe {
            None => None,
            Some(u) if digits(u) => Some(
                u.parse()
                    .map_err(|_| refuse("the risk universe is out of range"))?,
            ),
            Some(_) => return Err(refuse("the risk universe is not a number")),
        };
        Ok(OptionName {
            currency: currency.to_string(),
            date: date.to_string(),
            strike,
            right,
            universe,
        })
    }

    /// The ticker of this option's underlying index: `BTC-USD`.
    ///
    /// Composed, and checked against what the venue states at boot. Measured
    /// 2026-11-24: all eighteen underlyings name their index `<CCY>-USD`.
    pub fn index_ticker(&self) -> String {
        index_ticker_of(&self.currency)
    }
}

/// The index ticker for a currency.
pub fn index_ticker_of(currency: &str) -> String {
    format!("{currency}-USD")
}

/// The venue's name for a currency's perpetual: `BTC-PERP`.
///
/// Composed, and asked of the venue at boot under exactly this name. Measured
/// 2026-11-24: every one of the eighteen underlyings with options has one.
pub fn perpetual_of(currency: &str) -> String {
    format!("{currency}-PERP")
}

/// How the adapter is configured. Nothing here could hold a secret.
#[derive(Debug, Clone)]
pub struct Config {
    /// Which network.
    pub market: Market,
    /// The options to follow, by the venue's own names.
    pub instruments: Vec<String>,
}

/// The adapter.
#[derive(Debug)]
pub struct DeriveXyz {
    venue: Venue,
    market: Market,
    declaration: Declaration,
    symbols: Symbols,
    /// The declared options, in declaration order.
    options: Vec<String>,
    /// The underlyings they are on, sorted, each once.
    currencies: Vec<String>,
}

impl Construct for DeriveXyz {
    type Config = Config;

    fn new(config: Config, credential: Option<Credential>) -> Result<DeriveXyz, ConstructError> {
        // Refused rather than ignored: this venue's market data is public, and
        // being handed a key would mean somebody believed otherwise.
        if credential.is_some() {
            return Err(ConstructError::Rejected {
                venue: VENUE.into(),
                detail: "market data here is public; a credential was supplied and nothing would \
                         use it"
                    .into(),
            });
        }
        if config.instruments.len() > MAX_INSTRUMENTS as usize {
            return Err(ConstructError::Rejected {
                venue: VENUE.into(),
                detail: format!(
                    "{} options are declared and this venue's capture follows at most \
                     {MAX_INSTRUMENTS}",
                    config.instruments.len()
                ),
            });
        }

        let mut symbols = Symbols::new();
        let mut seen = BTreeSet::new();
        let mut currencies = BTreeSet::new();
        for name in &config.instruments {
            let parsed = OptionName::parse(name).map_err(|detail| ConstructError::Rejected {
                venue: VENUE.into(),
                detail,
            })?;
            if !seen.insert(name.as_str()) {
                return Err(ConstructError::Rejected {
                    venue: VENUE.into(),
                    detail: format!("{name} is declared twice"),
                });
            }
            // The venue's name is already a legal ticker, so it is the ticker.
            symbols.everywhere(name, name)?;
            currencies.insert(parsed.currency);
        }
        for currency in &currencies {
            // On its own channel only: the index is addressed by a currency,
            // and `BTC` must not resolve to it anywhere else.
            symbols.on_channel(wire::INDEX, currency, &index_ticker_of(currency))?;
            // The underlying's perpetual, by the venue's own name, which is
            // already a legal ticker.
            let perpetual = perpetual_of(currency);
            symbols.everywhere(&perpetual, &perpetual)?;
        }

        let declaration = Declaration {
            // **Not `trades`.** See the module documentation.
            streams: vec![Series::Quotes],
            // The venue serves trade and settlement history; this adapter
            // walks none of it.
            historical: Vec::new(),
            paging: Default::default(),
            // The documented limit without a credential: 25 requests in a five
            // second window. A live read showed about four times that
            // (2026-11-24); the lower, stated one is used.
            budget: Budget {
                weight_per_minute: 300.0,
                min_historical_interval_ms: 250,
            },
            // No lifetime is documented and none was observed in 630 s, so
            // nothing rotates ahead of one.
            connection: ConnectionPolicy::KeepAliveOnly {
                keepalive_secs: KEEPALIVE_SECS,
            },
            ws_url: config.market.ws_url(),
            rest_url: config.market.rest_url(),
            max_instruments: Some(MAX_INSTRUMENTS),
        };
        declaration.validate()?;

        Ok(DeriveXyz {
            venue: Venue::new(VENUE)?,
            market: config.market,
            declaration,
            symbols,
            options: config.instruments,
            currencies: currencies.into_iter().collect(),
        })
    }
}

impl DeriveXyz {
    /// The REST client. Behind `capture`: it makes requests.
    #[cfg(feature = "capture")]
    pub fn client(&self) -> client::Client {
        client::Client::new(self.market.rest_url())
    }

    /// The declared options, by the venue's names.
    pub fn options(&self) -> &[String] {
        &self.options
    }

    /// The underlyings the declared options are on.
    pub fn currencies(&self) -> &[String] {
        &self.currencies
    }

    /// The perpetual of each underlying in the set, by the venue's name.
    ///
    /// Declared by nobody and counted toward no cap, as the indexes are: which
    /// perpetuals these are follows from the options.
    pub fn perpetuals(&self) -> Vec<String> {
        self.currencies.iter().map(|c| perpetual_of(c)).collect()
    }

    /// Which network.
    pub fn market(&self) -> Market {
        self.market
    }

    /// Whether a ticker is one of the indexes this adapter added.
    fn index_currency(&self, ticker: &Ticker) -> Option<&str> {
        self.currencies
            .iter()
            .find(|c| index_ticker_of(c) == ticker.as_str())
            .map(String::as_str)
    }
}

impl Normalise for DeriveXyz {
    fn normalise(&self, payload: &Payload) -> Result<Vec<galata_wire::Envelope>, NormaliseError> {
        normalise::normalise(&self.venue, &self.symbols, payload)
    }

    fn venue(&self) -> &Venue {
        &self.venue
    }
}

impl Adapter for DeriveXyz {
    fn declaration(&self) -> &Declaration {
        &self.declaration
    }

    fn streaming(&self) -> Option<&dyn Streaming> {
        Some(self)
    }

    fn transport(&self) -> Transport {
        Transport::Stream {
            ws_url: Endpoint::public(self.declaration.ws_url),
            // The protocol's own ping. A text `ping` also works and is
            // answered `pong`, which would be one more frame a second-rate
            // parser has to step over, archived every minute for ever.
            keepalive: Keepalive::Protocol,
        }
    }

    fn series_of_channel(&self, channel: &str) -> Option<Series> {
        match channel {
            // The index rides `quotes` too: it is declared by nobody, and a
            // gap in it is reported against the series that subscribed it.
            wire::TICKER | wire::INDEX => Some(Series::Quotes),
            _ => None,
        }
    }

    fn classify(&self, bytes: &[u8], recv_micros: i64) -> Payload {
        let (channel, symbol) = wire::envelope_of(bytes);
        Payload {
            seq: 0,
            recv_micros,
            address: PayloadAddress::Venue(VENUE.into()),
            kind: self
                .series_of_channel(&channel)
                .map(|s| s.as_str().to_string())
                // A frame carrying no observation still arrived, and the
                // record records arrivals. It partitions under its own name.
                .unwrap_or_else(|| channel.clone()),
            channel,
            symbol,
            origin: Origin::Streamed,
            payload: bytes.to_vec(),
        }
    }

    fn reference(&self) -> Option<Reference> {
        if self.options.is_empty() {
            return None;
        }
        // The options, then the perpetual of each underlying. One the loop
        // has retired is left out of a refresh by the loop.
        let mut symbols = self.options.clone();
        symbols.extend(self.perpetuals());
        Some(Reference {
            interval_micros: REFERENCE_INTERVAL_MICROS,
            symbols,
        })
    }

    fn venue_symbol(&self, ticker: &Ticker) -> Option<String> {
        self.index_currency(ticker)
            .map(str::to_string)
            .or_else(|| self.symbols.venue_symbol_for(ticker).map(str::to_string))
    }

    /// This venue pushes no bars.
    fn interval_label(&self, _interval_micros: i64) -> Option<String> {
        None
    }

    fn venue_ticker(&self, channel: &str, venue_symbol: &str) -> Option<Ticker> {
        self.symbols.resolve(channel, venue_symbol).cloned()
    }

    /// The index of each underlying in the set, then its perpetual: what a
    /// hedge on this venue is priced from, beside the options it hedges.
    fn implied_subscriptions(&self) -> Vec<Subscription> {
        self.currencies
            .iter()
            .map(|currency| index_ticker_of(currency))
            .chain(self.perpetuals())
            .filter_map(|ticker| Ticker::new(ticker).ok())
            .map(|ticker| Subscription {
                ticker,
                series: Series::Quotes,
            })
            .collect()
    }
}

/// **The subscribing half.**
impl Streaming for DeriveXyz {
    fn channel_of(&self, subscription: &Subscription) -> String {
        match self.index_currency(&subscription.ticker) {
            Some(_) => wire::INDEX.to_string(),
            None => wire::TICKER.to_string(),
        }
    }

    /// One frame for the whole set: the venue takes every channel in a single
    /// `subscribe`. Sorted, so one set is one frame across restarts.
    fn subscribe_frames(&self, subscriptions: &[Subscription]) -> Vec<String> {
        let mut channels: Vec<String> = subscriptions
            .iter()
            .filter(|s| s.series == Series::Quotes)
            .filter_map(|s| match self.index_currency(&s.ticker) {
                Some(currency) => Some(wire::index_channel(currency)),
                None => self
                    .symbols
                    .venue_symbol_for(&s.ticker)
                    .map(wire::ticker_channel),
            })
            .collect();
        channels.sort();
        channels.dedup();
        if channels.is_empty() {
            return Vec::new();
        }
        vec![wire::subscribe_frame(&channels)]
    }
}

/// What the venue says of one declared option, read from its reference answer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Standing {
    /// Listed and active.
    Live,
    /// Known to the venue and no longer active: it expired, or was delisted.
    Left {
        /// When the venue says it expires, in microseconds.
        expiry_micros: i64,
    },
}

/// Judge one reference answer at boot.
///
/// `Ok` for an option the venue knows, live or not. `Err`, in words an
/// operator can act on, for everything a restart would not fix: a name the
/// venue has never listed, an answer that is not an option, and an index that
/// is not the one this adapter composes.
pub fn standing(name: &str, bytes: &[u8]) -> Result<Standing, String> {
    let answer: wire::InstrumentAnswer = serde_json::from_slice(bytes).map_err(|e| {
        format!("{VENUE}: the answer for {name} is not what the venue documents: {e}")
    })?;
    let spec = match (answer.result, answer.error) {
        (Some(spec), _) => spec,
        (None, Some(error)) if error.code == wire::INSTRUMENT_NOT_FOUND => {
            return Err(format!(
                "{VENUE}: {name:?} is not an instrument the venue has ever listed. The venue \
                 acknowledges a subscription to an unknown name and then sends nothing, so it is \
                 refused here rather than captured as silence. Expected {NAME_FORM}"
            ));
        }
        (None, Some(error)) => {
            return Err(format!(
                "{VENUE}: the venue refused to answer for {name}: {} ({})",
                error.message, error.code
            ));
        }
        (None, None) => {
            return Err(format!(
                "{VENUE}: the answer for {name} holds neither a result nor an error"
            ));
        }
    };
    let Some(details) = spec.option_details else {
        return Err(format!(
            "{VENUE}: {name} is a {}, and this capture follows options",
            spec.instrument_type
        ));
    };
    // The index ticker is composed from the name's currency. If the venue
    // names another, every index row this capture wrote would be filed under a
    // ticker the venue does not use.
    let composed = OptionName::parse(name)?.index_ticker();
    if details.index != composed {
        return Err(format!(
            "{VENUE}: {name} settles against {:?}, and this adapter would file its index under \
             {composed:?}",
            details.index
        ));
    }
    if spec.is_active {
        Ok(Standing::Live)
    } else {
        Ok(Standing::Left {
            expiry_micros: galata_wire::secs_to_micros(details.expiry),
        })
    }
}

/// What the venue says of one underlying's perpetual.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PerpetualStanding {
    /// Listed and active.
    Live,
    /// Known to the venue and not active. Its answer is taken through the one
    /// path, and the loop retires it from what that answer says.
    Inactive,
    /// The venue has no instrument of this name.
    Absent,
}

/// Judge a perpetual's reference answer at boot.
///
/// **An absent perpetual is not a refusal.** The options are what the
/// operator declared, and a hedge instrument the venue does not list is a
/// reason to capture them without it, said on the status surface. `Err` only
/// for an answer that cannot be read at all, which is the venue not answering
/// rather than answering no.
pub fn perpetual_standing(name: &str, bytes: &[u8]) -> Result<PerpetualStanding, String> {
    let answer: wire::InstrumentAnswer = serde_json::from_slice(bytes).map_err(|e| {
        format!("{VENUE}: the answer for {name} is not what the venue documents: {e}")
    })?;
    match (answer.result, answer.error) {
        (Some(spec), _) if spec.is_active => Ok(PerpetualStanding::Live),
        (Some(_), _) => Ok(PerpetualStanding::Inactive),
        (None, Some(error)) if error.code == wire::INSTRUMENT_NOT_FOUND => {
            Ok(PerpetualStanding::Absent)
        }
        (None, Some(error)) => Err(format!(
            "{VENUE}: the venue refused to answer for {name}: {} ({})",
            error.message, error.code
        )),
        (None, None) => Err(format!(
            "{VENUE}: the answer for {name} holds neither a result nor an error"
        )),
    }
}

/// Judge the whole declared set at boot.
///
/// A set with at least one live option starts: the ones that left are retired
/// by the capture loop when it takes their reference answers, with the reason
/// on the status surface. **A set with none refuses**, naming each, because a
/// capture of nothing is a configuration that will never fix itself — the
/// tree's standing rule for one.
pub fn judge_set(standings: &[(String, Standing)]) -> Result<(), String> {
    if standings.is_empty() || standings.iter().any(|(_, s)| *s == Standing::Live) {
        return Ok(());
    }
    let named: Vec<String> = standings
        .iter()
        .map(|(name, standing)| match standing {
            Standing::Left { expiry_micros } => {
                format!(
                    "{name} (expiry {})",
                    crate::calendar::date_of(*expiry_micros)
                )
            }
            Standing::Live => name.clone(),
        })
        .collect();
    Err(format!(
        "{VENUE}: every declared option has left the venue's listing, so there is nothing to \
         capture: {}. Declare options that are live",
        named.join(", ")
    ))
}

/// A reference answer, as the payload the one path takes.
///
/// Classified under [`REFERENCE_CHANNEL`] and the instrument's own name, so
/// the record says whose reference data it is without parsing it.
pub fn reference_page(name: &str, bytes: Vec<u8>, recv_micros: i64) -> Payload {
    Payload {
        seq: 0,
        recv_micros,
        address: PayloadAddress::Venue(VENUE.into()),
        channel: REFERENCE_CHANNEL.to_string(),
        kind: galata_wire::Kind::Instruments.as_str().to_string(),
        symbol: Some(name.to_string()),
        origin: Origin::Fetched,
        payload: bytes,
    }
}

#[cfg(test)]
mod tests;
