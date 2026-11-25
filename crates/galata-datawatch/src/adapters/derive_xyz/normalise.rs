//! One pure function from a raw payload to the events it carries.
//!
//! No transport and no clock, as every adapter's is. One ticker payload is
//! three events about one option — its quote, its marks and the venue's own
//! pricing — because the venue sends them together and a gap in one is a gap
//! in all three.
//!
//! **Nothing is computed.** A greek recomputed here would be a greek from
//! somebody else's model, and the reason to record the venue's is that its
//! margin engine uses the same ones.

use galata_wire::{
    Envelope, Event, Funding, Instrument, Mark, Num, OptionPricing, Quote, Ticker, Token, Venue,
    millis_to_micros, secs_to_micros,
};

use super::wire::{
    ANSWER, INDEX, INSTRUMENT_NOT_FOUND, IndexData, InstrumentAnswer, Pushed, TICKER, TickerData,
    UNKNOWN,
};
use crate::normalise::NormaliseError;
use crate::record::Payload;
use crate::venue::{REFERENCE_CHANNEL, Symbols};

/// Normalise a payload into the events it carries.
pub fn normalise(
    venue: &Venue,
    symbols: &Symbols,
    raw: &Payload,
) -> Result<Vec<Envelope>, NormaliseError> {
    match raw.channel.as_str() {
        TICKER => ticker(venue, symbols, raw),
        INDEX => index(venue, symbols, raw),
        REFERENCE_CHANNEL => reference(venue, symbols, raw),
        // An acknowledgement, or a text `pong`: it arrived and is recorded, and
        // it says nothing about a market.
        ANSWER | UNKNOWN => Ok(Vec::new()),
        other => Err(NormaliseError::UnknownChannel(other.to_string())),
    }
}

fn typed<T: serde::de::DeserializeOwned>(raw: &Payload) -> Result<T, NormaliseError> {
    serde_json::from_slice(&raw.payload).map_err(|e| NormaliseError::Json(e.to_string()))
}

/// The instrument a payload is about, by the symbol it was classified under.
fn resolve(symbols: &Symbols, raw: &Payload) -> Result<Ticker, NormaliseError> {
    let symbol = raw.symbol.as_deref().ok_or(NormaliseError::Shape {
        kind: "symbol",
        detail: format!("a {} payload names no instrument", raw.channel),
    })?;
    symbols
        .resolve(&raw.channel, symbol)
        .cloned()
        .ok_or_else(|| NormaliseError::Shape {
            kind: "symbol",
            detail: format!(
                "{symbol:?} on {} is not an instrument this capture declared",
                raw.channel
            ),
        })
}

/// A number the venue may not have sent.
fn num(token: Option<&Token>, field: &'static str) -> Result<Option<Num>, NormaliseError> {
    token
        .map(|t| t.require(field))
        .transpose()
        .map_err(Into::into)
}

/// One side of the top of book: `(price, amount)`, or absent.
///
/// **The venue prints an empty side as price `0` and amount `0`** (measured
/// 2026-11-24). Nobody bids zero for zero, and [`Quote`] documents `None` as
/// *the side is empty*, so that is what it becomes. A zero price beside a real
/// amount is not that state and is kept as the venue stated it; the archive
/// holds what was sent either way.
fn side(
    price: Option<&Token>,
    amount: Option<&Token>,
    price_field: &'static str,
    amount_field: &'static str,
) -> Result<(Option<Num>, Option<Num>), NormaliseError> {
    let price = num(price, price_field)?;
    let amount = num(amount, amount_field)?;
    let empty = price.is_some_and(|p| p.is_zero()) && amount.is_none_or(|a| a.is_zero());
    if empty {
        return Ok((None, None));
    }
    Ok((price, amount))
}

/// A quoted volatility: absent where its side of the book is.
///
/// The venue prints `0` for the bid volatility of an option nobody bids. A
/// volatility of zero is not a thing a market states, and a consumer averaging
/// it has a number that means nothing — so it is absent when its side is. A
/// zero printed beside a real quote is kept: that would be the venue's claim.
fn quoted_iv(
    iv: Option<&Token>,
    side_present: bool,
    field: &'static str,
) -> Result<Option<Num>, NormaliseError> {
    let iv = num(iv, field)?;
    if !side_present && iv.is_none_or(|v| v.is_zero()) {
        return Ok(None);
    }
    Ok(iv)
}

fn ticker(
    venue: &Venue,
    symbols: &Symbols,
    raw: &Payload,
) -> Result<Vec<Envelope>, NormaliseError> {
    let frame: Pushed<TickerData> = typed(raw)?;
    let top = frame.params.data.instrument_ticker;
    let instrument = resolve(symbols, raw)?;
    let at = Some(millis_to_micros(top.t));

    let (bid_px, bid_sz) = side(
        top.b.as_ref(),
        top.bid_amount.as_ref(),
        "bid price",
        "bid amount",
    )?;
    let (ask_px, ask_sz) = side(
        top.a.as_ref(),
        top.ask_amount.as_ref(),
        "ask price",
        "ask amount",
    )?;

    let envelope = |event| {
        Envelope::new(
            venue.clone(),
            instrument.clone(),
            at,
            raw.recv_micros,
            event,
        )
    };

    let mut events = vec![
        envelope(Event::Quote(Quote {
            bid_px,
            ask_px,
            bid_sz,
            ask_sz,
            // This venue states no spread of its own.
            bid_spread: None,
            ask_spread: None,
        })),
        envelope(Event::Mark(Mark {
            mark: num(top.mark.as_ref(), "mark price")?,
            index: num(top.index.as_ref(), "index price")?,
            // The venue publishes no separate oracle, book midpoint or
            // funding premium for an option.
            oracle: None,
            open_interest: num(
                top.stats.as_ref().and_then(|s| s.oi.as_ref()),
                "open interest",
            )?,
            mid: None,
            premium: None,
        })),
    ];

    // **A perpetual's ticker states its funding rate; an option's states
    // `null`.** The rate as the venue printed it, at the venue's time. The
    // ticker states no next funding time and no premium, so neither is
    // invented.
    if let Some(rate) = num(top.f.as_ref(), "funding rate")? {
        events.push(envelope(Event::Funding(Funding {
            rate,
            next_micros: None,
            premium: None,
        })));
    }

    // `null` for anything that is not an option. The ticker of one is still a
    // quote and a mark, so those two stand without it.
    if let Some(pricing) = top.option_pricing {
        events.push(envelope(Event::OptionPricing(OptionPricing {
            iv: num(pricing.i.as_ref(), "implied volatility")?,
            bid_iv: quoted_iv(pricing.bi.as_ref(), bid_px.is_some(), "bid volatility")?,
            ask_iv: quoted_iv(pricing.ai.as_ref(), ask_px.is_some(), "ask volatility")?,
            // **A greek of zero is a value.** An option far out of the money
            // has a delta of zero, and that is what the venue computed.
            delta: num(pricing.d.as_ref(), "delta")?,
            gamma: num(pricing.g.as_ref(), "gamma")?,
            vega: num(pricing.v.as_ref(), "vega")?,
            theta: num(pricing.t.as_ref(), "theta")?,
            rho: num(pricing.r.as_ref(), "rho")?,
            forward: num(pricing.f.as_ref(), "forward")?,
            discount_factor: num(pricing.df.as_ref(), "discount factor")?,
        })));
    }
    Ok(events)
}

fn index(venue: &Venue, symbols: &Symbols, raw: &Payload) -> Result<Vec<Envelope>, NormaliseError> {
    let frame: Pushed<IndexData> = typed(raw)?;
    let data = frame.params.data;
    let instrument = resolve(symbols, raw)?;
    // The subscription is to one currency, and the frame is keyed by it.
    let currency = raw.symbol.as_deref().unwrap_or_default();
    let feed = data
        .feeds
        .get(currency)
        .ok_or_else(|| NormaliseError::Shape {
            kind: "index",
            detail: format!("the feed for {currency:?} is not in its own frame"),
        })?;
    Ok(vec![Envelope::new(
        venue.clone(),
        instrument,
        Some(millis_to_micros(data.timestamp)),
        raw.recv_micros,
        // Only the index: this is the reference an option's mark is computed
        // against, and the feed states nothing else that is a price now.
        Event::Mark(Mark {
            index: Some(feed.price.require("index price")?),
            ..Mark::default()
        }),
    )])
}

/// `C` and `P`, as this tree spells them.
fn right_of(option_type: &str) -> Result<&'static str, NormaliseError> {
    match option_type {
        "C" => Ok("call"),
        "P" => Ok("put"),
        other => Err(NormaliseError::Shape {
            kind: "instrument",
            detail: format!("option_type {other:?} is neither C nor P"),
        }),
    }
}

fn reference(
    venue: &Venue,
    symbols: &Symbols,
    raw: &Payload,
) -> Result<Vec<Envelope>, NormaliseError> {
    let answer: InstrumentAnswer = typed(raw)?;
    let spec = match (answer.result, answer.error) {
        (Some(spec), _) => spec,
        (None, Some(error)) => {
            // Refused at boot, so reaching here means a name the venue knew an
            // hour ago and does not now. It lands in `unparsed` with its bytes
            // behind it, which is where an answer nobody expected belongs.
            let what = if error.code == INSTRUMENT_NOT_FOUND {
                "the venue does not know this instrument"
            } else {
                "the venue refused the request"
            };
            return Err(NormaliseError::Shape {
                kind: "instrument",
                detail: format!("{what}: {} ({})", error.message, error.code),
            });
        }
        (None, None) => {
            return Err(NormaliseError::Shape {
                kind: "instrument",
                detail: "an answer with neither a result nor an error".into(),
            });
        }
    };
    let instrument = resolve(symbols, raw)?;
    let details = spec.option_details.as_ref();

    Ok(vec![Envelope::new(
        venue.clone(),
        instrument,
        // The venue states no time for reference data; ours is `observed_at`.
        None,
        raw.recv_micros,
        Event::Instrument(Instrument {
            tick_size: spec.tick_size.require("tick size")?,
            lot_size: spec.amount_step.require("amount step")?,
            min_size: spec.minimum_amount.require("minimum amount")?,
            contract_type: spec.instrument_type.clone(),
            base: spec.base_currency.clone(),
            quote: spec.quote_currency.clone(),
            // Measured nowhere yet for an option; every instrument of this
            // venue is quoted around the clock until it expires.
            hours: "continuous".to_string(),
            active: spec.is_active,
            venue_index: None,
            ui_multiplier: None,
            observed_at: raw.recv_micros,
            strike: details.map(|d| d.strike.require("strike")).transpose()?,
            // **The venue's, in seconds.** The name carries a date; the hour is
            // here and nowhere else.
            expiry_micros: details.map(|d| secs_to_micros(d.expiry)),
            right: details
                .map(|d| right_of(&d.option_type).map(str::to_string))
                .transpose()?,
        }),
    )])
}
