//! One pure function from a raw payload to the events it carries.
//!
//! No transport, no clock. The only inputs are the payload and its receipt
//! time; the only outputs are shared events. That is what lets the live path, a
//! walk and replay share it — and what makes a replayed frame and a live frame
//! provably identical rather than conventionally so.
//!
//! A failure here is not an exception in the caller's sense: the payload is
//! already recorded, and the caller emits an anomaly naming the row holding its
//! bytes.

use galata_wire::{
    Candle, Envelope, Event, Funding, Mark, Quote, Side, Ticker, Trade, Venue, millis_to_micros,
};

use super::wire::{
    AssetCtx, FundingRow, Peek, WsActiveAssetCtx, WsBbo, WsCandle, WsTrade, is_control_frame,
};
use crate::normalise::NormaliseError;
use crate::record::Payload;
use crate::venue::Symbols;

/// Normalise a payload into the events it carries.
pub fn normalise(
    venue: &Venue,
    symbols: &Symbols,
    raw: &Payload,
) -> Result<Vec<Envelope>, NormaliseError> {
    // One borrowed peek — the channel, and `data` as the span it occupies — and
    // then one typed parse of that span alone. A subscription frame carries
    // `channel` at the top level; a bare data payload, as a fetched page is,
    // does not. No tree is built on the way.
    let peek: Option<Peek<'_>> = serde_json::from_slice(&raw.payload).ok();
    let whole;
    let (channel, data): (&str, &str) = match peek.as_ref().map(|p| (&p.channel, p.data)) {
        Some((Some(channel), Some(data))) if data.get() != "null" => (channel.as_ref(), data.get()),
        _ => {
            whole = std::str::from_utf8(&raw.payload)
                .map_err(|e| NormaliseError::Json(e.to_string()))?;
            (raw.channel.as_str(), whole)
        }
    };

    if is_control_frame(channel) {
        return Ok(Vec::new());
    }

    fn typed<T: serde::de::DeserializeOwned>(span: &str) -> Result<T, NormaliseError> {
        serde_json::from_str(span).map_err(|e| NormaliseError::Json(e.to_string()))
    }

    match channel {
        "trades" => {
            let trades: Vec<WsTrade> = typed(data)?;
            trades
                .into_iter()
                .map(|t| trade(venue, symbols, raw, t))
                .collect()
        }
        "bbo" => {
            let top: WsBbo = typed(data)?;
            Ok(vec![quote(venue, symbols, raw, top)?])
        }
        "candle" => {
            let c: WsCandle = typed(data)?;
            Ok(vec![candle(venue, symbols, raw, c, false)?])
        }
        "candleSnapshot" => {
            let candles: Vec<WsCandle> = typed(data)?;
            candles
                .into_iter()
                // A bar from a walk is final: the range it covers is closed and
                // the venue will not revise it.
                .map(|c| candle(venue, symbols, raw, c, true))
                .collect()
        }
        // A walked page: the rate that settled at each hour, one event per row
        // at the row's own time.
        "fundingHistory" => {
            let rows: Vec<FundingRow> = typed(data)?;
            rows.into_iter()
                .map(|row| {
                    let ticker = resolve(symbols, "fundingHistory", &row.coin)?;
                    Ok(Envelope::new(
                        venue.clone(),
                        ticker,
                        Some(millis_to_micros(row.time)),
                        raw.recv_micros,
                        Event::Funding(Funding {
                            rate: row.funding_rate.require("funding rate")?,
                            next_micros: None,
                        }),
                    ))
                })
                .collect()
        }
        "activeAssetCtx" => {
            let ctx: WsActiveAssetCtx = typed(data)?;
            asset_ctx(venue, symbols, raw, &ctx.coin, &ctx.ctx)
        }
        other => Err(NormaliseError::UnknownChannel(other.to_string())),
    }
}

fn resolve(symbols: &Symbols, channel: &str, venue_symbol: &str) -> Result<Ticker, NormaliseError> {
    symbols
        .resolve(channel, venue_symbol)
        .cloned()
        .ok_or_else(|| NormaliseError::Shape {
            kind: "symbol",
            detail: format!("{venue_symbol:?} on {channel} resolves to no declared ticker"),
        })
}

fn trade(
    venue: &Venue,
    symbols: &Symbols,
    raw: &Payload,
    t: WsTrade,
) -> Result<Envelope, NormaliseError> {
    let ticker = resolve(symbols, "trades", &t.coin)?;
    // The venue's own convention, translated once, here — `B` is a buy
    // crossing, `A` a sell. Anything else is a shape we do not know, refused
    // rather than guessed at.
    let aggressor = match t.side.as_str() {
        "B" => Side::Bid,
        "A" => Side::Ask,
        other => {
            return Err(NormaliseError::Shape {
                kind: "trade side",
                detail: format!("{other:?} is neither B nor A"),
            });
        }
    };
    Ok(Envelope::new(
        venue.clone(),
        ticker,
        Some(millis_to_micros(t.time)),
        raw.recv_micros,
        Event::Trade(Trade {
            price: t.px.require("trade price")?,
            size: t.sz.require("trade size")?,
            aggressor,
            trade_id: t.tid.map(|id| id.to_string()),
        }),
    ))
}

fn quote(
    venue: &Venue,
    symbols: &Symbols,
    raw: &Payload,
    top: WsBbo,
) -> Result<Envelope, NormaliseError> {
    let ticker = resolve(symbols, "bbo", &top.coin)?;

    // `bbo` is a flat `[bid, ask]`. A side may be absent, and that is a real
    // state — an empty book side, not a defect — so it yields `None` rather
    // than a zero nobody quoted.
    let best = |side: usize| -> Result<(Option<_>, Option<_>), NormaliseError> {
        match top.bbo.get(side) {
            Some(Some(level)) => Ok((
                Some(level.px.require("quote price")?),
                Some(level.sz.require("quote size")?),
            )),
            _ => Ok((None, None)),
        }
    };
    let (bid_px, bid_sz) = best(0)?;
    let (ask_px, ask_sz) = best(1)?;

    Ok(Envelope::new(
        venue.clone(),
        ticker,
        Some(millis_to_micros(top.time)),
        raw.recv_micros,
        Event::Quote(Quote {
            bid_px,
            ask_px,
            bid_sz,
            ask_sz,
            // This venue states no spread of its own; a broker that quotes one
            // fills these. `None` means THIS VENUE NEVER STATES IT.
            bid_spread: None,
            ask_spread: None,
        }),
    ))
}

fn candle(
    venue: &Venue,
    symbols: &Symbols,
    raw: &Payload,
    c: WsCandle,
    is_final: bool,
) -> Result<Envelope, NormaliseError> {
    let ticker = resolve(symbols, "candle", &c.s)?;
    Ok(Envelope::new(
        venue.clone(),
        ticker,
        // The bar's OPEN time.
        Some(millis_to_micros(c.t)),
        raw.recv_micros,
        Event::Candle(Candle {
            interval: c.i,
            open: c.o.require("candle open")?,
            high: c.h.require("candle high")?,
            low: c.l.require("candle low")?,
            close: c.c.require("candle close")?,
            volume: c.v.require("candle volume")?,
            trade_count: c.n,
            is_final,
        }),
    ))
}

/// An asset context carries a mark and, separately, a funding rate. **Two
/// events, not one**: they are different datasets, and folding them together
/// would put a funding rate in a row whose other columns are prices.
fn asset_ctx(
    venue: &Venue,
    symbols: &Symbols,
    raw: &Payload,
    coin: &str,
    ctx: &AssetCtx,
) -> Result<Vec<Envelope>, NormaliseError> {
    let ticker = resolve(symbols, "activeAssetCtx", coin)?;
    let mut out = Vec::new();

    let mark = Mark {
        mark: ctx
            .mark_px
            .as_ref()
            .map(|t| t.require("mark"))
            .transpose()?,
        // On this venue the oracle comes from validators and drives funding,
        // while the mark is derived from it together with the book and drives
        // liquidation. Four different numbers, none derivable from another,
        // each carried as the venue printed it.
        oracle: ctx
            .oracle_px
            .as_ref()
            .map(|t| t.require("oracle"))
            .transpose()?,
        // This channel prints no index. `midPx` is the book's midpoint, and
        // filing it as the index made every mark-to-index basis a mark-to-mid
        // one (galata-research, 2026-09-25).
        index: None,
        mid: ctx.mid_px.as_ref().map(|t| t.require("mid")).transpose()?,
        premium: ctx
            .premium
            .as_ref()
            .map(|t| t.require("premium"))
            .transpose()?,
        open_interest: ctx
            .open_interest
            .as_ref()
            .map(|t| t.require("open interest"))
            .transpose()?,
    };
    if mark != Mark::default() {
        out.push(Envelope::new(
            venue.clone(),
            ticker.clone(),
            None,
            raw.recv_micros,
            Event::Mark(mark),
        ));
    }

    if let Some(rate) = &ctx.funding {
        out.push(Envelope::new(
            venue.clone(),
            ticker,
            None,
            raw.recv_micros,
            Event::Funding(Funding {
                rate: rate.require("funding rate")?,
                next_micros: None,
            }),
        ));
    }

    Ok(out)
}
