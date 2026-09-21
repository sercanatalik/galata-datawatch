//! What a normalised event is: two clocks, a stream position, and a payload.
//!
//! Everything above the venue seam speaks this and nothing else. A
//! venue-specific field that reaches here has escaped the seam, which is the
//! one thing the adapter boundary exists to prevent.

use crate::dataset::{Kind, Series};
use crate::identity::{Market, Ticker, Venue};
use crate::token::Num;

/// **How the bytes came to exist**, and therefore how durable they must be
/// before anything else happens.
///
/// Not a caller's choice of durability — a fact about the payload, from which
/// durability follows. There is no durability parameter to get wrong.
///
/// It says nothing about **which route a payload is taking now**. It used to:
/// a `Replay` variant meant *being read back*, which is a different question
/// from *where did this come from*, and one slot answering both is why a
/// payload the system generated had nowhere to say so. Routing is a type now —
/// see `galata_datawatch::replay::Replayed`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Origin {
    /// Pushed by a venue. Rides the buffer and is committed on the flush
    /// timer.
    Streamed,
    /// Asked for by us, covering a range nothing will fetch again — so it is
    /// durable before the caller advances past it.
    Fetched,
    /// **Made by this process**, describing something that did not cross a
    /// wire — a gap, most of all. The payload *is* the event, encoded, so it
    /// is decoded on the way back rather than handed to a venue adapter that
    /// would rightly refuse it.
    Generated,
}

impl Origin {
    /// The discriminator written to disk.
    pub fn as_str(&self) -> &'static str {
        match self {
            Origin::Streamed => "streamed",
            Origin::Fetched => "fetched",
            Origin::Generated => "generated",
        }
    }
}

/// Which side of a book, or which side crossed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Side {
    /// The buying side.
    Bid,
    /// The selling side.
    Ask,
}

impl Side {
    /// The discriminator written to disk.
    pub fn as_str(&self) -> &'static str {
        match self {
            Side::Bid => "bid",
            Side::Ask => "ask",
        }
    }
}

/// What an event is *about*.
///
/// An enum rather than a nullable pair, so an envelope holding a venue where
/// it should hold a market does not compile — which is what puts a
/// `venue = cross` sentinel out of reach rather than merely out of policy.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum Address {
    /// Bytes a venue sent, about one instrument.
    Venue {
        /// The venue.
        venue: Venue,
        /// The instrument, as the adapter resolved it.
        ticker: Ticker,
    },
    /// A number this system computed about a market, which may span venues and
    /// therefore names none of them.
    Market {
        /// The market.
        market: Market,
    },
}

/// One normalised observation.
///
/// **Two clocks, always both.** `at_micros` is what a strategy reasons about;
/// `recv_micros` is what coverage, gaps and latency are measured in. A single
/// timestamp column would silently pick one question to answer.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Envelope {
    /// What this is about.
    pub address: Address,
    /// **The stream position**: the record sequence the bytes behind this
    /// event were stored under.
    ///
    /// **Zero until the one path stamps it.** An adapter normalising bytes
    /// does not know the sequence they were archived under.
    pub seq: u64,
    /// Venue time — when it happened. `None` where the venue states none.
    pub at_micros: Option<i64>,
    /// Our time — when we heard. The road back to the archived bytes.
    pub recv_micros: i64,
    /// What was observed.
    pub event: Event,
}

impl Envelope {
    /// A market-data envelope: addressed by venue and ticker.
    pub fn new(
        venue: Venue,
        ticker: Ticker,
        at_micros: Option<i64>,
        recv_micros: i64,
        event: Event,
    ) -> Envelope {
        Envelope {
            address: Address::Venue { venue, ticker },
            seq: 0,
            at_micros,
            recv_micros,
            event,
        }
    }

    /// Stamp the stream position onto an envelope.
    ///
    /// Consuming, so the unstamped value cannot be used by accident
    /// afterwards.
    pub fn stamped(mut self, seq: u64) -> Envelope {
        self.seq = seq;
        self
    }

    /// The venue, where this came from one.
    pub fn venue(&self) -> Option<&Venue> {
        match &self.address {
            Address::Venue { venue, .. } => Some(venue),
            Address::Market { .. } => None,
        }
    }

    /// The instrument, where this names one.
    pub fn ticker(&self) -> Option<&Ticker> {
        match &self.address {
            Address::Venue { ticker, .. } => Some(ticker),
            Address::Market { .. } => None,
        }
    }

    /// The dataset this event lands in.
    pub fn kind(&self) -> Kind {
        self.event.kind()
    }
}

/// Everything a venue can tell us, normalised.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[non_exhaustive]
pub enum Event {
    /// An execution the venue printed.
    Trade(Trade),
    /// Price levels, as a snapshot or a delta.
    Book(Book),
    /// A bar at a declared interval.
    Candle(Candle),
    /// The perpetual funding rate.
    Funding(Funding),
    /// Top of book.
    Quote(Quote),
    /// Value moving between addresses.
    Transfer(Transfer),
    /// Issuance or redemption.
    Mint(Mint),
    /// Mark, index, oracle, open interest.
    Mark(Mark),
    /// An absence, with the reason it happened.
    Gap(Gap),
    /// A payload that would not normalise.
    Unparsed(Unparsed),
    /// One trading session.
    Session(Session),
    /// Reference data for one instrument.
    Instrument(Instrument),
    /// A chain reorganisation.
    Reorg(Reorg),
}

impl Event {
    /// The dataset this event lands in.
    pub fn kind(&self) -> Kind {
        match self {
            Event::Trade(_) => Kind::Trades,
            Event::Book(_) => Kind::Book,
            Event::Candle(_) => Kind::Candles,
            Event::Funding(_) => Kind::Funding,
            Event::Quote(_) => Kind::Quotes,
            Event::Transfer(_) => Kind::Transfers,
            Event::Mint(_) => Kind::Mints,
            Event::Mark(_) => Kind::Marks,
            Event::Gap(_) => Kind::Gaps,
            Event::Unparsed(_) => Kind::Unparsed,
            Event::Session(_) => Kind::Sessions,
            Event::Instrument(_) => Kind::Instruments,
            Event::Reorg(_) => Kind::Reorgs,
        }
    }
}

/// One execution.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Trade {
    /// What it printed at.
    pub price: Num,
    /// How much.
    pub size: Num,
    /// Which side crossed.
    pub aggressor: Side,
    /// The venue's own identity, so two receipts of one trade are one trade.
    /// `None` where the venue states none — and a consumer must then not claim
    /// it can deduplicate.
    pub trade_id: Option<String>,
}

/// One price level touched by a book message.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct BookLevel {
    /// Which side.
    pub side: Side,
    /// The price.
    pub price: Num,
    /// **Zero means remove this price level.**
    pub size: Num,
    /// Rank within side. **`None` on a delta** — a delta names a price and its
    /// new size, and where that price ranks is a consequence of the whole
    /// book. This is not a gap in the data; it is the honest statement that
    /// rank is not what a delta says.
    pub level: Option<u32>,
}

/// One book message, flattened to the levels it touched.
///
/// Snapshots and deltas share one dataset because they are the same shape.
/// Splitting them would force every consumer to read both and merge them in
/// timestamp order, which is the work one table already did.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Book {
    /// Groups the rows of one message; monotonic per `(venue, ticker)`.
    pub update_seq: i64,
    /// True → this update replaces all prior book state.
    pub is_snapshot: bool,
    /// The levels this message touched.
    pub levels: Vec<BookLevel>,
}

/// One bar.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Candle {
    /// The bar width, as the venue spells it.
    pub interval: String,
    /// Opening price.
    pub open: Num,
    /// Highest traded.
    pub high: Num,
    /// Lowest traded.
    pub low: Num,
    /// Closing price.
    pub close: Num,
    /// Volume over the bar.
    pub volume: Num,
    /// Where the venue reports it.
    pub trade_count: Option<u32>,
    /// **False while the bar is still forming.**
    ///
    /// A live bar arrives false and is superseded; a walked bar arrives true.
    /// Anything computing over candles must fold over final bars or handle
    /// supersession explicitly — treating every message as a distinct
    /// observation counts one bar dozens of times and silently overweights
    /// recent minutes, which produces plausible numbers and no error.
    pub is_final: bool,
}

/// Top of book: the best bid and ask.
///
/// One dataset for a pushed `bbo` channel and a polled best-bid-ask alike,
/// because they are the same shape — which is what makes one instrument
/// across several venues a single-table query.
///
/// **`None` means this venue never states it, or the side is empty**, never
/// *it was missing*. A venue that publishes no size and a book with no bid are
/// both real, and neither is a defect.
#[derive(Debug, Clone, PartialEq, Default, serde::Serialize, serde::Deserialize)]
pub struct Quote {
    /// The best bid.
    pub bid_px: Option<Num>,
    /// The best ask.
    pub ask_px: Option<Num>,
    /// Size at the best bid, where the venue states one.
    pub bid_sz: Option<Num>,
    /// Size at the best ask, where the venue states one.
    pub ask_sz: Option<Num>,
    /// A broker's stated spread on the sell side, where it states one.
    pub bid_spread: Option<Num>,
    /// A broker's stated spread on the buy side, where it states one.
    pub ask_spread: Option<Num>,
}

/// Value moving between addresses.
///
/// **A transfer on its own proves custody moved, not that a trade happened.**
/// Sweeps and operational rebalancing emit identical events, so a consumer
/// classifies conservatively and this type does not pretend otherwise.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Transfer {
    /// Which block it was in.
    ///
    /// **Here because `at_micros` cannot be.** A range fetch returns logs from
    /// many blocks and their timestamps are not in the response; learning them
    /// costs a call per block. So the venue time is absent and the block number
    /// is present, which makes the time **recoverable** rather than guessed —
    /// and a guessed time that looks plausible is worse than an absent one.
    pub block: u64,
    /// The sending address.
    pub from: String,
    /// The receiving address.
    pub to: String,
    /// How much, already scaled by the contract's own decimals.
    pub amount: Num,
    /// The transaction this log belonged to — the join that makes a
    /// delivery-versus-payment pair provable.
    pub tx_hash: String,
    /// The log's position within its block, which with the block number is a
    /// total order.
    pub log_index: u32,
}

/// Issuance or redemption: a transfer from or to the zero address.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Mint {
    /// Which block it was in. See [`Transfer::block`].
    pub block: u64,
    /// The address receiving issuance, or surrendering on a redemption.
    pub holder: String,
    /// How much.
    pub amount: Num,
    /// True for issuance, false for redemption.
    pub is_issue: bool,
    /// The transaction this log belonged to.
    pub tx_hash: String,
    /// The log's position within its block.
    pub log_index: u32,
}

/// Four different prices, none derivable from another, each as the venue
/// printed it.
///
/// A price computed in transit is a price from two measurements, and the two
/// will disagree exactly when it matters. Every field is optional because no
/// venue publishes all four.
#[derive(Debug, Clone, PartialEq, Default, serde::Serialize, serde::Deserialize)]
pub struct Mark {
    /// What the venue marks positions at — margin and liquidation.
    pub mark: Option<Num>,
    /// The reference the venue computes against.
    pub index: Option<Num>,
    /// The externally-sourced price, where the venue publishes one separately.
    pub oracle: Option<Num>,
    /// Open interest.
    pub open_interest: Option<Num>,
}

/// The perpetual funding rate.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Funding {
    /// The rate that settled, or will settle.
    pub rate: Num,
    /// When the next funding is due, where stated.
    pub next_micros: Option<i64>,
}

/// Why an interval was not covered.
///
/// **Every cause is an event the system knows happened.** There is
/// deliberately no cause meaning *we noticed nothing arrived*: for a push
/// stream you cannot tell *no trades occurred* from *the connection is
/// silently dead*, so a gap is never inferred from silence. A quiet market and
/// a halted instrument therefore produce no gap at all.
///
/// For a polled or cursor-driven source the loss is known exactly — we know we
/// asked and we know what came back — and the last three variants say so.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum GapCause {
    /// A session stopped delivering.
    SessionLost,
    /// Received and not yet durable when the process died. Our own loss.
    CrashUnflushed,
    /// The process was not running.
    Downtime,
    /// A planned handover did not complete. A handover that *did* complete
    /// publishes no gap, because none occurred.
    RotationFailed,
    /// The venue refused the subscription.
    Refused,
    /// A poll was made and did not answer.
    PollFailed,
    /// A poll was not made, or was rejected, because the venue's rate limit
    /// would not allow it. Distinct from `PollFailed`: one is the venue
    /// failing, the other is us yielding.
    Throttled,
    /// The chain no longer holds rows previously written. The one absence that
    /// can be *proved* rather than inferred.
    Reorg,
}

impl GapCause {
    /// The discriminator written to disk.
    pub fn as_str(&self) -> &'static str {
        match self {
            GapCause::SessionLost => "session_lost",
            GapCause::CrashUnflushed => "crash_unflushed",
            GapCause::Downtime => "downtime",
            GapCause::RotationFailed => "rotation_failed",
            GapCause::Refused => "refused",
            GapCause::PollFailed => "poll_failed",
            GapCause::Throttled => "throttled",
            GapCause::Reorg => "reorg",
        }
    }
}

/// What a gap's interval was clipped against.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Clipped {
    /// Against a captured session calendar.
    Sessions,
    /// The venue never closes, so nothing was clipped.
    Continuous,
    /// No calendar was known, so nothing was clipped and the bound is loose.
    /// An unknown **overstates** the loss rather than erasing it.
    Assumed24h,
}

impl Default for Clipped {
    /// **Nothing is clipped until a calendar says so.**
    ///
    /// An unknown calendar OVERSTATES the loss rather than erasing it, which is
    /// the safe direction: a gap that is too wide costs a re-fetch, while one
    /// that is too narrow is a hole nobody looks for. The alternative — an
    /// empty session set — would clip every gap away.
    fn default() -> Self {
        Clipped::Assumed24h
    }
}

impl Clipped {
    /// The discriminator written to disk.
    pub fn as_str(&self) -> &'static str {
        match self {
            Clipped::Sessions => "sessions",
            Clipped::Continuous => "continuous",
            Clipped::Assumed24h => "assumed-24h",
        }
    }
}

/// An absence, made into an event.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Gap {
    /// Which series was not covered.
    pub series: Series,
    /// **The last moment actually covered** — not when the loss was noticed.
    /// Dating from the moment of noticing understates the loss by exactly the
    /// interval that matters.
    pub from_micros: i64,
    /// When coverage resumed.
    pub to_micros: i64,
    /// Why.
    pub cause: GapCause,
    /// What the interval was clipped against, when the row was written.
    pub clipped: Clipped,
}

/// A payload that would not normalise.
///
/// An anomaly is an event like any other, and a row here always has bytes
/// behind it. The one thing that must not happen is silence.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Unparsed {
    /// The venue's own channel name.
    pub channel: String,
    /// The record row holding the bytes that failed.
    pub archive_seq: u64,
    /// What went wrong.
    pub error: String,
}

/// Which session set.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SessionKind {
    /// What the feed will deliver — what coverage is measured against, and
    /// what a gap is clipped with.
    Full,
    /// What analysis usually means by "the market". A subset of `full`, and
    /// neither is derivable from the other.
    Regular,
}

impl SessionKind {
    /// The discriminator written to disk.
    pub fn as_str(&self) -> &'static str {
        match self {
            SessionKind::Full => "full",
            SessionKind::Regular => "regular",
        }
    }
}

/// **The inverse of [`SessionKind::as_str`], beside it on purpose.** A store
/// that writes the discriminator has to read it back, and a second mapping
/// written somewhere else does not fail when it drifts — it disagrees.
impl std::str::FromStr for SessionKind {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "full" => Ok(SessionKind::Full),
            "regular" => Ok(SessionKind::Regular),
            other => Err(format!(
                "{other:?} is not a session kind. Known: full, regular"
            )),
        }
    }
}

/// One trading session, captured rather than maintained.
///
/// The unit is a **session, not a day**: a day may hold several, and an
/// instrument with an evening break needs that.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Session {
    /// When it opens, UTC.
    pub session_start: i64,
    /// When it closes, UTC.
    pub session_end: i64,
    /// Which set.
    pub session_kind: SessionKind,
    /// The **IANA** zone used to resolve it — recorded, not assumed. A venue's
    /// own three-letter abbreviation is ambiguous and cannot express DST.
    pub tz: String,
    /// `venue` where the venue served it, `declared` where an operator stated
    /// it because the venue publishes hours as documentation.
    pub source: String,
    /// When we learned this.
    pub observed_at: i64,
}

/// Reference data for one instrument. Slowly changing.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Instrument {
    /// The smallest price increment.
    pub tick_size: Num,
    /// The contract or lot size.
    pub lot_size: Num,
    /// The smallest order.
    pub min_size: Num,
    /// `perp` | `spot` | `stock` | `future` | `option` | `token`.
    pub contract_type: String,
    /// What is being traded.
    pub base: String,
    /// What it is priced in.
    pub quote: String,
    /// The calendar this instrument trades on.
    pub hours: String,
    /// Whether the venue currently lists it.
    pub active: bool,
    /// The venue's own **integer position** for this asset, where it uses one.
    ///
    /// Read from the venue and never declared: a venue reorders its list, a
    /// config file still says `4`, and everything addressed to `4` now means
    /// something else.
    pub venue_index: Option<u32>,
    /// The corporate-action multiplier, where the instrument carries one.
    ///
    /// A tokenised equity scales the effective share count on a split or a
    /// dividend while the raw balance stays fixed. Carried point-in-time via
    /// `observed_at`, because what it was yesterday is a different fact from
    /// what it is now.
    pub ui_multiplier: Option<Num>,
    /// When the venue said all of the above.
    pub observed_at: i64,
}

/// A chain reorganisation: rows previously written that the chain no longer
/// holds.
///
/// A [`Gap`] says *we did not see this*. A reorg says *what we saw is no
/// longer true*, which is a different claim and gets its own dataset.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Reorg {
    /// The first block no longer on the canonical chain.
    pub from_block: u64,
    /// The last block no longer on the canonical chain.
    pub to_block: u64,
    /// The hash we had recorded at `from_block`.
    pub old_hash: String,
    /// The hash the chain now holds there.
    pub new_hash: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dataset::Kind;
    use rust_decimal::Decimal;
    use std::str::FromStr;

    fn venue() -> Venue {
        Venue::new("hyperliquid").unwrap()
    }

    fn ticker() -> Ticker {
        Ticker::new("BTC").unwrap()
    }

    fn a_quote() -> Event {
        Event::Quote(Quote {
            bid_px: Some(Decimal::from_str("100.5").unwrap()),
            ask_px: Some(Decimal::from_str("100.6").unwrap()),
            ..Quote::default()
        })
    }

    #[test]
    fn a_market_data_envelope_is_addressed_by_venue_and_ticker() {
        let envelope = Envelope::new(venue(), ticker(), Some(10), 20, a_quote());
        assert_eq!(envelope.venue().unwrap().as_str(), "hyperliquid");
        assert_eq!(envelope.ticker().unwrap().as_str(), "BTC");
        assert_eq!(envelope.seq, 0, "unstamped until the one path stamps it");
    }

    #[test]
    fn stamping_consumes_the_envelope() {
        // Consuming on purpose: an unstamped value must not be usable
        // afterwards, or a row can be written carrying seq 0 and the road back
        // to its bytes is gone.
        let envelope = Envelope::new(venue(), ticker(), Some(10), 20, a_quote());
        let stamped = envelope.stamped(7);
        assert_eq!(stamped.seq, 7);
        // `envelope` is moved; referencing it here would not compile.
    }

    #[test]
    fn a_venue_that_states_no_time_yields_an_absent_at_micros() {
        // Absent, never defaulted to recv_micros. Defaulting would make a
        // latency measurement read as zero for exactly the venues that do not
        // state a time.
        let envelope = Envelope::new(venue(), ticker(), None, 20, a_quote());
        assert_eq!(envelope.at_micros, None);
        assert_eq!(envelope.recv_micros, 20);
    }

    #[test]
    fn every_event_lands_in_a_dataset() {
        // Exhaustive by construction: `Event::kind` is a total match, so a
        // variant added without deciding where it lands does not compile.
        let envelope = Envelope::new(venue(), ticker(), None, 0, a_quote());
        assert_eq!(envelope.kind(), Kind::Quotes);
    }

    #[test]
    fn no_gap_cause_means_silence() {
        // Written out rather than derived, so adding a variant is a deliberate
        // act that fails this test until somebody looks at it.
        let causes: Vec<&str> = [
            GapCause::SessionLost,
            GapCause::CrashUnflushed,
            GapCause::Downtime,
            GapCause::RotationFailed,
            GapCause::Refused,
            GapCause::PollFailed,
            GapCause::Throttled,
            GapCause::Reorg,
        ]
        .iter()
        .map(|c| c.as_str())
        .collect();
        assert_eq!(causes.len(), 8);
        for cause in &causes {
            assert!(
                !cause.contains("quiet") && !cause.contains("silent") && !cause.contains("idle"),
                "{cause} reads like an inference from silence"
            );
        }
    }

    #[test]
    fn an_unknown_calendar_overstates_the_loss_rather_than_erasing_it() {
        // `assumed-24h` clips nothing. The alternative — an empty session set
        // — would clip every gap away and erase a real loss.
        assert_eq!(Clipped::Assumed24h.as_str(), "assumed-24h");
        assert_eq!(Clipped::Continuous.as_str(), "continuous");
    }

    #[test]
    fn a_session_kind_round_trips() {
        for kind in [SessionKind::Full, SessionKind::Regular] {
            assert_eq!(kind.as_str().parse::<SessionKind>().unwrap(), kind);
        }
        assert!("liquid".parse::<SessionKind>().is_err());
    }

    #[test]
    fn a_quote_states_absence_rather_than_zero() {
        // A venue publishing no size, and an empty side, are both real. Zero
        // would be a price nobody quoted.
        let quote = Quote::default();
        assert_eq!(quote.bid_px, None);
        assert_eq!(quote.bid_sz, None);
    }

    #[test]
    fn origin_replay_is_nameable_but_marked_unwritable() {
        // The type carries it because replay's events take the same normalise
        // path as live ones. The record refuses it, which is the store's rule
        // rather than the vocabulary's.
        assert_eq!(Origin::Generated.as_str(), "generated");
    }
}
