//! What a normalised event is: two clocks, a stream position, and a payload.
//!
//! Everything above the venue seam speaks this and nothing else. A
//! venue-specific field that reaches here has escaped the seam, which is the
//! one thing the adapter boundary exists to prevent.

use crate::dataset::{Kind, Series};
use crate::identity::{Account, Market, Ticker, Venue};
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
    /// What a venue said about one account, named by its alias.
    ///
    /// A third shape rather than a ticker standing in for the alias: an
    /// account's margin is about no instrument, and an envelope that claimed
    /// one would be a `ticker = main` sentinel.
    Account {
        /// The venue holding the account.
        venue: Venue,
        /// The account's alias. Never its address.
        account: Account,
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

    /// A ledger envelope: addressed by venue and account alias.
    pub fn for_account(
        venue: Venue,
        account: Account,
        at_micros: Option<i64>,
        recv_micros: i64,
        event: Event,
    ) -> Envelope {
        Envelope {
            address: Address::Account { venue, account },
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
            Address::Venue { venue, .. } | Address::Account { venue, .. } => Some(venue),
            Address::Market { .. } => None,
        }
    }

    /// The instrument, where this names one.
    pub fn ticker(&self) -> Option<&Ticker> {
        match &self.address {
            Address::Venue { ticker, .. } => Some(ticker),
            Address::Market { .. } | Address::Account { .. } => None,
        }
    }

    /// The account, where this is about one.
    pub fn account(&self) -> Option<&Account> {
        match &self.address {
            Address::Account { account, .. } => Some(account),
            Address::Venue { .. } | Address::Market { .. } => None,
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
    /// An account's margin on one dex.
    Margin(Margin),
    /// One open position on one dex.
    Position(Position),
    /// An account the ledger bound to an alias.
    AccountSeen(AccountSeen),
    /// One fill of the account's.
    Fill(Fill),
    /// Funding one position paid or received.
    FundingPayment(FundingPayment),
    /// A deposit, withdrawal, transfer, liquidation or vault move.
    LedgerUpdate(LedgerUpdate),
    /// How far the ledger holds one kind of the account's history.
    EventsReach(EventsReach),
    /// An option's implied volatility and greeks, as the venue computed them.
    OptionPricing(OptionPricing),
    /// One coin an account holds in spot.
    Balance(Balance),
    /// What an account has borrowed and supplied of one token.
    BorrowLend(BorrowLend),
    /// An account's borrowing health, as the venue states it.
    LendHealth(LendHealth),
    /// Whether an account is portfolio-margined, and its ratio, as stated.
    PortfolioMargin(PortfolioMargin),
    /// One token's margin figures for an account, as stated.
    TokenMargin(TokenMargin),
    /// A subaccount's margin figures and margin type, as the venue states them.
    MarginState(MarginState),
    /// One open position of a subaccount, options and perpetuals alike.
    PositionState(PositionState),
    /// One collateral asset of a subaccount, with its margin contributions.
    Collateral(Collateral),
    /// One order resting at the venue, in an account's complete set of them at
    /// one read.
    Order(Order),
    /// One change of an order's status, as the venue states it: the order as
    /// it stood then, its status always stated, its time the envelope's
    /// `at_micros`.
    OrderUpdate(Order),
    /// One point of the account-value and PnL history the venue states.
    PnlPoint(PnlPoint),
    /// **One read's whole set** of an account's state of one kind, as the bus
    /// carries it: the newest message on a subject is the account's state of
    /// that kind, and an empty set says it holds none.
    StateSet(StateSet),
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
            Event::Margin(_) => Kind::Margin,
            Event::Position(_) => Kind::Positions,
            Event::AccountSeen(_) | Event::EventsReach(_) => Kind::Accounts,
            Event::Fill(_) => Kind::Fills,
            Event::FundingPayment(_) => Kind::FundingPayments,
            Event::LedgerUpdate(_) => Kind::LedgerUpdates,
            Event::OptionPricing(_) => Kind::OptionPricing,
            Event::Balance(_) => Kind::Balances,
            Event::BorrowLend(_) => Kind::BorrowLend,
            Event::LendHealth(_) => Kind::LendHealth,
            Event::PortfolioMargin(_) => Kind::PortfolioMargin,
            Event::TokenMargin(_) => Kind::TokenMargin,
            Event::MarginState(_) => Kind::MarginState,
            Event::PositionState(_) => Kind::PositionState,
            Event::Collateral(_) => Kind::Collateral,
            Event::Order(_) => Kind::Orders,
            Event::OrderUpdate(_) => Kind::OrderUpdates,
            Event::PnlPoint(_) => Kind::PnlHistory,
            Event::StateSet(set) => set.kind,
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
    /// A broker's stated spread on the sell side, where it states one, **in
    /// the venue's own unit**. rh-crypto states a percent of the mid, not a
    /// price difference.
    pub bid_spread: Option<Num>,
    /// A broker's stated spread on the buy side, where it states one, in the
    /// venue's own unit.
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

/// The venue's own prices around the mark, none derivable from another, each
/// as the venue printed it.
///
/// A price computed in transit is a price from two measurements, and the two
/// will disagree exactly when it matters. Every field is optional because no
/// venue publishes all of them.
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
    /// The midpoint of the book, where the venue prints one of its own.
    ///
    /// Not the index. Hyperliquid's asset context prints `midPx` and no
    /// index at all, and until 2026-09-25 the adapter filed the one as the
    /// other, so a mark-to-index basis was a mark-to-mid basis.
    #[serde(default)]
    pub mid: Option<Num>,
    /// The premium over the oracle that the venue computes funding from.
    #[serde(default)]
    pub premium: Option<Num>,
}

/// What a venue's own model says about one option: implied volatility and
/// greeks, **each as the venue printed it and none computed here**.
///
/// A greek recomputed in transit is a greek from somebody else's model, and it
/// disagrees with the venue's margin engine exactly when that matters. Every
/// field is optional because a venue need not publish all of them, and because
/// a volatility with no quote behind it is absent rather than zero: the venue
/// this was written for prints `0` for the bid volatility of an option nobody
/// bids, and a consumer averaging that zero has a number that means nothing.
/// **A greek of zero is a value**, and is kept.
///
/// The option's mark is not here. It is a [`Mark`], beside the index and the
/// open interest, where every other instrument's is.
#[derive(Debug, Clone, PartialEq, Default, serde::Serialize, serde::Deserialize)]
pub struct OptionPricing {
    /// Implied volatility at the mark, as a fraction of one per year.
    pub iv: Option<Num>,
    /// Implied volatility at the best bid. Absent where nobody bids.
    pub bid_iv: Option<Num>,
    /// Implied volatility at the best ask. Absent where nobody offers.
    pub ask_iv: Option<Num>,
    /// Change in value per unit move of the underlying.
    pub delta: Option<Num>,
    /// Change in delta per unit move of the underlying.
    pub gamma: Option<Num>,
    /// Change in value per point of volatility, in the venue's own unit.
    pub vega: Option<Num>,
    /// Change in value per day, in the venue's own unit.
    pub theta: Option<Num>,
    /// Change in value per point of rate, in the venue's own unit.
    pub rho: Option<Num>,
    /// The forward the venue priced against.
    pub forward: Option<Num>,
    /// The discount factor the venue priced with.
    pub discount_factor: Option<Num>,
}

/// The perpetual funding rate.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Funding {
    /// The rate that settled, or will settle.
    pub rate: Num,
    /// When the next funding is due, where stated.
    pub next_micros: Option<i64>,
    /// The premium over the oracle the rate was computed from, where the venue
    /// prints it.
    ///
    /// **Not recoverable from the rate.** Hyperliquid's rate is the premium
    /// plus a clamped interest term, so while the clamp does not bind the rate
    /// is the interest floor whatever the premium was: BTC's last 48 hours on
    /// 2026-09-26 settled 13 distinct rates over 48 distinct premiums.
    /// Defaulted, so an envelope serialised before it still reads.
    #[serde(default)]
    pub premium: Option<Num>,
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
    /// The venue no longer holds history between the newest event recorded
    /// and the earliest it now returns. Known, not inferred: a full page that
    /// starts after our newest event is the venue saying so.
    BeyondReach,
    /// **Not a gap in the record.** The bus did not carry these events: they
    /// were archived, and capture could not hand them to the broker — its
    /// queue was full, or a publish was refused. The record holds them, so a
    /// consumer fetches the span from history. Published on the bus only,
    /// never archived, and so never in the tape. Every other cause says the
    /// data exists nowhere; this one says where it is.
    Unpublished,
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
            GapCause::BeyondReach => "beyond_reach",
            GapCause::Unpublished => "unpublished",
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
    /// The dex the gap is about, **where the thing that went uncovered was
    /// asked per dex**: a ledger snapshot is taken per (account, dex), and
    /// the two gaps of one missed pass must be told apart. `None` for a
    /// market-data gap, whose ticker already names its dex's instrument, and
    /// for every row written before the field existed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dex: Option<String>,
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
///
/// **One format for every venue**, stating each venue's rules without loss
/// (`normalise-instrument-reference`). A field the venue does not state is
/// absent, never a guess: Derive states a fixed tick and a size minimum, and
/// leaves `price_figures`, `min_notional` and the leverage fields empty;
/// Hyperliquid states a significant-figure rule and a value minimum, and
/// fills them. [`Instrument::price_is_valid`] applies the price rule.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Instrument {
    /// The grid every valid price lies on.
    ///
    /// Derive's fixed tick. Hyperliquid has no fixed tick: its grid is the
    /// finest its decimal places allow, `10^-(6 - szDecimals)` for a perp, and
    /// [`Instrument::price_figures`] limits a price further.
    pub tick_size: Num,
    /// The contract or lot size.
    pub lot_size: Num,
    /// The smallest order, as a size.
    ///
    /// Hyperliquid sets its minimum as a value, not a size: there it is the
    /// lot size, the smallest size the grid can state, and
    /// [`Instrument::min_notional`] carries the value.
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
    /// An option's strike. Absent for anything that is not an option.
    /// Defaulted, so an envelope serialised before it still reads.
    #[serde(default)]
    pub strike: Option<Num>,
    /// When an option expires, **as the venue stated it**.
    ///
    /// Never derived from the instrument's name: the date in a name says which
    /// day, and the hour is the venue's — measured 2026-11-24, one venue
    /// settles crypto options at 08:00 UTC, equity ones at 21:00 and crude at
    /// 19:30, all under names of the same shape. A capture loop that stops
    /// expecting an instrument at this moment has to be told the moment.
    #[serde(default)]
    pub expiry_micros: Option<i64>,
    /// `call` or `put`. Absent for anything that is not an option.
    #[serde(default)]
    pub right: Option<String>,
    /// **The venue's own name** for the instrument, as its wire spells it:
    /// `xyz:GOLD`, `BTC-PERP`.
    ///
    /// Recorded, never recomposed by a reader: the ticker is galata's, and on a
    /// venue with several dexes the ticker alone does not say which. The fields
    /// below are defaulted, so a row written before them still reads.
    #[serde(default)]
    pub venue_symbol: Option<String>,
    /// The dex the instrument is listed on, where the venue has more than one
    /// (Hyperliquid's HIP-3 dexes: `xyz`). Absent for a venue's main or only
    /// market.
    #[serde(default)]
    pub dex: Option<String>,
    /// The most significant figures a price may have, where the venue limits
    /// them: 5 for a Hyperliquid perp. A whole-number price is valid whatever
    /// its figures. Absent where the grid alone decides, as on Derive.
    #[serde(default)]
    pub price_figures: Option<u8>,
    /// The smallest order **as a value** in the quote asset, where the venue
    /// sets its minimum that way: Hyperliquid's 10. Never converted into a
    /// size, which would need a price and change with it.
    #[serde(default)]
    pub min_notional: Option<Num>,
    /// The most leverage the venue allows on it, where the venue states it per
    /// instrument.
    #[serde(default)]
    pub max_leverage: Option<u32>,
    /// Whether the venue allows only isolated margin on it, where it says.
    #[serde(default)]
    pub isolated_only: Option<bool>,
}

impl Instrument {
    /// **Whether a price is one the venue accepts**, by this row alone.
    ///
    /// On the [`tick_size`](Instrument::tick_size) grid, and, where
    /// [`price_figures`](Instrument::price_figures) is stated, either a whole
    /// number or within that many significant figures. Hyperliquid documents
    /// the whole-number exception: an integer price is valid whatever its
    /// figures. A price of zero or below is never valid.
    pub fn price_is_valid(&self, price: Num) -> bool {
        if price <= Num::ZERO || self.tick_size <= Num::ZERO {
            return false;
        }
        if !(price % self.tick_size).is_zero() {
            return false;
        }
        match self.price_figures {
            None => true,
            Some(_) if price.fract().is_zero() => true,
            Some(figures) => significant_figures(price) <= u32::from(figures),
        }
    }
}

/// The significant figures of a positive decimal, trailing zeros after the
/// point not counted: `60710.5` has 6, `0.00012` has 2, `1.50` has 2.
fn significant_figures(value: Num) -> u32 {
    let digits = value.normalize().mantissa().unsigned_abs().to_string();
    digits.trim_start_matches('0').len() as u32
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

/// How a venue says an account's collateral is held.
///
/// Hyperliquid states it with `userAbstraction`, as one of four strings
/// (`design/measured.md`, 2026-09-25). **The mode decides whether the perp
/// account value is the account's equity**: in the first two the collateral is
/// spot, and a flat account reads `0.0` beside real cash.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum AccountMode {
    /// `unifiedAccount`: perp and spot share collateral, held in spot.
    Unified,
    /// `portfolioMargin`: collateral held in spot, margined as a portfolio.
    PortfolioMargin,
    /// `disabled`: perp and spot kept apart.
    Disabled,
    /// `default`: the venue's default, perp and spot kept apart.
    Default,
    /// The venue did not say, or said something this build does not know.
    /// **Never assumed to be one of the others.**
    Unknown,
}

impl AccountMode {
    /// The mode for the venue's own string. An unrecognised one is
    /// [`AccountMode::Unknown`], because the read is undocumented and a guess
    /// would be a wrong equity.
    pub fn from_venue(value: &str) -> AccountMode {
        match value {
            "unifiedAccount" => AccountMode::Unified,
            "portfolioMargin" => AccountMode::PortfolioMargin,
            "disabled" => AccountMode::Disabled,
            "default" => AccountMode::Default,
            _ => AccountMode::Unknown,
        }
    }

    /// Why the perp account value is **not** this account's equity, or `None`
    /// where it is. For the two spot-collateral modes it says where the
    /// collateral is: in the account's [`Balance`] rows, which the ledger
    /// records beside the margin.
    pub fn equity_not_held(&self) -> Option<&'static str> {
        match self {
            AccountMode::Unified => {
                Some("collateral is spot (unifiedAccount), recorded as balances")
            }
            AccountMode::PortfolioMargin => {
                Some("collateral is spot (portfolioMargin), recorded as balances")
            }
            AccountMode::Unknown => Some("account mode unknown"),
            AccountMode::Disabled | AccountMode::Default => None,
        }
    }
}

/// An account's margin on one dex, as the venue stated it.
///
/// Every figure optional: **absent is not zero**. A flat account is a margin
/// row with no positions beside it, which is different from a failed poll.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Margin {
    /// The dex, where the venue has several. `None` is the main one.
    pub dex: Option<String>,
    /// How the venue holds this account's collateral.
    pub mode: AccountMode,
    /// Why [`Margin::account_value`] is not the account's equity, where it is
    /// not. **The one field a consumer must read before summing equity.**
    pub equity_not_held: Option<String>,
    /// The perp account value: margin held plus unrealised.
    pub account_value: Option<Num>,
    /// Total notional of open positions.
    pub total_notional: Option<Num>,
    /// The venue's raw USD balance on this dex.
    pub total_raw_usd: Option<Num>,
    /// Margin in use.
    pub margin_used: Option<Num>,
    /// Maintenance margin in use, cross.
    pub maintenance_margin_used: Option<Num>,
    /// What could be withdrawn now.
    pub withdrawable: Option<Num>,
}

/// One open position, as the venue stated it.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Position {
    /// The dex, where the venue has several. `None` is the main one.
    pub dex: Option<String>,
    /// The instrument, resolved as market data resolves it: the dex prefix
    /// composed away.
    pub ticker: Ticker,
    /// Signed: positive is long.
    pub size: Num,
    /// Average entry.
    pub entry_price: Option<Num>,
    /// The venue's mark, **where the answer carried one**. Hyperliquid's does
    /// not, so it is absent — never computed from the other fields.
    pub mark: Option<Num>,
    /// Notional at the venue's valuation.
    pub position_value: Option<Num>,
    /// Unrealised, as the venue computed it.
    pub unrealised_pnl: Option<Num>,
    /// Return on equity, as the venue computed it.
    pub return_on_equity: Option<Num>,
    /// Where the venue would liquidate. Absent where it states none.
    pub liquidation_price: Option<Num>,
    /// Leverage in force.
    pub leverage: Option<Num>,
    /// `cross` or `isolated`, as the venue spelled it.
    pub leverage_type: Option<String>,
    /// The most leverage the instrument allows.
    pub max_leverage: Option<u32>,
    /// Margin this position holds.
    pub margin_used: Option<Num>,
    /// Funding paid over the position's whole history. Positive is paid.
    pub funding_all_time: Option<Num>,
    /// Funding paid since this position opened.
    pub funding_since_open: Option<Num>,
    /// Funding paid since its size last changed.
    pub funding_since_change: Option<Num>,
}

/// One fill, as the venue recorded it. **No transaction hash**: on any block
/// explorer it resolves to the account's address. The hash stays in the
/// archived bytes, under the owner-only root.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Fill {
    /// The dex, where the venue has several. `None` is the main one.
    pub dex: Option<String>,
    /// The market, as the venue names it, a dex prefix composed away: `BTC`,
    /// `GOLD`, a Derive instrument, and a Hyperliquid spot pair as `@107` or
    /// `PURR/USDC`. **Text, not a [`Ticker`]**, as [`Order::market`] is: a
    /// spot pair is no token, and a page holding one spot fill used to fail
    /// whole (90 of 335 recorded pages, measured 2026-11-28).
    pub market: String,
    /// `Bid` bought, `Ask` sold.
    pub side: Side,
    /// What it filled at.
    pub price: Num,
    /// How much.
    pub size: Num,
    /// The position before this fill, signed.
    pub start_position: Option<Num>,
    /// The venue's own description (`Open Long`, `Close Short`, …), verbatim.
    pub direction: Option<String>,
    /// Realised P&L the venue booked on this fill.
    pub closed_pnl: Option<Num>,
    /// The fee charged.
    pub fee: Option<Num>,
    /// The token the fee was charged in.
    pub fee_token: Option<String>,
    /// A builder's fee, where one was charged.
    pub builder_fee: Option<Num>,
    /// Whether the order crossed the spread: took liquidity.
    pub crossed: Option<bool>,
    /// The venue's order id, **as text**: Hyperliquid's is a number, Derive's
    /// a UUID.
    pub order_id: String,
    /// The venue's trade id, as text. **Not unique alone**: the two sides of
    /// one trade share it, so identity is `(trade_id, order_id)`.
    pub trade_id: String,
    /// The TWAP order this fill belonged to, where one did.
    pub twap_id: Option<u64>,
    /// Whether this fill liquidated the account, where the venue says:
    /// Hyperliquid marks a liquidation fill with the user it liquidated, and
    /// this is whether that user is this account (a liquidator's own fill
    /// names somebody else). `None` where the venue does not say (Derive), or
    /// where it could not be told (`move-watcher-onto-markets`, design D9).
    /// Absent from an envelope published before it existed.
    #[serde(default)]
    pub liquidated: Option<bool>,
}

/// Funding one position paid or received in one settlement.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct FundingPayment {
    /// The dex, where the venue has several. `None` is the main one.
    pub dex: Option<String>,
    /// The instrument.
    pub ticker: Ticker,
    /// **As the venue signs it**: negative when the position paid (measured
    /// on mainnet, 2026-09-25).
    pub usdc: Num,
    /// The position it was paid on, signed, where the venue states it: Derive
    /// does not.
    pub size: Option<Num>,
    /// The rate that settled, where the venue states it: Derive does not.
    pub rate: Option<Num>,
    /// How many samples the venue averaged, where it says.
    pub samples: Option<u32>,
}

/// One movement of perp margin on one dex.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct DexEffect {
    /// The dex. `None` is the main one.
    pub dex: Option<String>,
    /// Signed: positive into perp margin, negative out of it.
    pub usdc: Num,
}

/// What a ledger update did to perp margin.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Effect {
    /// These movements, per dex. **Empty means none**: the update moved
    /// nothing in perp margin.
    Known(Vec<DexEffect>),
    /// A type this build does not know. Recorded, reported, never guessed.
    Unknown,
}

/// Who was on the other side of a ledger update.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Counterparty {
    /// One of this ledger's own accounts, by alias.
    Account(Account),
    /// Anyone else, by the keyed fingerprint of their address. Never the address.
    Fingerprint(String),
}

/// A deposit, withdrawal, transfer, liquidation or vault move, as the venue
/// recorded it: its type verbatim, and what it did to perp margin per dex.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct LedgerUpdate {
    /// The venue's type, verbatim (`send`, `deposit`, `accountClassTransfer`, …).
    pub kind: String,
    /// What it did to perp margin, per dex.
    pub effect: Effect,
    /// The other side, where there was one.
    pub counterparty: Option<Counterparty>,
    /// The token moved, where it was not USDC.
    pub token: Option<String>,
    /// The amount moved, in that token, as the venue stated it.
    pub amount: Option<Num>,
    /// The venue's fee on this update, **stated beside the effect and not
    /// folded into it**: whether an amount is gross or net of its fee is the
    /// venue's convention, and the fold's to reconcile.
    pub fee: Option<Num>,
}

/// What the record says about how much of a kind's history it holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum Reach {
    /// Proven missing: the account's own record shows events before the
    /// earliest one the venue still holds.
    Lost,
    /// Nothing in the record contradicts the history. **Not proof of
    /// completeness**; reconciling against snapshots is.
    Consistent,
    /// Nothing to judge against yet.
    Unknown,
}

/// How far the ledger holds one kind of an account's history, by evidence.
///
/// Measured 2026-09-25: the venue's documented fill bound is not its reach,
/// and it does drop history by a rule this ledger cannot apply. So the reach
/// is judged from the account's own events, never from a count.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct EventsReach {
    /// Which kind of history.
    pub kind: Kind,
    /// What the record says about it.
    pub reach: Reach,
    /// The earliest event the venue returned, where it returned one.
    pub earliest_micros: Option<i64>,
}

/// An account the ledger bound to an alias: addressed to that alias.
///
/// The binding is **written to the record and read back** at every boot, so
/// no side file can disagree with it. A name change is a new row with the same
/// ordinal and fingerprint.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct AccountSeen {
    /// The declared master it was discovered under.
    pub master: Account,
    /// Its ordinal under that master, assigned once.
    pub ordinal: u32,
    /// The keyed fingerprint of its address. Never the address.
    pub fingerprint: String,
    /// The name the venue gave it, which its owner may change.
    pub name: Option<String>,
}

/// One coin an account holds in spot, as the venue stated it.
///
/// Hyperliquid states these in `spotClearinghouseState`. Under portfolio
/// margin and unified account **this is where the collateral is**, which is
/// why [`Margin::account_value`] is not the equity there. Every figure is as
/// printed: a negative `hold` is what the venue said, not an error, and a
/// figure the venue did not state is absent, not zero.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Balance {
    /// The coin, as the venue names it.
    pub asset: String,
    /// The venue's index for the token: the key [`BorrowLend`] and
    /// [`TokenMargin`] name it by.
    pub token_index: Option<u32>,
    /// The amount held.
    pub total: Num,
    /// The amount on hold.
    pub hold: Option<Num>,
    /// The entry notional.
    pub entry_notional: Option<Num>,
    /// The amount on hold in spot, stated for a collateral token.
    pub spot_hold: Option<Num>,
    /// The token's loan-to-value, stated for a collateral token.
    pub ltv: Option<Num>,
    /// The amount supplied to lending, stated for a collateral token.
    pub supplied: Option<Num>,
}

/// What an account has borrowed and supplied of one token, as the venue
/// stated it. Hyperliquid states it in `borrowLendUserState`, by token index;
/// the name is the venue's own reference data's, absent where that names none.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct BorrowLend {
    /// The venue's index for the token, as stated.
    pub token_index: u32,
    /// The token's name, from the venue's reference data.
    pub token: Option<String>,
    /// The borrowed amount's basis.
    pub borrow_basis: Option<Num>,
    /// The borrowed amount's value.
    pub borrow_value: Option<Num>,
    /// The supplied amount's basis.
    pub supply_basis: Option<Num>,
    /// The supplied amount's value.
    pub supply_value: Option<Num>,
}

/// An account's borrowing health, as the venue states it: one per read, so an
/// account that borrows and supplies nothing still has one.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct LendHealth {
    /// The venue's word for it, such as `healthy`.
    pub health: Option<String>,
    /// The health factor; absent where the venue states `null`.
    pub health_factor: Option<Num>,
}

/// Whether an account is portfolio-margined, and its ratio, **as the venue
/// states them**. The ratio is the venue's own figure, never computed here;
/// an account whose answer states neither has no row.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct PortfolioMargin {
    /// Whether portfolio margin is enabled.
    pub enabled: Option<bool>,
    /// The portfolio-margin ratio; the venue liquidates above its threshold.
    pub ratio: Option<Num>,
}

/// One token's margin figures for an account, as the venue states them. Each
/// comes from its own list in the venue's answer, so each is absent where its
/// list does not name the token.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct TokenMargin {
    /// The venue's index for the token, as stated.
    pub token_index: u32,
    /// The token's name, from the venue's reference data.
    pub token: Option<String>,
    /// The token's portfolio borrow ratio.
    pub borrow_ratio: Option<Num>,
    /// The token's portfolio supply ratio.
    pub supply_ratio: Option<Num>,
    /// The amount of the token available after maintenance.
    pub available_after_maintenance: Option<Num>,
}

/// A subaccount's margin, **as the venue states it**: Derive's per-subaccount
/// figures, shaped by neither [`Margin`] (per dex, Hyperliquid's) nor any
/// computation here. Every figure is optional — absent where the venue stated
/// none, never zero.
///
/// **The margins are net, not gross requirements.** Each is what is left once
/// the requirement is met: the collateral's contribution, positive, plus the
/// positions', negative ([`Collateral`] and [`PositionState`] carry each one's
/// own). A positive figure is headroom, not an amount to subtract from the
/// value. Measured on Derive on 2026-11-27: a subaccount's portfolio figures
/// equal `private/get_margin`'s `pre_` figures, and equal the sum of its
/// collaterals' and positions' contributions.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct MarginState {
    /// The venue's margin type for the subaccount, such as `PM2` or `SM`.
    pub margin_type: Option<String>,
    /// The manager the subaccount was opened under, as the venue names it.
    pub manager: Option<String>,
    /// The risk universe it margins in, as the venue names it.
    pub risk_universe: Option<String>,
    /// The subaccount's value.
    pub value: Option<Num>,
    /// Its initial margin, net: what is left over the initial requirement.
    pub initial_margin: Option<Num>,
    /// Its maintenance margin, net: what is left over the maintenance
    /// requirement.
    pub maintenance_margin: Option<Num>,
    /// The value of its positions.
    pub positions_value: Option<Num>,
    /// The value of its collateral.
    pub collateral_value: Option<Num>,
    /// The margin its open orders hold.
    pub open_orders_margin: Option<Num>,
    /// Whether the venue says it is under liquidation.
    pub under_liquidation: Option<bool>,
}

/// One open position of a subaccount, options and perpetuals alike, **as the
/// venue states it**: the greeks are the venue's, never computed here.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct PositionState {
    /// The instrument, as the venue names it (`BTC-20261225-60000-P`).
    pub instrument: String,
    /// The venue's type for it: `option`, `perp`, `erc20`.
    pub instrument_type: Option<String>,
    /// Signed: positive is long.
    pub amount: Option<Num>,
    /// The average entry price.
    pub average_price: Option<Num>,
    /// The venue's mark.
    pub mark_price: Option<Num>,
    /// The position at the mark.
    pub mark_value: Option<Num>,
    /// Unrealised profit.
    pub unrealised_pnl: Option<Num>,
    /// Delta.
    pub delta: Option<Num>,
    /// Gamma.
    pub gamma: Option<Num>,
    /// Theta.
    pub theta: Option<Num>,
    /// Vega.
    pub vega: Option<Num>,
    /// The position's contribution to its subaccount's initial margin;
    /// negative where it uses margin.
    pub initial_margin: Option<Num>,
    /// The position's contribution to its subaccount's maintenance margin;
    /// negative where it uses margin.
    pub maintenance_margin: Option<Num>,
    /// Where the venue would liquidate it, where it states one.
    pub liquidation_price: Option<Num>,
    /// Funding paid or received since opened, for a perpetual.
    pub cumulative_funding: Option<Num>,
}

/// One collateral asset of a subaccount, with what it contributes to the
/// subaccount's margin, **as the venue states it**.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Collateral {
    /// The asset, as the venue names it.
    pub asset: String,
    /// The venue's type for it.
    pub asset_type: Option<String>,
    /// The amount held.
    pub amount: Option<Num>,
    /// The venue's mark.
    pub mark_price: Option<Num>,
    /// The amount at the mark.
    pub mark_value: Option<Num>,
    /// Its contribution to its subaccount's initial margin; positive where it
    /// adds headroom.
    pub initial_margin: Option<Num>,
    /// Its contribution to its subaccount's maintenance margin; positive where
    /// it adds headroom.
    pub maintenance_margin: Option<Num>,
    /// Interest accrued since held.
    pub cumulative_interest: Option<Num>,
    /// Interest not yet settled.
    pub pending_interest: Option<Num>,
}

/// One order of an account's, **as the venue states it**: a resting order in
/// an open-orders set ([`Event::Order`]), or an order as it stood at a change
/// of its status ([`Event::OrderUpdate`]).
///
/// The type and the status are the venue's own words, never mapped to a
/// common list: a client tells a stop from a take-profit by reading them, as
/// the watcher does. Every amount is a [`Num`].
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Order {
    /// The dex, where the venue has several. `None` is the main one.
    pub dex: Option<String>,
    /// The market, as the venue names it, a dex prefix composed away: `GOLD`,
    /// `ETH-20261225-4000-C`, and a Hyperliquid spot pair as `@107` or
    /// `PURR/USDC`. **Text, not a [`Ticker`]**: a spot pair is no token, and
    /// an account's row is never a partition or a subject, which is what a
    /// ticker's spelling guards.
    pub market: String,
    /// `Bid` buys, `Ask` sells.
    pub side: Side,
    /// The venue's order id, **as text**: Hyperliquid's is a number, Derive's
    /// a UUID. Neither names the account outside the venue.
    pub order_id: String,
    /// The id the client gave the order, where it gave one: Hyperliquid's
    /// `cloid`, Derive's `label`.
    pub client_id: Option<String>,
    /// The order type, as the venue names it: `Limit`, `Take Profit Market`;
    /// `limit`, `market`.
    pub order_type: String,
    /// The trigger type, where the venue states one apart from the order type:
    /// Derive's `stoploss` or `takeprofit`.
    pub trigger_type: Option<String>,
    /// Whether the order waits on a trigger, where the venue says.
    pub is_trigger: Option<bool>,
    /// Whether it is a take-profit or stop on the whole position, where the
    /// venue says (Hyperliquid's `isPositionTpsl`).
    pub position_tpsl: Option<bool>,
    /// The size as placed.
    pub size: Option<Num>,
    /// The size not yet filled.
    pub remaining: Option<Num>,
    /// The limit price.
    pub limit_price: Option<Num>,
    /// The price that triggers it, where it has one.
    pub trigger_price: Option<Num>,
    /// The trigger's condition, as the venue words it: Hyperliquid's
    /// `Price above 61240`, Derive's trigger price type, `mark` or `index`.
    pub trigger_condition: Option<String>,
    /// Whether it only reduces a position.
    pub reduce_only: Option<bool>,
    /// Its time in force, as the venue names it.
    pub time_in_force: Option<String>,
    /// Its status, as the venue names it: always stated on an update, and on
    /// a resting order where the read states one (Derive's `untriggered`).
    pub status: Option<String>,
    /// When it was placed, where the venue says.
    pub placed_micros: Option<i64>,
}

/// One read's whole set of an account's state of one kind
/// (`add-realtime-account-ledger`, design D3): **the rows of that read, every
/// one, or none**. Published as one message, so no reader ever sees part of a
/// set, and an empty set is a message like any other rather than a special
/// case. The read's time is the envelope's.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct StateSet {
    /// The kind every row is of: a state kind ([`Kind::is_state`]).
    pub kind: Kind,
    /// The rows, as read. Empty where the account held none.
    pub rows: Vec<Event>,
}

impl StateSet {
    /// A set of `kind`, refused (`None`) where `kind` is not a state kind or a
    /// row is of another kind: a set holding a fill, or positions filed as
    /// margin, would be state nobody could trust.
    pub fn of(kind: Kind, rows: Vec<Event>) -> Option<StateSet> {
        (kind.is_state() && rows.iter().all(|row| row.kind() == kind))
            .then_some(StateSet { kind, rows })
    }
}

/// One point of the account-value and PnL history **the venue states**, in
/// one of its windows. Nothing here is computed: a window the venue does not
/// state has no points. The point's time is the envelope's `at_micros`.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct PnlPoint {
    /// The window or series, as the venue names it: Hyperliquid's `day`,
    /// `week`, `month`, `allTime` and their `perp` forms; Derive's value
    /// history, by the period asked.
    pub window: String,
    /// The account's value at the point.
    pub account_value: Option<Num>,
    /// The PnL over the window to the point, where the venue states one.
    pub pnl: Option<Num>,
    /// The initial margin at the point, where the venue states one.
    pub initial_margin: Option<Num>,
    /// The maintenance margin at the point, where the venue states one.
    pub maintenance_margin: Option<Num>,
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

    fn num(s: &str) -> Option<Num> {
        Some(Decimal::from_str(s).unwrap())
    }

    /// A resting stop-market sell on a dex, as the order vocabulary carries it.
    fn stop_order(status: Option<&str>) -> Order {
        Order {
            dex: Some("xyz".into()),
            market: "GOLD".into(),
            side: Side::Ask,
            order_id: "9000000001".into(),
            client_id: None,
            order_type: "Stop Market".into(),
            trigger_type: None,
            is_trigger: Some(true),
            position_tpsl: Some(false),
            size: num("0.5"),
            remaining: num("0.5"),
            limit_price: num("4100.0"),
            trigger_price: num("4120.5"),
            trigger_condition: Some("Price below 4120.5".into()),
            reduce_only: Some(true),
            time_in_force: None,
            status: status.map(str::to_string),
            placed_micros: Some(1_700_000_000_000_000),
        }
    }

    #[test]
    fn an_empty_set_is_a_message_with_no_rows() {
        let set = StateSet::of(Kind::Positions, Vec::new()).unwrap();
        let envelope = Envelope::for_account(
            Venue::new("hyperliquid").unwrap(),
            Account::new("main").unwrap(),
            Some(7),
            7,
            Event::StateSet(set),
        );
        let json = serde_json::to_value(&envelope).unwrap();
        assert_eq!(json["event"]["StateSet"]["kind"], "positions");
        assert_eq!(json["event"]["StateSet"]["rows"], serde_json::json!([]));
        assert_eq!(envelope.kind(), Kind::Positions, "filed under its kind");
        let back: Envelope = serde_json::from_value(json).unwrap();
        assert_eq!(back, envelope);
    }

    #[test]
    fn a_set_holds_one_state_kind_and_carries_no_float() {
        let orders = vec![
            Event::Order(stop_order(None)),
            Event::Order(stop_order(Some("untriggered"))),
        ];
        let set = StateSet::of(Kind::Orders, orders.clone()).unwrap();
        let envelope = Envelope::for_account(
            Venue::new("hyperliquid").unwrap(),
            Account::new("main").unwrap(),
            None,
            9,
            Event::StateSet(set),
        );
        fn floats(v: &serde_json::Value, key: &str, out: &mut Vec<String>) {
            match v {
                serde_json::Value::Number(n)
                    if !["seq", "at_micros", "recv_micros", "placed_micros"].contains(&key) =>
                {
                    out.push(format!("{key} = {n}"))
                }
                serde_json::Value::Object(map) => map.iter().for_each(|(k, x)| floats(x, k, out)),
                serde_json::Value::Array(items) => items.iter().for_each(|x| floats(x, key, out)),
                _ => {}
            }
        }
        let mut bad = Vec::new();
        floats(&serde_json::to_value(&envelope).unwrap(), "", &mut bad);
        assert!(bad.is_empty(), "{bad:?}");
        // A set of events, or of two kinds, is no set.
        assert!(StateSet::of(Kind::OrderUpdates, Vec::new()).is_none());
        let mut mixed = orders;
        mixed.push(Event::OrderUpdate(stop_order(Some("filled"))));
        assert!(StateSet::of(Kind::Orders, mixed).is_none());
    }

    #[test]
    fn an_order_id_is_text_on_the_wire() {
        // Derive's order ids are UUIDs; a number here would hold Hyperliquid's
        // and refuse Derive's. Held on the WIRE, as the no-float test is.
        let json = serde_json::to_value(stop_order(None)).unwrap();
        assert!(json["order_id"].is_string(), "{json}");
        let derive = Order {
            order_id: "2c1f8a40-6d1e-4f0e-9b9a-0e5f3c2a7d11".into(),
            ..stop_order(Some("untriggered"))
        };
        let back: Order = serde_json::from_value(serde_json::to_value(&derive).unwrap()).unwrap();
        assert_eq!(back, derive);
    }

    #[test]
    fn the_order_vocabulary_lands_in_its_own_datasets() {
        let kinds = [
            Event::Order(stop_order(None)).kind(),
            Event::OrderUpdate(stop_order(Some("filled"))).kind(),
            Event::PnlPoint(PnlPoint {
                window: "day".into(),
                account_value: None,
                pnl: None,
                initial_margin: None,
                maintenance_margin: None,
            })
            .kind(),
        ];
        assert_eq!(kinds, [Kind::Orders, Kind::OrderUpdates, Kind::PnlHistory]);
        for kind in kinds {
            assert!(
                matches!(kind.addressing(), crate::Addressing::Account),
                "{kind}"
            );
        }
    }

    #[test]
    fn a_snapshot_row_carries_no_float() {
        // `check-no-float-money.sh` holds the TYPES; this holds the WIRE. A
        // margin and a position rendered the way the bus renders them must
        // carry every price and size as text, because a consumer's JSON parser
        // reads a bare number back as a double.
        let account = Account::new("main").unwrap();
        let margin = Envelope::for_account(
            venue(),
            account.clone(),
            None,
            20,
            Event::Margin(Margin {
                dex: Some("xyz".into()),
                mode: AccountMode::Disabled,
                equity_not_held: None,
                account_value: num("15322.02"),
                total_notional: num("172112.0"),
                total_raw_usd: num("-156789.98"),
                margin_used: num("6884.48"),
                maintenance_margin_used: num("0.1"),
                withdrawable: num("8437.54"),
            }),
        );
        let position = Envelope::for_account(
            venue(),
            account,
            None,
            20,
            Event::Position(Position {
                dex: Some("xyz".into()),
                ticker: Ticker::new("GOLD").unwrap(),
                size: Decimal::from_str("-40.0").unwrap(),
                entry_price: num("4301.5"),
                mark: None,
                position_value: num("172112.0"),
                unrealised_pnl: num("-58.0"),
                return_on_equity: num("-0.0067"),
                liquidation_price: None,
                leverage: num("25"),
                leverage_type: Some("cross".into()),
                max_leverage: Some(25),
                margin_used: num("6884.48"),
                funding_all_time: num("1023.081078"),
                funding_since_open: num("1.062131"),
                funding_since_change: num("0.0"),
            }),
        );
        // Integers are counts and clocks, never money.
        let integers = ["seq", "at_micros", "recv_micros", "max_leverage"];
        fn walk(value: &serde_json::Value, key: &str, integers: &[&str], bad: &mut Vec<String>) {
            match value {
                serde_json::Value::Number(_) if !integers.contains(&key) => {
                    bad.push(format!("{key} = {value}"))
                }
                serde_json::Value::Object(map) => {
                    for (k, v) in map {
                        walk(v, k, integers, bad);
                    }
                }
                serde_json::Value::Array(items) => {
                    for v in items {
                        walk(v, key, integers, bad);
                    }
                }
                _ => {}
            }
        }
        for envelope in [margin, position] {
            let json = serde_json::to_value(&envelope).unwrap();
            let mut bad = Vec::new();
            walk(&json, "", &integers, &mut bad);
            assert!(bad.is_empty(), "numbers on the wire: {bad:?}");
            let back: Envelope = serde_json::from_value(json).unwrap();
            assert_eq!(back, envelope, "the row round-trips exactly");
        }
    }

    #[test]
    fn a_balance_row_carries_no_float_and_round_trips() {
        // The figures of the public portfolio-margin account read on
        // 2026-11-26 (design/measured.md), a negative hold among them. Every
        // amount must cross the wire as text and come back digit for digit;
        // a token's index is a key, the one integer allowed.
        let account = Account::new("main").unwrap();
        let rows = [
            Event::Balance(Balance {
                asset: "USDC".into(),
                token_index: Some(0),
                total: Decimal::from_str("185390.10278742").unwrap(),
                hold: num("-366701.7986019201"),
                entry_notional: num("0.0"),
                spot_hold: num("44280.81087107"),
                ltv: num("0.0"),
                supplied: num("141087.89598842"),
            }),
            Event::Balance(Balance {
                asset: "WOW".into(),
                token_index: Some(98),
                total: Decimal::from_str("179.40071").unwrap(),
                hold: num("0.0"),
                entry_notional: num("3.44772284"),
                spot_hold: None,
                ltv: None,
                supplied: None,
            }),
            Event::BorrowLend(BorrowLend {
                token_index: 150,
                token: Some("HYPE".into()),
                borrow_basis: num("0.0"),
                borrow_value: num("0.0"),
                supply_basis: num("10376.86515991"),
                supply_value: num("10376.86515991"),
            }),
            Event::LendHealth(LendHealth {
                health: Some("healthy".into()),
                health_factor: None,
            }),
            Event::PortfolioMargin(PortfolioMargin {
                enabled: Some(true),
                ratio: num("0.0229134385"),
            }),
            Event::TokenMargin(TokenMargin {
                token_index: 150,
                token: Some("HYPE".into()),
                borrow_ratio: None,
                supply_ratio: num("0.0103768652"),
                available_after_maintenance: num("10376.86515991"),
            }),
        ];
        let integers = ["seq", "at_micros", "recv_micros", "token_index"];
        fn walk(value: &serde_json::Value, key: &str, integers: &[&str], bad: &mut Vec<String>) {
            match value {
                serde_json::Value::Number(_) if !integers.contains(&key) => {
                    bad.push(format!("{key} = {value}"))
                }
                serde_json::Value::Object(map) => {
                    for (k, v) in map {
                        walk(v, k, integers, bad);
                    }
                }
                serde_json::Value::Array(items) => {
                    for v in items {
                        walk(v, key, integers, bad);
                    }
                }
                _ => {}
            }
        }
        for event in rows {
            let kind = event.kind();
            let envelope = Envelope::for_account(venue(), account.clone(), None, 20, event);
            let json = serde_json::to_value(&envelope).unwrap();
            let mut bad = Vec::new();
            walk(&json, "", &integers, &mut bad);
            assert!(bad.is_empty(), "{kind}: numbers on the wire: {bad:?}");
            assert!(
                matches!(kind.addressing(), crate::dataset::Addressing::Account),
                "{kind} sits under the account"
            );
            let back: Envelope = serde_json::from_value(json).unwrap();
            assert_eq!(back, envelope, "{kind} round-trips exactly");
        }
    }

    #[test]
    fn a_subaccounts_margin_rows_carry_no_float_and_every_figure_is_optional() {
        // The figures of design.md's measured trade (2026-11-24, Derive's
        // public simulation): a short put and its perpetual hedge on USDC.
        let account = Account::new("derive_s1").unwrap();
        let full = [
            Event::MarginState(MarginState {
                margin_type: Some("PM2".into()),
                manager: Some("1".into()),
                risk_universe: Some("prime".into()),
                value: num("100000"),
                initial_margin: num("93635.12"),
                maintenance_margin: num("95210.4"),
                positions_value: num("-2145.61"),
                collateral_value: num("100000"),
                open_orders_margin: num("0"),
                under_liquidation: Some(false),
            }),
            Event::PositionState(PositionState {
                instrument: "BTC-20261225-60000-P".into(),
                instrument_type: Some("option".into()),
                amount: num("-1"),
                average_price: num("2145.61"),
                mark_price: num("2151.3"),
                mark_value: num("-2151.3"),
                unrealised_pnl: num("-5.69"),
                delta: num("-0.46"),
                gamma: num("0.000041"),
                theta: num("-31.2"),
                vega: num("82.4"),
                initial_margin: num("9819"),
                maintenance_margin: num("7364.25"),
                liquidation_price: None,
                cumulative_funding: None,
            }),
            Event::Collateral(Collateral {
                asset: "USDC".into(),
                asset_type: Some("erc20".into()),
                amount: num("100000"),
                mark_price: num("1"),
                mark_value: num("100000"),
                initial_margin: num("0"),
                maintenance_margin: num("0"),
                cumulative_interest: num("0"),
                pending_interest: num("0"),
            }),
        ];
        // Every figure absent: what a venue that states nothing yields.
        let bare = [
            Event::MarginState(MarginState {
                margin_type: None,
                manager: None,
                risk_universe: None,
                value: None,
                initial_margin: None,
                maintenance_margin: None,
                positions_value: None,
                collateral_value: None,
                open_orders_margin: None,
                under_liquidation: None,
            }),
            Event::PositionState(PositionState {
                instrument: "BTC-PERP".into(),
                instrument_type: None,
                amount: None,
                average_price: None,
                mark_price: None,
                mark_value: None,
                unrealised_pnl: None,
                delta: None,
                gamma: None,
                theta: None,
                vega: None,
                initial_margin: None,
                maintenance_margin: None,
                liquidation_price: None,
                cumulative_funding: None,
            }),
            Event::Collateral(Collateral {
                asset: "USDC".into(),
                asset_type: None,
                amount: None,
                mark_price: None,
                mark_value: None,
                initial_margin: None,
                maintenance_margin: None,
                cumulative_interest: None,
                pending_interest: None,
            }),
        ];
        let integers = ["seq", "at_micros", "recv_micros"];
        fn walk(value: &serde_json::Value, key: &str, integers: &[&str], bad: &mut Vec<String>) {
            match value {
                serde_json::Value::Number(_) if !integers.contains(&key) => {
                    bad.push(format!("{key} = {value}"))
                }
                serde_json::Value::Object(map) => {
                    for (k, v) in map {
                        walk(v, k, integers, bad);
                    }
                }
                serde_json::Value::Array(items) => {
                    for v in items {
                        walk(v, key, integers, bad);
                    }
                }
                _ => {}
            }
        }
        for event in full.into_iter().chain(bare) {
            let kind = event.kind();
            assert!(
                matches!(kind.addressing(), crate::dataset::Addressing::Account),
                "{kind} sits under the account"
            );
            let envelope = Envelope::for_account(venue(), account.clone(), None, 20, event);
            let json = serde_json::to_value(&envelope).unwrap();
            let mut bad = Vec::new();
            walk(&json, "", &integers, &mut bad);
            assert!(bad.is_empty(), "{kind}: numbers on the wire: {bad:?}");
            let back: Envelope = serde_json::from_value(json).unwrap();
            assert_eq!(back, envelope, "{kind} round-trips exactly");
        }
    }

    #[test]
    fn a_gap_written_before_its_dex_existed_still_decodes() {
        // Generated gaps are stored as their JSON and decoded on every
        // rebuild. A row from before `dex` must read as no dex, not fail.
        let old = r#"{"series":"candles","from_micros":1,"to_micros":2,"cause":"session_lost","clipped":"continuous"}"#;
        let gap: Gap = serde_json::from_str(old).unwrap();
        assert_eq!(gap.dex, None);
        // And a market-data gap still writes exactly what it wrote before.
        assert!(!serde_json::to_string(&gap).unwrap().contains("dex"));
    }

    #[test]
    fn an_event_row_carries_no_float_and_no_hash() {
        // The type has no hash field; this holds the WIRE, so a field added
        // later under that name, or a price that became a number, fails here.
        let account = Account::new("main").unwrap();
        let rows = [
            Event::Fill(Fill {
                dex: Some("xyz".into()),
                market: "GOLD".into(),
                side: Side::Bid,
                price: Decimal::from_str("4301.5").unwrap(),
                size: Decimal::from_str("0.5").unwrap(),
                start_position: num("-1.0"),
                direction: Some("Close Short".into()),
                closed_pnl: num("12.5"),
                fee: num("0.9"),
                fee_token: Some("USDC".into()),
                builder_fee: None,
                crossed: Some(true),
                order_id: "1".into(),
                trade_id: "2".into(),
                twap_id: None,
                liquidated: None,
            }),
            Event::FundingPayment(FundingPayment {
                dex: None,
                ticker: Ticker::new("BTC").unwrap(),
                usdc: Decimal::from_str("-0.1239").unwrap(),
                size: num("0.01"),
                rate: num("0.0000125"),
                samples: Some(60),
            }),
            Event::LedgerUpdate(LedgerUpdate {
                kind: "send".into(),
                effect: Effect::Known(vec![
                    DexEffect {
                        dex: None,
                        usdc: Decimal::from_str("-25.77").unwrap(),
                    },
                    DexEffect {
                        dex: Some("xyz".into()),
                        usdc: Decimal::from_str("25.77").unwrap(),
                    },
                ]),
                counterparty: Some(Counterparty::Fingerprint("0123456789abcdef".into())),
                token: None,
                amount: None,
                fee: None,
            }),
            Event::Order(stop_order(None)),
            Event::OrderUpdate(stop_order(Some("triggered"))),
            Event::PnlPoint(PnlPoint {
                window: "perpWeek".into(),
                account_value: num("1520.25"),
                pnl: num("-12.5"),
                initial_margin: None,
                maintenance_margin: None,
            }),
            Event::PnlPoint(PnlPoint {
                window: "3600".into(),
                account_value: num("980.1"),
                pnl: None,
                initial_margin: num("-120.5"),
                maintenance_margin: num("-60.25"),
            }),
        ];
        // Order and trade ids are text, so a number under either name fails.
        let integers = [
            "seq",
            "at_micros",
            "recv_micros",
            "twap_id",
            "samples",
            "placed_micros",
        ];
        fn walk(v: &serde_json::Value, key: &str, ints: &[&str], bad: &mut Vec<String>) {
            match v {
                serde_json::Value::Number(_) if !ints.contains(&key) => {
                    bad.push(format!("{key} = {v}"))
                }
                serde_json::Value::Object(map) => {
                    for (k, v) in map {
                        if k == "hash" {
                            bad.push("a hash field".into());
                        }
                        walk(v, k, ints, bad);
                    }
                }
                serde_json::Value::Array(items) => {
                    items.iter().for_each(|v| walk(v, key, ints, bad))
                }
                _ => {}
            }
        }
        for event in rows {
            let envelope = Envelope::for_account(venue(), account.clone(), Some(1), 2, event);
            let json = serde_json::to_value(&envelope).unwrap();
            let mut bad = Vec::new();
            walk(&json, "", &integers, &mut bad);
            assert!(bad.is_empty(), "{bad:?}");
            assert_eq!(serde_json::from_value::<Envelope>(json).unwrap(), envelope);
        }
    }

    #[test]
    fn an_account_envelope_names_no_instrument() {
        let envelope = Envelope::for_account(
            venue(),
            Account::new("main").unwrap(),
            None,
            1,
            Event::AccountSeen(AccountSeen {
                master: Account::new("main").unwrap(),
                ordinal: 1,
                fingerprint: "0123456789abcdef".into(),
                name: Some("arb".into()),
            }),
        );
        assert_eq!(envelope.venue().unwrap().as_str(), "hyperliquid");
        assert_eq!(envelope.account().unwrap().as_str(), "main");
        assert!(envelope.ticker().is_none(), "no `ticker = main` sentinel");
        assert_eq!(envelope.kind(), Kind::Accounts);
    }

    #[test]
    fn an_unrecognised_mode_is_unknown_and_holds_no_equity() {
        assert_eq!(
            AccountMode::from_venue("unifiedAccount"),
            AccountMode::Unified
        );
        assert_eq!(
            AccountMode::from_venue("portfolioMargin"),
            AccountMode::PortfolioMargin
        );
        assert_eq!(
            AccountMode::from_venue("somethingNew"),
            AccountMode::Unknown
        );
        // The two spot-collateral modes name themselves and where the
        // collateral is recorded, and no longer call it out of scope.
        for (mode, name) in [
            (AccountMode::Unified, "unifiedAccount"),
            (AccountMode::PortfolioMargin, "portfolioMargin"),
        ] {
            let said = mode.equity_not_held().unwrap();
            assert!(said.contains(name), "{said}");
            assert!(said.contains("recorded as balances"), "{said}");
            assert!(!said.contains("out of scope"), "{said}");
        }
        assert!(AccountMode::Unknown.equity_not_held().is_some());
        assert!(AccountMode::Disabled.equity_not_held().is_none());
        assert!(AccountMode::Default.equity_not_held().is_none());
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
    fn an_option_pricing_row_carries_no_float() {
        // The greeks are the smallest numbers this vocabulary holds, and the
        // ones a double is most tempted by. Rendered the way the bus renders
        // them, every one must be text.
        let pricing = OptionPricing {
            iv: num("0.66267"),
            bid_iv: None,
            ask_iv: num("0.68990"),
            delta: num("0.64729"),
            gamma: num("0.00410149"),
            vega: num("0.50759"),
            theta: num("-6.97601"),
            rho: num("0.35471"),
            forward: num("1681.4"),
            discount_factor: num("0.999"),
        };
        let envelope = Envelope::new(
            Venue::new("derive-xyz").unwrap(),
            Ticker::new("ETH-20261127-1650-C").unwrap(),
            Some(1),
            2,
            Event::OptionPricing(pricing),
        );
        assert_eq!(envelope.kind(), Kind::OptionPricing);
        let json = serde_json::to_value(&envelope).unwrap();
        fn walk(v: &serde_json::Value, key: &str, bad: &mut Vec<String>) {
            match v {
                serde_json::Value::Number(_)
                    if !["seq", "at_micros", "recv_micros"].contains(&key) =>
                {
                    bad.push(format!("{key} = {v}"))
                }
                serde_json::Value::Object(map) => map.iter().for_each(|(k, v)| walk(v, k, bad)),
                serde_json::Value::Array(items) => items.iter().for_each(|v| walk(v, key, bad)),
                _ => {}
            }
        }
        let mut bad = Vec::new();
        walk(&json, "", &mut bad);
        assert!(bad.is_empty(), "{bad:?}");
        assert!(json.to_string().contains("\"0.00410149\""), "{json}");
        assert_eq!(serde_json::from_value::<Envelope>(json).unwrap(), envelope);
    }

    #[test]
    fn an_instrument_written_before_it_knew_options_still_decodes() {
        // The three option fields are defaulted: a row on disk from before
        // them is still a row.
        let old = r#"{"tick_size":"0.1","lot_size":"0.001","min_size":"0.001","contract_type":"perp","base":"BTC","quote":"USD","hours":"continuous","active":true,"venue_index":0,"ui_multiplier":null,"observed_at":5}"#;
        let instrument: Instrument = serde_json::from_str(old).unwrap();
        assert_eq!(instrument.strike, None);
        assert_eq!(instrument.expiry_micros, None);
        assert_eq!(instrument.right, None);
    }

    /// A Hyperliquid perp on its `xyz` dex, every new field stated.
    fn a_hyperliquid_perp() -> Instrument {
        Instrument {
            tick_size: Decimal::from_str("0.1").unwrap(),
            lot_size: Decimal::from_str("0.00001").unwrap(),
            min_size: Decimal::from_str("0.00001").unwrap(),
            contract_type: "perp".into(),
            base: "GOLD".into(),
            quote: "USDC".into(),
            hours: "continuous".into(),
            active: true,
            venue_index: Some(3),
            ui_multiplier: None,
            observed_at: 5,
            strike: None,
            expiry_micros: None,
            right: None,
            venue_symbol: Some("xyz:GOLD".into()),
            dex: Some("xyz".into()),
            price_figures: Some(5),
            min_notional: num("10"),
            max_leverage: Some(20),
            isolated_only: Some(false),
        }
    }

    #[test]
    fn an_instrument_written_before_the_one_format_still_decodes() {
        // The six fields of `normalise-instrument-reference` are defaulted: a
        // Derive row on disk from before them reads, each one absent.
        let old = r#"{"tick_size":"0.5","lot_size":"0.01","min_size":"0.01","contract_type":"perp","base":"BTC","quote":"USDC","hours":"continuous","active":true,"venue_index":null,"ui_multiplier":null,"observed_at":5,"strike":null,"expiry_micros":null,"right":null}"#;
        let instrument: Instrument = serde_json::from_str(old).unwrap();
        assert_eq!(instrument.venue_symbol, None);
        assert_eq!(instrument.dex, None);
        assert_eq!(instrument.price_figures, None);
        assert_eq!(instrument.min_notional, None);
        assert_eq!(instrument.max_leverage, None);
        assert_eq!(instrument.isolated_only, None);
    }

    #[test]
    fn an_instrument_in_the_one_format_round_trips_with_no_float() {
        let perp = a_hyperliquid_perp();
        let json = serde_json::to_string(&perp).unwrap();
        assert_eq!(serde_json::from_str::<Instrument>(&json).unwrap(), perp);
        // The value minimum is a decimal and travels as text; the figure limit
        // and the leverage are counts. None of them is a float.
        let value: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(value["min_notional"], serde_json::json!("10"));
        assert!(value["price_figures"].is_u64(), "{json}");
        assert!(value["max_leverage"].is_u64(), "{json}");
        assert_eq!(value["venue_symbol"], serde_json::json!("xyz:GOLD"));
    }

    #[test]
    fn a_price_is_valid_on_the_grid_and_within_the_figures() {
        // The spec's scenarios: BTC on Hyperliquid, szDecimals 5, so a grid of
        // 0.1 and at most five significant figures.
        let btc = Instrument {
            tick_size: Decimal::from_str("0.1").unwrap(),
            ..a_hyperliquid_perp()
        };
        let valid = |p: &str| btc.price_is_valid(Decimal::from_str(p).unwrap());
        assert!(valid("60710"));
        assert!(!valid("60710.5"), "six figures");
        assert!(valid("6071.5"), "five figures, on the grid");
        assert!(!valid("6071.55"), "off the grid");
        assert!(valid("1234567"), "a whole number, whatever its figures");
        assert!(valid("60710.0"), "a whole number written with a point");
        assert!(!valid("0"));
        assert!(!valid("-1"));
    }

    #[test]
    fn a_price_with_no_figure_limit_needs_only_the_grid() {
        // Derive: a fixed tick of 0.5 and nothing else.
        let derive = Instrument {
            tick_size: Decimal::from_str("0.5").unwrap(),
            price_figures: None,
            ..a_hyperliquid_perp()
        };
        let valid = |p: &str| derive.price_is_valid(Decimal::from_str(p).unwrap());
        assert!(valid("60710.5"));
        assert!(valid("123456789.5"), "any multiple of the tick");
        assert!(!valid("60710.25"));
    }

    #[test]
    fn significant_figures_ignore_trailing_zeros_after_the_point() {
        let figures = |s: &str| significant_figures(Decimal::from_str(s).unwrap());
        assert_eq!(figures("60710.5"), 6);
        assert_eq!(figures("0.00012"), 2);
        assert_eq!(figures("1.50"), 2);
        assert_eq!(figures("6071.5"), 5);
    }

    #[test]
    fn every_event_lands_in_a_dataset() {
        // Exhaustive by construction: `Event::kind` is a total match, so a
        // variant added without deciding where it lands does not compile.
        let envelope = Envelope::new(venue(), ticker(), None, 0, a_quote());
        assert_eq!(envelope.kind(), Kind::Quotes);
    }

    #[test]
    fn unpublished_is_the_bus_missing_what_the_record_holds() {
        // Distinct from every cause that says the data exists nowhere, so a
        // consumer can tell "fetch it from history" from "it is gone".
        let record = [
            GapCause::SessionLost,
            GapCause::CrashUnflushed,
            GapCause::Downtime,
            GapCause::RotationFailed,
            GapCause::Refused,
            GapCause::PollFailed,
            GapCause::Throttled,
            GapCause::Reorg,
            GapCause::BeyondReach,
        ];
        assert!(record.iter().all(|c| *c != GapCause::Unpublished));
        assert!(record.iter().all(|c| c.as_str() != "unpublished"));

        let gap = Gap {
            series: Series::Quotes,
            from_micros: 1,
            to_micros: 2,
            cause: GapCause::Unpublished,
            clipped: Clipped::Continuous,
            dex: None,
        };
        let json = serde_json::to_string(&gap).unwrap();
        assert!(json.contains(r#""cause":"unpublished""#), "{json}");
        let back: Gap = serde_json::from_str(&json).unwrap();
        assert_eq!(back, gap);
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
            GapCause::BeyondReach,
            GapCause::Unpublished,
        ]
        .iter()
        .map(|c| c.as_str())
        .collect();
        assert_eq!(causes.len(), 10);
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
