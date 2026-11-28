//! Hyperliquid's answer to [`AccountVenue`](crate::ledger::run::AccountVenue): the info endpoint, asked about
//! accounts. Transport, like `client`; the reading is `ledger`'s.
//!
//! Every read is an unauthenticated `info` query by address, so the ledger
//! holds **no key** for this venue — only the addresses, which are secrets of
//! a different kind (`crate::ledger`).

use galata_wire::{Kind, Series, Venue};

use super::client::{Client, FetchError};
use super::events;
use super::ledger::{self, LedgerNormaliser, Role};
use crate::capture::Refusal;
use crate::config::Secret;
use crate::ledger::Listed;
use crate::ledger::run::{AccountVenue, Ask};
use crate::normalise::{Normalise, NormaliseError};

/// The ledger's view of Hyperliquid.
#[derive(Debug, Clone)]
pub struct HyperliquidAccounts {
    client: Client,
    normaliser: LedgerNormaliser,
    /// Where its account stream is, where one is read.
    stream: Option<crate::venue::Endpoint>,
}

/// What a streamed frame is to the loop.
pub(crate) fn stream_frame(frame: &[u8]) -> crate::ledger::run::StreamFrame {
    use crate::ledger::run::StreamFrame;
    match ledger::streamed(frame) {
        ledger::Streamed::Account {
            channel,
            kind,
            user,
        } => StreamFrame::Account {
            channel,
            kind,
            address: Secret::new(user),
            // A fill's order is read at once, so it arrives with its type.
            then: if kind == galata_wire::Kind::Fills {
                crate::ledger::run::Then::History
            } else {
                crate::ledger::run::Then::Nothing
            },
        },
        ledger::Streamed::OrderUpdates => StreamFrame::Changed {
            channel: ledger::WS_ORDER_UPDATES,
            kind: galata_wire::Kind::OrderUpdates,
        },
        ledger::Streamed::Control => StreamFrame::Control,
        ledger::Streamed::Other => StreamFrame::Other,
    }
}

/// A streamed frame as recorded: with the mode last heard, and — for the spot
/// state, whose balances name tokens by index — the names the reference gives
/// the indices it holds.
pub(crate) fn compose_stream(
    mode: Option<(&[u8], i64)>,
    reference: Option<&[u8]>,
    frame: &[u8],
) -> Vec<u8> {
    let names = match ledger::streamed(frame) {
        ledger::Streamed::Account {
            channel: ledger::WS_SPOT,
            ..
        } => serde_json::from_slice::<serde_json::Value>(frame)
            .ok()
            .and_then(|v| serde_json::to_vec(&v["data"]["spotState"]).ok())
            .and_then(|spot| ledger::names_for(reference, &spot)),
        _ => None,
    };
    ledger::stream_bytes(mode, names.as_ref(), frame)
}

impl HyperliquidAccounts {
    /// Over the venue's REST root.
    pub fn new(rest_url: &str) -> Result<HyperliquidAccounts, galata_wire::TokenError> {
        Ok(HyperliquidAccounts {
            client: Client::new(rest_url),
            normaliser: LedgerNormaliser::new()?,
            stream: None,
        })
    }

    /// Reading its accounts' streams too, at the network's websocket
    /// (`add-realtime-account-ledger`, design D1).
    pub fn with_stream(mut self, endpoint: crate::venue::Endpoint) -> HyperliquidAccounts {
        self.stream = Some(endpoint);
        self
    }

    /// Able to read ledger updates: the key decides which side of a transfer
    /// an account was on, and names its counterparty.
    pub fn with_key(mut self, key: crate::ledger::FingerprintKey) -> HyperliquidAccounts {
        self.normaliser = self.normaliser.with_key(key);
        self
    }
}

fn refused(e: FetchError) -> Refusal {
    e.refusal()
}

impl AccountVenue for HyperliquidAccounts {
    fn venue(&self) -> &Venue {
        self.normaliser.venue()
    }

    fn normaliser(&self) -> &dyn Normalise {
        &self.normaliser
    }

    fn channel(&self, ask: Ask) -> &'static str {
        match ask {
            Ask::Snapshot => ledger::SNAPSHOT_CHANNEL,
            Ask::Listing => ledger::LISTING_CHANNEL,
            Ask::Role => ledger::ROLE_CHANNEL,
            Ask::Mode => ledger::MODE_CHANNEL,
            Ask::Balances => ledger::BALANCES_CHANNEL,
            Ask::BorrowLend => ledger::BORROW_LEND_CHANNEL,
            Ask::OpenOrders => ledger::OPEN_ORDERS_CHANNEL,
            Ask::OrderHistory => ledger::ORDER_HISTORY_CHANNEL,
            Ask::Portfolio => ledger::PORTFOLIO_CHANNEL,
            // A wallet's history, which only a venue read by wallet asks.
            Ask::TradeHistory | Ask::FundingHistory | Ask::ValueHistory => "not-asked",
        }
    }

    fn states(
        &self,
        payload: &crate::record::Payload,
        rows: &[galata_wire::Envelope],
    ) -> Vec<crate::ledger::publish::Stated> {
        ledger::states(payload, rows)
    }

    fn stream_endpoint(&self) -> Option<crate::venue::Endpoint> {
        self.stream.clone()
    }

    /// Every declared dex's open orders, read together: the whole set.
    fn orders_answer(&self) -> Option<&'static str> {
        Some(ledger::OPEN_ORDERS_CHANNEL)
    }

    fn stream_subscriptions(&self, account: &crate::ledger::ResolvedAccount) -> Vec<String> {
        ledger::subscriptions(account.address().expose(), &account.dexes)
    }

    fn stream_keepalive(&self) -> crate::venue::Keepalive {
        crate::venue::Keepalive::Frame(ledger::KEEPALIVE.into())
    }

    fn stream_frame(&self, frame: &[u8]) -> crate::ledger::run::StreamFrame {
        stream_frame(frame)
    }

    fn compose_stream(
        &self,
        _account: &crate::ledger::ResolvedAccount,
        mode: Option<(&[u8], i64)>,
        reference: Option<&[u8]>,
        frame: &[u8],
    ) -> Vec<u8> {
        compose_stream(mode, reference, frame)
    }

    fn account_wide_asks(&self) -> &'static [(Ask, Series)] {
        &[
            (Ask::Balances, Series::Balances),
            (Ask::BorrowLend, Series::BorrowLend),
            (Ask::OpenOrders, Series::Orders),
        ]
    }

    fn history_asks(&self) -> &'static [(Ask, Series)] {
        &[
            (Ask::OrderHistory, Series::OrderUpdates),
            (Ask::Portfolio, Series::PnlHistory),
        ]
    }

    async fn account_wide(
        &self,
        ask: Ask,
        address: &Secret,
        dexes: &[String],
    ) -> Result<Vec<u8>, Refusal> {
        match ask {
            Ask::Balances => self.client.spot_clearinghouse_state(address).await,
            Ask::BorrowLend => self.client.borrow_lend_user_state(address).await,
            // Every dex in one answer, measured 2026-11-27.
            Ask::OrderHistory => self.client.historical_orders(address).await,
            Ask::Portfolio => self.client.portfolio(address).await,
            // One request per declared dex, composed into one complete set. A
            // dex that does not answer fails the whole read, so no set is ever
            // recorded missing a dex (design D3 of
            // `add-account-vocabulary-orders`).
            Ask::OpenOrders => {
                let mut answers = Vec::with_capacity(dexes.len());
                for dex in dexes {
                    let answer = self
                        .client
                        .frontend_open_orders(address, dex)
                        .await
                        .map_err(refused)?;
                    answers.push((dex.as_str(), answer));
                }
                return Ok(ledger::open_orders_bytes(&answers));
            }
            // Not an account-wide read: the loop never asks one.
            _ => return Err(Refusal::Unreachable),
        }
        .map_err(refused)
    }

    async fn reference(&self) -> Result<Vec<u8>, Refusal> {
        self.client.spot_meta().await.map_err(refused)
    }

    fn compose_wide(&self, ask: Ask, reference: Option<&[u8]>, answer: &[u8]) -> Vec<u8> {
        // Composed already where composed, and naming no token index: only
        // the spot and borrowing answers travel with the reference's names.
        if !matches!(ask, Ask::Balances | Ask::BorrowLend) {
            return answer.to_vec();
        }
        let key = self.channel(ask);
        ledger::wide_bytes(key, ledger::names_for(reference, answer).as_ref(), answer)
    }

    async fn snapshot(&self, address: &Secret, dex: &str) -> Result<Vec<u8>, Refusal> {
        self.client
            .clearinghouse_state(address, dex)
            .await
            .map_err(refused)
    }

    async fn listing(&self, address: &Secret) -> Result<Vec<u8>, Refusal> {
        self.client.sub_accounts(address).await.map_err(refused)
    }

    async fn role(&self, address: &Secret) -> Result<Vec<u8>, Refusal> {
        self.client.user_role(address).await.map_err(refused)
    }

    async fn mode(&self, address: &Secret) -> Result<Vec<u8>, Refusal> {
        self.client.user_abstraction(address).await.map_err(refused)
    }

    async fn dex_known(&self, dex: &str) -> Result<bool, Refusal> {
        match self.client.dex_known(dex).await {
            Ok(Some(known)) => Ok(known),
            Ok(None) => Err(Refusal::Unreachable),
            Err(e) => Err(refused(e)),
        }
    }

    fn compose(&self, dex: &str, mode: Option<(&[u8], i64)>, state: &[u8]) -> Vec<u8> {
        ledger::snapshot_bytes(dex, mode, state)
    }

    fn listed(&self, answer: &[u8]) -> Result<Vec<Listed>, NormaliseError> {
        ledger::listing_of(answer)
    }

    fn is_sub_account(&self, answer: &[u8]) -> Result<bool, NormaliseError> {
        Ok(ledger::role_of(answer)? == Role::SubAccount)
    }

    fn events_channel(&self, kind: Kind) -> Option<&'static str> {
        match kind {
            Kind::Fills => Some(events::FILLS_CHANNEL),
            Kind::FundingPayments => Some(events::FUNDING_CHANNEL),
            Kind::LedgerUpdates => Some(events::UPDATES_CHANNEL),
            _ => None,
        }
    }

    async fn events_page(
        &self,
        kind: Kind,
        address: &Secret,
        start_micros: i64,
    ) -> Result<Vec<u8>, Refusal> {
        let Some(channel) = self.events_channel(kind) else {
            return Err(Refusal::Unreachable);
        };
        // The venue pages by milliseconds, inclusive: asking from the newest
        // recorded event's own millisecond repeats it, which identity makes
        // free, and never skips a neighbour that shares it.
        self.client
            .events_page(channel, address, start_micros / 1_000)
            .await
            .map_err(refused)
    }

    fn page_size(&self, kind: Kind) -> usize {
        self.events_channel(kind)
            .and_then(events::page_size)
            .unwrap_or(usize::MAX)
    }

    fn page_span(&self, page: &[u8]) -> Option<(i64, i64, usize)> {
        events::page_span(page)
    }
}
