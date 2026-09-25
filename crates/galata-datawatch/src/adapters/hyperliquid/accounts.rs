//! Hyperliquid's answer to [`AccountVenue`](crate::ledger::run::AccountVenue): the info endpoint, asked about
//! accounts. Transport, like `client`; the reading is `ledger`'s.
//!
//! Every read is an unauthenticated `info` query by address, so the ledger
//! holds **no key** for this venue — only the addresses, which are secrets of
//! a different kind (`crate::ledger`).

use galata_wire::{Kind, Venue};

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
}

impl HyperliquidAccounts {
    /// Over the venue's REST root.
    pub fn new(rest_url: &str) -> Result<HyperliquidAccounts, galata_wire::TokenError> {
        Ok(HyperliquidAccounts {
            client: Client::new(rest_url),
            normaliser: LedgerNormaliser::new()?,
        })
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
        }
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
        let (last, rows) = events::page_end(page)?;
        Some((events::page_start(page)?, last, rows))
    }
}
