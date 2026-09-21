//! A `getLogs` response becomes events.
//!
//! **The response is the unit, not the log.** A trade on this chain is provable
//! only by matching a stock-token `Transfer` and a USDG `Transfer` inside one
//! `transactionHash`; split one log per payload and that evidence lands in
//! different rows, where a pure `normalise` cannot see it.
//!
//! **One unreadable log does not cost the others.** A single ERC-721 in a
//! response of four thousand must not lose the other 3,999 — and there really
//! are ERC-721s in there, measured at nearly three percent.

use std::collections::BTreeMap;

use galata_wire::{Envelope, Event, Mint, Ticker, Transfer as WireTransfer, Venue};

use super::wire::{DecodeError, Log};

/// What a response amounted to.
// Not `Eq`: an envelope carries decimals, which have no total equality.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Read {
    /// The events.
    pub events: Vec<Envelope>,
    /// Logs that were not transfers at all — the ordinary case, and not a
    /// problem.
    pub not_transfers: usize,
    /// Logs refused, with the reason.
    ///
    /// **Counted rather than dropped**: a decoder silently skipping three
    /// percent of its input is a decoder nobody notices is broken.
    pub refused: Vec<(String, DecodeError)>,
}

/// Turn one response into events.
///
/// `tickers` maps a contract to the instrument it is, and a contract absent
/// from it is skipped — a chain carries everybody's tokens, and capturing all
/// of them is not what was asked for.
pub fn read(
    venue: &Venue,
    logs: &[Log],
    tickers: &BTreeMap<String, Ticker>,
    decimals: &BTreeMap<String, u32>,
    recv_micros: i64,
    at_micros: Option<i64>,
) -> Read {
    let mut out = Read::default();
    for log in logs {
        let contract = log.address.to_ascii_lowercase();
        let Some(ticker) = tickers.get(&contract) else {
            // Somebody else's token. Not an error and not a refusal.
            continue;
        };
        match log.transfer(decimals) {
            Err(DecodeError::NotATransfer) => out.not_transfers += 1,
            Err(error) => out.refused.push((contract, error)),
            Ok(transfer) => {
                // **Issuance and redemption are transfers at the zero
                // address**, and they are a different dataset: a supply event
                // is not custody moving between holders.
                let event = if transfer.is_supply() {
                    Event::Mint(Mint {
                        holder: if transfer.is_issue() {
                            transfer.to.clone()
                        } else {
                            transfer.from.clone()
                        },
                        amount: transfer.amount,
                        is_issue: transfer.is_issue(),
                        tx_hash: transfer.transaction_hash.clone(),
                        log_index: transfer.log_index,
                    })
                } else {
                    Event::Transfer(WireTransfer {
                        from: transfer.from.clone(),
                        to: transfer.to.clone(),
                        amount: transfer.amount,
                        tx_hash: transfer.transaction_hash.clone(),
                        log_index: transfer.log_index,
                    })
                };
                out.events.push(Envelope::new(
                    venue.clone(),
                    ticker.clone(),
                    at_micros,
                    recv_micros,
                    event,
                ));
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    const CONTRACT: &str = "0x0bd7d308f8e1639fab988df18a8011f41eacad73";
    const OTHER: &str = "0x3429ddc7b45830c4d66c1fdf521967d8dc901cbf";
    const NFT: &str = "0x58daec3116aae6d93017baaea7749052e8a04fa7";

    fn venue() -> Venue {
        Venue::new("rh-chain").unwrap()
    }

    fn tickers() -> BTreeMap<String, Ticker> {
        BTreeMap::from([
            (CONTRACT.into(), Ticker::new("NVDA").unwrap()),
            (OTHER.into(), Ticker::new("USDG").unwrap()),
            (NFT.into(), Ticker::new("SOMENFT").unwrap()),
        ])
    }

    fn decimals() -> BTreeMap<String, u32> {
        BTreeMap::from([(CONTRACT.into(), 18), (OTHER.into(), 18), (NFT.into(), 18)])
    }

    /// Captured verbatim from Robinhood Chain, 2026-09-21: a mint, an ordinary
    /// transfer **in the same transaction**, and the ERC-721 that shares the
    /// ERC-20 topic0.
    fn response() -> Vec<Log> {
        serde_json::from_str(
            r#"[
              {"address":"0x0bd7d308f8e1639fab988df18a8011f41eacad73",
               "topics":["0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
                 "0x0000000000000000000000000000000000000000000000000000000000000000",
                 "0x00000000000000000000000065050a9b7e5075a2ba5ced7b1b64ee66262c40dc"],
               "data":"0x000000000000000000000000000000000000000000000000015fb7f9b8c38000",
               "blockNumber":"0x4176ed2",
               "transactionHash":"0x08dd4d916d95830ae4a8889818a5fc27e2deef7e0fad9298f2d6dde52f24aa63",
               "logIndex":"0x1"},
              {"address":"0x3429ddc7b45830c4d66c1fdf521967d8dc901cbf",
               "topics":["0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
                 "0x0000000000000000000000008366a39cc670b4001a1121b8f6a443a643e40951",
                 "0x000000000000000000000000e5e702641ea86f4ae6cc3cdaed2b886f976be044"],
               "data":"0x000000000000000000000000000000000000000000000b2387b64d75d2094cee",
               "blockNumber":"0x4176ed2",
               "transactionHash":"0x08dd4d916d95830ae4a8889818a5fc27e2deef7e0fad9298f2d6dde52f24aa63",
               "logIndex":"0x3"},
              {"address":"0x58daec3116aae6d93017baaea7749052e8a04fa7",
               "topics":["0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
                 "0x0000000000000000000000000000000000000000000000000000000000000000",
                 "0x000000000000000000000000b241d3e5d123f0c30f17ab7f214a0df1d09c39dd",
                 "0x00000000000000000000000000000000000000000000000000000000002e4b95"],
               "data":"0x",
               "blockNumber":"0x4176edf",
               "transactionHash":"0x2fb686c965c071de3ecd407cb9c25675d7052fc55243d1bd7389a0f345358e9d",
               "logIndex":"0x3"}
            ]"#,
        )
        .expect("real logs")
    }

    #[test]
    fn one_unreadable_log_does_not_cost_the_others() {
        // The ERC-721 is refused and the two beside it still become events.
        let read = read(
            &venue(),
            &response(),
            &tickers(),
            &decimals(),
            100,
            Some(90),
        );
        assert_eq!(read.events.len(), 2);
        assert_eq!(read.refused.len(), 1);
        assert!(matches!(
            read.refused[0].1,
            DecodeError::NotFungible { topics: 4 }
        ));
    }

    #[test]
    fn a_refusal_is_counted_rather_than_dropped() {
        // A decoder silently skipping three percent of its input is a decoder
        // nobody notices is broken.
        let read = read(&venue(), &response(), &tickers(), &decimals(), 100, None);
        assert_eq!(read.refused[0].0, NFT, "which contract it was");
    }

    #[test]
    fn issuance_and_a_movement_land_in_different_datasets() {
        // A supply event is not custody moving between holders.
        let read = read(
            &venue(),
            &response(),
            &tickers(),
            &decimals(),
            100,
            Some(90),
        );
        let kinds: Vec<_> = read.events.iter().map(|e| e.kind()).collect();
        assert_eq!(
            kinds,
            vec![galata_wire::Kind::Mints, galata_wire::Kind::Transfers]
        );

        match &read.events[0].event {
            Event::Mint(mint) => {
                assert!(mint.is_issue);
                // The HOLDER is the receiver, because the sender is nobody.
                assert_eq!(mint.holder, "0x65050a9b7e5075a2ba5ced7b1b64ee66262c40dc");
                assert_eq!(mint.amount, galata_wire::Num::from_str("0.099").unwrap());
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn both_events_carry_the_transaction_that_proves_a_trade() {
        let read = read(
            &venue(),
            &response(),
            &tickers(),
            &decimals(),
            100,
            Some(90),
        );
        let hashes: Vec<String> = read
            .events
            .iter()
            .map(|e| match &e.event {
                Event::Mint(m) => m.tx_hash.clone(),
                Event::Transfer(t) => t.tx_hash.clone(),
                other => panic!("{other:?}"),
            })
            .collect();
        assert_eq!(hashes[0], hashes[1], "the two sides of one transaction");
    }

    #[test]
    fn somebody_elses_token_is_skipped_without_complaint() {
        // A chain carries everybody's tokens, and capturing all of them is not
        // what was asked for.
        let read = read(
            &venue(),
            &response(),
            &BTreeMap::new(),
            &decimals(),
            100,
            None,
        );
        assert!(read.events.is_empty());
        assert!(read.refused.is_empty(), "skipping is not refusing");
    }

    #[test]
    fn a_venue_time_the_chain_did_not_state_stays_absent() {
        let read = read(&venue(), &response(), &tickers(), &decimals(), 100, None);
        assert!(read.events[0].at_micros.is_none());
        assert_eq!(read.events[0].recv_micros, 100);
    }
}
