//! Reading a chain's logs, purely.
//!
//! Every trap here is one the real chain was checked for. Over three hundred
//! blocks, 2026-09-21:
//!
//! ```text
//!   4,362 logs carry the ERC-20 Transfer topic0
//!     4,239  three topics   ERC-20
//!       123  FOUR topics    ERC-721, which shares that topic0
//! ```
//!
//! Decoded as ERC-20, each of those 123 yields an amount of **zero** — an
//! ERC-721 keeps its token id in `topics[3]` and leaves `data` empty — and a
//! plausible transfer that never happened. Nearly three percent, silently.

use std::collections::BTreeMap;

use galata_wire::Num;

/// `keccak256("Transfer(address,address,uint256)")`.
///
/// **ERC-20 and ERC-721 hash to this same value.** They differ only in arity:
/// ERC-20 indexes two arguments, ERC-721 indexes three. That is the whole of
/// how they are told apart, and it is why [`Log::transfer`] refuses by topic
/// count rather than by a contract allow-list — arity is a property of the log,
/// and an allow-list is a second thing to maintain.
pub const TRANSFER: &str = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef";

/// The address that means issuance when it sends and redemption when it
/// receives.
pub const ZERO: &str = "0x0000000000000000000000000000000000000000";

/// One log, as the node sends it.
#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize)]
pub struct Log {
    /// The contract that emitted it.
    pub address: String,
    /// Indexed arguments, the first being the event signature.
    pub topics: Vec<String>,
    /// Unindexed arguments, hex.
    pub data: String,
    /// Which block, hex.
    #[serde(rename = "blockNumber")]
    pub block_number: String,
    /// Which transaction. **The join key for a trade**: a trade is provable
    /// only by matching a stock transfer and a USDG transfer inside one of
    /// these.
    #[serde(rename = "transactionHash")]
    pub transaction_hash: String,
    /// Position within the block's logs — with the hash, the identity of this
    /// event on chain, and what makes re-reading a block idempotent.
    #[serde(rename = "logIndex")]
    pub log_index: String,
}

/// Why a log could not be read.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
#[non_exhaustive]
pub enum DecodeError {
    /// Not a transfer at all.
    #[error("topic0 is not the Transfer signature")]
    NotATransfer,
    /// A non-fungible transfer wearing the same signature.
    #[error(
        "a Transfer log with {topics} topics is ERC-721, which shares ERC-20's topic0 and keeps \
         its token id in topics[3]. Read as ERC-20 it yields an amount of zero and a transfer \
         that never happened"
    )]
    NotFungible {
        /// How many it carried.
        topics: usize,
    },
    /// A topic too short to hold an address.
    #[error("topic {index} is {len} characters and an address needs a 32-byte word")]
    ShortTopic {
        /// Which.
        index: usize,
        /// How long it was.
        len: usize,
    },
    /// A contract nobody declared.
    #[error(
        "contract {address} has no declared decimals. Stock tokens carry 18 and USDG carries 6, \
         so a default is right for one and wrong for the other by a factor of a trillion — and a \
         wrong amount that looks reasonable is worse than a missing row"
    )]
    UnknownContract {
        /// Which.
        address: String,
    },
    /// The data field is not a number.
    #[error("the amount {data} is not a 32-byte word")]
    BadAmount {
        /// What it said.
        data: String,
    },
}

/// A fungible transfer, decoded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Transfer {
    /// Who sent it. [`ZERO`] means this is issuance.
    pub from: String,
    /// Who received it. [`ZERO`] means this is redemption.
    pub to: String,
    /// How much, scaled by the contract's own decimals.
    pub amount: Num,
    /// Which contract.
    pub contract: String,
    /// Which transaction.
    pub transaction_hash: String,
    /// Where in the block.
    pub log_index: u32,
    /// Which block.
    pub block: u64,
}

impl Transfer {
    /// Whether this is issuance — a transfer **from** nowhere.
    pub fn is_issue(&self) -> bool {
        self.from == ZERO
    }

    /// Whether this is redemption — a transfer **to** nowhere.
    pub fn is_redemption(&self) -> bool {
        self.to == ZERO
    }

    /// Whether this is a supply event rather than a movement between holders.
    pub fn is_supply(&self) -> bool {
        self.is_issue() || self.is_redemption()
    }
}

impl Log {
    /// Read this log as a fungible transfer.
    ///
    /// `decimals` is per contract and has **no default**, because the two
    /// contracts on this chain that matter disagree by twelve orders of
    /// magnitude.
    pub fn transfer(&self, decimals: &BTreeMap<String, u32>) -> Result<Transfer, DecodeError> {
        match self.topics.first() {
            Some(topic0) if topic0.eq_ignore_ascii_case(TRANSFER) => {}
            _ => return Err(DecodeError::NotATransfer),
        }
        // **Arity, before anything else.** Three topics is ERC-20; four is
        // ERC-721 wearing the same signature.
        if self.topics.len() != 3 {
            return Err(DecodeError::NotFungible {
                topics: self.topics.len(),
            });
        }

        let address = self.address.to_ascii_lowercase();
        let scale = *decimals
            .get(&address)
            .ok_or_else(|| DecodeError::UnknownContract {
                address: address.clone(),
            })?;

        Ok(Transfer {
            from: topic_address(&self.topics[1], 1)?,
            to: topic_address(&self.topics[2], 2)?,
            amount: scaled(&self.data, scale)?,
            contract: address,
            transaction_hash: self.transaction_hash.to_ascii_lowercase(),
            log_index: hex_u64(&self.log_index).unwrap_or(0) as u32,
            block: hex_u64(&self.block_number).unwrap_or(0),
        })
    }
}

/// The address a 32-byte topic holds: **the last twenty bytes**.
///
/// An address is 20 bytes in a 32-byte word, left-padded with zeros. Taking the
/// first twenty would read padding and produce `0x0000…` for every log — which
/// is also the zero address, so every transfer would look like issuance.
pub fn topic_address(topic: &str, index: usize) -> Result<String, DecodeError> {
    let hex = topic.strip_prefix("0x").unwrap_or(topic);
    if hex.len() != 64 {
        return Err(DecodeError::ShortTopic {
            index,
            len: hex.len(),
        });
    }
    Ok(format!("0x{}", hex[24..].to_ascii_lowercase()))
}

/// A 32-byte word as a number, scaled by `decimals`.
fn scaled(data: &str, decimals: u32) -> Result<Num, DecodeError> {
    let hex = data.strip_prefix("0x").unwrap_or(data);
    if hex.is_empty() || hex.len() > 64 {
        return Err(DecodeError::BadAmount {
            data: data.to_string(),
        });
    }
    let raw = u128::from_str_radix(hex.trim_start_matches('0'), 16).or_else(|_| {
        if hex.chars().all(|c| c == '0') {
            Ok(0)
        } else {
            Err(DecodeError::BadAmount {
                data: data.to_string(),
            })
        }
    })?;
    // `Num` is a 96-bit mantissa, so a raw wei-scale value can exceed it. The
    // scaling is what brings it back, and it is done by `set_scale` rather than
    // by dividing — dividing rounds, and this must not.
    let mut value = Num::from(raw);
    value
        .set_scale(decimals)
        .map_err(|_| DecodeError::BadAmount {
            data: data.to_string(),
        })?;
    Ok(value)
}

fn hex_u64(value: &str) -> Option<u64> {
    u64::from_str_radix(value.strip_prefix("0x").unwrap_or(value), 16).ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    /// The two stock-token contracts these fixtures came from, and the
    /// decimals they were declared with.
    fn decimals() -> BTreeMap<String, u32> {
        BTreeMap::from([
            ("0x0bd7d308f8e1639fab988df18a8011f41eacad73".into(), 18),
            ("0x3429ddc7b45830c4d66c1fdf521967d8dc901cbf".into(), 18),
            ("0x58daec3116aae6d93017baaea7749052e8a04fa7".into(), 18),
        ])
    }

    /// **Captured verbatim from Robinhood Chain, 2026-09-21.** A mint: the
    /// sender is the zero address.
    const MINT: &str = r#"{
      "address": "0x0bd7d308f8e1639fab988df18a8011f41eacad73",
      "topics": [
        "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
        "0x0000000000000000000000000000000000000000000000000000000000000000",
        "0x00000000000000000000000065050a9b7e5075a2ba5ced7b1b64ee66262c40dc"
      ],
      "data": "0x000000000000000000000000000000000000000000000000015fb7f9b8c38000",
      "blockNumber": "0x4176ed2",
      "transactionHash": "0x08dd4d916d95830ae4a8889818a5fc27e2deef7e0fad9298f2d6dde52f24aa63",
      "logIndex": "0x1"
    }"#;

    /// The other transfer **in the same transaction** — which is the shape a
    /// trade is proved by, and the reason the payload unit is the whole
    /// response rather than one log.
    const SAME_TX: &str = r#"{
      "address": "0x3429ddc7b45830c4d66c1fdf521967d8dc901cbf",
      "topics": [
        "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
        "0x0000000000000000000000008366a39cc670b4001a1121b8f6a443a643e40951",
        "0x000000000000000000000000e5e702641ea86f4ae6cc3cdaed2b886f976be044"
      ],
      "data": "0x000000000000000000000000000000000000000000000b2387b64d75d2094cee",
      "blockNumber": "0x4176ed2",
      "transactionHash": "0x08dd4d916d95830ae4a8889818a5fc27e2deef7e0fad9298f2d6dde52f24aa63",
      "logIndex": "0x3"
    }"#;

    /// **The trap, verbatim.** Four topics, the same topic0 — and `data` is
    /// EMPTY, because the token id is in `topics[3]`.
    const ERC721: &str = r#"{
      "address": "0x58daec3116aae6d93017baaea7749052e8a04fa7",
      "topics": [
        "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
        "0x0000000000000000000000000000000000000000000000000000000000000000",
        "0x000000000000000000000000b241d3e5d123f0c30f17ab7f214a0df1d09c39dd",
        "0x00000000000000000000000000000000000000000000000000000000002e4b95"
      ],
      "data": "0x",
      "blockNumber": "0x4176edf",
      "transactionHash": "0x2fb686c965c071de3ecd407cb9c25675d7052fc55243d1bd7389a0f345358e9d",
      "logIndex": "0x3"
    }"#;

    fn log(json: &str) -> Log {
        serde_json::from_str(json).expect("a real log from the chain")
    }

    #[test]
    fn a_four_topic_transfer_is_refused_by_arity() {
        // The whole reason this function exists. Measured at 123 of 4,362 over
        // three hundred blocks — and note this fixture's `data` is "0x", so
        // decoding it as ERC-20 would have produced a transfer of ZERO that
        // never happened.
        let error = log(ERC721).transfer(&decimals()).unwrap_err();
        assert_eq!(error, DecodeError::NotFungible { topics: 4 });
        assert!(error.to_string().contains("ERC-721"), "{error}");
        assert_eq!(log(ERC721).data, "0x", "the fixture's data really is empty");
    }

    #[test]
    fn a_real_mint_is_recognised_as_issuance() {
        let transfer = log(MINT).transfer(&decimals()).unwrap();
        assert!(transfer.is_issue());
        assert!(transfer.is_supply());
        assert!(!transfer.is_redemption());
        assert_eq!(transfer.from, ZERO);
        assert_eq!(transfer.to, "0x65050a9b7e5075a2ba5ced7b1b64ee66262c40dc");
        // 0x015fb7f9b8c38000 = 99,000,000,000,000,000, which at 18 decimals is
        // 0.099 EXACTLY — not approximately, which is the point of holding it
        // in a decimal rather than a float.
        //
        // This assertion was first written as 0.0987 from arithmetic done by
        // eye, and the decoder disagreed. The decoder was right. Hence the
        // full raw value here rather than a rounded one: a fixture whose
        // expectation was guessed is a fixture that tests the guess.
        assert_eq!(transfer.amount, Num::from_str("0.099").unwrap());
        assert_eq!(
            transfer.amount * Num::from_str("1000000000000000000").unwrap(),
            Num::from_str("99000000000000000").unwrap()
        );
        assert_eq!(transfer.block, 0x4176ed2);
        assert_eq!(transfer.log_index, 1);
    }

    #[test]
    fn two_transfers_in_one_transaction_share_its_hash() {
        // A trade is provable only by matching them, which is why the payload
        // unit is the whole response: split into one log per payload, this
        // evidence would be in different rows and `normalise` would have to
        // look sideways to see it.
        let a = log(MINT).transfer(&decimals()).unwrap();
        let b = log(SAME_TX).transfer(&decimals()).unwrap();
        assert_eq!(a.transaction_hash, b.transaction_hash);
        assert_ne!(a.contract, b.contract);
        assert_ne!(a.log_index, b.log_index);
    }

    #[test]
    fn an_address_comes_from_the_last_twenty_bytes() {
        // Taking the FIRST twenty reads padding and yields 0x0000… for every
        // log — which is also the zero address, so every transfer would look
        // like issuance.
        let topic = "0x0000000000000000000000008366a39cc670b4001a1121b8f6a443a643e40951";
        assert_eq!(
            topic_address(topic, 1).unwrap(),
            "0x8366a39cc670b4001a1121b8f6a443a643e40951"
        );
        assert!(!topic_address(topic, 1).unwrap().ends_with("000000000000"));
    }

    #[test]
    fn a_short_topic_is_refused_rather_than_padded() {
        assert_eq!(
            topic_address("0xdeadbeef", 2).unwrap_err(),
            DecodeError::ShortTopic { index: 2, len: 8 }
        );
    }

    #[test]
    fn an_undeclared_contract_is_refused() {
        // Stock tokens carry 18 and USDG carries 6. A default is right for one
        // and wrong for the other by a factor of a trillion.
        let error = log(MINT).transfer(&BTreeMap::new()).unwrap_err();
        assert!(
            matches!(error, DecodeError::UnknownContract { .. }),
            "{error}"
        );
        assert!(error.to_string().contains("trillion"), "{error}");
    }

    #[test]
    fn decimals_are_per_contract_and_change_the_answer() {
        // The same bytes, read at 18 and at 6, differ by a factor of 10^12.
        let contract = "0x0bd7d308f8e1639fab988df18a8011f41eacad73".to_string();
        let at18 = log(MINT)
            .transfer(&BTreeMap::from([(contract.clone(), 18)]))
            .unwrap()
            .amount;
        let at6 = log(MINT)
            .transfer(&BTreeMap::from([(contract, 6)]))
            .unwrap()
            .amount;
        assert_eq!(at6 / at18, Num::from_str("1000000000000").unwrap());
    }

    #[test]
    fn a_log_that_is_not_a_transfer_says_so() {
        let mut other = log(MINT);
        other.topics[0] = "0xabc".into();
        assert_eq!(
            other.transfer(&decimals()).unwrap_err(),
            DecodeError::NotATransfer
        );
    }

    #[test]
    fn an_amount_that_does_not_fit_is_refused_rather_than_wrapped() {
        // A 256-bit value exceeds what `Num` holds. Refusing names it; wrapping
        // would produce a number that is wrong and looks fine.
        let mut huge = log(MINT);
        huge.data = format!("0x{}", "f".repeat(64));
        assert!(matches!(
            huge.transfer(&decimals()).unwrap_err(),
            DecodeError::BadAmount { .. }
        ));
    }
}
