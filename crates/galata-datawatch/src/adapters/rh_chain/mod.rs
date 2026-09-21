//! Robinhood Chain: an Arbitrum Orbit rollup carrying tokenised equities.
//!
//! **Chain 4663**, settling to Ethereum, ETH for gas. The public endpoint needs
//! no key and keeps no archive, which is why a provider's earliest block is a
//! declared fact rather than an assumption.
//!
//! The two things that make this venue different from an exchange:
//!
//! 1. **History pages by block, not by time.** Twenty consecutive blocks carry
//!    four distinct timestamps. See [`crate::source::cursor`].
//! 2. **There are two frontiers.** What arrived, and what cannot be taken back.

pub mod wire;

/// The venue's name, as it appears in a partition and on a subject.
pub const VENUE: &str = "rh-chain";

/// The chain's own identifier, for refusing a provider pointed elsewhere.
pub const CHAIN_ID: u64 = 4663;

/// The public endpoint. **No key, no archive, no SLA.**
pub const PUBLIC_RPC: &str = "https://rpc.mainnet.chain.robinhood.com";
