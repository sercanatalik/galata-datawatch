//! The ledger: **what an account holds**, recorded as the venue stated it.
//!
//! ```text
//!   accounts    alias ↔ address, fingerprinted; sub-accounts bound by ordinal
//!   snapshot    perp margin and positions, per (account, dex), on a cadence
//! ```
//!
//! Two kinds of account fact exist, and only one of them can be fetched later:
//! fills and transfers are **events** the venue keeps; positions and margin are
//! **states** it answers for *now* only. A snapshot not taken today can never
//! be taken, which is why this half comes first
//! (`design/roadmap.md`, Tiers 11–12).
//!
//! **The record knows an alias; only the vault knows the address.** An address
//! is not a credential on Hyperliquid, but it names its owner on a public
//! chain, and a leak cannot be taken back. So it is held as a
//! [`Secret`](crate::config::Secret), and what reaches a path, a subject, a
//! status field or a log is the alias.
//!
//! Everything here is a pure function of configuration, secrets and the record
//! on disk. Polling the venue is the capture half's, behind `capture`.

pub mod accounts;
pub mod events;
pub mod fold;
/// The loop: asking the venue, on a cadence. Needs a runtime.
#[cfg(feature = "capture")]
pub mod run;

/// One sub-account a master's listing names, as a venue adapter read it.
#[derive(Debug, Clone)]
pub struct Listed {
    /// Its address. Held so it cannot print itself.
    pub address: crate::config::Secret,
    /// The name its owner gave it.
    pub name: Option<String>,
}

pub use accounts::{
    Bindings, FingerprintKey, LedgerError, ResolvedAccount, Seen, check_fingerprints, check_root,
    fingerprint, resolve, sub_alias,
};
