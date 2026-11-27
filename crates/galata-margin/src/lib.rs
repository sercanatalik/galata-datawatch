//! **Where to place an option and its perpetual hedge**, and what each
//! placement costs against the owner's real book
//! (`add-cross-venue-margin-optimiser`).
//!
//! Derive and Hyperliquid share no margin. A hedge placed beside its option
//! on Derive can lower the requirement under portfolio margin; held on the
//! other venue it is margined in full on both sides. This crate compares the
//! placements and proposes one. It advises and does nothing else: it names no
//! request that places, amends or cancels an order or moves funds.
//!
//! - **Derive's figure is Derive's**: its own simulation of the owner's
//!   subaccount with the placement's positions added (design D3).
//! - **Hyperliquid's figure is computed** from the venue's published formula,
//!   and labelled an estimate until it agrees with what the venue states for
//!   the account (D4).
//! - **Every figure carries its origin and the time** of the snapshot it was
//!   computed from (D8).
//!
//! **An estimate per venue and account** ([`estimate`]) states the margin
//! before and after a placement's positions are added. Each figure is an
//! [`advice::Figure`]:
//! - its **origin**, [`advice::Origin::Venue`] or [`advice::Origin::Computed`];
//! - its **estimate label**, [`advice::EstimateLabel`], on a computed figure
//!   while galata's ratio disagrees with the venue's beyond the declared
//!   tolerance or has not been compared ([`agreement`]);
//! - its **time**, that of the snapshot or simulation it rests on.
//!
//! A venue with no figure says why, in [`advice::VenueEstimate::Missing`], and
//! does not stop the other.
//!
//! | Module | What it holds |
//! |---|---|
//! | [`config`] | the `[margin]` block, read and bounded |
//! | [`state`] | the owner's accounts, as the ledger last projected them |
//! | [`derive`](mod@derive) | Derive's simulation, and the key's check at start |
//! | [`hyperliquid`] | Hyperliquid's initial and portfolio margin, computed |
//! | [`agreement`] | galata's ratio against the venue's, and the label |
//! | [`estimate`] | each venue's estimate, or why it has none |
//! | [`advice`] | what an estimate and an advice hold |
//!
//! Not published (D10): the four published crates link none of it.

pub mod advice;
pub mod agreement;
pub mod command;
pub mod config;
pub mod derive;
pub mod estimate;
pub mod hyperliquid;
pub mod market;
pub mod placement;
pub mod render;
pub mod state;
