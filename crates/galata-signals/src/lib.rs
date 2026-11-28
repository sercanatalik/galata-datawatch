//! **The market-data signals, computed in Rust** (`add-galata-quant`, design D6).
//!
//! `py/signals`' calculators read the tape through galata-research and hand
//! their rows to `galata-signals-commit`; this crate does both itself, with
//! galata-quant's arithmetic — the one implementation galata-research's
//! `gr.models.corr` also calls. Slice 1 is `varcov`'s EWMA horizons (30m from
//! 1m bars, 1d and 1w from 1d, λ = 0.94) with what is derived from their Σ:
//! beta to BTC, absorption, surprise and turbulence. The rows are the signals
//! schema's, as `py/signals` writes them for these horizons, field for field.
//!
//! **The tape is the cursor**: a horizon is computed only when its newest
//! closed bar is later than the newest `asof` stored for it, and history is
//! never backfilled. **A refusal is a row**: a figure galata-quant will not give
//! is written absent with its reason. **One run, one commit**: a run already on
//! the tape is not written again (design D7).
//!
//! Nothing here reads a clock: the binary reads it once and passes
//! `computed_micros` down, so every rule is tested by setting it.

pub mod bars;
pub mod declared;
pub mod rows;
pub mod store;
pub mod varcov;

/// Which galata-quant computed a row: `galata-quant@<version>+<commit>`,
/// from the lockfile at build time.
pub const CODE: &str = env!("GALATA_QUANT_CODE");

/// The signal this slice writes.
pub const VARCOV: &str = "varcov";
