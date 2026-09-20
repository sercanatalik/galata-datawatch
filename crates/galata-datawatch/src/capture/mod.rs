//! The capture process: the loop that owns the clock, and everything it drives.
//!
//! **The loop owns the clock.** Every timestamp reaching a payload, a gap or a
//! status message originates from a [`Clock`] injected here; no component below
//! reads one, and `scripts/check-clock-discipline.sh` asserts it. That is what
//! lets rotation, flush and staleness be driven at controlled times without
//! waiting.
//!
//! **Level-triggered everywhere.** Subscriptions converge toward a declared set
//! and status is a full snapshot on a timer. *A component that reacts to events
//! is wrong after any event it missed.*

pub mod clock;
pub mod coverage;
pub mod session;
pub mod subscriptions;

pub use clock::{Clock, SystemClock, TestClock};
pub use coverage::Coverage;
pub use session::{Act, Session};
pub use subscriptions::{Held, Outcome};
