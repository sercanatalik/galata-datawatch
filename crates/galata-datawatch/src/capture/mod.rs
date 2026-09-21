//! The capture process: the loop that owns the clock, and everything it drives.
//!
//! **The loop owns the clock.** Every timestamp reaching a payload, a gap or a
//! status message originates from a [`Clock`](crate::capture::Clock) injected here; no component below
//! reads one, and `scripts/check-clock-discipline.sh` asserts it. That is what
//! lets rotation, flush and staleness be driven at controlled times without
//! waiting.
//!
//! **Level-triggered everywhere.** Subscriptions converge toward a declared set
//! and status is a full snapshot on a timer. *A component that reacts to events
//! is wrong after any event it missed.*

pub mod clock;
pub mod coverage;
#[cfg(feature = "capture")]
pub mod cursor;
#[cfg(feature = "capture")]
pub mod poll;
pub mod run;
pub mod session;
pub mod status;
pub mod subscriptions;
pub mod walk;

pub use clock::{Clock, SystemClock, TestClock};
pub use coverage::Coverage;
#[cfg(feature = "capture")]
pub use cursor::Pass;
#[cfg(feature = "capture")]
pub use poll::{Polls, Refusal};
pub use run::{Capture, CaptureError, Fetch, WalkRequest, Wiring};
pub use session::{Act, Session};
pub use status::{Connection, PairState, PairStatus, Status, StatusFile};
pub use subscriptions::{Held, Outcome};
pub use walk::{Ask, Step, Walk, WalkInterval, WalkOutcome};
