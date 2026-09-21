//! The connection's lifetime, and replacing it on the venue's declared policy.
//!
//! **The invariant is universal; the policy is declared per venue.** What must
//! be true everywhere is only that a handover the system chose to make does not
//! produce a gap, because none occurred. How that is achieved is the venue's
//! own fact.
//!
//! ```text
//!   NAIVE                          ROTATE AHEAD
//!   -----                          ------------
//!   session closes at ~10.4 min    open + SUBSCRIBE the replacement,
//!   reconnect, resubscribe         THEN close the current one, on a timer
//!   publish a gap                  set INSIDE the observed lifetime
//!
//!   p50 handover   1.00 s          coverage is continuous
//!   max handover  66.00 s          no gap published, because none occurred
//!   uncovered      0.371%
//!   ~1,200 book frames/day, gone
//! ```
//!
//! 0.371% sounds small and is roughly 1,200 frames a day on one series of one
//! instrument — lost in bursts, at reconnection, which is exactly when a market
//! is most likely to be moving.
//!
//! A venue stating no lifetime gets **no rotation timer**. A timer inherited
//! from another venue would rotate a connection that had no cap, on a cadence
//! measured somewhere else.
//!
//! **Nothing here reads a clock**: every moment is passed in by the loop, which
//! owns it.

use crate::venue::ConnectionPolicy;

/// What the session should do next, decided at a moment the caller supplies.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Act {
    /// Nothing is due.
    Wait,
    /// Send the venue's keepalive.
    Keepalive,
    /// Open and **subscribe** a replacement. The current connection stays open
    /// and delivering until the replacement is subscribed — which is what makes
    /// the handover free.
    OpenReplacement,
    /// The replacement is subscribed. Close the one it replaces.
    CloseReplaced,
}

/// One connection's lifetime under a venue's declared policy.
#[derive(Debug)]
pub struct Session {
    policy: ConnectionPolicy,
    opened_at_micros: i64,
    last_keepalive_micros: i64,
    replacement: Replacement,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Replacement {
    None,
    /// Opened, and not yet subscribed. The current connection is still the one
    /// delivering.
    Opening,
    /// Subscribed. The current connection can go.
    Subscribed,
}

impl Session {
    /// A session opened at a moment.
    pub fn opened(policy: ConnectionPolicy, at_micros: i64) -> Session {
        Session {
            policy,
            opened_at_micros: at_micros,
            last_keepalive_micros: at_micros,
            replacement: Replacement::None,
        }
    }

    /// What is due at this moment.
    ///
    /// Rotation outranks the keepalive: a connection being replaced does not
    /// need keeping alive, and sending one during a handover spends a message
    /// on a socket that is about to close.
    pub fn act(&self, now_micros: i64) -> Act {
        if self.replacement == Replacement::Subscribed {
            return Act::CloseReplaced;
        }
        if self.replacement == Replacement::None
            && let Some(after) = self.policy.rotate_after_secs()
            && now_micros - self.opened_at_micros >= (after as i64) * 1_000_000
        {
            return Act::OpenReplacement;
        }
        if self.replacement == Replacement::None {
            let due = (self.policy.keepalive_secs() as i64) * 1_000_000;
            if due > 0 && now_micros - self.last_keepalive_micros >= due {
                return Act::Keepalive;
            }
        }
        Act::Wait
    }

    /// Record that the keepalive went out.
    pub fn keepalive_sent(&mut self, at_micros: i64) {
        self.last_keepalive_micros = at_micros;
    }

    /// Record that a replacement is open but not yet subscribed.
    pub fn replacement_opening(&mut self) {
        self.replacement = Replacement::Opening;
    }

    /// Record that the replacement is subscribed. Only now may the replaced
    /// connection be closed — which is the whole of the handover argument.
    pub fn replacement_subscribed(&mut self) {
        self.replacement = Replacement::Subscribed;
    }

    /// The replacement could not be opened, so there is none in flight.
    ///
    /// **The current connection is kept.** A replacement that will not open is
    /// a reason to hold on to the one that works — and the venue's own
    /// lifetime is the bound on how long that can last, which is why
    /// `rotate_after_secs` is strictly inside it.
    pub fn replacement_abandoned(&mut self) {
        self.replacement = Replacement::None;
    }

    /// Whether a handover is under way.
    pub fn rotating(&self) -> bool {
        self.replacement != Replacement::None
    }

    /// How long this connection has been open, in seconds.
    pub fn age_secs(&self, now_micros: i64) -> u64 {
        ((now_micros - self.opened_at_micros).max(0) / 1_000_000) as u64
    }

    /// How long until the handover begins, where the venue declares a
    /// lifetime.
    pub fn next_handover_in_secs(&self, now_micros: i64) -> Option<u64> {
        let after = self.policy.rotate_after_secs()?;
        let due = self.opened_at_micros + (after as i64) * 1_000_000;
        Some(((due - now_micros).max(0) / 1_000_000) as u64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SEC: i64 = 1_000_000;

    fn rotating() -> ConnectionPolicy {
        ConnectionPolicy::RotateAhead {
            observed_lifetime_secs: 624,
            rotate_after_secs: 480,
            keepalive_secs: 20,
        }
    }

    #[test]
    fn the_replacement_is_subscribed_before_the_replaced_is_closed() {
        // The whole handover argument, as a state machine: nothing closes
        // until something else is already delivering.
        let mut s = Session::opened(rotating(), 0);
        assert_eq!(s.act(480 * SEC), Act::OpenReplacement);

        s.replacement_opening();
        assert_eq!(
            s.act(481 * SEC),
            Act::Wait,
            "opened but not subscribed: the current connection must keep delivering"
        );

        s.replacement_subscribed();
        assert_eq!(s.act(481 * SEC), Act::CloseReplaced);
    }

    #[test]
    fn a_venue_with_no_lifetime_is_not_rotated() {
        // A timer inherited from another venue would rotate a connection that
        // had no cap, on a cadence measured somewhere else.
        let s = Session::opened(ConnectionPolicy::KeepAliveOnly { keepalive_secs: 60 }, 0);
        assert_eq!(s.next_handover_in_secs(0), None);
        for hours in 1..24 {
            assert_ne!(s.act(hours * 3_600 * SEC), Act::OpenReplacement);
        }
    }

    #[test]
    fn the_keepalive_is_due_on_its_own_cadence() {
        let mut s = Session::opened(rotating(), 0);
        assert_eq!(s.act(19 * SEC), Act::Wait);
        assert_eq!(s.act(20 * SEC), Act::Keepalive);
        s.keepalive_sent(20 * SEC);
        assert_eq!(s.act(21 * SEC), Act::Wait);
        assert_eq!(s.act(40 * SEC), Act::Keepalive);
    }

    #[test]
    fn a_rotating_session_does_not_also_keepalive() {
        // A socket about to close does not need keeping alive, and the message
        // would be spent on it.
        let mut s = Session::opened(rotating(), 0);
        s.replacement_opening();
        assert_eq!(s.act(1_000 * SEC), Act::Wait);
    }

    #[test]
    fn rotation_begins_inside_the_observed_lifetime() {
        // 480 < 624, with margin. The relationship is also a const assertion
        // at the venue's constants; this asserts the session honours it.
        let s = Session::opened(rotating(), 0);
        assert_eq!(s.next_handover_in_secs(0), Some(480));
        assert_eq!(s.next_handover_in_secs(479 * SEC), Some(1));
        assert_eq!(s.next_handover_in_secs(700 * SEC), Some(0));
        assert_eq!(s.age_secs(700 * SEC), 700);
    }
}
