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
//!
//! **A dead connection is an unanswered keepalive.** A connection can die
//! without closing, and a quiet market and a dead connection deliver the same
//! nothing. A venue that declares how soon it answers its keepalive
//! ([`ConnectionPolicy::reply_secs`]) can tell them apart: once a keepalive has
//! gone unanswered for that long — no frame of any kind since — the connection
//! is dead, whatever the market is doing. Measured on 2026-11-25: a change in
//! the host's network killed a container's connection, capture said
//! "connected" for minutes, and a handover then claimed the dead minutes as
//! covered. A venue that declares no reply time keeps the older rule: nothing
//! is inferred from silence.

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
    /// The replacement is subscribed, and the connection it replaces has shown
    /// it was alive since. Close the replaced one; coverage was continuous.
    CloseReplaced,
    /// The replacement is subscribed, and the connection it replaces showed
    /// nothing within the venue's reply time. It was dead: the time since each
    /// pair's last receipt is a gap, up to the replacement's subscription.
    CloseDead,
    /// A keepalive went unanswered for the venue's reply time. The connection
    /// is dead though it has not closed.
    Dead,
}

/// One connection's lifetime under a venue's declared policy.
#[derive(Debug)]
pub struct Session {
    policy: ConnectionPolicy,
    opened_at_micros: i64,
    last_keepalive_micros: i64,
    /// The last frame of any kind this connection delivered.
    last_frame_micros: i64,
    /// The oldest keepalive no frame has followed yet.
    unanswered_since_micros: Option<i64>,
    replacement: Replacement,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Replacement {
    None,
    /// Opened, and not yet subscribed. The current connection is still the one
    /// delivering.
    Opening,
    /// Subscribed at this moment. The current connection can go once it has
    /// shown it was alive after it.
    Subscribed {
        at_micros: i64,
    },
}

impl Session {
    /// A session opened at a moment.
    pub fn opened(policy: ConnectionPolicy, at_micros: i64) -> Session {
        Session {
            policy,
            opened_at_micros: at_micros,
            last_keepalive_micros: at_micros,
            last_frame_micros: at_micros,
            unanswered_since_micros: None,
            replacement: Replacement::None,
        }
    }

    /// The venue's reply time, in microseconds, where it declares one.
    fn reply_micros(&self) -> Option<i64> {
        self.policy.reply_secs().map(|s| (s as i64) * 1_000_000)
    }

    /// What is due at this moment.
    ///
    /// **A dead connection outranks everything**: a handover from it would
    /// claim its silence as covered, and a keepalive on it is sent to nobody.
    /// Rotation outranks the keepalive: a connection being replaced does not
    /// need keeping alive, and sending one during a handover spends a message
    /// on a socket that is about to close — except the one keepalive that asks
    /// the replaced connection to show it is still alive.
    pub fn act(&self, now_micros: i64) -> Act {
        if let Replacement::Subscribed { at_micros } = self.replacement {
            let Some(reply) = self.reply_micros() else {
                // Nothing can be asked of a venue that does not answer: the
                // handover is as it always was.
                return Act::CloseReplaced;
            };
            if self.last_frame_micros > at_micros {
                return Act::CloseReplaced;
            }
            if now_micros - at_micros >= reply {
                return Act::CloseDead;
            }
            if self.last_keepalive_micros < at_micros {
                return Act::Keepalive;
            }
            return Act::Wait;
        }
        if let (Some(reply), Some(since)) = (self.reply_micros(), self.unanswered_since_micros)
            && now_micros - since >= reply
        {
            return Act::Dead;
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
        // The OLDEST unanswered one: a second keepalive sent into a silence
        // does not restart the count.
        if self.unanswered_since_micros.is_none() {
            self.unanswered_since_micros = Some(at_micros);
        }
    }

    /// Record that this connection delivered a frame of any kind: data, a
    /// keepalive's answer, an acknowledgement, a protocol ping or pong. Any of
    /// them shows the connection alive.
    pub fn frame_received(&mut self, at_micros: i64) {
        self.last_frame_micros = self.last_frame_micros.max(at_micros);
        if self
            .unanswered_since_micros
            .is_some_and(|since| at_micros >= since)
        {
            self.unanswered_since_micros = None;
        }
    }

    /// How long this connection has delivered nothing, in seconds.
    pub fn silent_secs(&self, now_micros: i64) -> u64 {
        ((now_micros - self.last_frame_micros).max(0) / 1_000_000) as u64
    }

    /// Record that a replacement is open but not yet subscribed.
    pub fn replacement_opening(&mut self) {
        self.replacement = Replacement::Opening;
    }

    /// Record that the replacement is subscribed. Only now may the replaced
    /// connection be closed — which is the whole of the handover argument —
    /// and, where the venue answers its keepalive, only once the replaced
    /// connection has shown it was alive after this moment.
    pub fn replacement_subscribed(&mut self, at_micros: i64) {
        self.replacement = Replacement::Subscribed { at_micros };
    }

    /// When the replacement was subscribed, while a handover waits on it. A
    /// dead replaced connection's gaps end here, because the replacement covers
    /// from here.
    pub fn replacement_subscribed_at(&self) -> Option<i64> {
        match self.replacement {
            Replacement::Subscribed { at_micros } => Some(at_micros),
            Replacement::None | Replacement::Opening => None,
        }
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
            reply_secs: None,
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

        s.replacement_subscribed(481 * SEC);
        assert_eq!(
            s.act(481 * SEC),
            Act::CloseReplaced,
            "a venue declaring no reply time cannot be asked to show it is alive: closed at once, as always"
        );
    }

    // ---- a dead connection is an unanswered keepalive -----------------------

    /// Hyperliquid's shape: a ping every 20 s, answered within 10 s.
    fn answering() -> ConnectionPolicy {
        ConnectionPolicy::RotateAhead {
            observed_lifetime_secs: 624,
            rotate_after_secs: 480,
            keepalive_secs: 20,
            reply_secs: Some(10),
        }
    }

    #[test]
    fn an_unanswered_keepalive_is_dead_at_exactly_the_reply_time() {
        let mut s = Session::opened(answering(), 0);
        assert_eq!(s.act(20 * SEC), Act::Keepalive);
        s.keepalive_sent(20 * SEC);
        assert_eq!(s.act(30 * SEC - 1), Act::Wait, "inside the reply time");
        assert_eq!(s.act(30 * SEC), Act::Dead);
        assert_eq!(s.silent_secs(30 * SEC), 30, "silent since it opened");

        // A second keepalive sent into the silence does not restart the count.
        let mut s = Session::opened(answering(), 0);
        s.keepalive_sent(20 * SEC);
        s.keepalive_sent(28 * SEC);
        assert_eq!(s.act(30 * SEC), Act::Dead);
    }

    #[test]
    fn a_frame_from_before_the_keepalive_is_not_its_answer() {
        let mut s = Session::opened(answering(), 0);
        s.frame_received(19 * SEC);
        s.keepalive_sent(20 * SEC);
        assert_eq!(s.act(30 * SEC), Act::Dead);
    }

    #[test]
    fn a_quiet_hour_with_every_keepalive_answered_is_never_dead() {
        // No data at all, only the venue's answers 300 ms after each keepalive:
        // a quiet market on a live connection. Silence is still not a death.
        let mut s = Session::opened(
            ConnectionPolicy::KeepAliveOnly {
                keepalive_secs: 20,
                reply_secs: Some(10),
            },
            0,
        );
        for second in 1..=3_600 {
            let now = second * SEC;
            let act = s.act(now);
            assert_ne!(
                act,
                Act::Dead,
                "found dead at {second} s on a live connection"
            );
            if act == Act::Keepalive {
                s.keepalive_sent(now);
                s.frame_received(now + 300_000);
            }
        }
    }

    #[test]
    fn a_venue_declaring_no_reply_time_is_never_dead() {
        // Nothing can be inferred from the silence of a venue that does not
        // say it answers: an hour of unanswered keepalives is not a death.
        let mut s = Session::opened(
            ConnectionPolicy::KeepAliveOnly {
                keepalive_secs: 20,
                reply_secs: None,
            },
            0,
        );
        for second in 1..=3_600 {
            let now = second * SEC;
            let act = s.act(now);
            assert_ne!(act, Act::Dead);
            if act == Act::Keepalive {
                s.keepalive_sent(now);
            }
        }
    }

    #[test]
    fn a_dead_connection_outranks_a_due_handover() {
        // A handover from a dead connection would claim its silence as covered.
        let mut s = Session::opened(answering(), 0);
        s.keepalive_sent(470 * SEC);
        assert_eq!(s.act(480 * SEC), Act::Dead, "the handover is due too");
    }

    // ---- a handover proves the old connection alive ---------------------------

    #[test]
    fn a_handover_closes_a_live_connection_once_it_has_shown_itself_alive() {
        let mut s = Session::opened(answering(), 0);
        assert_eq!(s.act(480 * SEC), Act::OpenReplacement);
        s.replacement_subscribed(481 * SEC);
        assert_eq!(s.replacement_subscribed_at(), Some(481 * SEC));

        assert_eq!(
            s.act(481 * SEC),
            Act::Keepalive,
            "the replaced connection is asked to show it is alive"
        );
        s.keepalive_sent(481 * SEC);
        assert_eq!(s.act(481 * SEC + 1), Act::Wait);

        s.frame_received(481 * SEC);
        assert_eq!(
            s.act(481 * SEC + 1),
            Act::Wait,
            "a frame at the subscription's own moment is not after it"
        );
        s.frame_received(481 * SEC + 200_000);
        assert_eq!(s.act(481 * SEC + 300_000), Act::CloseReplaced);
    }

    #[test]
    fn a_handover_from_a_connection_that_shows_nothing_closes_it_as_dead() {
        let mut s = Session::opened(answering(), 0);
        s.frame_received(470 * SEC);
        s.replacement_subscribed(481 * SEC);
        s.keepalive_sent(481 * SEC);
        assert_eq!(s.act(491 * SEC - 1), Act::Wait, "inside the reply time");
        assert_eq!(s.act(491 * SEC), Act::CloseDead);
        assert_eq!(
            s.replacement_subscribed_at(),
            Some(481 * SEC),
            "the dead connection's gaps end where the replacement's coverage begins"
        );
    }

    #[test]
    fn a_venue_with_no_lifetime_is_not_rotated() {
        // A timer inherited from another venue would rotate a connection that
        // had no cap, on a cadence measured somewhere else.
        let s = Session::opened(
            ConnectionPolicy::KeepAliveOnly {
                keepalive_secs: 60,
                reply_secs: None,
            },
            0,
        );
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
