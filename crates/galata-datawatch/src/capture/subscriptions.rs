//! Converging toward a declared set.
//!
//! **Level-triggered, not event-driven.** The loop asks what it is missing and
//! sends that; it never replays a diff against what it sent last time.
//!
//! *A component that reacts to events is wrong after any event it missed.* A
//! reconnect that replayed a diff would be correct only if it knew what the
//! previous connection held — and after an unexpected close it does not.
//! Converging toward the declared set needs no memory of what was sent.

use std::collections::BTreeMap;

use crate::venue::Subscription;

/// What the venue said about one subscription.
///
/// **Three outcomes, not two.** A venue may refuse for reasons that have
/// nothing to do with the request, and collapsing *refused* into *pending*
/// claims coverage the process does not have: a pending subscription may yet
/// arrive, and a refused one never will.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Outcome {
    /// **Sent, and nothing has arrived for it.**
    ///
    /// Reachable and meaningful: a subscription stays here until a payload
    /// arrives carrying its ticker and series. A venue that quietly ignored
    /// one leaves it here, and `subs_held` below `subs_declared` is what says
    /// so.
    Pending,
    /// **The venue is delivering it** — a payload has arrived for it.
    ///
    /// Not *sent*, and not *acknowledged*. This loop marked it on send once,
    /// which made the status surface read 24 of 24 whether or not the venue
    /// answered.
    Held,
    /// The venue said no.
    ///
    /// **Unreached on the venues in this tree**, and kept for one that
    /// refuses. Hyperliquid does not: an unlisted coin makes it **hang up**,
    /// taking every other subscription on the socket with it — measured at
    /// seventeen disconnections in eighteen seconds, which is why the universe
    /// check runs before anything connects.
    Refused {
        /// What it said.
        reason: String,
    },
}

/// What the loop is missing, and what it holds.
#[derive(Debug, Default)]
pub struct Held {
    declared: Vec<Subscription>,
    outcomes: BTreeMap<Subscription, Outcome>,
}

/// What a convergence pass produced.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct Convergence {
    /// What to send now.
    pub to_subscribe: Vec<Subscription>,
}

impl Held {
    /// An empty ledger.
    pub fn new() -> Held {
        Held::default()
    }

    /// Replace the declared set. **Only an explicit operator act reaches
    /// this** — configuration is never re-read because a file changed.
    pub fn declare(&mut self, declared: Vec<Subscription>) {
        self.declared = declared;
        // An outcome for something no longer declared is not a fact about the
        // current declaration, and keeping it would let a retired instrument
        // keep appearing as refused forever.
        self.outcomes.retain(|s, _| self.declared.contains(s));
    }

    /// What is declared and not yet held.
    ///
    /// A refused subscription is **not** retried here: the venue said no, and
    /// asking again every pass turns a refusal into a flood. It is retried on
    /// reconnect, because a new connection is a new answer.
    pub fn converge(&self) -> Convergence {
        Convergence {
            to_subscribe: self
                .declared
                .iter()
                .filter(|s| {
                    !matches!(
                        self.outcomes.get(s),
                        Some(Outcome::Held) | Some(Outcome::Refused { .. })
                    )
                })
                .cloned()
                .collect(),
        }
    }

    /// Record that a subscription was sent.
    pub fn mark_sent(&mut self, subscription: &Subscription) {
        self.outcomes
            .entry(subscription.clone())
            .or_insert(Outcome::Pending);
    }

    /// Record that the venue is delivering it.
    pub fn mark_held(&mut self, subscription: &Subscription) {
        self.outcomes.insert(subscription.clone(), Outcome::Held);
    }

    /// Record that the venue refused it.
    pub fn mark_refused(&mut self, subscription: &Subscription, reason: impl Into<String>) {
        self.outcomes.insert(
            subscription.clone(),
            Outcome::Refused {
                reason: reason.into(),
            },
        );
    }

    /// The connection went away. **Everything is forgotten**, including
    /// refusals: a new connection is a new answer, and carrying a refusal
    /// across one would leave a pair permanently unsubscribed because of a
    /// transient state at the venue.
    ///
    /// **The predecessor decided this the other way** — *refusals stand,
    /// because the venue's answer has not changed* — and offered an operator a
    /// way to clear one deliberately. Both readings are defensible and
    /// **neither has been exercised**, because no venue here produces a
    /// refusal. Recorded rather than argued: the first venue that refuses a
    /// subscription is what settles it.
    pub fn connection_lost(&mut self) {
        self.outcomes.clear();
    }

    /// What the venue said about one subscription, if anything.
    pub fn outcome(&self, subscription: &Subscription) -> Option<&Outcome> {
        self.outcomes.get(subscription)
    }

    /// The declared set.
    pub fn declared(&self) -> &[Subscription] {
        &self.declared
    }

    /// How many the venue is delivering.
    pub fn count_held(&self) -> usize {
        self.outcomes
            .values()
            .filter(|o| **o == Outcome::Held)
            .count()
    }

    /// How many the venue refused.
    pub fn count_refused(&self) -> usize {
        self.outcomes
            .values()
            .filter(|o| matches!(o, Outcome::Refused { .. }))
            .count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use galata_wire::{Series, Ticker};

    fn sub(ticker: &str, series: Series) -> Subscription {
        Subscription {
            ticker: Ticker::new(ticker).unwrap(),
            series,
        }
    }

    fn declared() -> Vec<Subscription> {
        vec![
            sub("BTC", Series::Quotes),
            sub("ETH", Series::Quotes),
            sub("BTC", Series::Trades),
        ]
    }

    #[test]
    fn reconnect_reissues_every_subscription() {
        // Without reference to what the previous connection held — which after
        // an unexpected close is not knowable.
        let mut held = Held::new();
        held.declare(declared());
        for s in declared() {
            held.mark_held(&s);
        }
        assert!(held.converge().to_subscribe.is_empty());

        held.connection_lost();
        assert_eq!(held.converge().to_subscribe.len(), 3);
    }

    #[test]
    fn a_refused_pair_is_never_reported_as_live() {
        let mut held = Held::new();
        held.declare(declared());
        held.mark_refused(&sub("ETH", Series::Quotes), "unknown coin");

        assert_eq!(held.count_refused(), 1);
        assert_eq!(held.count_held(), 0);
        assert!(matches!(
            held.outcome(&sub("ETH", Series::Quotes)),
            Some(Outcome::Refused { .. })
        ));
    }

    #[test]
    fn a_refusal_is_not_retried_every_pass_but_is_after_a_reconnect() {
        // Asking again every pass turns a refusal into a flood. A new
        // connection is a new answer.
        let mut held = Held::new();
        held.declare(declared());
        held.mark_refused(&sub("ETH", Series::Quotes), "unknown coin");
        assert_eq!(held.converge().to_subscribe.len(), 2, "not the refused one");

        held.connection_lost();
        assert_eq!(held.converge().to_subscribe.len(), 3, "all three again");
    }

    #[test]
    fn convergence_is_a_set_difference_not_a_diff() {
        let mut held = Held::new();
        held.declare(declared());
        held.mark_held(&sub("BTC", Series::Quotes));
        // Asked twice, the answer is the same — it is a question about state,
        // not about what happened since.
        assert_eq!(held.converge(), held.converge());
        assert_eq!(held.converge().to_subscribe.len(), 2);
    }

    #[test]
    fn an_outcome_for_something_undeclared_is_dropped() {
        // Otherwise a retired instrument keeps appearing as refused forever.
        let mut held = Held::new();
        held.declare(declared());
        held.mark_refused(&sub("ETH", Series::Quotes), "gone");
        held.declare(vec![sub("BTC", Series::Quotes)]);
        assert_eq!(held.count_refused(), 0);
    }
}
