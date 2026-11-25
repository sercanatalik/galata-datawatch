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

use galata_wire::Ticker;

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
    /// Instruments that left the set while this process ran, by ticker.
    ///
    /// **Beside the outcomes, not among them**, because the two have different
    /// lives: an outcome is a fact about one connection and is forgotten when
    /// that connection is lost, while an instrument that expired has expired
    /// on the next connection too. Kept among the outcomes, a reconnect would
    /// clear it and subscribe the venue to something that no longer exists.
    retired: BTreeMap<Ticker, Retirement>,
}

/// Why an instrument left the set, and when this process noticed.
///
/// Not a refusal and not a gap. A refusal is the venue declining something
/// that could have been served; a gap is coverage lost. An instrument that
/// expired was served until it stopped existing, and nothing was lost.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Retirement {
    /// In plain words: `expired`, or `no longer listed`.
    pub reason: String,
    /// When the loop retired it, on the loop's own clock.
    pub at_micros: i64,
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
        // A new declaration is a new statement of what is wanted. An
        // instrument retired under the old one is asked for again if the new
        // one names it, and retired again by the same evidence if it is still
        // gone — which costs one pass and keeps this from remembering a
        // verdict about a declaration that no longer exists.
        self.retired.clear();
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

    /// An instrument left the set: every series of it stops being declared,
    /// its outcomes are dropped, and the reason is kept.
    ///
    /// Returns whether anything changed, so a caller that retires on every
    /// reference refresh logs it once rather than every five minutes.
    pub fn retire(&mut self, ticker: &Ticker, reason: impl Into<String>, at_micros: i64) -> bool {
        if self.retired.contains_key(ticker) {
            return false;
        }
        if !self.declared.iter().any(|s| &s.ticker == ticker) {
            return false;
        }
        self.declared.retain(|s| &s.ticker != ticker);
        self.outcomes.retain(|s, _| &s.ticker != ticker);
        self.retired.insert(
            ticker.clone(),
            Retirement {
                reason: reason.into(),
                at_micros,
            },
        );
        true
    }

    /// Every instrument that left the set, in ticker order.
    pub fn retired(&self) -> impl Iterator<Item = (&Ticker, &Retirement)> {
        self.retired.iter()
    }

    /// Whether this instrument left the set.
    pub fn is_retired(&self, ticker: &Ticker) -> bool {
        self.retired.contains_key(ticker)
    }

    /// How many instruments left the set.
    pub fn count_retired(&self) -> usize {
        self.retired.len()
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

    #[test]
    fn a_retired_instrument_is_not_asked_for_again() {
        let mut held = Held::new();
        held.declare(declared());
        for s in declared() {
            held.mark_held(&s);
        }
        let btc = Ticker::new("BTC").unwrap();
        assert!(held.retire(&btc, "expired", 77));

        // Both of its series are gone from the declaration, and it is neither
        // held nor refused: it is not there to be either.
        assert_eq!(held.declared(), [sub("ETH", Series::Quotes)]);
        assert_eq!(held.count_held(), 1);
        assert_eq!(held.count_refused(), 0);
        assert_eq!(held.count_retired(), 1);
        assert!(held.converge().to_subscribe.is_empty());
        let (ticker, why) = held.retired().next().unwrap();
        assert_eq!(ticker, &btc);
        assert_eq!(why.reason, "expired");
        assert_eq!(why.at_micros, 77);
    }

    #[test]
    fn a_reconnect_does_not_bring_a_retired_instrument_back() {
        // An outcome is forgotten with its connection. An expiry is not: the
        // instrument has expired on the next connection too.
        let mut held = Held::new();
        held.declare(declared());
        held.retire(&Ticker::new("BTC").unwrap(), "expired", 1);

        held.connection_lost();
        assert_eq!(
            held.converge().to_subscribe,
            vec![sub("ETH", Series::Quotes)]
        );
        assert!(held.is_retired(&Ticker::new("BTC").unwrap()));
    }

    #[test]
    fn retiring_twice_or_retiring_a_stranger_changes_nothing() {
        let mut held = Held::new();
        held.declare(declared());
        let btc = Ticker::new("BTC").unwrap();
        assert!(held.retire(&btc, "expired", 1));
        // The second says nothing new, and keeps the first reason and time.
        assert!(!held.retire(&btc, "no longer listed", 2));
        assert_eq!(held.retired().next().unwrap().1.at_micros, 1);
        // An instrument that was never declared cannot leave.
        assert!(!held.retire(&Ticker::new("SOL").unwrap(), "expired", 3));
        assert_eq!(held.count_retired(), 1);
    }

    #[test]
    fn a_new_declaration_forgets_who_retired() {
        let mut held = Held::new();
        held.declare(declared());
        held.retire(&Ticker::new("BTC").unwrap(), "expired", 1);
        held.declare(declared());
        assert_eq!(held.count_retired(), 0);
        assert_eq!(held.converge().to_subscribe.len(), 3);
    }
}
