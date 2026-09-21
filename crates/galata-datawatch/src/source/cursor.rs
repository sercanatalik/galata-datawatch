//! Paging a chain by **block number**, because a timestamp does not identify a
//! block.
//!
//! **Measured on Robinhood Chain, 2026-09-21.** Twenty consecutive blocks carry
//! four distinct timestamps:
//!
//! ```text
//!   1789978217  →  blocks 68642966 … 68642974     nine blocks
//!   1789978218  →  blocks 68642975 … 68642983     nine blocks
//! ```
//!
//! Timestamps have one-second granularity and the chain produces about nine
//! blocks a second, so a timestamp names **nine blocks**. The predecessor pages
//! history by bumping `last_micros + 1ms` and asking for what follows; here
//! that asks for *everything after the second the last block was in*, and
//! **eight blocks in nine disappear** — silently, because the venue answered
//! every request correctly.
//!
//! A block number identifies exactly one block. That is the whole argument.

/// One request over a range of blocks, inclusive at both ends.
///
/// Inclusive because a chain's blocks are counted, not measured: there is no
/// value between block 10 and block 11 for a half-open end to exclude, and an
/// exclusive end invites an off-by-one that costs exactly one block per step.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlockStep {
    /// First block, included.
    pub from: u64,
    /// Last block, included.
    pub to: u64,
}

impl BlockStep {
    /// How many blocks this asks for.
    pub fn blocks(&self) -> u64 {
        self.to.saturating_sub(self.from).saturating_add(1)
    }
}

/// Why a range cannot be planned.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
#[non_exhaustive]
pub enum PlanError {
    /// The provider does not hold blocks that old.
    #[error(
        "block {from} is before this provider's earliest block {earliest}. A public node keeps \
         no archive, and asking anyway returns an empty answer that looks exactly like a range \
         with nothing in it"
    )]
    BeforeReach {
        /// What was asked for.
        from: u64,
        /// What the provider states it holds.
        earliest: u64,
    },
}

/// How a provider serves ranges of blocks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlockPaging {
    /// The most blocks one request may span.
    pub max_span: u64,
    /// The earliest block this provider holds.
    ///
    /// `None` where it states none. **Stated rather than assumed**: a public
    /// node with no archive answers a pre-reach range with an empty result,
    /// which is indistinguishable from a range that genuinely held nothing.
    pub earliest: Option<u64>,
}

impl BlockPaging {
    /// The steps covering `from..=to`.
    ///
    /// **No block is left unrequested**, and no step exceeds the declared span.
    /// Steps abut rather than overlap — unlike the time-ranged walk, where a
    /// venue's handling of a chunk boundary is its own business. Here a block
    /// number is exact, and re-requesting one costs a duplicate for no
    /// information.
    pub fn plan(&self, from: u64, to: u64) -> Result<Vec<BlockStep>, PlanError> {
        if let Some(earliest) = self.earliest
            && from < earliest
        {
            return Err(PlanError::BeforeReach { from, earliest });
        }
        if to < from || self.max_span == 0 {
            return Ok(Vec::new());
        }
        let mut steps = Vec::new();
        let mut cursor = from;
        loop {
            let end = cursor.saturating_add(self.max_span - 1).min(to);
            steps.push(BlockStep {
                from: cursor,
                to: end,
            });
            if end >= to {
                break;
            }
            cursor = end + 1;
        }
        Ok(steps)
    }
}

/// Which frontier a question is about.
///
/// **Two, and they are different questions.**
///
/// ```text
///   Head        what has arrived. Capture follows this, because a block that
///               is later reorganised away still ARRIVED, and the record
///               records arrivals.
///
///   Finalized   what cannot be taken back. A reader's bound is this, because
///               a bound that can move backwards is not a bound.
/// ```
///
/// **Measured, within a minute of each other:**
///
/// ```text
///   latest     68,642,714     ts 1789978193
///   safe       68,634,888      7,826 behind   13.1 min
///   finalized  68,631,036     11,678 behind   19.6 min
/// ```
///
/// `safe` is *not* offered. It is the figure that gets quoted — the roadmap
/// said "~13 min" and meant this one — and it can still be reorganised under a
/// fault. A bound taken there can move backwards.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Frontier {
    /// The newest block the node has.
    Head,
    /// The newest block that cannot be reorganised away.
    Finalized,
}

impl Frontier {
    /// The block tag a JSON-RPC call names.
    pub fn tag(&self) -> &'static str {
        match self {
            Frontier::Head => "latest",
            Frontier::Finalized => "finalized",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn paging(max_span: u64) -> BlockPaging {
        BlockPaging {
            max_span,
            earliest: None,
        }
    }

    #[test]
    fn no_block_is_left_unrequested() {
        // The property that matters. A gap between steps is a block nobody
        // asked for, and nothing would ever report it.
        let steps = paging(10).plan(100, 134).unwrap();
        let mut expected = 100;
        for step in &steps {
            assert_eq!(step.from, expected, "a gap before {step:?}");
            expected = step.to + 1;
        }
        assert_eq!(expected, 135, "the last step stops short");
        assert_eq!(steps.iter().map(|s| s.blocks()).sum::<u64>(), 35);
    }

    #[test]
    fn a_step_never_exceeds_the_declared_span() {
        for step in paging(10).plan(0, 99).unwrap() {
            assert!(step.blocks() <= 10, "{step:?} spans {}", step.blocks());
        }
    }

    #[test]
    fn steps_abut_rather_than_overlap() {
        // Unlike the time-ranged walk, where a venue's handling of a chunk
        // boundary is its own business. A block number is exact.
        let steps = paging(5).plan(0, 14).unwrap();
        for pair in steps.windows(2) {
            assert_eq!(pair[1].from, pair[0].to + 1, "{pair:?}");
        }
    }

    #[test]
    fn one_block_is_one_step_of_one() {
        let steps = paging(10).plan(7, 7).unwrap();
        assert_eq!(steps, vec![BlockStep { from: 7, to: 7 }]);
        assert_eq!(steps[0].blocks(), 1);
    }

    #[test]
    fn an_empty_or_backwards_range_plans_nothing() {
        assert!(paging(10).plan(10, 9).unwrap().is_empty());
        assert!(paging(0).plan(1, 100).unwrap().is_empty());
    }

    #[test]
    fn a_range_before_the_providers_reach_is_refused_by_name() {
        // A public node with no archive answers a pre-reach range with an
        // EMPTY RESULT, which is indistinguishable from a range that genuinely
        // held nothing. Refusing is the only way to tell them apart.
        let paging = BlockPaging {
            max_span: 10,
            earliest: Some(1_000),
        };
        let error = paging.plan(900, 1_100).unwrap_err();
        assert_eq!(
            error,
            PlanError::BeforeReach {
                from: 900,
                earliest: 1_000
            }
        );
        assert!(error.to_string().contains("no archive"), "{error}");
        // And a range inside the reach plans normally.
        assert!(!paging.plan(1_000, 1_010).unwrap().is_empty());
    }

    #[test]
    fn the_readers_frontier_is_finality_and_not_safe() {
        // `safe` is the figure that gets quoted and it can still be
        // reorganised under a fault. A bound that can move backwards is not a
        // bound, so it is not offered at all.
        assert_eq!(Frontier::Finalized.tag(), "finalized");
        assert_eq!(Frontier::Head.tag(), "latest");
        for frontier in [Frontier::Head, Frontier::Finalized] {
            assert_ne!(frontier.tag(), "safe");
        }
    }

    #[test]
    fn a_large_range_plans_without_overflowing() {
        let steps = paging(1_000).plan(u64::MAX - 10, u64::MAX).unwrap();
        assert_eq!(
            steps,
            vec![BlockStep {
                from: u64::MAX - 10,
                to: u64::MAX
            }]
        );
    }
}
