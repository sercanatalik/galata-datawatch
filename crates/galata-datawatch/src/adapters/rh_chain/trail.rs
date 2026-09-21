//! **The one absence this system can prove.**
//!
//! Every other gap is inferred from an event we witnessed — a session lost, a
//! crash, a restart — because on a stream, *nothing arrived* and *nothing
//! happened* are the same picture. Invariant 3 exists for that reason.
//!
//! A chain reorganisation is the exception. When the chain replaces a block,
//! the rows captured from the old one describe something that **provably did
//! not happen**, and the proof is two hashes at one height.
//!
//! ```text
//!   captured:  … ─[A]─[B]─[C]
//!   arriving:           [D] parentHash = C   the chain agrees, extend
//!   arriving:           [D] parentHash = X   X is not the C we hold — a fork
//! ```
//!
//! Detection is by **parent linkage**, not by re-fetching hashes and comparing
//! them. Every block already carries its parent's hash, so the check costs
//! nothing beyond what was fetched anyway, and the fork is known at the moment
//! the replacement arrives rather than whenever somebody next looks.

use std::collections::BTreeMap;

/// A block, as far as the trail cares.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Seen {
    /// Its height.
    pub number: u64,
    /// Its own hash.
    pub hash: String,
    /// Its predecessor's hash — **the whole detection mechanism**.
    pub parent_hash: String,
}

/// A fork, with the proof carried alongside the claim.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Reorg {
    /// The lowest height known to have changed.
    pub from_block: u64,
    /// The highest.
    pub to_block: u64,
    /// The hash the trail held there.
    pub old_hash: String,
    /// The hash the chain now presents.
    pub new_hash: String,
}

impl Reorg {
    /// How many blocks were replaced.
    pub fn depth(&self) -> u64 {
        self.to_block
            .saturating_sub(self.from_block)
            .saturating_add(1)
    }
}

/// What taking a block told us.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Advance {
    /// The chain agreed and the trail extended.
    Extended,
    /// The block does not follow the last one held, so linkage says nothing.
    ///
    /// **Not a reorganisation.** A trail that has not seen the predecessor
    /// cannot tell a fork from a first sight, and claiming one would be an
    /// inference dressed as a proof — which is the one thing this module must
    /// not do.
    NotLinked,
    /// The chain disagreed.
    Reorganised(Reorg),
}

/// The hashes recently captured, bounded by finality.
#[derive(Debug, Clone)]
pub struct BlockTrail {
    seen: BTreeMap<u64, String>,
    depth: u64,
}

impl BlockTrail {
    /// A trail retaining `depth` blocks.
    ///
    /// **More than the measured finality lag, not exactly it.** A block at or
    /// below the finalized frontier cannot be reorganised, so its hash is dead
    /// weight — but the lag is a measurement of one moment rather than a
    /// guarantee, and a trail one block too short cannot tell *a fork deeper
    /// than I remember* from *no fork*. Too long costs a hash.
    pub fn new(depth: u64) -> BlockTrail {
        BlockTrail {
            seen: BTreeMap::new(),
            depth: depth.max(1),
        }
    }

    /// How many hashes are held.
    pub fn len(&self) -> usize {
        self.seen.len()
    }

    /// Whether any are.
    pub fn is_empty(&self) -> bool {
        self.seen.is_empty()
    }

    /// The highest block held.
    pub fn tip(&self) -> Option<u64> {
        self.seen.keys().next_back().copied()
    }

    /// The hash held at a height.
    pub fn hash_at(&self, number: u64) -> Option<&str> {
        self.seen.get(&number).map(String::as_str)
    }

    /// Take a block, and say what it told us.
    pub fn advance(&mut self, block: &Seen) -> Advance {
        let outcome = match block.number.checked_sub(1).and_then(|n| self.seen.get(&n)) {
            // Nothing to link against: a first sight, or a jump. Silence is the
            // honest answer, and it is not a reorganisation.
            None => Advance::NotLinked,
            Some(held) if held.eq_ignore_ascii_case(&block.parent_hash) => Advance::Extended,
            Some(held) => {
                let parent = block.number - 1;
                // How far back the trail stops agreeing is unknowable from one
                // block — the arriving block names only its parent. So the
                // claim is exactly what the evidence supports: THIS height
                // changed, and here are the two hashes.
                Advance::Reorganised(Reorg {
                    from_block: parent,
                    to_block: self.tip().unwrap_or(parent).max(parent),
                    old_hash: held.clone(),
                    new_hash: block.parent_hash.clone(),
                })
            }
        };

        if let Advance::Reorganised(ref reorg) = outcome {
            // The replaced heights are dropped from the TRAIL, so the next
            // block links against the new chain. The RECORD keeps its rows —
            // those bytes arrived, and the reorganisation is a second fact
            // recorded beside them.
            self.seen.retain(|height, _| *height < reorg.from_block);
        }

        self.seen.insert(block.number, block.hash.clone());
        self.trim();
        outcome
    }

    fn trim(&mut self) {
        let Some(tip) = self.tip() else { return };
        let floor = tip.saturating_sub(self.depth);
        self.seen.retain(|height, _| *height >= floor);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn block(number: u64, hash: &str, parent: &str) -> Seen {
        Seen {
            number,
            hash: hash.into(),
            parent_hash: parent.into(),
        }
    }

    /// A trail holding three linked blocks: 10←11←12.
    fn linked() -> BlockTrail {
        let mut trail = BlockTrail::new(100);
        trail.advance(&block(10, "0xaa", "0x09"));
        trail.advance(&block(11, "0xbb", "0xaa"));
        trail.advance(&block(12, "0xcc", "0xbb"));
        trail
    }

    #[test]
    fn the_chain_agreeing_reports_nothing() {
        let mut trail = linked();
        assert_eq!(trail.advance(&block(13, "0xdd", "0xcc")), Advance::Extended);
        assert_eq!(trail.tip(), Some(13));
    }

    #[test]
    fn a_fork_names_both_hashes() {
        // The proof carried with the claim. A reorganisation without the two
        // hashes is not checkable, and being checkable is the whole point.
        let mut trail = linked();
        let outcome = trail.advance(&block(13, "0xdd", "0xFORKED"));
        match outcome {
            Advance::Reorganised(reorg) => {
                assert_eq!(reorg.from_block, 12);
                assert_eq!(reorg.old_hash, "0xcc", "the hash we held");
                assert_eq!(reorg.new_hash, "0xFORKED", "the hash now present");
                assert!(reorg.depth() >= 1);
            }
            other => panic!("expected a reorganisation, got {other:?}"),
        }
    }

    #[test]
    fn a_first_sight_is_not_a_reorganisation() {
        // A trail with nothing to link against cannot tell a fork from a first
        // sight, and claiming one would be an inference dressed as a proof.
        let mut trail = BlockTrail::new(100);
        assert_eq!(
            trail.advance(&block(500, "0xaa", "0x4ff")),
            Advance::NotLinked
        );
        assert_eq!(trail.len(), 1, "it is still recorded");
    }

    #[test]
    fn a_jump_in_the_trail_is_not_a_reorganisation() {
        let mut trail = linked();
        assert_eq!(
            trail.advance(&block(99, "0xzz", "0xyy")),
            Advance::NotLinked
        );
    }

    #[test]
    fn a_hash_comparison_is_case_insensitive() {
        // Nodes disagree about hex case, and a fork reported because one said
        // 0xAA and another 0xaa would be a false alarm on every block.
        let mut trail = BlockTrail::new(100);
        trail.advance(&block(10, "0xABCDEF", "0x09"));
        assert_eq!(
            trail.advance(&block(11, "0xbb", "0xabcdef")),
            Advance::Extended
        );
    }

    #[test]
    fn after_a_fork_the_trail_follows_the_new_chain() {
        let mut trail = linked();
        trail.advance(&block(13, "0xdd", "0xFORKED"));
        // The replaced heights are gone from the trail, and the next block on
        // the new chain links cleanly.
        assert_eq!(trail.hash_at(12), None);
        assert_eq!(trail.advance(&block(14, "0xee", "0xdd")), Advance::Extended);
    }

    #[test]
    fn the_trail_is_trimmed_to_its_depth() {
        // A block at or below the finalized frontier cannot be reorganised, so
        // its hash is dead weight.
        let mut trail = BlockTrail::new(5);
        let mut parent = "0x00".to_string();
        for n in 1..=50u64 {
            let hash = format!("0x{n:02x}");
            trail.advance(&block(n, &hash, &parent));
            parent = hash;
        }
        assert!(trail.len() <= 6, "held {}", trail.len());
        assert_eq!(trail.tip(), Some(50));
        assert!(trail.hash_at(1).is_none(), "an ancient hash is still held");
        assert!(trail.hash_at(50).is_some());
    }

    #[test]
    fn a_depth_of_zero_still_links_one_block() {
        // Otherwise the trail can never link anything and reports NotLinked for
        // ever — a detector that never detects.
        let mut trail = BlockTrail::new(0);
        trail.advance(&block(10, "0xaa", "0x09"));
        assert_eq!(trail.advance(&block(11, "0xbb", "0xaa")), Advance::Extended);
    }

    /// Six consecutive blocks **captured from Robinhood Chain, 2026-09-21**,
    /// with their real hashes.
    fn real_chain() -> Vec<Seen> {
        vec![
            block(
                68648093,
                "0xe93c183f4dc195dc1342555071b69906e0e89bf52f99a39043373a046c29847c",
                "0x5e10d0259c15ca2ffc5b4a1c5c5019d63e5b4a9124126f28d548040c368ffaf1",
            ),
            block(
                68648094,
                "0xdf06e72a00f5e6a072b235087d05112bfd4f7a588d66b3bb991cbd15108b15c5",
                "0xe93c183f4dc195dc1342555071b69906e0e89bf52f99a39043373a046c29847c",
            ),
            block(
                68648095,
                "0x7b3e7be33b086cac593a109bad9d4e38661947d2956644aa6208fec54f8948ae",
                "0xdf06e72a00f5e6a072b235087d05112bfd4f7a588d66b3bb991cbd15108b15c5",
            ),
            block(
                68648096,
                "0x0d8151d267590b7eaec9e25c9483c2e8bb99ab66d5d5c6ccda5998149ad8040b",
                "0x7b3e7be33b086cac593a109bad9d4e38661947d2956644aa6208fec54f8948ae",
            ),
            block(
                68648097,
                "0xde3c663d4ab51830a3ebe789dd2425b7bf4ed1c2d988c9be1f1aec4a35293cd1",
                "0x0d8151d267590b7eaec9e25c9483c2e8bb99ab66d5d5c6ccda5998149ad8040b",
            ),
            block(
                68648098,
                "0xa676b6a7bcfc7db9a8cb5fc39bed77b49102d4d8cf513b2ec7abddc827e6cc25",
                "0xde3c663d4ab51830a3ebe789dd2425b7bf4ed1c2d988c9be1f1aec4a35293cd1",
            ),
        ]
    }

    #[test]
    fn a_real_unforked_chain_reports_nothing() {
        // The detector's cost of being wrong is a false alarm on every block,
        // so it is checked against a chain that really did not fork.
        let mut trail = BlockTrail::new(100);
        let blocks = real_chain();
        assert_eq!(trail.advance(&blocks[0]), Advance::NotLinked, "first sight");
        for block in &blocks[1..] {
            assert_eq!(
                trail.advance(block),
                Advance::Extended,
                "block {} should have linked",
                block.number
            );
        }
        assert_eq!(trail.tip(), Some(blocks.last().unwrap().number));
    }

    #[test]
    fn one_altered_hash_in_a_real_chain_is_caught() {
        // The same blocks, with one hash changed as a reorganisation would
        // change it. Nothing else differs.
        let mut trail = BlockTrail::new(100);
        let mut blocks = real_chain();
        for block in &blocks[..3] {
            trail.advance(block);
        }
        let held = blocks[2].hash.clone();
        blocks[3].parent_hash = "0xdeadbeef".repeat(8);

        match trail.advance(&blocks[3]) {
            Advance::Reorganised(reorg) => {
                assert_eq!(reorg.from_block, blocks[2].number);
                assert_eq!(reorg.old_hash, held);
                assert!(reorg.new_hash.starts_with("0xdeadbeef"));
            }
            other => panic!("a changed parent went unnoticed: {other:?}"),
        }
    }
}
