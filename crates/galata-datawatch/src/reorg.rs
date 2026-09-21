//! Which rows a reorganisation contradicts.
//!
//! A [`Gap`](galata_wire::Gap) says *we did not see this*. A reorganisation
//! says *what we saw is no longer true*, and until something joins the two
//! datasets that second claim sits in the record unread.
//!
//! ```text
//!   seq  block  what
//!   ───  ─────  ────────────────────────────────────────────
//!   100   4100  transfer          ← old chain
//!   101   4101  transfer          ← old chain
//!   150      —  REORG 4100..4101  ← the divergence is recorded
//!   151   4100  transfer          ← new chain, SAME BLOCK
//!   152   4101  transfer          ← new chain, SAME BLOCK
//! ```
//!
//! **Blocks 4100–4101 appear twice, and only the first pair is superseded.**
//! So the test is two clauses, never one:
//!
//! ```text
//!   reorg.from_block <= row.block <= reorg.to_block
//!   row.stream_seq   <  reorg.stream_seq
//! ```
//!
//! The sequence clause is what makes the cursor's rewind safe, and the rewind
//! is what makes the sequence clause necessary. Neither works alone.
//!
//! # Derived, never stored
//!
//! The archive is append-only and a tape segment is rebuilt from it byte for
//! byte, so a `superseded` column would have to be written by editing rows
//! that are already durable — the one thing this store does not do.
//!
//! It is also **honest about time**. A row is superseded *as of the
//! reorganisations known so far*. As a stored column that would read as a
//! permanent property; derived from the reorg rows a reader happens to hold,
//! it reads as what it is.
//!
//! # What a clean row does NOT mean
//!
//! A row that no reorganisation contradicts is **not thereby confirmed**. It is
//! only *not contradicted by a reorganisation this reader knows about* — and a
//! reorganisation is detected by the hash trail, which sees only the blocks
//! capture actually asked for. A deep reorganisation during an outage leaves no
//! row at all.

/// A reorganisation, as much of one as this join needs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Reorganised {
    /// The first block no longer on the canonical chain.
    pub from_block: u64,
    /// The last block no longer on the canonical chain.
    pub to_block: u64,
    /// **When it was recorded**, in stream sequence — which is what separates
    /// the rows it contradicts from the rows that replaced them.
    pub stream_seq: u64,
}

impl Reorganised {
    /// Whether this reorganisation contradicts a row.
    pub fn contradicts(&self, block: u64, stream_seq: u64) -> bool {
        (self.from_block..=self.to_block).contains(&block) && stream_seq < self.stream_seq
    }

    /// How many blocks it replaced.
    pub fn depth(&self) -> u64 {
        self.to_block.saturating_sub(self.from_block) + 1
    }
}

/// One row, as much of one as this join needs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Row {
    /// The block it came from.
    pub block: u64,
    /// Where it sits in the stream.
    pub stream_seq: u64,
}

/// What became of one row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Standing {
    /// No known reorganisation contradicts it. **Not the same as confirmed** —
    /// see the module documentation.
    Uncontradicted,
    /// A reorganisation replaced the block this row came from, after this row
    /// was captured.
    Superseded(Reorganised),
}

impl Standing {
    /// Whether a reorganisation contradicts this row.
    pub fn is_superseded(&self) -> bool {
        matches!(self, Standing::Superseded(_))
    }
}

/// Where one row stands against every known reorganisation.
///
/// **The latest one wins** where several apply, which is the one a reader would
/// name when asked *what replaced this*. They are all in the record either way.
pub fn standing(reorgs: &[Reorganised], row: Row) -> Standing {
    reorgs
        .iter()
        .filter(|r| r.contradicts(row.block, row.stream_seq))
        .max_by_key(|r| r.stream_seq)
        .map(|r| Standing::Superseded(*r))
        .unwrap_or(Standing::Uncontradicted)
}

/// The standing of every row, in order.
pub fn standings(reorgs: &[Reorganised], rows: &[Row]) -> Vec<Standing> {
    rows.iter().map(|row| standing(reorgs, *row)).collect()
}

/// How many of these rows a reorganisation contradicts.
///
/// The number an operator wants after a reorganisation: *how much of what I
/// captured is no longer true?*
pub fn superseded_count(reorgs: &[Reorganised], rows: &[Row]) -> usize {
    rows.iter()
        .filter(|row| standing(reorgs, **row).is_superseded())
        .count()
}

/// The join as SQL, for a reader holding the tape rather than these types.
///
/// Written here, beside the implementation, so the two cannot drift into saying
/// different things — the failure mode a second implementation of a rule has.
pub const AS_SQL: &str = "\
SELECT t.*, r.old_hash AS superseded_by
FROM   transfers t
LEFT JOIN reorgs r
       ON t.block BETWEEN r.from_block AND r.to_block
      AND t.stream_seq < r.stream_seq";

#[cfg(test)]
mod tests {
    use super::*;

    /// The table from the module documentation, exactly.
    fn after_a_rewind() -> (Vec<Reorganised>, Vec<Row>) {
        let reorgs = vec![Reorganised {
            from_block: 4100,
            to_block: 4101,
            stream_seq: 150,
        }];
        let rows = vec![
            Row {
                block: 4100,
                stream_seq: 100,
            },
            Row {
                block: 4101,
                stream_seq: 101,
            },
            Row {
                block: 4100,
                stream_seq: 151,
            },
            Row {
                block: 4101,
                stream_seq: 152,
            },
        ];
        (reorgs, rows)
    }

    #[test]
    fn the_old_rows_are_superseded_and_the_replacements_are_not() {
        let (reorgs, rows) = after_a_rewind();
        let standing = standings(&reorgs, &rows);
        assert!(standing[0].is_superseded());
        assert!(standing[1].is_superseded());
        // **The clause a block-range test alone would get wrong.** These carry
        // the same blocks and are the chain that replaced them.
        assert!(!standing[2].is_superseded());
        assert!(!standing[3].is_superseded());
        assert_eq!(superseded_count(&reorgs, &rows), 2);
    }

    #[test]
    fn a_block_outside_the_range_is_untouched() {
        let (reorgs, _) = after_a_rewind();
        // One block either side. A reorganisation replaces what it replaced.
        for block in [4099, 4102] {
            assert!(
                !standing(
                    &reorgs,
                    Row {
                        block,
                        stream_seq: 1
                    }
                )
                .is_superseded()
            );
        }
    }

    #[test]
    fn a_superseded_row_names_what_replaced_it() {
        let (reorgs, rows) = after_a_rewind();
        let Standing::Superseded(by) = standing(&reorgs, rows[0]) else {
            panic!("superseded");
        };
        // Not a boolean: *which* reorganisation is what a reader chases.
        assert_eq!(by, reorgs[0]);
        assert_eq!(by.depth(), 2);
    }

    #[test]
    fn the_latest_reorganisation_is_the_one_named() {
        // A block replaced twice. Both are in the record; the later one is
        // what a reader means by "what replaced this".
        let reorgs = vec![
            Reorganised {
                from_block: 10,
                to_block: 12,
                stream_seq: 50,
            },
            Reorganised {
                from_block: 11,
                to_block: 11,
                stream_seq: 90,
            },
        ];
        let Standing::Superseded(by) = standing(
            &reorgs,
            Row {
                block: 11,
                stream_seq: 1,
            },
        ) else {
            panic!("superseded");
        };
        assert_eq!(by.stream_seq, 90);
    }

    #[test]
    fn a_row_recorded_at_the_same_sequence_is_not_superseded() {
        // Strictly below. A row cannot be contradicted by something recorded at
        // the same instant as itself, and `<=` here would supersede the reorg
        // row's own dataset on a shared sequence.
        let reorgs = vec![Reorganised {
            from_block: 1,
            to_block: 9,
            stream_seq: 7,
        }];
        assert!(
            !standing(
                &reorgs,
                Row {
                    block: 5,
                    stream_seq: 7
                }
            )
            .is_superseded()
        );
    }

    #[test]
    fn no_reorganisations_contradicts_nothing() {
        let (_, rows) = after_a_rewind();
        assert_eq!(superseded_count(&[], &rows), 0);
        assert!(standings(&[], &rows).iter().all(|s| !s.is_superseded()));
    }

    #[test]
    fn the_sql_states_both_clauses() {
        // The documented query and the implementation must say the same thing;
        // a second implementation of a rule disagrees rather than fails.
        assert!(AS_SQL.contains("BETWEEN r.from_block AND r.to_block"));
        assert!(AS_SQL.contains("t.stream_seq < r.stream_seq"));
    }
}
