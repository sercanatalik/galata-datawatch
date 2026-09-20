//! What a segment's name carries: the position range it covers.
//!
//! **Naming carries the durability fact.** A directory listing has to answer
//! *what is durable* without opening a file, because that is what the restart
//! path needs — the record's gap-on-restart reads the last durable position,
//! and a resume reads it too. Both come from the filename.
//!
//! # Why three variants and not one number
//!
//! The predecessor had two segment names, one per store: an arrival range in
//! microseconds for the record, and a stream range for the cache. The record
//! could only use the first.
//!
//! A venue that pages by block number has no microsecond to resume from.
//! Worse, the predecessor's forward-paging advance is
//! `cursor = last_micros + 1_000`, which is correct for hourly rows and, on a
//! chain whose block times are documented as sub-second and irregular, skips
//! every block sharing a millisecond with the last one read. The defect is
//! silent: the walk reports success and the record has holes nobody can see.
//!
//! Naming a segment by the position the source actually advances through
//! removes the conversion in which the loss happens.

use std::fmt;

/// The position range a segment covers.
///
/// `#[non_exhaustive]`: a fourth variant is a minor version rather than a
/// breaking one, which matters because this crate may publish before its two
/// consumers exist.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum Cursor {
    /// An arrival range, in microseconds on **our** clock.
    ///
    /// Carries a pid and a flush counter because two payloads can legitimately
    /// share a receipt microsecond — without them a second flush in the same
    /// microsecond would overwrite the first segment, and the loss would be
    /// invisible.
    Time {
        /// The earliest arrival in the segment.
        first_micros: i64,
        /// The latest arrival in the segment. This is what a restart reads.
        last_micros: i64,
        /// The writing process.
        pid: u32,
        /// Monotonic within the process, per flush.
        seq: u64,
    },
    /// A block range on a chain.
    ///
    /// No tiebreaker: a block range is already unique per source, because one
    /// writer owns one partition and blocks do not repeat.
    Block {
        /// The first block covered, inclusive.
        first: u64,
        /// The last block covered, inclusive.
        last: u64,
    },
    /// A stream position range.
    ///
    /// A redelivered batch produces a range overlapping an existing one, which
    /// is visible from the names alone — stronger than a dedup key, which
    /// collapses duplicates without recording that it did.
    Seq {
        /// The first position covered, inclusive.
        first: u64,
        /// The last position covered, inclusive.
        last: u64,
    },
}

/// The extension every committed segment carries.
const EXT: &str = ".parquet";
/// The suffix a segment carries while it is still being written.
const WRITING: &str = ".writing";

impl Cursor {
    /// The committed filename.
    ///
    /// Fields are separated by `_` rather than `-` so that a negative
    /// microsecond round-trips: `-` is also a minus sign, and the predecessor's
    /// `-`-separated form cannot be parsed back for any timestamp before 1970.
    /// That is not a case the record produces, and a name that silently fails
    /// to parse is not a property worth leaving to luck.
    pub fn file_name(&self) -> String {
        match self {
            Cursor::Time {
                first_micros,
                last_micros,
                pid,
                seq,
            } => format!("t-{first_micros}_{last_micros}_{pid}_{seq}{EXT}"),
            Cursor::Block { first, last } => format!("b-{first}_{last}{EXT}"),
            Cursor::Seq { first, last } => format!("s-{first}_{last}{EXT}"),
        }
    }

    /// The temporary this segment is written under before it is committed.
    ///
    /// A leading dot and a `.writing` suffix, so a partial file is not merely
    /// unfinished but obviously **not a segment** — and so a listing filtering
    /// on the extension skips it.
    pub fn temp_file_name(&self) -> String {
        format!(".{}{WRITING}", self.file_name())
    }

    /// Whether a filename is a temporary rather than a segment.
    pub fn is_temp(file_name: &str) -> bool {
        file_name.starts_with('.') && file_name.ends_with(WRITING)
    }

    /// Recover a cursor from a filename, so a directory listing answers the
    /// durability question without any file being opened.
    ///
    /// `None` for anything that is not a segment name, including a temporary.
    pub fn parse(file_name: &str) -> Option<Cursor> {
        let stem = file_name.strip_suffix(EXT)?;
        let (tag, rest) = stem.split_once('-')?;
        let fields: Vec<&str> = rest.split('_').collect();
        match (tag, fields.as_slice()) {
            ("t", [first, last, pid, seq]) => Some(Cursor::Time {
                first_micros: first.parse().ok()?,
                last_micros: last.parse().ok()?,
                pid: pid.parse().ok()?,
                seq: seq.parse().ok()?,
            }),
            ("b", [first, last]) => Some(Cursor::Block {
                first: first.parse().ok()?,
                last: last.parse().ok()?,
            }),
            ("s", [first, last]) => Some(Cursor::Seq {
                first: first.parse().ok()?,
                last: last.parse().ok()?,
            }),
            _ => None,
        }
    }

    /// Which variant this is, for a listing that must refuse to order two
    /// segments whose positions are not comparable.
    pub fn variant(&self) -> Variant {
        match self {
            Cursor::Time { .. } => Variant::Time,
            Cursor::Block { .. } => Variant::Block,
            Cursor::Seq { .. } => Variant::Seq,
        }
    }

    /// The last position this segment covers, as a number within its own
    /// variant.
    ///
    /// Comparable **only** against another cursor of the same variant. A block
    /// height and a stream position order the same way and mean different
    /// things, so [`Variant`] must be checked before this is used.
    pub fn last_position(&self) -> i128 {
        match self {
            Cursor::Time { last_micros, .. } => *last_micros as i128,
            Cursor::Block { last, .. } | Cursor::Seq { last, .. } => *last as i128,
        }
    }

    /// The first position this segment covers, under the same rule as
    /// [`Cursor::last_position`].
    pub fn first_position(&self) -> i128 {
        match self {
            Cursor::Time { first_micros, .. } => *first_micros as i128,
            Cursor::Block { first, .. } | Cursor::Seq { first, .. } => *first as i128,
        }
    }

    /// The sort key within a partition: the start, then the flush counter
    /// where one exists, so two flushes in one microsecond order stably.
    pub(crate) fn sort_key(&self) -> (i128, u64) {
        match self {
            Cursor::Time {
                first_micros, seq, ..
            } => (*first_micros as i128, *seq),
            Cursor::Block { first, .. } | Cursor::Seq { first, .. } => (*first as i128, 0),
        }
    }
}

impl fmt::Display for Cursor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.file_name())
    }
}

/// Which kind of position a cursor carries.
///
/// A distinct type rather than a comparison on the numbers, because a block
/// height and a stream position order identically and mean different things: a
/// store that confused them would report a frontier wrong by a factor nobody
/// would notice.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[non_exhaustive]
pub enum Variant {
    /// Microseconds on our clock.
    Time,
    /// Block height on a chain.
    Block,
    /// Position in a stream.
    Seq,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn time() -> Cursor {
        Cursor::Time {
            first_micros: 1_758_326_400_000_000,
            last_micros: 1_758_326_460_000_000,
            pid: 4711,
            seq: 3,
        }
    }

    #[test]
    fn a_cursor_round_trips_through_its_filename() {
        for cursor in [
            time(),
            Cursor::Block {
                first: 1,
                last: 2_000,
            },
            Cursor::Seq {
                first: 123_456,
                last: 234_567,
            },
        ] {
            let name = cursor.file_name();
            assert_eq!(Cursor::parse(&name), Some(cursor), "{name}");
        }
    }

    #[test]
    fn a_negative_arrival_round_trips() {
        // Not a case the record produces — it would be an arrival before 1970
        // — but the predecessor's `-`-separated form cannot parse one back at
        // all, and a name that silently fails to parse is a segment that
        // vanishes from every listing.
        let cursor = Cursor::Time {
            first_micros: -1,
            last_micros: -1,
            pid: 1,
            seq: 0,
        };
        assert_eq!(Cursor::parse(&cursor.file_name()), Some(cursor));
    }

    #[test]
    fn a_block_and_a_sequence_are_not_the_same_position() {
        // They carry identical numbers and order identically. Only the variant
        // tells them apart, which is why it is in the name and in the type.
        let block = Cursor::Block { first: 1, last: 9 };
        let seq = Cursor::Seq { first: 1, last: 9 };
        assert_ne!(block, seq);
        assert_ne!(block.file_name(), seq.file_name());
        assert_ne!(block.variant(), seq.variant());
        assert_eq!(block.last_position(), seq.last_position());
    }

    #[test]
    fn two_flushes_in_one_microsecond_do_not_overwrite() {
        // The whole reason Time carries a pid and a counter.
        let a = Cursor::Time {
            first_micros: 10,
            last_micros: 10,
            pid: 7,
            seq: 1,
        };
        let b = Cursor::Time {
            first_micros: 10,
            last_micros: 10,
            pid: 7,
            seq: 2,
        };
        assert_ne!(a.file_name(), b.file_name());
        assert!(a.sort_key() < b.sort_key(), "and they order stably");
    }

    #[test]
    fn a_temporary_is_recognisably_not_a_segment() {
        let temp = time().temp_file_name();
        assert!(temp.starts_with('.'));
        assert!(temp.ends_with(".writing"));
        assert!(Cursor::is_temp(&temp));
        assert_eq!(
            Cursor::parse(&temp),
            None,
            "a listing must not mistake a partial write for a segment"
        );
    }

    #[test]
    fn a_foreign_filename_parses_as_nothing() {
        for name in [
            "README.md",
            "notes.parquet",
            "t-1_2_3.parquet",      // too few fields for Time
            "b-1_2_3.parquet",      // too many for Block
            "x-1_2.parquet",        // unknown tag
            "t-a_b_c_d.parquet",    // unparseable fields
            ".compact.lock",
        ] {
            assert_eq!(Cursor::parse(name), None, "{name} must not parse");
        }
    }

    #[test]
    fn the_first_and_last_position_bracket_the_range() {
        let cursor = Cursor::Block {
            first: 100,
            last: 200,
        };
        assert_eq!(cursor.first_position(), 100);
        assert_eq!(cursor.last_position(), 200);
    }
}
