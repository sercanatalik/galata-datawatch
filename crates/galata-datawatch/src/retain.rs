//! Retention: the mechanism that enacts a **declared** policy, and nothing
//! where none is declared.
//!
//! > *Numbers are the measurer's; horizons are the operator's.*
//!
//! The mechanism is the builder's, and this is it. **No horizon has a
//! default**, the shipped configuration declares none, and a tree with no
//! policy expires nothing — which is the only safe answer to *"how long should
//! this be kept?"* from someone who does not know what it is.
//!
//! **The whole `date=` directory is the unit of expiry.**
//!
//! ```text
//!   var/archive/venue=hyperliquid/kind=quotes/date=2026-09-14/
//!   └─ expired as ONE THING, by its name
//! ```
//!
//! Never a segment, never a row. Every partition bottoms out at `date=`, so
//! dropping a day by name is the only deletion that cannot **half-drop** a
//! segment — and it is judged against the calendar the store already uses,
//! rather than by opening files.
//!
//! **Unknown means untouched.** A subtree the classifier does not recognise is
//! reported and left alone. Discovery, never listing: a classifier that swept
//! what it did not recognise would delete precisely the thing somebody put
//! there by hand and forgot to tell it about.

use std::path::{Path, PathBuf};

use crate::calendar::midnight_of;

const DAY: i64 = 86_400_000_000;

/// Which declared horizon selected a candidate.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Family {
    /// One venue's raw capture, under its own horizon.
    ///
    /// **A record.** Losing it is unrecoverable, which is why its horizon is
    /// declared per venue rather than shared with anything.
    Venue(String),
    /// The tape.
    ///
    /// **A cache.** Anything dropped comes back from a rebuild, so its horizon
    /// is a convenience number and may be far shorter.
    Tape,
}

impl Family {
    /// The name a policy declares it under.
    pub fn as_str(&self) -> &str {
        match self {
            Family::Venue(venue) => venue,
            Family::Tape => "tape",
        }
    }
}

/// One whole dated directory a declared horizon has expired.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Candidate {
    /// What would be removed.
    pub path: PathBuf,
    /// Which horizon selected it.
    pub family: Family,
    /// The partition's own date, from its name.
    pub date: String,
    /// How many bytes it holds.
    pub bytes: u64,
}

/// What one sweep saw.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct Sweep {
    /// What a declared horizon expired.
    pub candidates: Vec<Candidate>,
    /// What the classifier could not place — **reported, never selected**.
    pub unclassified: Vec<PathBuf>,
}

impl Sweep {
    /// Bytes the candidates hold.
    pub fn bytes(&self) -> u64 {
        self.candidates.iter().map(|c| c.bytes).sum()
    }

    /// Whether there is anything to do.
    pub fn is_empty(&self) -> bool {
        self.candidates.is_empty()
    }

    /// A line an operator can act on.
    pub fn report(&self) -> String {
        format!(
            "{} partitions, {:.1} MB, {} unclassified",
            self.candidates.len(),
            self.bytes() as f64 / 1_048_576.0,
            self.unclassified.len()
        )
    }
}

/// A declared horizon, in days.
///
/// **There is no `Default`.** A policy that could be defaulted would be one the
/// builder answered on the operator's behalf.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Horizon {
    /// How many days are kept.
    pub days: u32,
}

/// What the operator declared, if anything.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Policy {
    /// Per venue. A venue absent from here expires nothing.
    pub venues: std::collections::BTreeMap<String, Horizon>,
    /// The tape. `None` expires nothing.
    pub tape: Option<Horizon>,
}

impl Policy {
    /// Whether anything at all is declared.
    pub fn is_empty(&self) -> bool {
        self.venues.is_empty() && self.tape.is_none()
    }

    fn horizon_for(&self, family: &Family) -> Option<Horizon> {
        match family {
            Family::Venue(venue) => self.venues.get(venue).copied(),
            Family::Tape => self.tape,
        }
    }
}

/// What a declared policy expires, as of `now_micros`.
///
/// Removes nothing. The caller decides, and the tool that wraps this deletes
/// only under an explicit flag.
pub fn sweep(archive_root: &Path, tape_root: &Path, policy: &Policy, now_micros: i64) -> Sweep {
    let mut out = Sweep::default();
    // A tree with no policy expires nothing, and is not even walked — there is
    // no question to ask of it.
    if policy.is_empty() {
        return out;
    }
    collect(archive_root, &Archive, policy, now_micros, &mut out);
    collect(tape_root, &TapeRoot, policy, now_micros, &mut out);
    out.candidates.sort_by(|a, b| a.path.cmp(&b.path));
    out.unclassified.sort();
    out
}

/// How a root's top level names a family.
trait Classify {
    fn family_of(&self, top: &str) -> Option<Family>;
}

struct Archive;
impl Classify for Archive {
    fn family_of(&self, top: &str) -> Option<Family> {
        top.strip_prefix("venue=")
            .map(|venue| Family::Venue(venue.to_string()))
    }
}

struct TapeRoot;
impl Classify for TapeRoot {
    fn family_of(&self, top: &str) -> Option<Family> {
        top.starts_with("kind=").then_some(Family::Tape)
    }
}

fn collect(
    root: &Path,
    classify: &dyn Classify,
    policy: &Policy,
    now_micros: i64,
    out: &mut Sweep,
) {
    for top in children(root) {
        let Some(name) = file_name(&top) else {
            continue;
        };
        let Some(family) = classify.family_of(&name) else {
            // Discovery, never listing. Somebody put this here.
            out.unclassified.push(top);
            continue;
        };
        // A family with no declared horizon is not swept, and is not
        // *unclassified* either — it was recognised and nothing was asked of
        // it.
        let Some(horizon) = policy.horizon_for(&family) else {
            continue;
        };
        dated(&top, &family, horizon, now_micros, out);
    }
}

/// Every `date=` directory under a family, at whatever depth.
fn dated(dir: &Path, family: &Family, horizon: Horizon, now_micros: i64, out: &mut Sweep) {
    for child in children(dir) {
        let Some(name) = file_name(&child) else {
            continue;
        };
        let Some(date) = name.strip_prefix("date=") else {
            dated(&child, family, horizon, now_micros, out);
            continue;
        };
        let Some(midnight) = midnight_of(date) else {
            // A name the calendar refuses. Reported, never selected — a
            // deletion is never a guess.
            out.unclassified.push(child);
            continue;
        };
        // **The day's END**, not its start: a day whose last hour is still
        // inside the horizon is still wanted.
        let ends = midnight.saturating_add(DAY);
        if ends <= now_micros.saturating_sub((horizon.days as i64).saturating_mul(DAY)) {
            out.candidates.push(Candidate {
                bytes: size_of(&child),
                path: child,
                family: family.clone(),
                date: date.to_string(),
            });
        }
    }
}

fn size_of(dir: &Path) -> u64 {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return 0;
    };
    entries
        .filter_map(|e| e.ok())
        .map(|e| match e.path() {
            p if p.is_dir() => size_of(&p),
            p => std::fs::metadata(&p).map(|m| m.len()).unwrap_or(0),
        })
        .sum()
}

fn children(dir: &Path) -> Vec<PathBuf> {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return Vec::new();
    };
    let mut out: Vec<PathBuf> = entries
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.is_dir())
        .collect();
    out.sort();
    out
}

fn file_name(path: &Path) -> Option<String> {
    Some(path.file_name()?.to_string_lossy().to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    const NOW: i64 = 100 * DAY;

    fn tree(dirs: &[&str]) -> tempfile::TempDir {
        let root = tempfile::tempdir().unwrap();
        for dir in dirs {
            let path = root.path().join(dir);
            std::fs::create_dir_all(&path).unwrap();
            std::fs::write(path.join("s-1_2.parquet"), b"0123456789").unwrap();
        }
        root
    }

    fn policy(days: u32) -> Policy {
        Policy {
            venues: [("hyperliquid".to_string(), Horizon { days })]
                .into_iter()
                .collect(),
            tape: None,
        }
    }

    fn day(n: i64) -> String {
        crate::calendar::date_of(n * DAY)
    }

    #[test]
    fn no_policy_expires_nothing() {
        // Numbers are the measurer's and horizons are the operator's. A default
        // is the builder answering a question they cannot answer.
        let root = tree(&[&format!("venue=hyperliquid/kind=quotes/date={}", day(1))]);
        let empty = tempfile::tempdir().unwrap();
        let swept = sweep(root.path(), empty.path(), &Policy::default(), NOW);
        assert!(swept.is_empty());
        assert!(swept.unclassified.is_empty(), "nothing is even walked");
    }

    #[test]
    fn a_day_expires_by_its_end_and_not_its_start() {
        // A day whose last hour is still inside the horizon is still wanted.
        let empty = tempfile::tempdir().unwrap();
        // The horizon is 30 days, so `now - 30d` is day 70. Day 69 ends at day
        // 70 and is expired; day 70 ends at day 71 and is not.
        let root = tree(&[
            &format!("venue=hyperliquid/kind=quotes/date={}", day(69)),
            &format!("venue=hyperliquid/kind=quotes/date={}", day(70)),
        ]);
        let swept = sweep(root.path(), empty.path(), &policy(30), NOW);
        let dates: Vec<&str> = swept.candidates.iter().map(|c| c.date.as_str()).collect();
        assert_eq!(dates, vec![day(69).as_str()], "{swept:?}");
    }

    #[test]
    fn an_unrecognised_subtree_is_reported_and_survives() {
        // A classifier that swept what it did not recognise would delete
        // precisely the thing somebody put there by hand.
        let empty = tempfile::tempdir().unwrap();
        let root = tree(&[
            &format!("venue=hyperliquid/kind=quotes/date={}", day(1)),
            "somebodys-backup/important",
        ]);
        let swept = sweep(root.path(), empty.path(), &policy(30), NOW);
        assert_eq!(swept.unclassified.len(), 1);
        assert!(swept.unclassified[0].ends_with("somebodys-backup"));
        assert!(
            !swept
                .candidates
                .iter()
                .any(|c| c.path.to_string_lossy().contains("backup")),
            "an unclassified subtree was selected"
        );
    }

    #[test]
    fn an_impossible_date_is_reported_never_selected() {
        // A deletion is never a guess.
        let empty = tempfile::tempdir().unwrap();
        let root = tree(&["venue=hyperliquid/kind=quotes/date=2026-02-30"]);
        let swept = sweep(root.path(), empty.path(), &policy(1), NOW);
        assert!(swept.candidates.is_empty());
        assert_eq!(swept.unclassified.len(), 1);
        assert!(
            swept.unclassified[0]
                .to_string_lossy()
                .contains("2026-02-30")
        );
    }

    #[test]
    fn a_venue_with_no_declared_horizon_is_left_alone_and_is_not_unclassified() {
        // It was recognised, and nothing was asked of it. Reporting it as
        // unclassified would train an operator to ignore that list.
        let empty = tempfile::tempdir().unwrap();
        let root = tree(&[&format!("venue=rh-crypto/kind=quotes/date={}", day(1))]);
        let swept = sweep(root.path(), empty.path(), &policy(30), NOW);
        assert!(swept.candidates.is_empty());
        assert!(swept.unclassified.is_empty(), "{swept:?}");
    }

    #[test]
    fn the_tape_is_swept_under_its_own_horizon() {
        // A cache, not a record: anything dropped comes back from a rebuild, so
        // its horizon may be far shorter than any venue's.
        let archive = tempfile::tempdir().unwrap();
        let tape = tree(&[&format!("kind=quotes/date={}", day(90))]);
        let policy = Policy {
            venues: Default::default(),
            tape: Some(Horizon { days: 2 }),
        };
        let swept = sweep(archive.path(), tape.path(), &policy, NOW);
        assert_eq!(swept.candidates.len(), 1);
        assert_eq!(swept.candidates[0].family, Family::Tape);
        assert_eq!(swept.bytes(), 10);
    }

    #[test]
    fn a_sweep_removes_nothing() {
        // The mechanism reports; the tool that wraps it deletes, and only under
        // an explicit flag.
        let empty = tempfile::tempdir().unwrap();
        let root = tree(&[&format!("venue=hyperliquid/kind=quotes/date={}", day(1))]);
        let swept = sweep(root.path(), empty.path(), &policy(30), NOW);
        assert_eq!(swept.candidates.len(), 1);
        assert!(
            swept.candidates[0].path.is_dir(),
            "the sweep deleted something"
        );
    }
}
