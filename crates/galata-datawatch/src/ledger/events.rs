//! An account's history, read back: **one row per event**, however often its
//! bytes were archived.
//!
//! ```text
//!   record  (every page, verbatim, overlaps and all)
//!     ──normalise──▶ rows ──dedupe by identity──▶ ──alias our own──▶ read()
//! ```
//!
//! Legacy recorded 8,214 rows for 3,270 fills (`design/risk/risk.md`,
//! 2026-09-07). Here the archive keeps every page, because the record is
//! verbatim; what a reader gets is one row per identity:
//!
//! | kind | identity |
//! |---|---|
//! | fill | `(trade_id, order_id)`: a trade id alone is shared by two sides |
//! | funding payment, ledger update | venue time and the whole row |
//!
//! A ledger update's identity is its time and content rather than its hash,
//! because the hash is kept out of rows (it resolves to an address). Two
//! identical updates in one millisecond would read as one; none was seen in
//! 5,275 measured.
//!
//! Also here, pure: the reach judged by evidence, and the check that says a
//! page after a break starts beyond what the venue still holds.

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::path::Path;

use galata_wire::{Account, Counterparty, Envelope, Event, Kind, Reach};

use crate::ledger::accounts::LedgerError;
use crate::normalise::Normalise;

/// The kinds that make an account's history, in the order a walk asks them:
/// funding before fills, because a funding payment is the evidence a fill
/// history is judged against.
pub const EVENT_KINDS: [Kind; 3] = [Kind::FundingPayments, Kind::Fills, Kind::LedgerUpdates];

/// A row's identity: equal for two receipts of one event, and only for those.
pub fn identity(envelope: &Envelope) -> String {
    match &envelope.event {
        Event::Fill(f) => format!("fill:{}:{}", f.trade_id, f.order_id),
        other => format!(
            "{}:{}:{}",
            envelope.kind(),
            envelope.at_micros.unwrap_or_default(),
            serde_json::to_string(other).unwrap_or_default()
        ),
    }
}

/// One account's events of the given kinds, one row per identity, oldest
/// first, a counterparty that is one of ours named by alias.
///
/// `ours` maps an address fingerprint to the alias it is bound to: every
/// declared master and every bound sub-account.
pub fn read(
    root: &Path,
    venue: &str,
    account: &str,
    kinds: &[Kind],
    normaliser: &dyn Normalise,
    ours: &BTreeMap<String, Account>,
) -> Result<Vec<Envelope>, LedgerError> {
    let mut history = History::default();
    history.extend(root, venue, account, kinds, normaliser, ours)?;
    Ok(history.rows)
}

/// One account's history, kept between reads and **extended** rather than
/// read again.
///
/// The fold runs every events cadence over the account's whole history, and
/// reading that back from the record each time — every margin and position
/// snapshot since the account was first polled — made each pass slower than
/// the last, without bound. The ledger commits every answer as it arrives
/// (`Origin::Fetched`), so by the time a pass runs, everything received up to
/// the last pass is on disk and nothing new is older than it: the next pass
/// reads only receipts from that point on.
///
/// Rebuilt from nothing when the kinds asked for or the aliases of our own
/// accounts change, since cached rows carry the aliasing they were read with.
#[derive(Debug, Default)]
pub struct History {
    rows: Vec<Envelope>,
    seen: HashSet<String>,
    /// The newest receipt read. The next read starts **at** it, inclusive:
    /// two payloads can share a microsecond, and identity drops the repeat.
    through_micros: Option<i64>,
    kinds: Vec<Kind>,
    ours: BTreeMap<String, Account>,
}

impl History {
    /// The rows so far, venue time then receipt.
    pub fn rows(&self) -> &[Envelope] {
        &self.rows
    }

    /// Read what the record gained since the last call, and merge it in.
    ///
    /// **All or nothing**: a read that fails leaves the history as it was, so
    /// the next call asks the same range again.
    pub fn extend(
        &mut self,
        root: &Path,
        venue: &str,
        account: &str,
        kinds: &[Kind],
        normaliser: &dyn Normalise,
        ours: &BTreeMap<String, Account>,
    ) -> Result<usize, LedgerError> {
        if self.kinds != kinds || &self.ours != ours {
            *self = History {
                kinds: kinds.to_vec(),
                ours: ours.clone(),
                ..History::default()
            };
        }
        // **One scope per kind**, so a partition of a kind not asked for is
        // never opened. Reading the account's whole subtree and dropping
        // kinds afterwards decoded every snapshot to answer a question about
        // fills.
        let scopes: Vec<String> = kinds
            .iter()
            .map(|k| format!("venue={venue}/account={account}/kind={}", k.as_str()))
            .collect();
        let scopes: Vec<&str> = scopes.iter().map(String::as_str).collect();
        let from = self.through_micros.unwrap_or(i64::MIN);
        let replayed = crate::replay::read_range(root, Some(&scopes), from, i64::MAX)?;

        let wanted: BTreeSet<&str> = kinds.iter().map(|k| k.as_str()).collect();
        let mut through = self.through_micros;
        let mut fresh = Vec::new();
        let mut fresh_ids: HashSet<String> = HashSet::new();
        for replayed in replayed {
            let payload = replayed.payload();
            through = Some(through.map_or(payload.recv_micros, |t| t.max(payload.recv_micros)));
            if !wanted.contains(payload.kind.as_str()) {
                continue;
            }
            // A page that would not normalise is still in the record, with a
            // failure row naming it; it contributes no rows here.
            let Ok(rows) = normaliser.normalise(payload) else {
                continue;
            };
            for mut row in rows {
                if !wanted.contains(row.kind().as_str()) {
                    continue;
                }
                let id = identity(&row);
                if self.seen.contains(&id) || !fresh_ids.insert(id) {
                    continue;
                }
                if let Event::LedgerUpdate(update) = &mut row.event
                    && let Some(Counterparty::Fingerprint(fp)) = &update.counterparty
                    && let Some(alias) = ours.get(fp)
                {
                    update.counterparty = Some(Counterparty::Account(alias.clone()));
                }
                fresh.push(row);
            }
        }

        let added = fresh.len();
        self.seen.extend(fresh_ids);
        self.through_micros = through;
        if added > 0 {
            self.rows.extend(fresh);
            // Venue time, then receipt: the order the events happened in. The
            // rows already held are sorted, so this is a sorted run plus a
            // short tail — which the standard library's stable sort detects
            // and merges in close to linear time.
            self.rows
                .sort_by_key(|e| (e.at_micros.unwrap_or(e.recv_micros), e.recv_micros));
        }
        Ok(added)
    }
}

/// The newest venue time recorded for one account and kind: where the next
/// ask starts. Read from the record, never from a cursor file.
pub fn newest(rows: &[Envelope], kind: Kind) -> Option<i64> {
    rows.iter()
        .filter(|e| e.kind() == kind)
        .filter_map(|e| e.at_micros)
        .max()
}

/// The fill history's reach, **by evidence**: a funding payment on an open
/// position proves a fill at or before it.
///
/// Measured 2026-09-25: the venue's documented bound is not its reach, and
/// it does drop fills by a rule this ledger cannot apply, so no count is a
/// verdict here.
pub fn fills_reach(earliest_fill: Option<i64>, earliest_funding_held: Option<i64>) -> Reach {
    match (earliest_fill, earliest_funding_held) {
        // A position paid funding before any fill the venue still holds.
        (Some(fill), Some(funding)) if funding < fill => Reach::Lost,
        (None, Some(_)) => Reach::Lost,
        (Some(_), _) => Reach::Consistent,
        (None, None) => Reach::Unknown,
    }
}

/// Whether a page asked from the newest recorded event starts **beyond** it:
/// a full page whose earliest event is later than our newest is the venue no
/// longer holding what lay between. Never judged from a short page, which is
/// simply a quiet account.
pub fn beyond_reach(newest_recorded: i64, page_first: i64, page_full: bool) -> bool {
    page_full && page_first > newest_recorded
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fills_that_start_after_a_funding_payment_are_lost() {
        assert_eq!(fills_reach(Some(200), Some(100)), Reach::Lost);
        assert_eq!(
            fills_reach(None, Some(100)),
            Reach::Lost,
            "a position with no fill at all"
        );
    }

    #[test]
    fn a_history_with_nothing_against_it_is_consistent() {
        assert_eq!(fills_reach(Some(100), Some(200)), Reach::Consistent);
        assert_eq!(fills_reach(Some(100), None), Reach::Consistent);
        assert_eq!(fills_reach(None, None), Reach::Unknown);
    }

    #[test]
    fn more_than_a_count_is_not_a_verdict() {
        // The judgement takes times, never a count: 25,655 fills reaching
        // back to before the first funding payment is consistent.
        assert_eq!(
            fills_reach(Some(1_692_203_037), Some(1_700_524_800)),
            Reach::Consistent
        );
    }

    #[test]
    fn a_quiet_account_is_not_a_gap() {
        assert!(
            !beyond_reach(100, 500, false),
            "a short page is a quiet account"
        );
        assert!(
            !beyond_reach(100, 100, true),
            "a page that starts at our newest reaches it"
        );
        assert!(beyond_reach(100, 500, true));
    }
}
