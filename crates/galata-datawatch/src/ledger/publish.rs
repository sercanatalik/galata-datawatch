//! The ledger, published (`add-realtime-account-ledger`, design D3 and D4):
//! **the record is the queue.**
//!
//! ```text
//!   var/ledger            every answer and frame, archived first
//!     ──read past the cursor──▶ rows ──▶ events, each once, by its identity
//!                                    └─▶ state, one whole set per account and kind
//!     ──acknowledged──▶ the cursor moves      (var/ledger/publication-<venue>.json)
//! ```
//!
//! - **An event is published once**: its message id is the venue, the alias
//!   and its identity (`ledger::events::identity`), and an id already published
//!   is never sent again, however often the venue repeats it. The ids published
//!   are rebuilt from the record up to the cursor at start.
//! - **State is a set**: a payload states some kinds of some accounts, each
//!   within a scope (a dex, where the venue answers per dex). The set published
//!   is every scope's newest rows together, so the newest message on a subject
//!   is the whole account's state of that kind, and a read that found none is a
//!   set with no rows. At most one set per account and kind goes each second,
//!   the newest; the record keeps every read. **A set identical to the last one
//!   published is not published again**: Hyperliquid pushes its state every
//!   5 s, changed or not (measured 2026-11-28). Each run publishes every
//!   account's current sets once, at its start.
//! - **Nothing is counted until the stream acknowledges it.** The cursor is the
//!   last payload whose events were all acknowledged, kept in a file of its own,
//!   so a broker outage or a restart resumes there, in order, and the ledger's
//!   memory stays bounded however long the broker is away.
//! - **The stream begins where the ledger first publishes.** With no cursor
//!   yet, it starts at the record's end, with each account's current sets: the
//!   history before it is the projection's, not a seven-day stream's.

use std::collections::{BTreeMap, HashSet};
use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;

use galata_wire::{Account, Envelope, Event, Kind, StateSet, Venue};

use crate::normalise::Normalise;
use crate::record::Payload;

/// The event kinds the ledger publishes, each event once. An account's other
/// rows are state ([`Kind::is_state`]), or the ledger's own (its bindings and
/// reach, its gaps), which stay in the record.
pub const PUBLISHED_EVENTS: [Kind; 5] = [
    Kind::Fills,
    Kind::FundingPayments,
    Kind::LedgerUpdates,
    Kind::OrderUpdates,
    Kind::PnlHistory,
];

/// The least time between two sets of one account and kind.
pub const SET_EVERY_MICROS: i64 = 1_000_000;

/// What one payload states **whole**: an account's kind within a scope. A
/// payload stating a scope with no rows says the account holds none there.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Stated {
    /// Whose.
    pub account: Account,
    /// Which state kind.
    pub kind: Kind,
    /// The dex, where the venue answers per dex. `None` is the main one, or
    /// the whole account where the venue answers for it at once.
    pub scope: Option<String>,
}

/// The account stream, as the ledger sees it: one envelope with its id,
/// acknowledged or refused. `boot` gives it the broker; a test, a recorder.
pub trait AccountBus: Send + Sync {
    /// Publish and wait for the stream's acknowledgement.
    fn publish_acked<'a>(
        &'a self,
        id: &'a str,
        envelope: &'a Envelope,
    ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>>;
}

/// Why a publication pass stopped short.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum PublicationError {
    /// The record would not read.
    #[error("the record would not read for publication: {0}")]
    Record(String),
    /// The cursor file would not read or write.
    #[error("the publication cursor at {path}: {reason}")]
    Cursor {
        /// Where.
        path: String,
        /// Why.
        reason: String,
    },
}

/// What one pass published.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Published {
    /// Events acknowledged.
    pub events: u32,
    /// Sets acknowledged.
    pub sets: u32,
    /// Sets waiting out their second.
    pub held: u32,
    /// Whether the bus refused one, leaving the rest for the next pass.
    pub stopped: bool,
}

/// One account's kind, by scope (a dex, or `None`): the newest read of that
/// scope, and its rows.
type Scopes = BTreeMap<Option<String>, (i64, Vec<Event>)>;

#[derive(serde::Serialize, serde::Deserialize)]
struct CursorFile {
    /// The record position of the last payload whose events were all
    /// acknowledged.
    seq: u64,
    /// Its receipt, where the next read of the record starts.
    recv_micros: i64,
}

/// One venue's publication: its cursor, the ids it published, and its
/// accounts' state.
#[derive(Debug)]
pub struct Publication {
    venue: Venue,
    cursor_path: PathBuf,
    cursor: Option<(u64, i64)>,
    loaded: bool,
    published: HashSet<String>,
    /// Each account's kind, by scope: that scope's newest rows and their read.
    book: BTreeMap<(Account, Kind), Scopes>,
    /// When each account's kind last had a set published.
    last_set: BTreeMap<(Account, Kind), i64>,
    /// The rows of each account's kind's last set published this run.
    last_rows: BTreeMap<(Account, Kind), Vec<Event>>,
    /// Sets not yet published: the newest of each, waiting out its second.
    due: BTreeMap<(Account, Kind), Envelope>,
}

impl Publication {
    /// One venue's publication, its cursor in `cursor_path`.
    pub fn new(venue: Venue, cursor_path: PathBuf) -> Publication {
        Publication {
            venue,
            cursor_path,
            cursor: None,
            loaded: false,
            published: HashSet::new(),
            book: BTreeMap::new(),
            last_set: BTreeMap::new(),
            last_rows: BTreeMap::new(),
            due: BTreeMap::new(),
        }
    }

    /// Where a venue's cursor is kept: in the venue's own subtree of the
    /// record, which replay passes over (it reads segments only).
    pub fn cursor_path(root: &Path, venue: &str) -> PathBuf {
        root.join(format!("venue={venue}")).join("publication.json")
    }

    /// The record position acknowledged last, where one has been.
    pub fn cursor(&self) -> Option<u64> {
        self.cursor.map(|(seq, _)| seq)
    }

    /// Publish what the record holds past the cursor, and any set whose second
    /// has come.
    ///
    /// `states` says what a payload states whole (the venue's own reading);
    /// `now_micros` is the loop's clock.
    pub async fn publish(
        &mut self,
        root: &Path,
        normaliser: &dyn Normalise,
        states: &(dyn Fn(&Payload, &[Envelope]) -> Vec<Stated> + Sync),
        bus: &dyn AccountBus,
        now_micros: i64,
    ) -> Result<Published, PublicationError> {
        let mut out = Published::default();
        if !self.loaded {
            self.load(root, normaliser, states)?;
        }
        let scope = format!("venue={}", self.venue.as_str());
        let from = self.cursor.map_or(i64::MIN, |(_, recv)| recv);
        let mut pending: Vec<crate::replay::Replayed> =
            crate::replay::read_range(root, Some(&[scope.as_str()]), from, i64::MAX)
                .map_err(|e| PublicationError::Record(e.to_string()))?
                .into_iter()
                .filter(|r| self.cursor.is_none_or(|(seq, _)| r.seq() > seq))
                .collect();
        pending.sort_by_key(|r| r.seq());

        for replayed in &pending {
            let payload = replayed.payload();
            let Ok(rows) = normaliser.normalise(payload) else {
                // A payload that would not normalise holds nothing to publish;
                // its failure row is the record's.
                self.advance(replayed.seq(), payload.recv_micros)?;
                continue;
            };
            for row in &rows {
                if !PUBLISHED_EVENTS.contains(&row.kind()) {
                    continue;
                }
                let Some(id) = self.event_id(row) else {
                    continue;
                };
                if self.published.contains(&id) {
                    continue;
                }
                if bus.publish_acked(&id, row).await.is_err() {
                    out.stopped = true;
                    out.held = self.due.len() as u32;
                    return Ok(out);
                }
                self.published.insert(id);
                out.events += 1;
            }
            for key in self.take(payload, &rows, states) {
                if let Some(set) = self.set_of(&key) {
                    if self.unchanged(&key, &set) {
                        // The venue said it again: nothing to say on the bus,
                        // and nothing older due in its place.
                        self.due.remove(&key);
                    } else {
                        self.due.insert(key, set);
                    }
                }
            }
            self.advance(replayed.seq(), payload.recv_micros)?;
        }
        self.flush_due(bus, now_micros, &mut out).await;
        out.held = self.due.len() as u32;
        Ok(out)
    }

    /// The ids published and the state as of the cursor, read back from the
    /// record. With no cursor, the stream begins at the record's end: the
    /// cursor is set there, and every account's current set is due.
    fn load(
        &mut self,
        root: &Path,
        normaliser: &dyn Normalise,
        states: &(dyn Fn(&Payload, &[Envelope]) -> Vec<Stated> + Sync),
    ) -> Result<(), PublicationError> {
        self.cursor = self.read_cursor()?;
        let scope = format!("venue={}", self.venue.as_str());
        let mut held = crate::replay::read_range(root, Some(&[scope.as_str()]), i64::MIN, i64::MAX)
            .map_err(|e| PublicationError::Record(e.to_string()))?;
        held.sort_by_key(|r| r.seq());
        let through = match self.cursor {
            Some((seq, _)) => seq,
            None => held.last().map_or(0, |r| r.seq()),
        };
        let mut last = None;
        for replayed in held.iter().filter(|r| r.seq() <= through) {
            let payload = replayed.payload();
            last = Some((replayed.seq(), payload.recv_micros));
            let Ok(rows) = normaliser.normalise(payload) else {
                continue;
            };
            for row in &rows {
                if PUBLISHED_EVENTS.contains(&row.kind())
                    && let Some(id) = self.event_id(row)
                {
                    self.published.insert(id);
                }
            }
            self.take(payload, &rows, states);
        }
        if self.cursor.is_none()
            && let Some((seq, recv)) = last
        {
            self.advance(seq, recv)?;
        }
        // Every account's current state is due once, at start: what a reader
        // takes as "now" is then this run's, whatever the last run published.
        let keys: Vec<(Account, Kind)> = self.book.keys().cloned().collect();
        for key in keys {
            if let Some(set) = self.set_of(&key) {
                self.due.insert(key, set);
            }
        }
        self.loaded = true;
        Ok(())
    }

    /// Fold a payload's state into the book: each scope it states whole takes
    /// that payload's rows of it, none where it holds none. The accounts and
    /// kinds it touched.
    fn take(
        &mut self,
        payload: &Payload,
        rows: &[Envelope],
        states: &(dyn Fn(&Payload, &[Envelope]) -> Vec<Stated> + Sync),
    ) -> Vec<(Account, Kind)> {
        let mut touched = Vec::new();
        for stated in states(payload, rows) {
            let held: Vec<Event> = rows
                .iter()
                .filter(|e| {
                    e.kind() == stated.kind
                        && e.account() == Some(&stated.account)
                        && scope_of(&e.event) == stated.scope
                })
                .map(|e| e.event.clone())
                .collect();
            let key = (stated.account.clone(), stated.kind);
            self.book
                .entry(key.clone())
                .or_default()
                .insert(stated.scope.clone(), (payload.recv_micros, held));
            if !touched.contains(&key) {
                touched.push(key);
            }
        }
        touched
    }

    /// An account's kind as one set: every scope's newest rows, at the newest
    /// read among them.
    fn set_of(&self, key: &(Account, Kind)) -> Option<Envelope> {
        let scopes = self.book.get(key)?;
        let read = scopes.values().map(|(at, _)| *at).max()?;
        let rows: Vec<Event> = scopes
            .values()
            .flat_map(|(_, rows)| rows.iter().cloned())
            .collect();
        let set = StateSet::of(key.1, rows)?;
        Some(Envelope::for_account(
            self.venue.clone(),
            key.0.clone(),
            Some(read),
            read,
            Event::StateSet(set),
        ))
    }

    /// Publish each due set whose second has come; hold the rest.
    async fn flush_due(&mut self, bus: &dyn AccountBus, now_micros: i64, out: &mut Published) {
        let ready: Vec<(Account, Kind)> = self
            .due
            .keys()
            .filter(|key| {
                self.last_set
                    .get(*key)
                    .is_none_or(|at| now_micros - at >= SET_EVERY_MICROS)
            })
            .cloned()
            .collect();
        for key in ready {
            let Some(set) = self.due.get(&key) else {
                continue;
            };
            let id = format!(
                "{}:{}:{}:set:{}",
                self.venue.as_str(),
                key.0.as_str(),
                key.1.as_str(),
                set.recv_micros
            );
            if bus.publish_acked(&id, set).await.is_err() {
                out.stopped = true;
                return;
            }
            if let Some(set) = self.due.remove(&key)
                && let Event::StateSet(s) = set.event
            {
                self.last_rows.insert(key.clone(), s.rows);
            }
            self.last_set.insert(key, now_micros);
            out.sets += 1;
        }
    }

    /// Whether a set holds exactly the rows of the last one published for its
    /// account and kind this run.
    fn unchanged(&self, key: &(Account, Kind), set: &Envelope) -> bool {
        match (&set.event, self.last_rows.get(key)) {
            (Event::StateSet(s), Some(rows)) => &s.rows == rows,
            _ => false,
        }
    }

    /// An event's message id: unique across the stream, whose duplicate window
    /// spans every subject.
    fn event_id(&self, row: &Envelope) -> Option<String> {
        let account = row.account()?;
        Some(format!(
            "{}:{}:{}",
            self.venue.as_str(),
            account.as_str(),
            crate::ledger::events::identity(row)
        ))
    }

    fn read_cursor(&self) -> Result<Option<(u64, i64)>, PublicationError> {
        match std::fs::read(&self.cursor_path) {
            Ok(bytes) => {
                let file: CursorFile =
                    serde_json::from_slice(&bytes).map_err(|e| PublicationError::Cursor {
                        path: self.cursor_path.display().to_string(),
                        reason: e.to_string(),
                    })?;
                Ok(Some((file.seq, file.recv_micros)))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(PublicationError::Cursor {
                path: self.cursor_path.display().to_string(),
                reason: e.to_string(),
            }),
        }
    }

    /// Move the cursor past a payload, and keep it: written beside and then
    /// renamed over, so a crash leaves the old cursor or the new, never half.
    fn advance(&mut self, seq: u64, recv_micros: i64) -> Result<(), PublicationError> {
        self.cursor = Some((seq, recv_micros));
        let fail = |e: std::io::Error| PublicationError::Cursor {
            path: self.cursor_path.display().to_string(),
            reason: e.to_string(),
        };
        let json = serde_json::to_vec(&CursorFile { seq, recv_micros }).unwrap_or_default();
        if let Some(dir) = self.cursor_path.parent() {
            std::fs::create_dir_all(dir).map_err(fail)?;
        }
        let fresh = self.cursor_path.with_extension("json.new");
        std::fs::write(&fresh, json).map_err(fail)?;
        std::fs::rename(&fresh, &self.cursor_path).map_err(fail)
    }
}

/// The scope a state row sits in: its dex, where it names one.
fn scope_of(event: &Event) -> Option<String> {
    match event {
        Event::Margin(m) => m.dex.clone(),
        Event::Position(p) => p.dex.clone(),
        Event::Order(o) => o.dex.clone(),
        _ => None,
    }
}
