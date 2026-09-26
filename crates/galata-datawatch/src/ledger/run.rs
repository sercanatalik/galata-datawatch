//! The ledger's loop: **ask, record, and say what happened**.
//!
//! ```text
//!   boot        each declared master's role, once   ──▶ refuse a sub-account
//!               discovery                            ──▶ bindings, modes
//!   every snapshot_secs   each (account, dex)        ──▶ the one path
//!                         a failure                  ──▶ a gap, from the last answer
//!   every discover_secs   discovery again            ──▶ a report, always
//! ```
//!
//! **Venue-free.** What to ask and how to read the answers is an
//! [`AccountVenue`](crate::ledger::run::AccountVenue)'s; this module holds the order and the rules, which are
//! the poll lane's: a failed poll is a gap bounded by the cadence
//! ([`Cadence`](crate::venue::Cadence)), throttling backs off and unreachability does not
//! ([`Backoff`](crate::source::Backoff)), and every answer is archived before anything reads it.

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::time::Duration;

use galata_wire::{
    Account, Effect, Envelope, Event, EventsReach, Gap, GapCause, Kind, Origin, Reach, Series,
    Venue,
};

use crate::capture::{Clock, Refusal, StatusFile};
use crate::config::Secret;
use crate::ingest::{ingest, record_generated_at};
use crate::ledger::Listed;
use crate::ledger::accounts::{Bindings, FingerprintKey, LedgerError, ResolvedAccount, Seen};
use crate::normalise::{Normalise, NormaliseError};
use crate::record::{Archive, Payload, PayloadAddress, RecordError};
use crate::sink::Sink;
use crate::source::Backoff;
use crate::venue::{Cadence, Polled};

/// Which question an answer was to, for the channel it is recorded under.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Ask {
    /// One account's perp state on one dex.
    Snapshot,
    /// A master's sub-accounts.
    Listing,
    /// What an address is.
    Role,
    /// How an account's collateral is held.
    Mode,
}

/// What a venue must answer for its ledger to run. Its adapter implements it.
pub trait AccountVenue: Send + Sync {
    /// Which venue.
    fn venue(&self) -> &Venue;
    /// Its ledger normaliser: recorded answers to rows.
    fn normaliser(&self) -> &dyn Normalise;
    /// The channel an answer to `ask` is recorded under.
    fn channel(&self, ask: Ask) -> &'static str;
    /// One account's perp state on one dex, raw.
    fn snapshot(
        &self,
        address: &Secret,
        dex: &str,
    ) -> impl Future<Output = Result<Vec<u8>, Refusal>> + Send;
    /// A master's sub-accounts, raw.
    fn listing(&self, address: &Secret) -> impl Future<Output = Result<Vec<u8>, Refusal>> + Send;
    /// What the venue says an address is, raw.
    fn role(&self, address: &Secret) -> impl Future<Output = Result<Vec<u8>, Refusal>> + Send;
    /// How an account's collateral is held, raw.
    fn mode(&self, address: &Secret) -> impl Future<Output = Result<Vec<u8>, Refusal>> + Send;
    /// Whether the venue knows a dex: `Ok(false)` **only** where it said so.
    fn dex_known(&self, dex: &str) -> impl Future<Output = Result<bool, Refusal>> + Send;
    /// A snapshot payload from the dex, the mode last heard and the state.
    fn compose(&self, dex: &str, mode: Option<(&[u8], i64)>, state: &[u8]) -> Vec<u8>;
    /// The sub-accounts a listing names.
    fn listed(&self, answer: &[u8]) -> Result<Vec<Listed>, NormaliseError>;
    /// Whether a role answer says the address is a sub-account.
    fn is_sub_account(&self, answer: &[u8]) -> Result<bool, NormaliseError>;
    /// The channel a page of `kind` is recorded under, where the venue keeps
    /// that history.
    fn events_channel(&self, kind: Kind) -> Option<&'static str>;
    /// One page of `kind`, the oldest at or after `start_micros`, raw.
    fn events_page(
        &self,
        kind: Kind,
        address: &Secret,
        start_micros: i64,
    ) -> impl Future<Output = Result<Vec<u8>, Refusal>> + Send;
    /// How many rows a full page of `kind` holds: a full page asks for the next.
    fn page_size(&self, kind: Kind) -> usize;
    /// A page's earliest and newest venue time and its row count. `None` for
    /// an empty page.
    fn page_span(&self, page: &[u8]) -> Option<(i64, i64, usize)>;
}

/// How often, in microseconds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Cadences {
    /// Between snapshots of each account and dex. Also a failed one's gap width.
    pub snapshot_micros: i64,
    /// Between discovery runs.
    pub discover_micros: i64,
    /// Between asking each account for its new events.
    pub events_micros: i64,
    /// Between two pages of one walk, so a catch-up stays inside the
    /// ledger's share of the venue's budget.
    pub page_pause_micros: i64,
}

/// One discovery run under one master, as reported.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct Discovery {
    /// The master.
    pub master: String,
    /// When it ran.
    pub at_micros: i64,
    /// Whether the listing answered. A run that could not ask is reported too.
    pub answered: bool,
    /// Sub-accounts the listing named.
    pub seen: usize,
    /// Of those, bound for the first time.
    pub new: Vec<String>,
    /// Bound earlier and absent from this listing. Kept, and no longer polled.
    pub missing: Vec<String>,
}

/// One account as the status surface shows it: **states and counts, never a
/// verdict**, and never an address.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize)]
pub struct AccountStatus {
    /// Declared, or the master it was discovered under.
    pub master: Option<String>,
    /// Whether it is polled now.
    pub polled: bool,
    /// When each dex last answered, by dex (`""` is the main one).
    pub answered_micros: BTreeMap<String, i64>,
    /// Snapshots that did not answer, since start.
    pub missed: u64,
    /// When a discovery run last did not list it, where one did not.
    pub not_seen_since_micros: Option<i64>,
    /// Its history, by kind.
    pub events: BTreeMap<String, EventsStatus>,
}

/// One kind of an account's history, as the status surface shows it.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize)]
pub struct EventsStatus {
    /// The newest event recorded: where the next ask starts.
    pub newest_micros: Option<i64>,
    /// What the record says about how much of this history it holds.
    pub reach: Option<Reach>,
    /// Pages asked for since start.
    pub pages: u64,
    /// Asks that did not answer since start. Not gaps: the venue still holds
    /// the history, and the next ask fetches it.
    pub missed: u64,
    /// A walk stopped because a full page did not advance, where one did.
    pub stalled_at_micros: Option<i64>,
    /// Ledger update types whose effect on perp margin is unknown.
    pub unknown_types: BTreeSet<String>,
}

/// What the ledger says about itself.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize)]
pub struct LedgerStatus {
    /// Which venue.
    pub venue: String,
    /// When this was written.
    pub at_micros: i64,
    /// By alias.
    pub accounts: BTreeMap<String, AccountStatus>,
    /// The most recent discovery run per master.
    pub discovery: BTreeMap<String, Discovery>,
    /// Masters whose role could not yet be asked; asked again each discovery
    /// run until answered.
    pub role_unverified: Vec<String>,
    /// Declared dexes the venue could not be asked about at boot.
    pub dex_unverified: BTreeSet<String>,
}

/// The ledger for one venue.
pub struct LedgerRun<V: AccountVenue, C: Clock> {
    venue: V,
    clock: C,
    archive: Archive,
    sink: Box<dyn Sink>,
    key: FingerprintKey,
    cadences: Cadences,
    status_file: Option<StatusFile>,
    masters: Vec<ResolvedAccount>,
    /// alias → (master alias, the sub-account).
    subs: BTreeMap<Account, (Account, ResolvedAccount)>,
    polled_subs: BTreeSet<Account>,
    bindings: Bindings,
    /// The most recent mode answer per account, raw, and when it arrived.
    modes: BTreeMap<Account, (Vec<u8>, i64)>,
    /// (account, dex) → its cadence.
    polls: BTreeMap<(Account, String), Cadence>,
    role_unverified: BTreeSet<Account>,
    backoff: Backoff,
    status: LedgerStatus,
    /// (account, kind) → the newest venue time recorded. Read from the record
    /// the first time an account's history is asked for.
    newest: BTreeMap<(Account, Kind), i64>,
    /// (account, kind) → the earliest venue time the venue returned.
    earliest: BTreeMap<(Account, Kind), i64>,
    /// Accounts whose history has been read back from the record.
    loaded: BTreeSet<Account>,
    /// (account, kind) whose reach has been recorded.
    reached: BTreeSet<(Account, Kind)>,
    /// Where the fold's report goes, and the tolerances it checks at.
    fold: Option<(StatusFile, crate::ledger::fold::Tolerances)>,
    /// Where each fold pass projects the rows it read, if anywhere.
    projection: Option<std::path::PathBuf>,
}

/// What one pass over the accounts' histories did.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct EventsPass {
    /// Pages recorded.
    pub pages: u32,
    /// Asks that did not answer.
    pub missed: u32,
    /// `beyond_reach` gaps recorded.
    pub gaps: u32,
    /// Walks stopped because a full page did not advance.
    pub stalled: u32,
    /// Whether any refusal was a throttle.
    pub throttled: bool,
}

/// What one pass of snapshots did.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Pass {
    /// Snapshots that answered, all archived.
    pub answered: u32,
    /// Snapshots that did not.
    pub missed: u32,
    /// Gaps recorded.
    pub gaps: u32,
    /// Whether any refusal was a throttle, which backs the loop off.
    pub throttled: bool,
}

impl<V: AccountVenue, C: Clock> LedgerRun<V, C> {
    /// A ledger over resolved masters, writing into an archive rooted at the
    /// ledger root. The bindings come from that root, read back by the caller.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        venue: V,
        clock: C,
        archive: Archive,
        sink: Box<dyn Sink>,
        key: FingerprintKey,
        cadences: Cadences,
        masters: Vec<ResolvedAccount>,
        bindings: Bindings,
    ) -> LedgerRun<V, C> {
        let status = LedgerStatus {
            venue: venue.venue().to_string(),
            ..LedgerStatus::default()
        };
        LedgerRun {
            venue,
            clock,
            archive,
            sink,
            key,
            cadences,
            status_file: None,
            masters,
            subs: BTreeMap::new(),
            polled_subs: BTreeSet::new(),
            bindings,
            modes: BTreeMap::new(),
            polls: BTreeMap::new(),
            role_unverified: BTreeSet::new(),
            backoff: Backoff::default(),
            status,
            newest: BTreeMap::new(),
            earliest: BTreeMap::new(),
            loaded: BTreeSet::new(),
            reached: BTreeSet::new(),
            fold: None,
            projection: None,
        }
    }

    /// Fold every polled account after each events pass, writing the report
    /// here.
    pub fn with_fold(
        mut self,
        file: StatusFile,
        tolerances: crate::ledger::fold::Tolerances,
    ) -> LedgerRun<V, C> {
        self.fold = Some((file, tolerances));
        self
    }

    /// Project the rows each fold pass reads into `root` (`ledger.tape`).
    pub fn with_projection(mut self, root: Option<std::path::PathBuf>) -> LedgerRun<V, C> {
        self.projection = root;
        self
    }

    /// Fold every polled account from the record, and write the report.
    ///
    /// Best effort in the way the status file is: a report that cannot be
    /// written must not stop the ledger recording, which is what it reports on.
    pub fn fold_all(&mut self) -> Result<Option<crate::ledger::fold::FoldReport>, LedgerError> {
        let Some((file, tolerances)) = self.fold.clone() else {
            return Ok(None);
        };
        let ours = self.ours();
        let mut report = crate::ledger::fold::FoldReport {
            venue: self.venue.venue().to_string(),
            at_micros: self.clock.now_micros(),
            position_tolerance: tolerances.position,
            relative_tolerance: tolerances.relative,
            ..Default::default()
        };
        for account in self.polled() {
            let rows = crate::ledger::events::read(
                self.archive.root(),
                account.venue.as_str(),
                account.alias.as_str(),
                &crate::ledger::fold::FOLD_KINDS,
                self.venue.normaliser(),
                &ours,
            )?;
            // The same rows, projected. Best effort, as the report's own write
            // is: a projection that cannot be written must not stop the ledger
            // recording, which is what it projects.
            if let Some(root) = &self.projection
                && let Err(error) = crate::ledger::project::write(
                    root,
                    account.venue.as_str(),
                    account.alias.as_str(),
                    &rows,
                )
            {
                tracing::warn!(account = account.alias.as_str(), %error, "the ledger was not projected");
            }
            report.accounts.insert(
                account.alias.to_string(),
                crate::ledger::fold::fold(&rows, &tolerances),
            );
        }
        if let Ok(json) = serde_json::to_string_pretty(&report) {
            let _ = file.write(&json);
        }
        Ok(Some(report))
    }

    /// Write the status surface here on every pass.
    pub fn with_status_file(mut self, file: StatusFile) -> LedgerRun<V, C> {
        self.status_file = Some(file);
        self
    }

    /// The status as it stands.
    pub fn status(&self) -> &LedgerStatus {
        &self.status
    }

    fn record(
        &mut self,
        address: &ResolvedAccount,
        ask: Ask,
        bytes: Vec<u8>,
    ) -> Result<(), RecordError> {
        let kind = match ask {
            Ask::Snapshot | Ask::Mode => Series::Margin.kind(),
            Ask::Listing | Ask::Role => galata_wire::Kind::Accounts,
        };
        let payload = Payload {
            seq: 0,
            recv_micros: self.clock.now_micros(),
            address: PayloadAddress::Account(address.record_address()),
            channel: self.venue.channel(ask).to_string(),
            kind: kind.as_str().to_string(),
            symbol: None,
            // An answer to a question about now: nothing will fetch it again,
            // so it is durable before the loop moves on — as the polled venue's
            // answers are.
            origin: Origin::Fetched,
            payload: bytes,
        };
        ingest(
            &mut self.archive,
            self.venue.normaliser(),
            self.sink.as_ref(),
            payload,
        )?;
        Ok(())
    }

    /// **Boot**: ask each declared master's role once, refusing one the venue
    /// calls a sub-account, then run discovery.
    ///
    /// A master whose role could not be asked is not refused — the venue did
    /// not say — and is asked again at each discovery run until it answers.
    pub async fn boot(&mut self) -> Result<(), LedgerError> {
        // A dex the venue does not know answers every snapshot with a failure,
        // which would read as an outage forever. Refused here, by name.
        for master in &self.masters {
            for dex in master.dexes.iter().filter(|d| !d.is_empty()) {
                match self.venue.dex_known(dex).await {
                    Ok(false) => {
                        return Err(LedgerError::UnknownDex {
                            alias: master.alias.to_string(),
                            dex: dex.clone(),
                        });
                    }
                    Ok(true) => {}
                    Err(_) => {
                        self.status.dex_unverified.insert(dex.clone());
                    }
                }
            }
        }
        for master in self.masters.clone() {
            self.check_role(&master).await?;
        }
        self.discover().await?;
        Ok(())
    }

    async fn check_role(&mut self, master: &ResolvedAccount) -> Result<(), LedgerError> {
        match self.venue.role(master.address()).await {
            Ok(answer) => {
                let is_sub = self.venue.is_sub_account(&answer);
                self.record(master, Ask::Role, answer)
                    .map_err(record_error)?;
                self.role_unverified.remove(&master.alias);
                if is_sub.unwrap_or(false) {
                    return Err(LedgerError::NotAMaster {
                        alias: master.alias.to_string(),
                    });
                }
            }
            Err(_) => {
                self.role_unverified.insert(master.alias.clone());
            }
        }
        Ok(())
    }

    /// One discovery run over every master: bind what is new, record a name
    /// that changed, stop polling what vanished, read every account's mode —
    /// and report the run whatever it found.
    pub async fn discover(&mut self) -> Result<Vec<Discovery>, LedgerError> {
        for master in self.masters.clone() {
            if self.role_unverified.contains(&master.alias) {
                self.check_role(&master).await?;
            }
        }
        let mut runs = Vec::new();
        for master in self.masters.clone() {
            let at = self.clock.now_micros();
            let mut run = Discovery {
                master: master.alias.to_string(),
                at_micros: at,
                answered: false,
                seen: 0,
                new: Vec::new(),
                missing: Vec::new(),
            };
            if let Ok(answer) = self.venue.listing(master.address()).await {
                let listed = self.venue.listed(&answer);
                self.record(&master, Ask::Listing, answer)
                    .map_err(record_error)?;
                if let Ok(listed) = listed {
                    run.answered = true;
                    run.seen = listed.len();
                    let mut present = BTreeSet::new();
                    for entry in listed {
                        let sub = ResolvedAccount::discovered(
                            master.alias.clone(),
                            master.venue.clone(),
                            master.dexes.clone(),
                            &self.key,
                            entry.address,
                        );
                        present.insert(sub.fingerprint.clone());
                        let seen = self.bindings.observe(
                            &master.alias,
                            &sub.fingerprint,
                            entry.name.as_deref(),
                        )?;
                        let alias = match seen {
                            Seen::New { alias, seen } => {
                                run.new.push(alias.to_string());
                                self.record_binding(&alias, &sub, seen)?;
                                alias
                            }
                            Seen::Renamed { alias, seen } => {
                                self.record_binding(&alias, &sub, seen)?;
                                alias
                            }
                            Seen::Known { alias } => alias,
                        };
                        let sub = sub.with_alias(alias.clone());
                        self.polled_subs.insert(alias.clone());
                        let entry = self.status.accounts.entry(alias.to_string()).or_default();
                        entry.master = Some(master.alias.to_string());
                        entry.polled = true;
                        entry.not_seen_since_micros = None;
                        self.subs.insert(alias, (master.alias.clone(), sub));
                    }
                    for gone in self.bindings.missing(&master.alias, &present) {
                        run.missing.push(gone.to_string());
                        self.polled_subs.remove(&gone);
                        let entry = self.status.accounts.entry(gone.to_string()).or_default();
                        entry.master = Some(master.alias.to_string());
                        entry.polled = false;
                        entry.not_seen_since_micros.get_or_insert(at);
                    }
                }
            }
            self.status
                .discovery
                .insert(master.alias.to_string(), run.clone());
            runs.push(run);
        }
        for account in self.polled() {
            if let Ok(answer) = self.venue.mode(account.address()).await {
                let at = self.clock.now_micros();
                self.modes
                    .insert(account.alias.clone(), (answer.clone(), at));
                self.record(&account, Ask::Mode, answer)
                    .map_err(record_error)?;
            }
        }
        self.write_status();
        Ok(runs)
    }

    fn record_binding(
        &mut self,
        alias: &Account,
        sub: &ResolvedAccount,
        seen: galata_wire::AccountSeen,
    ) -> Result<(), LedgerError> {
        let mut address = sub.record_address();
        address.account = alias.to_string();
        let envelope = Envelope::for_account(
            sub.venue.clone(),
            alias.clone(),
            None,
            self.clock.now_micros(),
            Event::AccountSeen(seen),
        );
        record_generated_at(
            &mut self.archive,
            self.sink.as_ref(),
            PayloadAddress::Account(address),
            envelope,
        )
        .map_err(record_error)?;
        Ok(())
    }

    /// Every account polled now: the declared masters and the sub-accounts the
    /// latest discovery listed.
    fn polled(&self) -> Vec<ResolvedAccount> {
        let mut out = self.masters.clone();
        out.extend(
            self.subs
                .iter()
                .filter(|(alias, _)| self.polled_subs.contains(*alias))
                .map(|(_, (_, sub))| sub.clone()),
        );
        out
    }

    /// Snapshot every polled account on every dex once.
    pub async fn snapshot_all(&mut self) -> Result<Pass, LedgerError> {
        let mut pass = Pass::default();
        for account in self.polled() {
            for dex in account.dexes.clone() {
                let asked = self.clock.now_micros();
                let key = (account.alias.clone(), dex.clone());
                let interval = self.cadences.snapshot_micros;
                match self.venue.snapshot(account.address(), &dex).await {
                    Ok(state) => {
                        let mode = self.modes.get(&account.alias);
                        let bytes = self.venue.compose(
                            &dex,
                            mode.map(|(answer, at)| (answer.as_slice(), *at)),
                            &state,
                        );
                        self.record(&account, Ask::Snapshot, bytes)
                            .map_err(record_error)?;
                        self.polls
                            .entry(key)
                            .or_insert_with(|| Cadence::new(interval))
                            .answered(asked);
                        let entry = self
                            .status
                            .accounts
                            .entry(account.alias.to_string())
                            .or_default();
                        entry.polled = true;
                        entry.answered_micros.insert(dex.clone(), asked);
                        pass.answered += 1;
                    }
                    Err(refusal) => {
                        pass.missed += 1;
                        pass.throttled |= refusal == Refusal::Throttled;
                        self.status
                            .accounts
                            .entry(account.alias.to_string())
                            .or_default()
                            .missed += 1;
                        let missed = self
                            .polls
                            .entry(key)
                            .or_insert_with(|| Cadence::new(interval))
                            .missed(asked, refusal.cause());
                        if let Polled::Missed {
                            from_micros,
                            to_micros,
                            cause,
                        } = missed
                        {
                            self.record_gap(&account, &dex, from_micros, to_micros, cause)?;
                            pass.gaps += 1;
                        }
                    }
                }
            }
        }
        // A throttle backs the loop off (`run`); anything else resets it.
        if !pass.throttled {
            self.backoff.reset();
        }
        self.write_status();
        Ok(pass)
    }

    fn record_gap(
        &mut self,
        account: &ResolvedAccount,
        dex: &str,
        from_micros: i64,
        to_micros: i64,
        cause: GapCause,
    ) -> Result<(), LedgerError> {
        // Which of the account's snapshots went uncovered, spelled as its
        // margin rows spell it: `None` is the main dex.
        let dex = (!dex.is_empty()).then(|| dex.to_string());
        self.record_gap_for(account, Series::Margin, dex, from_micros, to_micros, cause)
    }

    fn record_gap_for(
        &mut self,
        account: &ResolvedAccount,
        series: Series,
        dex: Option<String>,
        from_micros: i64,
        to_micros: i64,
        cause: GapCause,
    ) -> Result<(), LedgerError> {
        let envelope = Envelope::for_account(
            account.venue.clone(),
            account.alias.clone(),
            None,
            self.clock.now_micros(),
            Event::Gap(Gap {
                series,
                from_micros,
                to_micros,
                cause,
                // Account state has no session calendar: the venue answers at
                // any hour, so nothing is clipped and the bound is exact.
                clipped: galata_wire::Clipped::Continuous,
                dex,
            }),
        );
        record_generated_at(
            &mut self.archive,
            self.sink.as_ref(),
            PayloadAddress::Account(account.record_address()),
            envelope,
        )
        .map_err(record_error)?;
        Ok(())
    }

    /// Every account this ledger knows, by address fingerprint: what turns a
    /// counterparty that is one of ours into its alias.
    pub fn ours(&self) -> BTreeMap<String, Account> {
        self.masters
            .iter()
            .map(|m| (m.fingerprint.clone(), m.alias.clone()))
            .chain(
                self.subs
                    .values()
                    .map(|(_, sub)| (sub.fingerprint.clone(), sub.alias.clone())),
            )
            .collect()
    }

    /// Read an account's history back from the record, once: where each
    /// kind resumes, how early the venue has answered, which reach rows exist.
    fn load(&mut self, account: &ResolvedAccount) -> Result<(), LedgerError> {
        if !self.loaded.insert(account.alias.clone()) {
            return Ok(());
        }
        let mut kinds = crate::ledger::events::EVENT_KINDS.to_vec();
        kinds.push(Kind::Accounts);
        let rows = crate::ledger::events::read(
            self.archive.root(),
            account.venue.as_str(),
            account.alias.as_str(),
            &kinds,
            self.venue.normaliser(),
            &BTreeMap::new(),
        )?;
        for kind in crate::ledger::events::EVENT_KINDS {
            let key = (account.alias.clone(), kind);
            if let Some(newest) = crate::ledger::events::newest(&rows, kind) {
                self.newest.insert(key.clone(), newest);
            }
            if let Some(first) = rows
                .iter()
                .filter(|e| e.kind() == kind)
                .filter_map(|e| e.at_micros)
                .min()
            {
                self.earliest.insert(key, first);
            }
        }
        for row in &rows {
            if let Event::EventsReach(r) = &row.event {
                self.reached.insert((account.alias.clone(), r.kind));
            }
            if let Event::LedgerUpdate(u) = &row.event
                && u.effect == Effect::Unknown
            {
                self.events_status(&account.alias, Kind::LedgerUpdates)
                    .unknown_types
                    .insert(u.kind.clone());
            }
        }
        Ok(())
    }

    fn events_status(&mut self, alias: &Account, kind: Kind) -> &mut EventsStatus {
        self.status
            .accounts
            .entry(alias.to_string())
            .or_default()
            .events
            .entry(kind.as_str().to_string())
            .or_default()
    }

    /// Record one page, and note any ledger update type whose effect is
    /// unknown. The page is archived before anything reads it.
    fn record_page(
        &mut self,
        account: &ResolvedAccount,
        kind: Kind,
        channel: &'static str,
        bytes: Vec<u8>,
    ) -> Result<(), LedgerError> {
        let payload = Payload {
            seq: 0,
            recv_micros: self.clock.now_micros(),
            address: PayloadAddress::Account(account.record_address()),
            channel: channel.to_string(),
            kind: kind.as_str().to_string(),
            symbol: None,
            origin: Origin::Fetched,
            payload: bytes,
        };
        let unknown: Vec<String> = match self.venue.normaliser().normalise(&payload) {
            Ok(rows) => rows
                .into_iter()
                .filter_map(|e| match e.event {
                    Event::LedgerUpdate(u) if u.effect == Effect::Unknown => Some(u.kind),
                    _ => None,
                })
                .collect(),
            Err(_) => Vec::new(),
        };
        ingest(
            &mut self.archive,
            self.venue.normaliser(),
            self.sink.as_ref(),
            payload,
        )
        .map_err(record_error)?;
        self.events_status(&account.alias, Kind::LedgerUpdates)
            .unknown_types
            .extend(unknown);
        Ok(())
    }

    /// Record how far this ledger holds one kind of an account's history, by
    /// evidence (`ledger::events::fills_reach`).
    fn record_reach(
        &mut self,
        account: &ResolvedAccount,
        kind: Kind,
        reach: Reach,
    ) -> Result<(), LedgerError> {
        let earliest = self.earliest.get(&(account.alias.clone(), kind)).copied();
        let envelope = Envelope::for_account(
            account.venue.clone(),
            account.alias.clone(),
            None,
            self.clock.now_micros(),
            Event::EventsReach(EventsReach {
                kind,
                reach,
                earliest_micros: earliest,
            }),
        );
        record_generated_at(
            &mut self.archive,
            self.sink.as_ref(),
            PayloadAddress::Account(account.record_address()),
            envelope,
        )
        .map_err(record_error)?;
        self.reached.insert((account.alias.clone(), kind));
        self.events_status(&account.alias, kind).reach = Some(reach);
        Ok(())
    }

    /// Walk one kind of one account's history forward from the newest event
    /// recorded, paging while a page comes back full.
    async fn walk(
        &mut self,
        account: &ResolvedAccount,
        kind: Kind,
        pass: &mut EventsPass,
    ) -> Result<(), LedgerError> {
        let Some(channel) = self.venue.events_channel(kind) else {
            return Ok(());
        };
        let key = (account.alias.clone(), kind);
        let recorded = self.newest.get(&key).copied();
        let full = self.venue.page_size(kind);
        let mut start = recorded.unwrap_or(0);
        let mut first_page = true;
        loop {
            let page = match self.venue.events_page(kind, account.address(), start).await {
                Ok(page) => page,
                Err(refusal) => {
                    pass.missed += 1;
                    pass.throttled |= refusal == Refusal::Throttled;
                    self.events_status(&account.alias, kind).missed += 1;
                    return Ok(());
                }
            };
            let span = self.venue.page_span(&page);
            self.record_page(account, kind, channel, page)?;
            pass.pages += 1;
            self.events_status(&account.alias, kind).pages += 1;
            let Some((first, last, rows)) = span else {
                break;
            };
            let page_full = rows >= full;
            if first_page
                && let Some(recorded) = recorded
                && crate::ledger::events::beyond_reach(recorded, first, page_full)
            {
                self.record_gap_for(
                    account,
                    kind_series(kind),
                    None,
                    recorded,
                    first,
                    GapCause::BeyondReach,
                )?;
                pass.gaps += 1;
                self.record_reach(account, kind, Reach::Lost)?;
            }
            first_page = false;
            self.earliest
                .entry(key.clone())
                .and_modify(|e| *e = (*e).min(first))
                .or_insert(first);
            let newest = self.newest.entry(key.clone()).or_insert(last);
            *newest = (*newest).max(last);
            self.events_status(&account.alias, kind).newest_micros = Some(*newest);
            if !page_full {
                break;
            }
            // A full page that does not move the start forward would be
            // asked again forever: more rows share one moment than a page
            // holds. Stopped by name, on the status surface.
            if last <= start {
                pass.stalled += 1;
                self.events_status(&account.alias, kind).stalled_at_micros = Some(start);
                break;
            }
            start = last;
            if self.cadences.page_pause_micros > 0 {
                tokio::time::sleep(Duration::from_micros(
                    self.cadences.page_pause_micros as u64,
                ))
                .await;
            }
        }
        Ok(())
    }

    /// One pass over every polled account's history: funding first, then
    /// fills, then ledger updates, and each kind's reach recorded once.
    pub async fn events_all(&mut self) -> Result<EventsPass, LedgerError> {
        let mut pass = EventsPass::default();
        for account in self.polled() {
            self.load(&account)?;
            for kind in crate::ledger::events::EVENT_KINDS {
                self.walk(&account, kind, &mut pass).await?;
            }
            for kind in crate::ledger::events::EVENT_KINDS {
                if self.reached.contains(&(account.alias.clone(), kind)) {
                    continue;
                }
                let earliest = |k: Kind| self.earliest.get(&(account.alias.clone(), k)).copied();
                let reach = match kind {
                    Kind::Fills => crate::ledger::events::fills_reach(
                        earliest(Kind::Fills),
                        earliest(Kind::FundingPayments),
                    ),
                    other if earliest(other).is_some() => Reach::Consistent,
                    _ => Reach::Unknown,
                };
                // Unknown is not recorded: it is the absence of evidence, and
                // a later pass may find some. Every other state is a fact.
                if reach != Reach::Unknown {
                    self.record_reach(&account, kind, reach)?;
                } else {
                    self.events_status(&account.alias, kind).reach = Some(Reach::Unknown);
                }
            }
        }
        if !pass.throttled {
            self.backoff.reset();
        }
        self.write_status();
        Ok(pass)
    }

    fn write_status(&mut self) {
        self.status.at_micros = self.clock.now_micros();
        self.status.role_unverified = self.role_unverified.iter().map(|a| a.to_string()).collect();
        if let Some(file) = &self.status_file
            && let Ok(json) = serde_json::to_string_pretty(&self.status)
        {
            // Best effort: a status file that cannot be written must not stop
            // the ledger recording, which is the thing the status describes.
            let _ = file.write(&json);
        }
    }

    /// Run until cancelled: boot, then a snapshot pass every cadence and a
    /// discovery run every discovery cadence.
    pub async fn run(
        &mut self,
        shutdown: tokio_util::sync::CancellationToken,
    ) -> Result<(), LedgerError> {
        self.boot().await?;
        let mut next_discovery = self.clock.now_micros() + self.cadences.discover_micros;
        let mut next_events = self.clock.now_micros();
        while !shutdown.is_cancelled() {
            let pass = self.snapshot_all().await?;
            let mut throttled = pass.throttled;
            if self.clock.now_micros() >= next_events {
                throttled |= self.events_all().await?.throttled;
                self.fold_all()?;
                next_events = self.clock.now_micros() + self.cadences.events_micros;
            }
            if self.clock.now_micros() >= next_discovery {
                self.discover().await?;
                next_discovery = self.clock.now_micros() + self.cadences.discover_micros;
            }
            let wait = if throttled {
                self.backoff.next_wait()
            } else {
                Duration::from_micros(self.cadences.snapshot_micros.max(1) as u64)
            };
            tokio::select! {
                _ = shutdown.cancelled() => break,
                _ = tokio::time::sleep(wait) => {}
            }
        }
        self.write_status();
        Ok(())
    }
}

/// The series a gap in one kind of history is about.
fn kind_series(kind: Kind) -> Series {
    match kind {
        Kind::Fills => Series::Fills,
        Kind::FundingPayments => Series::FundingPayments,
        _ => Series::LedgerUpdates,
    }
}

fn record_error(e: RecordError) -> LedgerError {
    LedgerError::Record(e)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adapters::hyperliquid::ledger as hl;
    use crate::capture::TestClock;
    use crate::config::SecretSource;
    use crate::sink::NullSink;
    use std::sync::Mutex;

    const MAIN: &str = "0x3f9aa0c1d2e3f4a5b6c7d8e9f0a1b2c3d4e5f6a7";
    const SUB_A: &str = "0x00000000000000000000000000000000000000aa";
    const SUB_B: &str = "0x00000000000000000000000000000000000000bb";
    const FLAT: &[u8] = br#"{"marginSummary":{"accountValue":"0.0","totalNtlPos":"0.0","totalRawUsd":"0.0","totalMarginUsed":"0.0"},"crossMaintenanceMarginUsed":"0.0","withdrawable":"0.0","assetPositions":[],"time":1790332213000}"#;

    /// Pages scripted per (address, kind), answered in order.
    type Scripts = BTreeMap<(String, Kind), std::collections::VecDeque<Vec<u8>>>;

    /// A venue whose answers the test decides.
    struct Scripted {
        venue: Venue,
        normaliser: hl::LedgerNormaliser,
        snapshot: Mutex<Result<Vec<u8>, Refusal>>,
        listing: Mutex<Result<Vec<u8>, Refusal>>,
        role: Mutex<Result<Vec<u8>, Refusal>>,
        /// (address, kind) → the pages it will answer, in order; `[]` after.
        events: Mutex<Scripts>,
        /// Every events ask: (address, kind, start).
        asked: Mutex<Vec<(String, Kind, i64)>>,
    }

    impl Scripted {
        fn new() -> Scripted {
            Scripted {
                venue: Venue::new("hyperliquid").unwrap(),
                normaliser: hl::LedgerNormaliser::new()
                    .unwrap()
                    .with_key(FingerprintKey::new(Secret::new("deployment-key"))),
                snapshot: Mutex::new(Ok(FLAT.to_vec())),
                listing: Mutex::new(Ok(b"null".to_vec())),
                role: Mutex::new(Ok(br#"{"role":"user"}"#.to_vec())),
                events: Mutex::new(BTreeMap::new()),
                asked: Mutex::new(Vec::new()),
            }
        }
        fn pages(&self, address: &str, kind: Kind, pages: &[&str]) {
            self.events.lock().unwrap().insert(
                (address.to_string(), kind),
                pages.iter().map(|p| p.as_bytes().to_vec()).collect(),
            );
        }
        fn starts(&self, address: &str, kind: Kind) -> Vec<i64> {
            self.asked
                .lock()
                .unwrap()
                .iter()
                .filter(|(a, k, _)| a == address && *k == kind)
                .map(|(_, _, s)| *s)
                .collect()
        }
        fn listing_of(addresses: &[(&str, &str)]) -> Vec<u8> {
            let entries: Vec<String> = addresses
                .iter()
                .map(|(a, n)| {
                    format!(
                        r#"{{"name":"{n}","master":"{MAIN}","subAccountUser":"{a}","clearinghouseState":{{}},"spotState":{{}}}}"#
                    )
                })
                .collect();
            format!("[{}]", entries.join(",")).into_bytes()
        }
    }

    impl AccountVenue for Scripted {
        fn venue(&self) -> &Venue {
            &self.venue
        }
        fn normaliser(&self) -> &dyn Normalise {
            &self.normaliser
        }
        fn channel(&self, ask: Ask) -> &'static str {
            match ask {
                Ask::Snapshot => hl::SNAPSHOT_CHANNEL,
                Ask::Listing => hl::LISTING_CHANNEL,
                Ask::Role => hl::ROLE_CHANNEL,
                Ask::Mode => hl::MODE_CHANNEL,
            }
        }
        async fn snapshot(&self, _: &Secret, _: &str) -> Result<Vec<u8>, Refusal> {
            self.snapshot.lock().unwrap().clone()
        }
        async fn listing(&self, _: &Secret) -> Result<Vec<u8>, Refusal> {
            self.listing.lock().unwrap().clone()
        }
        async fn role(&self, _: &Secret) -> Result<Vec<u8>, Refusal> {
            self.role.lock().unwrap().clone()
        }
        async fn mode(&self, _: &Secret) -> Result<Vec<u8>, Refusal> {
            Ok(br#""disabled""#.to_vec())
        }
        async fn dex_known(&self, dex: &str) -> Result<bool, Refusal> {
            Ok(dex != "nosuchdex")
        }
        fn compose(&self, dex: &str, mode: Option<(&[u8], i64)>, state: &[u8]) -> Vec<u8> {
            hl::snapshot_bytes(dex, mode, state)
        }
        fn listed(&self, answer: &[u8]) -> Result<Vec<Listed>, NormaliseError> {
            hl::listing_of(answer)
        }
        fn is_sub_account(&self, answer: &[u8]) -> Result<bool, NormaliseError> {
            Ok(hl::role_of(answer)? == hl::Role::SubAccount)
        }
        fn events_channel(&self, kind: Kind) -> Option<&'static str> {
            match kind {
                Kind::Fills => Some(ev::FILLS_CHANNEL),
                Kind::FundingPayments => Some(ev::FUNDING_CHANNEL),
                Kind::LedgerUpdates => Some(ev::UPDATES_CHANNEL),
                _ => None,
            }
        }
        async fn events_page(
            &self,
            kind: Kind,
            address: &Secret,
            start: i64,
        ) -> Result<Vec<u8>, Refusal> {
            let address = address.expose().to_string();
            self.asked
                .lock()
                .unwrap()
                .push((address.clone(), kind, start));
            Ok(self
                .events
                .lock()
                .unwrap()
                .get_mut(&(address, kind))
                .and_then(|q| q.pop_front())
                .unwrap_or_else(|| b"[]".to_vec()))
        }
        fn page_size(&self, _: Kind) -> usize {
            2
        }
        fn page_span(&self, page: &[u8]) -> Option<(i64, i64, usize)> {
            let (last, rows) = ev::page_end(page)?;
            Some((ev::page_start(page)?, last, rows))
        }
    }

    use crate::adapters::hyperliquid::events as ev;

    fn fill(tid: u64, oid: u64, ms: i64) -> String {
        format!(
            r#"{{"coin":"BTC","px":"100","sz":"1","side":"B","time":{ms},"startPosition":"0","dir":"Open Long","closedPnl":"0","hash":"0xh","oid":{oid},"crossed":true,"fee":"0.1","feeToken":"USDC","tid":{tid},"twapId":null}}"#
        )
    }
    fn page(rows: &[String]) -> String {
        format!("[{}]", rows.join(","))
    }
    fn funding(ms: i64) -> String {
        format!(
            r#"{{"time":{ms},"hash":"0xh","delta":{{"type":"funding","coin":"BTC","usdc":"-0.1","szi":"1","fundingRate":"0.0001","nSamples":null}}}}"#
        )
    }
    fn read_events(
        run: &LedgerRun<Scripted, TestClock>,
        root: &std::path::Path,
        kind: Kind,
    ) -> Vec<Envelope> {
        crate::ledger::events::read(
            root,
            "hyperliquid",
            "main",
            &[kind],
            run.venue.normaliser(),
            &run.ours(),
        )
        .unwrap()
    }
    fn reach_rows(root: &std::path::Path) -> Vec<EventsReach> {
        payloads(root, "accounts")
            .iter()
            .filter_map(|p| crate::ingest::generated_envelope(&p.payload).ok())
            .filter_map(|e| match e.event {
                Event::EventsReach(r) => Some(r),
                _ => None,
            })
            .collect()
    }

    #[tokio::test]
    async fn a_fold_pass_projects_what_it_read() {
        let dir = tempfile::tempdir().unwrap();
        let (run, root) = ledger(dir.path());
        let tape = dir.path().join("ledger-tape");
        let file = StatusFile::named(&root.join("status"), "ledger-fold-hyperliquid");
        let mut run = run
            .with_fold(
                file,
                crate::ledger::fold::Tolerances {
                    position: "0".parse().unwrap(),
                    relative: "0.00002".parse().unwrap(),
                },
            )
            .with_projection(Some(tape.clone()));
        // One fill, archived twice: the record keeps both pages.
        run.venue.pages(
            MAIN,
            Kind::Fills,
            &[&page(&[fill(1, 1, 1000)]), &page(&[fill(1, 1, 1000)])],
        );
        run.events_all().await.unwrap();
        run.events_all().await.unwrap();
        run.fold_all().unwrap();

        let rows = |kind: Kind| -> usize {
            // Under the alias: an address never names a projected path.
            let path = crate::ledger::project::path_of(&tape, "hyperliquid", "main", kind);
            galata_segments::read_segment(&path)
                .unwrap()
                .iter()
                .map(|b| b.num_rows())
                .sum()
        };
        assert_eq!(rows(Kind::Fills), 1, "one fill, however often archived");
        assert_eq!(rows(Kind::FundingPayments), 0, "none, and a file saying so");
        let everything = projected_paths(&tape);
        assert!(
            everything.iter().all(|p| !p.contains(MAIN)),
            "no address in any projected path: {everything:?}"
        );
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&tape).unwrap().permissions().mode() & 0o777;
            assert_eq!(mode, 0o700, "held as the ledger is");
        }
    }

    fn projected_paths(dir: &std::path::Path) -> Vec<String> {
        let mut out = Vec::new();
        for entry in std::fs::read_dir(dir).unwrap().flatten() {
            let path = entry.path();
            out.push(path.display().to_string());
            if path.is_dir() {
                out.extend(projected_paths(&path));
            }
        }
        out
    }

    #[tokio::test]
    async fn without_a_projection_root_nothing_is_projected() {
        let dir = tempfile::tempdir().unwrap();
        let (run, root) = ledger(dir.path());
        let file = StatusFile::named(&root.join("status"), "ledger-fold-hyperliquid");
        let mut run = run.with_fold(
            file,
            crate::ledger::fold::Tolerances {
                position: "0".parse().unwrap(),
                relative: "0.00002".parse().unwrap(),
            },
        );
        run.venue
            .pages(MAIN, Kind::Fills, &[&page(&[fill(1, 1, 1000)])]);
        run.events_all().await.unwrap();
        run.fold_all().unwrap();
        assert!(!dir.path().join("ledger-tape").exists());
    }

    #[tokio::test]
    async fn two_passes_over_one_record_write_one_report() {
        let dir = tempfile::tempdir().unwrap();
        let (run, root) = ledger(dir.path());
        let file = StatusFile::named(&root.join("status"), "ledger-fold-hyperliquid");
        let mut run = run.with_fold(
            file.clone(),
            crate::ledger::fold::Tolerances {
                position: "0".parse().unwrap(),
                relative: "0.00002".parse().unwrap(),
            },
        );
        run.venue.pages(
            MAIN,
            Kind::Fills,
            &[&page(&[fill(1, 1, 1000)]), &page(&[fill(1, 1, 1000)])],
        );
        run.events_all().await.unwrap();
        let first = run.fold_all().unwrap().unwrap();
        run.events_all().await.unwrap();
        let second = run.fold_all().unwrap().unwrap();
        assert_eq!(
            first.accounts, second.accounts,
            "the same record folds the same"
        );
        let written: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(file.path()).unwrap()).unwrap();
        assert_eq!(written["accounts"]["main"]["books"][0]["position"], "1");
    }

    #[tokio::test]
    async fn a_full_page_asks_for_the_next() {
        let dir = tempfile::tempdir().unwrap();
        let (mut run, root) = ledger(dir.path());
        run.venue.pages(
            MAIN,
            Kind::Fills,
            &[
                &page(&[fill(1, 1, 1000), fill(2, 2, 2000)]),
                &page(&[fill(2, 2, 2000), fill(3, 3, 3000)]),
                &page(&[fill(3, 3, 3000)]),
            ],
        );
        let pass = run.events_all().await.unwrap();
        assert_eq!(
            run.venue.starts(MAIN, Kind::Fills),
            vec![0, 2_000_000, 3_000_000],
            "each next page from the last one's newest, inclusive"
        );
        assert!(pass.pages >= 3);
        // And the two overlaps are archived twice and read once.
        let fills = read_events(&run, &root, Kind::Fills);
        assert_eq!(fills.len(), 3, "a fill heard twice is one fill");
    }

    #[tokio::test]
    async fn a_fill_heard_twice_is_one_fill() {
        let dir = tempfile::tempdir().unwrap();
        let (mut run, root) = ledger(dir.path());
        run.venue.pages(
            MAIN,
            Kind::Fills,
            &[&page(&[fill(7, 7, 1000)]), &page(&[fill(7, 7, 1000)])],
        );
        run.events_all().await.unwrap();
        run.events_all().await.unwrap();
        let archived = payloads(&root, "fills").len();
        assert!(archived >= 2, "both pages archived");
        assert_eq!(read_events(&run, &root, Kind::Fills).len(), 1);
    }

    #[tokio::test]
    async fn a_self_trade_is_two_fills() {
        let dir = tempfile::tempdir().unwrap();
        let (mut run, root) = ledger(dir.path());
        run.venue.pages(
            MAIN,
            Kind::Fills,
            &[&page(&[fill(9, 1, 1000), fill(9, 2, 1000)])],
        );
        run.events_all().await.unwrap();
        assert_eq!(
            read_events(&run, &root, Kind::Fills).len(),
            2,
            "one trade id, two orders"
        );
    }

    #[tokio::test]
    async fn a_restart_resumes_from_the_record() {
        let dir = tempfile::tempdir().unwrap();
        {
            let (mut run, _) = ledger(dir.path());
            run.venue
                .pages(MAIN, Kind::Fills, &[&page(&[fill(1, 1, 1000)])]);
            run.events_all().await.unwrap();
        }
        let (mut run, _) = ledger(dir.path());
        run.events_all().await.unwrap();
        assert_eq!(
            run.venue.starts(MAIN, Kind::Fills),
            vec![1_000_000],
            "from the newest recorded, with no state but the record"
        );
    }

    #[tokio::test]
    async fn a_page_that_does_not_advance_stops_by_name() {
        let dir = tempfile::tempdir().unwrap();
        let (mut run, _) = ledger(dir.path());
        let stuck = page(&[fill(1, 1, 1000), fill(2, 2, 1000)]);
        run.venue
            .pages(MAIN, Kind::Fills, &[&stuck, &stuck, &stuck]);
        let pass = run.events_all().await.unwrap();
        assert_eq!(pass.stalled, 1);
        assert_eq!(run.venue.starts(MAIN, Kind::Fills), vec![0, 1_000_000]);
        let status = &run.status().accounts["main"].events["fills"];
        assert_eq!(status.stalled_at_micros, Some(1_000_000));
    }

    #[tokio::test]
    async fn fills_that_left_the_window_are_a_gap() {
        let dir = tempfile::tempdir().unwrap();
        let (mut run, root) = ledger(dir.path());
        run.venue.pages(
            MAIN,
            Kind::Fills,
            &[
                &page(&[fill(1, 1, 1000)]),
                // After a break: a full page that starts after our newest.
                &page(&[fill(5, 5, 5000), fill(6, 6, 6000)]),
            ],
        );
        run.events_all().await.unwrap();
        let pass = run.events_all().await.unwrap();
        assert_eq!(pass.gaps, 1);
        let gap = gaps(&root)
            .into_iter()
            .find(|g| g.cause == GapCause::BeyondReach)
            .unwrap();
        assert_eq!(gap.series, Series::Fills);
        assert_eq!((gap.from_micros, gap.to_micros), (1_000_000, 5_000_000));
        assert!(
            reach_rows(&root)
                .iter()
                .any(|r| r.kind == Kind::Fills && r.reach == Reach::Lost)
        );
    }

    #[tokio::test]
    async fn a_quiet_account_is_not_a_gap() {
        let dir = tempfile::tempdir().unwrap();
        let (mut run, root) = ledger(dir.path());
        run.venue
            .pages(MAIN, Kind::Fills, &[&page(&[fill(1, 1, 1000)])]);
        run.events_all().await.unwrap();
        run.events_all().await.unwrap();
        assert!(gaps(&root).is_empty());
    }

    #[tokio::test]
    async fn fills_that_start_after_a_funding_payment_are_recorded_lost() {
        let dir = tempfile::tempdir().unwrap();
        let (mut run, root) = ledger(dir.path());
        run.venue
            .pages(MAIN, Kind::FundingPayments, &[&page(&[funding(500)])]);
        run.venue
            .pages(MAIN, Kind::Fills, &[&page(&[fill(1, 1, 1000)])]);
        run.events_all().await.unwrap();
        let fills = reach_rows(&root)
            .into_iter()
            .find(|r| r.kind == Kind::Fills)
            .unwrap();
        assert_eq!(fills.reach, Reach::Lost);
        assert_eq!(fills.earliest_micros, Some(1_000_000));
        // Recorded once: a second pass adds no reach row.
        run.events_all().await.unwrap();
        assert_eq!(
            reach_rows(&root)
                .iter()
                .filter(|r| r.kind == Kind::Fills)
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn a_history_with_nothing_against_it_is_recorded_consistent() {
        let dir = tempfile::tempdir().unwrap();
        let (mut run, root) = ledger(dir.path());
        run.venue
            .pages(MAIN, Kind::FundingPayments, &[&page(&[funding(2000)])]);
        run.venue
            .pages(MAIN, Kind::Fills, &[&page(&[fill(1, 1, 1000)])]);
        run.events_all().await.unwrap();
        let fills = reach_rows(&root)
            .into_iter()
            .find(|r| r.kind == Kind::Fills)
            .unwrap();
        assert_eq!(fills.reach, Reach::Consistent);
    }

    #[tokio::test]
    async fn a_transfer_to_a_bound_sub_account_names_its_alias() {
        let dir = tempfile::tempdir().unwrap();
        let (mut run, root) = ledger(dir.path());
        *run.venue.listing.lock().unwrap() = Ok(Scripted::listing_of(&[(SUB_A, "arb")]));
        run.boot().await.unwrap();
        let transfer = format!(
            r#"[{{"time":1000,"hash":"0xh","delta":{{"type":"subAccountTransfer","usdc":"5.0","user":"{MAIN}","destination":"{SUB_A}"}}}}]"#
        );
        run.venue.pages(MAIN, Kind::LedgerUpdates, &[&transfer]);
        run.events_all().await.unwrap();
        let rows = read_events(&run, &root, Kind::LedgerUpdates);
        let Event::LedgerUpdate(u) = &rows[0].event else {
            panic!()
        };
        assert_eq!(
            u.counterparty,
            Some(galata_wire::Counterparty::Account(
                Account::new("main_s1").unwrap()
            ))
        );
        assert_eq!(
            u.effect,
            Effect::Known(vec![galata_wire::DexEffect {
                dex: None,
                usdc: "-5.0".parse().unwrap(),
            }])
        );
    }

    #[tokio::test]
    async fn an_unknown_ledger_update_type_is_named_on_the_status_surface() {
        let dir = tempfile::tempdir().unwrap();
        let (mut run, _) = ledger(dir.path());
        run.venue.pages(
            MAIN,
            Kind::LedgerUpdates,
            &[r#"[{"time":1000,"hash":"0xh","delta":{"type":"somethingNew","usdc":"1.0"}}]"#],
        );
        run.events_all().await.unwrap();
        let status = &run.status().accounts["main"].events["ledger_updates"];
        assert!(status.unknown_types.contains("somethingNew"));
    }

    struct Env;
    impl SecretSource for Env {
        fn secret(&self, name: &str) -> Result<Secret, crate::config::ConfigError> {
            Ok(Secret::new(match name {
                "KEY" => "deployment-key",
                _ => MAIN,
            }))
        }
    }

    const SECOND: i64 = 1_000_000;
    const T0: i64 = 1_790_332_213 * SECOND;

    fn ledger(root: &std::path::Path) -> (LedgerRun<Scripted, TestClock>, std::path::PathBuf) {
        let config = crate::config::Ledger {
            root: root.to_path_buf(),
            tape: None,
            snapshot_secs: 10,
            discover_secs: 600,
            events_secs: Some(300),
            fold_position_tolerance: Some(0.0),
            fold_relative_tolerance: Some(0.00002),
            ledger_share: 0.25,
            fingerprint_key_var: "KEY".into(),
            account: [(
                "main".to_string(),
                crate::config::LedgerAccount {
                    venue: "hyperliquid".into(),
                    address_var: "MAIN".into(),
                    dexes: vec![String::new(), "xyz".into()],
                    address: None,
                },
            )]
            .into(),
        };
        let (key, masters) = crate::ledger::resolve(&config, "hyperliquid", &Env).unwrap();
        let bindings = Bindings::read(root, "hyperliquid").unwrap();
        let run = LedgerRun::new(
            Scripted::new(),
            TestClock::at(T0),
            Archive::open(root),
            Box::new(NullSink),
            key,
            Cadences {
                snapshot_micros: 10 * SECOND,
                discover_micros: 600 * SECOND,
                events_micros: 300 * SECOND,
                page_pause_micros: 0,
            },
            masters,
            bindings,
        );
        (run, root.to_path_buf())
    }

    fn payloads(root: &std::path::Path, kind: &str) -> Vec<Payload> {
        crate::replay::read_all(root)
            .unwrap()
            .into_iter()
            .map(|r| r.payload().clone())
            .filter(|p| p.kind == kind)
            .collect()
    }

    fn gaps(root: &std::path::Path) -> Vec<Gap> {
        payloads(root, "gaps")
            .iter()
            .filter_map(|p| crate::ingest::generated_envelope(&p.payload).ok())
            .filter_map(|e| match e.event {
                Event::Gap(g) => Some(g),
                _ => None,
            })
            .collect()
    }

    #[tokio::test]
    async fn two_declared_dexes_give_two_margin_rows_per_cadence() {
        let dir = tempfile::tempdir().unwrap();
        let (mut run, root) = ledger(dir.path());
        run.boot().await.unwrap();
        let pass = run.snapshot_all().await.unwrap();
        assert_eq!(pass.answered, 2);
        let snapshots: Vec<Payload> = payloads(&root, "margin")
            .into_iter()
            .filter(|p| p.channel == hl::SNAPSHOT_CHANNEL)
            .collect();
        assert_eq!(snapshots.len(), 2);
        let dexes: BTreeSet<String> = snapshots
            .iter()
            .flat_map(|p| {
                hl::snapshot_rows(
                    &run.venue.venue,
                    &Account::new("main").unwrap(),
                    0,
                    &p.payload,
                )
                .unwrap()
            })
            .filter_map(|e| match e.event {
                Event::Margin(m) => Some(m.dex.unwrap_or_default()),
                _ => None,
            })
            .collect();
        assert_eq!(dexes, ["".to_string(), "xyz".to_string()].into());
    }

    #[tokio::test]
    async fn ten_identical_answers_are_ten_archived_payloads() {
        let dir = tempfile::tempdir().unwrap();
        let (mut run, root) = ledger(dir.path());
        run.boot().await.unwrap();
        for _ in 0..10 {
            run.snapshot_all().await.unwrap();
            run.clock.advance_secs(10);
        }
        let snapshots = payloads(&root, "margin")
            .into_iter()
            .filter(|p| p.channel == hl::SNAPSHOT_CHANNEL)
            .count();
        assert_eq!(
            snapshots, 20,
            "ten passes over two dexes, every one archived"
        );
    }

    #[tokio::test]
    async fn an_unreachable_venue_gaps_every_polled_account_one_cadence_wide() {
        let dir = tempfile::tempdir().unwrap();
        let (mut run, root) = ledger(dir.path());
        run.boot().await.unwrap();
        run.snapshot_all().await.unwrap();
        run.clock.advance_secs(10);
        *run.venue.snapshot.lock().unwrap() = Err(Refusal::Unreachable);
        let pass = run.snapshot_all().await.unwrap();
        assert_eq!(pass.gaps, 2, "one per (account, dex)");
        let gaps = gaps(&root);
        assert_eq!(gaps.len(), 2);
        for gap in gaps {
            assert_eq!(gap.series, Series::Margin);
            assert_eq!(gap.cause, GapCause::PollFailed);
            assert_eq!(
                gap.to_micros - gap.from_micros,
                10 * SECOND,
                "the first miss is one cadence wide"
            );
        }
    }

    #[tokio::test]
    async fn a_gap_names_its_dex() {
        // Found by the outage test (measured.md, 2026-09-25): the two gaps of
        // one missed pass were the main dex's and xyz's, and nothing in the
        // record told them apart.
        let dir = tempfile::tempdir().unwrap();
        let (mut run, root) = ledger(dir.path());
        run.boot().await.unwrap();
        run.snapshot_all().await.unwrap();
        run.clock.advance_secs(10);
        *run.venue.snapshot.lock().unwrap() = Err(Refusal::Unreachable);
        run.snapshot_all().await.unwrap();
        let dexes: BTreeSet<Option<String>> = gaps(&root).into_iter().map(|g| g.dex).collect();
        assert_eq!(
            dexes,
            [None, Some("xyz".to_string())].into(),
            "one gap per dex, each named"
        );
    }

    #[tokio::test]
    async fn consecutive_misses_nest_from_the_last_answer() {
        // The poll lane's specified behaviour (`poll-source`): each missed
        // pass records a gap from the last answer to now, so consecutive
        // misses NEST, and a reader must take their union, never their sum.
        let dir = tempfile::tempdir().unwrap();
        let (mut run, root) = ledger(dir.path());
        run.boot().await.unwrap();
        run.snapshot_all().await.unwrap();
        *run.venue.snapshot.lock().unwrap() = Err(Refusal::Unreachable);
        for _ in 0..3 {
            run.clock.advance_secs(10);
            run.snapshot_all().await.unwrap();
        }
        let main: Vec<Gap> = gaps(&root)
            .into_iter()
            .filter(|g| g.dex.is_none())
            .collect();
        let widths: Vec<i64> = main
            .iter()
            .map(|g| (g.to_micros - g.from_micros) / SECOND)
            .collect();
        assert_eq!(widths, vec![10, 20, 30]);
        assert!(
            main.iter().all(|g| g.from_micros == T0),
            "every one from the last answer"
        );
    }

    #[tokio::test]
    async fn a_throttled_snapshot_gaps_with_its_own_cause() {
        let dir = tempfile::tempdir().unwrap();
        let (mut run, root) = ledger(dir.path());
        run.boot().await.unwrap();
        run.snapshot_all().await.unwrap();
        run.clock.advance_secs(10);
        *run.venue.snapshot.lock().unwrap() = Err(Refusal::Throttled);
        let pass = run.snapshot_all().await.unwrap();
        assert!(pass.throttled);
        assert!(gaps(&root).iter().all(|g| g.cause == GapCause::Throttled));
    }

    #[tokio::test]
    async fn an_undeclared_dex_is_refused_at_load() {
        let dir = tempfile::tempdir().unwrap();
        let (mut run, root) = ledger(dir.path());
        run.masters[0].dexes = vec![String::new(), "nosuchdex".into()];
        let err = run.boot().await.unwrap_err();
        assert!(matches!(err, LedgerError::UnknownDex { .. }), "{err}");
        assert!(err.to_string().contains("nosuchdex"), "{err}");
        assert!(
            payloads(&root, "margin").is_empty(),
            "refused before anything was asked"
        );
    }

    #[tokio::test]
    async fn a_sub_account_declared_as_a_master_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let (mut run, _) = ledger(dir.path());
        *run.venue.role.lock().unwrap() =
            Ok(br#"{"role":"subAccount","data":{"master":"0x01"}}"#.to_vec());
        let err = run.boot().await.unwrap_err();
        assert!(matches!(err, LedgerError::NotAMaster { .. }), "{err}");
        assert!(err.to_string().contains("`main`"), "{err}");
    }

    #[tokio::test]
    async fn an_unanswered_role_is_asked_again_and_not_refused() {
        let dir = tempfile::tempdir().unwrap();
        let (mut run, _) = ledger(dir.path());
        *run.venue.role.lock().unwrap() = Err(Refusal::Unreachable);
        run.boot()
            .await
            .expect("the venue did not say, so nothing is refused");
        assert_eq!(run.status().role_unverified, vec!["main".to_string()]);
        *run.venue.role.lock().unwrap() = Ok(br#"{"role":"user"}"#.to_vec());
        run.discover().await.unwrap();
        assert!(run.status().role_unverified.is_empty());
    }

    #[tokio::test]
    async fn a_null_listing_is_a_run_that_found_none() {
        let dir = tempfile::tempdir().unwrap();
        let (mut run, _) = ledger(dir.path());
        run.boot().await.unwrap();
        let report = &run.status().discovery["main"];
        assert!(report.answered, "null is an answer");
        assert_eq!(
            (report.seen, report.new.len(), report.missing.len()),
            (0, 0, 0)
        );
    }

    #[tokio::test]
    async fn a_discovery_that_finds_nothing_is_still_reported() {
        let dir = tempfile::tempdir().unwrap();
        let (mut run, _) = ledger(dir.path());
        *run.venue.listing.lock().unwrap() = Ok(Scripted::listing_of(&[(SUB_A, "arb")]));
        run.boot().await.unwrap();
        run.clock.advance_secs(600);
        let runs = run.discover().await.unwrap();
        assert_eq!(runs.len(), 1);
        assert_eq!(runs[0].seen, 1);
        assert!(runs[0].new.is_empty() && runs[0].missing.is_empty());
        assert_eq!(run.status().discovery["main"].at_micros, T0 + 600 * SECOND);
    }

    #[tokio::test]
    async fn a_discovered_sub_account_is_snapshotted_under_its_ordinal() {
        let dir = tempfile::tempdir().unwrap();
        let (mut run, root) = ledger(dir.path());
        *run.venue.listing.lock().unwrap() =
            Ok(Scripted::listing_of(&[(SUB_A, "arb"), (SUB_B, "hedge")]));
        run.boot().await.unwrap();
        assert_eq!(
            run.status().discovery["main"].new,
            vec!["main_s1", "main_s2"]
        );
        run.snapshot_all().await.unwrap();
        assert!(root.join("venue=hyperliquid/account=main_s1").is_dir());
        assert!(root.join("venue=hyperliquid/account=main_s2").is_dir());
        // No address reached a path.
        for entry in walk(&root) {
            let path = entry.to_string_lossy().to_lowercase();
            for address in [MAIN, SUB_A, SUB_B] {
                assert!(!path.contains(&address[2..]), "{path}");
            }
        }
    }

    #[tokio::test]
    async fn a_vanished_sub_account_keeps_its_history_and_is_reported() {
        let dir = tempfile::tempdir().unwrap();
        let (mut run, root) = ledger(dir.path());
        *run.venue.listing.lock().unwrap() =
            Ok(Scripted::listing_of(&[(SUB_A, "arb"), (SUB_B, "hedge")]));
        run.boot().await.unwrap();
        run.snapshot_all().await.unwrap();

        run.clock.advance_secs(600);
        *run.venue.listing.lock().unwrap() = Ok(Scripted::listing_of(&[(SUB_B, "hedge")]));
        let runs = run.discover().await.unwrap();
        assert_eq!(runs[0].missing, vec!["main_s1"]);
        let status = &run.status().accounts["main_s1"];
        assert!(!status.polled);
        assert_eq!(status.not_seen_since_micros, Some(T0 + 600 * SECOND));
        assert!(
            root.join("venue=hyperliquid/account=main_s1").is_dir(),
            "history kept"
        );

        let pass = run.snapshot_all().await.unwrap();
        assert_eq!(
            pass.answered, 4,
            "main and main_s2, two dexes each: s1 is not polled"
        );
    }

    #[tokio::test]
    async fn ordinals_survive_a_restart_of_the_whole_loop() {
        let dir = tempfile::tempdir().unwrap();
        {
            let (mut run, _) = ledger(dir.path());
            *run.venue.listing.lock().unwrap() =
                Ok(Scripted::listing_of(&[(SUB_A, "arb"), (SUB_B, "hedge")]));
            run.boot().await.unwrap();
        }
        let (mut run, _) = ledger(dir.path());
        // The listing now comes back in the other order.
        *run.venue.listing.lock().unwrap() =
            Ok(Scripted::listing_of(&[(SUB_B, "hedge"), (SUB_A, "arb")]));
        run.boot().await.unwrap();
        assert!(
            run.status().discovery["main"].new.is_empty(),
            "nothing is new after a restart"
        );
        assert_eq!(
            run.status().accounts["main_s1"].master.as_deref(),
            Some("main")
        );
    }

    fn walk(root: &std::path::Path) -> Vec<std::path::PathBuf> {
        let mut out = Vec::new();
        let mut stack = vec![root.to_path_buf()];
        while let Some(dir) = stack.pop() {
            for entry in std::fs::read_dir(&dir).unwrap().filter_map(|e| e.ok()) {
                let path = entry.path();
                if path.is_dir() {
                    stack.push(path.clone());
                }
                out.push(path);
            }
        }
        out
    }
}
