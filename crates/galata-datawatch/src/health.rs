//! The deployment's health: **one judge of every service's own latest
//! report** (`add-service-health-monitoring`, design D1 to D3).
//!
//! Pure: the reports, the cadences and the time in; each service's state and
//! findings out. It reads no file, no bus and no clock. galata-markets gathers
//! the reports and asks at each request, so a judgement is never itself stale.
//!
//! ```text
//!   service            its report                       allowed age
//!   capture:<venue>    its status, as last published    60 s
//!   ledger:<venue>     ledger-<venue>.json              2 × snapshot_secs + 60 s
//!     its fold         ledger-fold-<venue>.json         2 × events_secs
//!   <any service>      service-<service>.json           judged, not aged
//!   record             watch.json                       2 h
//! ```
//!
//! **A finding names nothing private.** It is made of service names, account
//! aliases, counts, times and reasons. The reports' other fields may hold an
//! address, an identifier or an amount, and this reads none of them: each
//! report is read through a view that names only what is judged.
//!
//! **Each finding has an identity**, `<service>/<kind>[/<subject>]`, that is
//! the same for as long as the fact holds and differs for a different fact,
//! so whoever alerts on it can say it once.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::calendar::time_of;
use crate::watch::Written;

/// How long capture's status may go unpublished: it publishes every second.
pub const CAPTURE_ALLOWANCE_SECS: i64 = 60;
/// How long after its start capture may hold fewer subscriptions than it
/// declares. Measured on 2026-11-27: a restarted capture held 4 of 27 at
/// 14 s and all 27 within 74 s, so a minute's grace alerted every restart.
pub const SUBSCRIBE_GRACE_SECS: i64 = 3 * 60;
/// How long the record's judge's report may age: it runs hourly.
pub const RECORD_ALLOWANCE_SECS: i64 = 2 * 3_600;
/// The window a restart loop is counted in, and how many starts make one.
pub const RESTART_WINDOW_SECS: i64 = 10 * 60;
/// Starts in [`RESTART_WINDOW_SECS`] that make a loop.
pub const RESTARTS: usize = 3;

const SECOND: i64 = 1_000_000;

/// What the judge is told: each report as its service wrote it, and the
/// configuration's cadences.
#[derive(Debug, Clone, Default)]
pub struct Reports {
    /// Each declared capture's venue, and its latest status if it has
    /// published one.
    pub captures: Vec<(String, Option<serde_json::Value>)>,
    /// Each venue the ledger's accounts name.
    pub ledgers: Vec<LedgerReports>,
    /// Each start record in the status folder.
    pub starts: Vec<serde_json::Value>,
    /// The record's judge's latest report, where there is one.
    pub record: Option<serde_json::Value>,
    /// The ledger's snapshot cadence, in seconds.
    pub snapshot_secs: u64,
    /// The ledger's events cadence, in seconds, where it declares one.
    pub events_secs: Option<u64>,
}

/// One ledger venue's reports.
#[derive(Debug, Clone, Default)]
pub struct LedgerReports {
    /// The venue.
    pub venue: String,
    /// `ledger-<venue>.json`, where it exists.
    pub status: Option<serde_json::Value>,
    /// `ledger-fold-<venue>.json`, where it exists.
    pub fold: Option<serde_json::Value>,
}

/// How a service stands.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum State {
    /// No finding.
    Well,
    /// Something is wrong, and the service is doing its work.
    Warning,
    /// The service is not doing its work: not reporting, stopped, refused,
    /// restarting or disconnected.
    Down,
}

/// Something wrong, said once: what was observed, against what was expected.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Finding {
    /// `<service>/<kind>[/<subject>]`: the same while the fact holds.
    pub id: String,
    /// What kind of fact.
    pub kind: String,
    /// What was observed.
    pub observed: String,
    /// What was expected.
    pub expected: String,
    /// Since when, where the report says.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub since_micros: Option<i64>,
}

/// One service as judged.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ServiceHealth {
    /// `capture:<venue>`, `ledger:<venue>`, `record`, or the service a start
    /// record names.
    pub service: String,
    /// The worst of its findings' states.
    pub state: State,
    /// When it last reported, where it has.
    pub reported_at_micros: Option<i64>,
    /// What is wrong, by identity.
    pub findings: Vec<Finding>,
}

/// The deployment as judged at one moment.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Judgement {
    /// When it was judged.
    pub at_micros: i64,
    /// The worst service's state.
    pub state: State,
    /// Every service, by name.
    pub services: Vec<ServiceHealth>,
}

// The views: what is judged of each report, and nothing else of it.

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct CaptureView {
    observed_at_micros: Option<i64>,
    started_at_micros: Option<i64>,
    connection: Option<String>,
    subs_held: Option<u64>,
    subs_declared: Option<u64>,
    pairs: Vec<PairView>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct PairView {
    last_recv_micros: Option<i64>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct LedgerView {
    at_micros: Option<i64>,
    accounts: BTreeMap<String, AccountView>,
    held_until_micros: Option<i64>,
    stopped: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct AccountView {
    polled: bool,
    answered_micros: BTreeMap<String, i64>,
    key_expires_micros: Option<i64>,
    key_warning: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct FoldView {
    at_micros: Option<i64>,
    unread: Vec<String>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct StartsView {
    service: String,
    starts: Vec<StartView>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct StartView {
    at_micros: i64,
    outcome: String,
    reason: Option<String>,
}

fn read<T: for<'de> Deserialize<'de> + Default>(document: &serde_json::Value) -> T {
    T::deserialize(document).unwrap_or_default()
}

/// One service's findings, gathered.
struct Judged {
    reported_at_micros: Option<i64>,
    findings: Vec<(State, Finding)>,
}

impl Judged {
    fn at(reported_at_micros: Option<i64>) -> Judged {
        Judged {
            reported_at_micros,
            findings: Vec::new(),
        }
    }

    fn say(
        &mut self,
        state: State,
        id: String,
        kind: &str,
        observed: String,
        expected: String,
        since_micros: Option<i64>,
    ) {
        self.findings.push((
            state,
            Finding {
                id,
                kind: kind.to_string(),
                observed,
                expected,
                since_micros,
            },
        ));
    }
}

/// A report older than its allowance, or none at all.
fn aged(
    judged: &mut Judged,
    service: &str,
    reported: Option<i64>,
    allowance_secs: i64,
    now: i64,
    state: State,
) {
    match reported {
        None => judged.say(
            state,
            format!("{service}/never-reported"),
            "never-reported",
            format!("{service} has never reported"),
            format!("a report at least every {allowance_secs} s"),
            None,
        ),
        Some(at) if now - at > allowance_secs * SECOND => judged.say(
            state,
            format!("{service}/not-reporting"),
            "not-reporting",
            format!(
                "{service} last reported at {}, {} s ago",
                time_of(at),
                (now - at) / SECOND
            ),
            format!("a report at least every {allowance_secs} s"),
            Some(at),
        ),
        Some(_) => {}
    }
}

fn capture(venue: &str, status: Option<&serde_json::Value>, now: i64) -> (String, Judged) {
    let service = format!("capture:{venue}");
    let view: CaptureView = status.map(read).unwrap_or_default();
    let reported = status.and(view.observed_at_micros);
    let mut judged = Judged::at(reported);
    aged(
        &mut judged,
        &service,
        reported,
        CAPTURE_ALLOWANCE_SECS,
        now,
        State::Down,
    );
    let Some(observed) = reported else {
        return (service, judged);
    };
    // Capture states that it is down, not since when: down, with nothing
    // received in the minute before it said so, is down for a minute at least.
    if view.connection.as_deref() == Some("down") {
        let newest = view.pairs.iter().filter_map(|p| p.last_recv_micros).max();
        if newest.is_none_or(|n| observed - n > CAPTURE_ALLOWANCE_SECS * SECOND) {
            judged.say(
                State::Down,
                format!("{service}/disconnected"),
                "disconnected",
                match newest {
                    Some(n) => format!(
                        "{service} is disconnected; it last received at {}",
                        time_of(n)
                    ),
                    None => format!("{service} is disconnected and has received nothing"),
                },
                "a connection delivering".into(),
                newest,
            );
        }
    }
    // A capture just started is still subscribing: short is a finding only
    // past the grace, as down is only past a minute.
    if let (Some(held), Some(declared)) = (view.subs_held, view.subs_declared)
        && held < declared
        && view
            .started_at_micros
            .is_none_or(|started| observed - started > SUBSCRIBE_GRACE_SECS * SECOND)
    {
        judged.say(
            State::Warning,
            format!("{service}/subscriptions"),
            "subscriptions",
            format!("{service} holds {held} subscriptions of {declared} declared"),
            format!("all {declared} held"),
            None,
        );
    }
    (service, judged)
}

fn ledger(
    reports: &LedgerReports,
    snapshot_secs: u64,
    events_secs: Option<u64>,
    now: i64,
) -> (String, Judged) {
    let service = format!("ledger:{}", reports.venue);
    let view: LedgerView = reports.status.as_ref().map(read).unwrap_or_default();
    let reported = reports.status.as_ref().and(view.at_micros);
    let mut judged = Judged::at(reported);
    let snapshot = snapshot_secs as i64;
    // The latest report while it is within its allowance. What a report says
    // of the accounts and the fold is judged against that report's own time,
    // and only while the ledger still reports: a stopped ledger is one fact,
    // not one more per account and one for its fold. Measured: a ledger
    // stopped by an uninstall at 14:30 read as four silent accounts and a
    // silent fold at 14:37, and as not reporting only at 14:42 (its clean
    // stop wrote its status once more).
    let live = reported.filter(|at| now - at <= (2 * snapshot + 60) * SECOND);

    // Stopped is the fact; that it no longer reports follows from it.
    if let Some(reason) = &view.stopped {
        judged.say(
            State::Down,
            format!("{service}/stopped"),
            "stopped",
            format!("{service} stopped: {reason}"),
            "a ledger running".into(),
            reported,
        );
    } else {
        aged(
            &mut judged,
            &service,
            reported,
            2 * snapshot + 60,
            now,
            State::Down,
        );
    }
    if let Some(until) = view.held_until_micros
        && until > now
    {
        judged.say(
            State::Warning,
            format!("{service}/held"),
            "held",
            format!(
                "{service} is held for a rate refusal until {}",
                time_of(until)
            ),
            "no hold".into(),
            None,
        );
    }
    for (alias, account) in &view.accounts {
        if let Some(warning) = &account.key_warning {
            judged.say(
                State::Warning,
                format!("{service}/key-expiry/{alias}"),
                "key-expiry",
                match account.key_expires_micros {
                    Some(at) => format!("{service} `{alias}`: {warning} (expires {})", time_of(at)),
                    None => format!("{service} `{alias}`: {warning}"),
                },
                "a session key with more than its warning's days left".into(),
                account.key_expires_micros,
            );
        }
        // Only an account that has answered and was still polled: one just
        // bound has not been asked yet.
        if account.polled
            && let Some(at) = live
            && let Some(last) = account.answered_micros.values().max().copied()
            && view.stopped.is_none()
            && at - last > 2 * snapshot * SECOND
        {
            judged.say(
                State::Warning,
                format!("{service}/no-answer/{alias}"),
                "no-answer",
                format!(
                    "{service} `{alias}` last answered at {}, {} s before the ledger's report at {}",
                    time_of(last),
                    (at - last) / SECOND,
                    time_of(at)
                ),
                format!("an answer at least every {} s", 2 * snapshot),
                Some(last),
            );
        }
    }
    if let Some(events) = events_secs
        && let Some(status_at) = live
        && view.stopped.is_none()
    {
        let fold: FoldView = reports.fold.as_ref().map(read).unwrap_or_default();
        let folded = reports.fold.as_ref().and(fold.at_micros);
        let allowance = 2 * events as i64;
        if let Some(at) = folded
            && status_at - at > allowance * SECOND
        {
            judged.say(
                State::Warning,
                format!("{service}/fold-not-reporting"),
                "fold-not-reporting",
                format!(
                    "{service}'s fold last reported at {}, {} s before the ledger's report at {}",
                    time_of(at),
                    (status_at - at) / SECOND,
                    time_of(status_at)
                ),
                format!("a fold report at least every {allowance} s"),
                Some(at),
            );
        }
        for alias in &fold.unread {
            judged.say(
                State::Warning,
                format!("{service}/fold-unread/{alias}"),
                "fold-unread",
                format!("{service}'s fold could not read `{alias}`'s record"),
                "every polled account folded".into(),
                folded,
            );
        }
    }
    (service, judged)
}

fn starts(document: &serde_json::Value, now: i64, judged: &mut BTreeMap<String, Judged>) {
    let view: StartsView = read(document);
    if view.service.is_empty() {
        return;
    }
    let service = view.service.clone();
    let entry = judged
        .entry(service.clone())
        .or_insert_with(|| Judged::at(None));
    let Some(latest) = view.starts.iter().max_by_key(|s| s.at_micros) else {
        return;
    };
    if latest.outcome == "refused" {
        entry.say(
            State::Down,
            format!("{service}/refused"),
            "refused",
            format!(
                "{service} was refused at its last start, at {}: {}",
                time_of(latest.at_micros),
                latest.reason.as_deref().unwrap_or("no reason given")
            ),
            "a service its launcher starts".into(),
            Some(latest.at_micros),
        );
    }
    let recent = view
        .starts
        .iter()
        .filter(|s| now - s.at_micros <= RESTART_WINDOW_SECS * SECOND && s.at_micros <= now)
        .count();
    if recent >= RESTARTS {
        entry.say(
            State::Down,
            format!("{service}/restarting"),
            "restarting",
            format!(
                "{service} started {recent} times in ten minutes; the latest was {}",
                latest.outcome
            ),
            format!("fewer than {RESTARTS} starts in ten minutes"),
            Some(latest.at_micros),
        );
    }
}

/// FNV-1a, for a stable identity of a fact that has no name of its own.
fn fnv(text: &str) -> String {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in text.bytes() {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x0100_0000_01b3);
    }
    format!("{hash:016x}")
}

fn record(document: Option<&serde_json::Value>, now: i64) -> Judged {
    let written: Option<Written> = document.and_then(|d| Written::deserialize(d).ok());
    let reported = written.as_ref().map(|w| w.judged_at_micros);
    let mut judged = Judged::at(reported);
    aged(
        &mut judged,
        "record",
        reported,
        RECORD_ALLOWANCE_SECS,
        now,
        State::Down,
    );
    for finding in written.iter().flat_map(|w| &w.findings) {
        // Identified by where and against what bound: the number observed
        // changes from one run to the next while the fact stays the same.
        judged.say(
            State::Warning,
            format!(
                "record/finding/{}",
                fnv(&format!("{}\n{}", finding.at, finding.expected))
            ),
            "record",
            format!("{} — {}", finding.observed, finding.at),
            finding.expected.clone(),
            None,
        );
    }
    judged
}

/// Judge the deployment at `now_micros`.
pub fn judge(reports: &Reports, now_micros: i64) -> Judgement {
    let now = now_micros;
    let mut judged: BTreeMap<String, Judged> = BTreeMap::new();
    for (venue, status) in &reports.captures {
        let (service, j) = capture(venue, status.as_ref(), now);
        judged.insert(service, j);
    }
    for ledger_reports in &reports.ledgers {
        let (service, j) = ledger(
            ledger_reports,
            reports.snapshot_secs,
            reports.events_secs,
            now,
        );
        judged.insert(service, j);
    }
    judged.insert("record".into(), record(reports.record.as_ref(), now));
    for document in &reports.starts {
        starts(document, now, &mut judged);
    }

    let services: Vec<ServiceHealth> = judged
        .into_iter()
        .map(|(service, j)| {
            let state = j
                .findings
                .iter()
                .map(|(s, _)| *s)
                .max()
                .unwrap_or(State::Well);
            let mut findings: Vec<Finding> = j.findings.into_iter().map(|(_, f)| f).collect();
            findings.sort_by(|a, b| a.id.cmp(&b.id));
            ServiceHealth {
                service,
                state,
                reported_at_micros: j.reported_at_micros,
                findings,
            }
        })
        .collect();
    Judgement {
        at_micros: now,
        state: services
            .iter()
            .map(|s| s.state)
            .max()
            .unwrap_or(State::Well),
        services,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    const T: i64 = 1_795_800_000_000_000;
    const S: i64 = SECOND;

    fn capture_status(observed: i64) -> serde_json::Value {
        json!({
            "venue": "hyperliquid", "observed_at_micros": observed, "build": "b",
            "started_at_micros": observed - 3_600 * S, "config_hash": "c",
            "connection": "connected", "subs_held": 54, "subs_declared": 54, "subs_refused": 0,
            "pairs": [{"ticker": "BTC", "series": "quotes", "state": "live",
                       "last_recv_micros": observed - S, "count": 10}]
        })
    }

    fn ledger_status(at: i64) -> serde_json::Value {
        json!({
            "venue": "derive-xyz", "at_micros": at,
            "accounts": {
                "owner": {"polled": true, "answered_micros": {"": at}, "missed": 0,
                          "key_expires_micros": T + 90 * 86_400 * S},
                "owner_s1": {"master": "owner", "polled": true, "answered_micros": {"": at}, "missed": 0},
                "owner_s2": {"master": "owner", "polled": true, "answered_micros": {"": at}, "missed": 0}
            },
            "discovery": {}, "role_unverified": [], "dex_unverified": []
        })
    }

    fn well() -> Reports {
        Reports {
            captures: vec![("hyperliquid".into(), Some(capture_status(T - 2 * S)))],
            ledgers: vec![LedgerReports {
                venue: "derive-xyz".into(),
                status: Some(ledger_status(T - 100 * S)),
                fold: Some(
                    json!({"venue": "derive-xyz", "at_micros": T - 100 * S, "accounts": {}, "unread": []}),
                ),
            }],
            starts: vec![json!({"service": "ledger:derive-xyz",
                                "starts": [{"at_micros": T - 3_600 * S, "outcome": "started"}]})],
            record: Some(json!({"judged_at_micros": T - 600 * S, "checked": 12, "findings": []})),
            snapshot_secs: 300,
            events_secs: Some(300),
        }
    }

    fn service<'a>(judgement: &'a Judgement, name: &str) -> &'a ServiceHealth {
        judgement
            .services
            .iter()
            .find(|s| s.service == name)
            .unwrap_or_else(|| panic!("no {name} in {judgement:?}"))
    }

    fn finding<'a>(judgement: &'a Judgement, id: &str) -> &'a Finding {
        judgement
            .services
            .iter()
            .flat_map(|s| &s.findings)
            .find(|f| f.id == id)
            .unwrap_or_else(|| panic!("no {id} in {judgement:#?}"))
    }

    fn ids(judgement: &Judgement) -> Vec<&str> {
        judgement
            .services
            .iter()
            .flat_map(|s| &s.findings)
            .map(|f| f.id.as_str())
            .collect()
    }

    #[test]
    fn every_service_reporting_on_time_holds_no_finding() {
        let judged = judge(&well(), T);
        assert_eq!(ids(&judged), Vec::<&str>::new(), "{judged:#?}");
        assert_eq!(judged.state, State::Well);
        let names: Vec<&str> = judged.services.iter().map(|s| s.service.as_str()).collect();
        assert_eq!(
            names,
            vec!["capture:hyperliquid", "ledger:derive-xyz", "record"]
        );
        assert_eq!(
            service(&judged, "ledger:derive-xyz").reported_at_micros,
            Some(T - 100 * S)
        );
    }

    #[test]
    fn a_ledger_that_stops_reporting_is_down_naming_when() {
        let mut reports = well();
        let at = T - 700 * S;
        reports.ledgers[0].status = Some(ledger_status(at));
        let judged = judge(&reports, T);
        let f = finding(&judged, "ledger:derive-xyz/not-reporting");
        assert!(
            f.observed.contains(&time_of(at)) && f.observed.contains("700 s"),
            "{f:?}"
        );
        assert_eq!(f.since_micros, Some(at));
        assert_eq!(service(&judged, "ledger:derive-xyz").state, State::Down);
        assert_eq!(judged.state, State::Down);
        // 699 s old is within 2 × 300 + 60.
        reports.ledgers[0].status = Some(ledger_status(T - 659 * S));
        assert!(!ids(&judge(&reports, T)).contains(&"ledger:derive-xyz/not-reporting"));
    }

    #[test]
    fn a_declared_service_that_never_reported_is_a_finding() {
        let mut reports = well();
        reports.ledgers[0].status = None;
        reports.captures[0].1 = None;
        reports.record = None;
        let judged = judge(&reports, T);
        for id in [
            "ledger:derive-xyz/never-reported",
            "capture:hyperliquid/never-reported",
            "record/never-reported",
        ] {
            assert!(ids(&judged).contains(&id), "{id}: {judged:#?}");
        }
    }

    #[test]
    fn what_capture_reports_wrong_is_a_finding() {
        let mut reports = well();
        let mut status = capture_status(T - 2 * S);
        status["subs_held"] = json!(50);
        reports.captures[0].1 = Some(status);
        let judged = judge(&reports, T);
        let f = finding(&judged, "capture:hyperliquid/subscriptions");
        assert!(
            f.observed.contains("50") && f.observed.contains("54"),
            "{f:?}"
        );
        assert_eq!(
            service(&judged, "capture:hyperliquid").state,
            State::Warning
        );

        // Silent for 90 s: not reporting.
        reports.captures[0].1 = Some(capture_status(T - 90 * S));
        assert!(ids(&judge(&reports, T)).contains(&"capture:hyperliquid/not-reporting"));

        // Down, and nothing received for two minutes before it said so.
        let mut status = capture_status(T - 2 * S);
        status["connection"] = json!("down");
        status["pairs"][0]["last_recv_micros"] = json!(T - 122 * S);
        reports.captures[0].1 = Some(status.clone());
        let judged = judge(&reports, T);
        assert_eq!(
            finding(&judged, "capture:hyperliquid/disconnected").since_micros,
            Some(T - 122 * S)
        );
        // Down a moment ago: not yet a minute.
        status["pairs"][0]["last_recv_micros"] = json!(T - 10 * S);
        reports.captures[0].1 = Some(status);
        assert!(!ids(&judge(&reports, T)).contains(&"capture:hyperliquid/disconnected"));
    }

    #[test]
    fn a_capture_just_started_is_still_subscribing() {
        // As measured on 2026-11-27: 4 of 27 held 14 s after a restart.
        let mut reports = well();
        let observed = T - 2 * S;
        let mut status = capture_status(observed);
        status["started_at_micros"] = json!(observed - 14 * S);
        status["subs_held"] = json!(4);
        status["subs_declared"] = json!(27);
        reports.captures[0].1 = Some(status.clone());
        let judged = judge(&reports, T);
        assert!(
            !ids(&judged).contains(&"capture:hyperliquid/subscriptions"),
            "{judged:#?}"
        );
        assert_eq!(service(&judged, "capture:hyperliquid").state, State::Well);
        // Still short three minutes after its start: a finding.
        status["started_at_micros"] = json!(observed - (SUBSCRIBE_GRACE_SECS + 1) * S);
        reports.captures[0].1 = Some(status);
        assert!(ids(&judge(&reports, T)).contains(&"capture:hyperliquid/subscriptions"));
    }

    #[test]
    fn a_session_key_near_its_expiry_is_named_with_the_warning_and_the_expiry() {
        let mut reports = well();
        let expiry = T + 5 * 86_400 * S;
        let status = reports.ledgers[0].status.as_mut().unwrap();
        status["accounts"]["owner"]["key_expires_micros"] = json!(expiry);
        status["accounts"]["owner"]["key_warning"] = json!("the session key expires in 5 days");
        let judged = judge(&reports, T);
        let f = finding(&judged, "ledger:derive-xyz/key-expiry/owner");
        assert!(
            f.observed.contains("expires in 5 days") && f.observed.contains(&time_of(expiry)),
            "{f:?}"
        );
        assert_eq!(service(&judged, "ledger:derive-xyz").state, State::Warning);
    }

    #[test]
    fn a_ledger_refused_at_start_is_down_with_its_reason_and_only_that() {
        let mut reports = well();
        // Its status is old too: stopped is the fact, not-reporting follows.
        let mut status = ledger_status(T - 3_600 * S);
        status["stopped"] = json!(
            "`owner`: the session key holds `withdraw`, which the ledger does not read with. \
             Nothing is read with it until the key is fixed"
        );
        reports.ledgers[0].status = Some(status);
        let judged = judge(&reports, T);
        let f = finding(&judged, "ledger:derive-xyz/stopped");
        assert!(f.observed.contains("`withdraw`"), "{f:?}");
        assert_eq!(service(&judged, "ledger:derive-xyz").state, State::Down);
        let found = ids(&judged);
        assert!(
            !found.contains(&"ledger:derive-xyz/not-reporting"),
            "{found:?}"
        );
        assert!(
            !found.iter().any(|id| id.contains("no-answer")),
            "{found:?}"
        );
    }

    #[test]
    fn a_held_venue_is_one_fact_however_often_asked() {
        let mut reports = well();
        let until = T + 3 * S;
        reports.ledgers[0].status.as_mut().unwrap()["held_until_micros"] = json!(until);
        let first = judge(&reports, T);
        let second = judge(&reports, T + S);
        let f = finding(&first, "ledger:derive-xyz/held");
        assert!(f.observed.contains(&time_of(until)), "{f:?}");
        assert_eq!(f.id, finding(&second, "ledger:derive-xyz/held").id);
        // A hold that has passed is no finding.
        assert!(!ids(&judge(&reports, until + S)).contains(&"ledger:derive-xyz/held"));
    }

    #[test]
    fn an_account_without_an_answer_is_named_by_its_alias() {
        // The ledger reported at T - 100 s; owner_s2 had not answered for
        // 700 s by then.
        let mut reports = well();
        reports.ledgers[0].status.as_mut().unwrap()["accounts"]["owner_s2"]["answered_micros"] =
            json!({"": T - 800 * S});
        let judged = judge(&reports, T);
        let f = finding(&judged, "ledger:derive-xyz/no-answer/owner_s2");
        assert!(
            f.observed.contains("`owner_s2`") && f.observed.contains("700 s"),
            "{f:?}"
        );
        // 600 s before the report is within twice the cadence.
        reports.ledgers[0].status.as_mut().unwrap()["accounts"]["owner_s2"]["answered_micros"] =
            json!({"": T - 700 * S});
        assert!(
            !ids(&judge(&reports, T))
                .iter()
                .any(|id| id.contains("no-answer"))
        );
    }

    #[test]
    fn the_folds_unread_accounts_and_its_silence_are_findings() {
        let mut reports = well();
        reports.ledgers[0].fold =
            Some(json!({"venue": "derive-xyz", "at_micros": T - 800 * S, "unread": ["owner_s1"]}));
        let judged = judge(&reports, T);
        assert!(
            finding(&judged, "ledger:derive-xyz/fold-unread/owner_s1")
                .observed
                .contains("`owner_s1`")
        );
        let f = finding(&judged, "ledger:derive-xyz/fold-not-reporting");
        assert!(f.observed.contains("700 s"), "{f:?}");
    }

    #[test]
    fn a_stopped_ledger_is_one_finding_not_one_per_account() {
        // As measured on 2026-11-27: the last pass at 14:26:21, the clean
        // stop's status at 14:30:06.
        let pass = T - 865 * S;
        let stop = T - 640 * S;
        let mut reports = well();
        let mut status = ledger_status(pass);
        status["at_micros"] = json!(stop);
        reports.ledgers[0].status = Some(status);
        reports.ledgers[0].fold =
            Some(json!({"venue": "derive-xyz", "at_micros": pass, "accounts": {}, "unread": []}));
        let ledger = |at| {
            let judged = judge(&reports, at);
            ids(&judged)
                .into_iter()
                .filter(|id| id.starts_with("ledger:"))
                .map(str::to_string)
                .collect::<Vec<_>>()
        };
        // Its accounts are not yet silent as of its last report, whatever the
        // clock says now.
        assert_eq!(ledger(T), Vec::<String>::new());
        // Past the allowance from that report: one fact.
        assert_eq!(
            ledger(T + 30 * S),
            vec!["ledger:derive-xyz/not-reporting".to_string()]
        );
        assert_eq!(
            ledger(T + 3_600 * S),
            vec!["ledger:derive-xyz/not-reporting".to_string()]
        );
    }

    #[test]
    fn what_the_launcher_records_wrong_is_a_finding() {
        let mut reports = well();
        reports.starts = vec![json!({"service": "flows", "starts": [
            {"at_micros": T - 3_600 * S, "outcome": "started"},
            {"at_micros": T - 60 * S, "outcome": "refused", "reason": "no folder at var/tape"}
        ]})];
        let judged = judge(&reports, T);
        let f = finding(&judged, "flows/refused");
        assert!(f.observed.contains("no folder at var/tape"), "{f:?}");
        assert_eq!(service(&judged, "flows").state, State::Down);

        // A refusal followed by a start is over.
        reports.starts = vec![json!({"service": "flows", "starts": [
            {"at_micros": T - 120 * S, "outcome": "refused", "reason": "x"},
            {"at_micros": T - 60 * S, "outcome": "started"}
        ]})];
        assert!(!ids(&judge(&reports, T)).contains(&"flows/refused"));

        // Four starts in ten minutes: a loop.
        let starts: Vec<serde_json::Value> = (1..=4)
            .map(|n| json!({"at_micros": T - n * 10 * S, "outcome": "started"}))
            .collect();
        reports.starts = vec![json!({"service": "ledger:derive-xyz", "starts": starts})];
        let judged = judge(&reports, T);
        let f = finding(&judged, "ledger:derive-xyz/restarting");
        assert!(
            f.observed.contains("4 times") && f.observed.contains("started"),
            "{f:?}"
        );
        assert_eq!(service(&judged, "ledger:derive-xyz").state, State::Down);
    }

    #[test]
    fn the_record_judges_findings_are_the_deployments_by_where_and_bound() {
        let mut reports = well();
        let written = |count: u32| {
            json!({"judged_at_micros": T - 600 * S, "checked": 12, "findings": [{
                "observed": format!("{count} segments in a closed partition"),
                "expected": "at most 64",
                "at": "var/archive/venue=hyperliquid/kind=trades/date=2026-11-20"
            }]})
        };
        reports.record = Some(written(3_589));
        let first = judge(&reports, T);
        let f = &service(&first, "record").findings[0];
        assert!(
            f.observed.contains("3589 segments") && f.observed.contains("date=2026-11-20"),
            "{f:?}"
        );
        assert_eq!(f.expected, "at most 64");
        // The count moves; the fact, and its identity, do not.
        reports.record = Some(written(3_601));
        assert_eq!(service(&judge(&reports, T), "record").findings[0].id, f.id);
        // Three hours without a report: the judge has stopped judging.
        reports.record =
            Some(json!({"judged_at_micros": T - 3 * 3_600 * S, "checked": 1, "findings": []}));
        assert!(ids(&judge(&reports, T)).contains(&"record/not-reporting"));
    }

    #[test]
    fn a_finding_names_no_address_identifier_or_amount() {
        let mut reports = well();
        let status = reports.ledgers[0].status.as_mut().unwrap();
        // Fields a report may hold that the judge must never repeat.
        status["wallet"] = json!("0x0009000900090009000900090009000900090009");
        status["accounts"]["owner_s2"]["subaccount_id"] = json!(424_242_001);
        status["accounts"]["owner_s2"]["initial_margin"] = json!("932.2013060107");
        status["accounts"]["owner_s2"]["answered_micros"] = json!({"": T - 700 * S});
        status["held_until_micros"] = json!(T + S);
        reports.ledgers[0].fold = Some(json!({"at_micros": T - 700 * S, "unread": ["owner_s2"],
                                             "accounts": {"owner_s2": {"value": "932.2013060107"}}}));
        let judged = judge(&reports, T);
        let text = serde_json::to_string(&judged).unwrap();
        for private in ["0x0009", "424242001", "932.2013"] {
            assert!(!text.contains(private), "{private} in {text}");
        }
        assert!(text.contains("owner_s2"), "the alias is what names it");
    }

    #[test]
    fn the_state_is_the_worst_finding() {
        let mut reports = well();
        reports.ledgers[0].status.as_mut().unwrap()["held_until_micros"] = json!(T + S);
        let judged = judge(&reports, T);
        assert_eq!(judged.state, State::Warning);
        reports.captures[0].1 = Some(capture_status(T - 120 * S));
        let judged = judge(&reports, T);
        assert_eq!(judged.state, State::Down);
        assert_eq!(service(&judged, "ledger:derive-xyz").state, State::Warning);
        let as_json = serde_json::to_value(&judged).unwrap();
        assert_eq!(as_json["state"], "down");
    }
}
