//! What the process says about itself.
//!
//! **Two rules, and they are the whole of it.**
//!
//! *The local file is written first.* It is the surface that works when the
//! sink does not, and a surface that exists only on a bus cannot report that
//! the bus is unreachable.
//!
//! *It reports and never judges.* Every field is an elapsed time, a count or a
//! state; none says whether any of them is bad. A threshold inside the capture
//! process cannot be changed without a deploy, and is wrong for the next
//! instrument anyway. The component that judges is a different one, and it can
//! be changed without stopping capture.

use std::path::{Path, PathBuf};

use galata_wire::{Series, Ticker, Venue};

/// Whether the process is connected.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "lowercase")]
#[non_exhaustive]
pub enum Connection {
    /// Nothing is open.
    Down,
    /// One connection, delivering.
    Connected,
    /// A handover is under way: two connections, one of them still delivering.
    Rotating,
}

/// What is true of one pair.
///
/// **States, not verdicts.** `Stale` says nothing arrived in the counting
/// window; it does not say that is wrong. A quiet instrument at four in the
/// morning is stale and healthy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "lowercase")]
#[non_exhaustive]
pub enum PairState {
    /// Held, and something arrived in the counting window.
    Live,
    /// Held, and nothing arrived in the counting window.
    Stale,
    /// Held, and the venue's calendar says it should not be trading.
    Closed,
    /// The venue refused the subscription.
    Refused,
    /// Not subscribed, or not yet confirmed.
    NotSubscribed,
}

impl PairState {
    /// The discriminator.
    pub fn as_str(&self) -> &'static str {
        match self {
            PairState::Live => "live",
            PairState::Stale => "stale",
            PairState::Closed => "closed",
            PairState::Refused => "refused",
            PairState::NotSubscribed => "not_subscribed",
        }
    }
}

/// One pair's facts.
#[derive(Debug, Clone, serde::Serialize)]
pub struct PairStatus {
    /// Which instrument.
    pub ticker: Ticker,
    /// Which series.
    pub series: Series,
    /// What is true of it.
    pub state: PairState,
    /// **Our** clock: when we last heard anything.
    pub last_recv_micros: Option<i64>,
    /// **The venue's** clock: the last event time it stated.
    ///
    /// Both are carried because the difference between them is the number that
    /// matters, and a surface reporting one of them cannot produce it.
    pub last_event_micros: Option<i64>,
    /// Messages in the counting window.
    ///
    /// **Not a rate.** The window's age is `count_window_secs` on the
    /// snapshot, and it is the denominator — this was called `count_1m` and
    /// held between 55% and 63% of a minute, differing per pair.
    pub count: u32,
    /// What the venue said, where it refused.
    pub reason: Option<String>,
}

/// A walk in progress.
///
/// **Present only while one is running.** An absent field is the honest answer
/// to "is it walking" — a `WalkStatus` left behind from the last one would say
/// a backfill is under way an hour after it finished, and nothing reading this
/// could tell.
///
/// Facts like every other field here: a range, a point and a count. Nothing
/// says whether the progress is acceptable, because a long backfill and a stuck
/// one look identical from inside the process and the component that decides
/// which is a different one.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct WalkStatus {
    /// Which series is being walked.
    pub series: Series,
    /// The bar width, in microseconds.
    pub interval_micros: i64,
    /// Where the plan began, after the venue's reach clipped it.
    pub from_micros: i64,
    /// Where it was asked to reach.
    pub to_micros: i64,
    /// The point covered so far.
    pub reached_micros: i64,
    /// Requests made so far.
    pub requests_made: u32,
}

/// Everything the process knows about itself, at one moment.
#[derive(Debug, Clone, serde::Serialize)]
pub struct Status {
    /// Which venue this process captures.
    pub venue: Venue,
    /// When this snapshot was taken.
    pub observed_at_micros: i64,
    /// What is running.
    pub build: String,
    /// When it started.
    pub started_at_micros: i64,
    /// What it was told, as an identifier — so a consumer can notice that two
    /// processes disagree about their configuration, which is otherwise
    /// invisible.
    pub config_hash: String,
    /// Whether it is connected.
    pub connection: Connection,
    /// How long the current connection has been open.
    pub session_age_secs: Option<u64>,
    /// How long until the handover begins.
    pub next_handover_in_secs: Option<u64>,
    /// How many subscriptions the venue is delivering.
    pub subs_held: usize,
    /// How many were declared.
    pub subs_declared: usize,
    /// How many the venue refused.
    pub subs_refused: usize,
    /// **How long the counting window has been open**, which is every pair's
    /// `count` denominator.
    ///
    /// One window for all of them: per pair, each rolled on its own schedule
    /// and two counts were over different spans without saying so.
    ///
    /// **Truncated to whole seconds**, like the other durations here, so a
    /// count covers up to a second more than this states — 2.4% at a
    /// thirty-seven second window, and less as it fills. Said rather than
    /// hidden: a denominator with an unstated error is the thing this field
    /// exists to remove.
    pub count_window_secs: u64,
    /// When the buffer was last committed.
    pub last_flush_micros: Option<i64>,
    /// **Payloads received and not yet durable.**
    ///
    /// The live size of the window a crash would convert into a gap — the one
    /// number here that is a direct measure of standing risk.
    pub buffered: usize,
    /// **Events the sink dropped** because it could not keep up.
    ///
    /// Zero for a sink that cannot drop. A non-zero value here is the one
    /// number that says the stream is behind the record — and the record is
    /// still complete, which is why capture did not stop for it.
    pub sink_dropped: u64,
    /// A walk, while one is running — **absent otherwise**, so a long backfill
    /// is visible rather than silent and a finished one leaves nothing stale.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub walking: Option<WalkStatus>,
    /// Every pair the process knows about, declared or merely seen.
    pub pairs: Vec<PairStatus>,
}

impl Status {
    /// As JSON.
    pub fn to_json(&self) -> String {
        serde_json::to_string_pretty(self).unwrap_or_else(|e| format!("{{\"error\":\"{e}\"}}"))
    }
}

/// The local status file.
///
/// Written **before** the snapshot is emitted, because this is the surface that
/// answers when the sink does not.
#[derive(Debug, Clone)]
pub struct StatusFile {
    path: PathBuf,
}

impl StatusFile {
    /// The file one process writes: one per venue, so two processes sharing a
    /// directory do not overwrite each other.
    pub fn new(dir: &Path, venue: &Venue) -> StatusFile {
        StatusFile {
            path: dir.join(format!("datawatch-{venue}.json")),
        }
    }

    /// Where it lands.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Write it.
    ///
    /// Through a temporary and a rename, the same discipline the record uses: a
    /// reader must never see half a snapshot, and a dashboard polling this file
    /// will eventually read it mid-write otherwise.
    pub fn write(&self, json: &str) -> std::io::Result<()> {
        if let Some(parent) = self.path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let temp = self.path.with_extension("json.writing");
        std::fs::write(&temp, json)?;
        std::fs::rename(&temp, &self.path)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn status() -> Status {
        Status {
            venue: Venue::new("hyperliquid").unwrap(),
            observed_at_micros: 1_000,
            build: "galata-datawatch 0.1.0".into(),
            started_at_micros: 0,
            config_hash: "abc".into(),
            connection: Connection::Connected,
            session_age_secs: Some(10),
            next_handover_in_secs: Some(470),
            subs_held: 6,
            subs_declared: 6,
            subs_refused: 0,
            count_window_secs: 60,
            last_flush_micros: Some(900),
            buffered: 3,
            sink_dropped: 0,
            walking: None,
            pairs: vec![PairStatus {
                ticker: Ticker::new("BTC").unwrap(),
                series: Series::Quotes,
                state: PairState::Live,
                last_recv_micros: Some(990),
                last_event_micros: Some(980),
                count: 42,
                reason: None,
            }],
        }
    }

    #[test]
    fn no_field_is_a_verdict() {
        // A threshold inside the capture process cannot be changed without a
        // deploy, and is wrong for the next instrument anyway.
        let json = status().to_json();
        for verdict in [
            "healthy",
            "unhealthy",
            "ok\"",
            "severity",
            "alarm",
            "alert",
            "warning",
            "critical",
            "degraded",
        ] {
            assert!(
                !json.to_lowercase().contains(verdict),
                "{verdict:?} is a judgement, and this surface does not make them:\n{json}"
            );
        }
    }

    #[test]
    fn both_clocks_are_reported() {
        // The difference between them is the number that matters, and a
        // surface reporting one of them cannot produce it.
        let s = status();
        assert!(s.pairs[0].last_recv_micros.is_some());
        assert!(s.pairs[0].last_event_micros.is_some());
    }

    #[test]
    fn the_standing_risk_is_reported() {
        assert_eq!(status().buffered, 3);
    }

    #[test]
    fn the_local_file_answers_when_the_sink_does_not() {
        let dir = tempfile::tempdir().unwrap();
        let file = StatusFile::new(dir.path(), &Venue::new("hyperliquid").unwrap());
        file.write(&status().to_json()).unwrap();

        let read = std::fs::read_to_string(file.path()).unwrap();
        assert!(read.contains("hyperliquid"));
        assert!(read.contains("\"buffered\": 3"));
    }

    #[test]
    fn a_partial_snapshot_is_never_visible() {
        // Through a temporary and a rename, the same discipline the record
        // uses: a dashboard polling this file will otherwise read it mid-write.
        let dir = tempfile::tempdir().unwrap();
        let file = StatusFile::new(dir.path(), &Venue::new("hyperliquid").unwrap());
        file.write(&status().to_json()).unwrap();
        file.write(&status().to_json()).unwrap();

        let leftovers: Vec<String> = std::fs::read_dir(dir.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().to_string())
            .filter(|n| n.ends_with(".writing"))
            .collect();
        assert!(leftovers.is_empty(), "{leftovers:?}");
    }

    #[test]
    fn two_venues_do_not_share_a_file() {
        let dir = tempfile::tempdir().unwrap();
        let a = StatusFile::new(dir.path(), &Venue::new("hyperliquid").unwrap());
        let b = StatusFile::new(dir.path(), &Venue::new("rh-crypto").unwrap());
        assert_ne!(a.path(), b.path());
    }

    #[test]
    fn a_state_is_not_a_judgement() {
        // `Stale` says nothing arrived in the window. It does not say that is
        // wrong — a quiet instrument at four in the morning is stale and fine.
        assert_eq!(PairState::Stale.as_str(), "stale");
        assert_eq!(PairState::NotSubscribed.as_str(), "not_subscribed");
    }

    #[test]
    fn no_walk_means_no_walk_field() {
        // An absent field is the honest answer. A WalkStatus left behind from
        // the last walk would claim a backfill an hour after it finished.
        let json = status().to_json();
        assert!(!json.contains("walking"), "{json}");
    }

    #[test]
    fn a_running_walk_appears_and_is_not_a_verdict() {
        let mut s = status();
        s.walking = Some(WalkStatus {
            series: Series::Candles,
            interval_micros: 60_000_000,
            from_micros: 1_000,
            to_micros: 9_000,
            reached_micros: 4_000,
            requests_made: 12,
        });
        let json = s.to_json();
        assert!(json.contains("\"walking\""), "{json}");
        assert!(json.contains("\"reached_micros\": 4000"), "{json}");
        for verdict in ["behind", "stuck", "healthy", "slow", "degraded"] {
            assert!(
                !json.to_lowercase().contains(verdict),
                "{verdict:?}:\n{json}"
            );
        }
    }
}
