//! **Each venue's latest status**, as its capture last reported it
//! (`market-metadata`, design D8).
//!
//! Capture publishes its status to `status.<venue>` every second, and the
//! STATUS stream keeps the last per venue. galata-markets follows it as
//! `markets` — an ordered consumer from the stream's start, which is one
//! message per venue, then each new one — and holds the latest in memory.
//! A status is a claim with a moment: a venue whose last report is older
//! than [`CURRENT_FOR_MICROS`] is shown with that moment, never as current.

use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use async_nats::jetstream::{self, consumer::DeliverPolicy, consumer::pull::OrderedConfig};
use futures_util::StreamExt;
use serde::Serialize;

/// How old a status may be and still be called current: capture reports
/// every second, so a minute of silence is a capture that stopped reporting.
pub const CURRENT_FOR_MICROS: i64 = 60_000_000;

/// One venue's capture, as it last reported itself.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct VenueStatus {
    /// The venue.
    pub venue: String,
    /// When capture took the report.
    pub reported_at_micros: i64,
    /// `connected`, `rotating` or `down`, as capture said.
    pub connection: String,
    /// Subscriptions capture declared.
    pub subs_declared: u64,
    /// Subscriptions the venue holds.
    pub subs_held: u64,
    /// The newest event any pair received.
    pub last_event_micros: Option<i64>,
    /// Each pair capture follows: ticker and dataset.
    pub pairs: Vec<(String, String)>,
    /// When capture started, for the deployment's health only: not shown.
    #[serde(skip)]
    pub started_at_micros: Option<i64>,
    /// The current connection's age, for the deployment's health only: not
    /// shown. **Three states**, as capture writes them: absent from a capture
    /// that never wrote it (`None`), `null` while no connection is open
    /// (`Some(None)`), or its age (`Some(Some(_))`). The judge graces a
    /// subscription shortfall from it (`judge-subscriptions-per-connection`,
    /// design D4): dropped here, every handover read as a shortfall.
    #[serde(skip)]
    pub session_age_secs: Option<Option<u64>>,
    /// Each pair as capture stated it, for the deployment's health only: not
    /// shown. The judge excuses a quiet market's candles by its quotes' state
    /// (`judge-quiet-candles-as-held`).
    #[serde(skip)]
    pub pair_reports: Vec<PairReport>,
}

/// One pair, as the deployment's health judges it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PairReport {
    /// The instrument.
    pub ticker: String,
    /// `quotes`, `candles`, `funding`…
    pub series: String,
    /// As capture stated it: `live`, `stale`, `closed`, `refused`,
    /// `notsubscribed`.
    pub state: Option<String>,
    /// Its newest receipt.
    pub last_recv_micros: Option<i64>,
}

impl VenueStatus {
    /// A status message's JSON, reading only what is shown. A message that is
    /// not a status is `None`, not an error: the subject is capture's.
    pub fn parse(bytes: &[u8]) -> Option<VenueStatus> {
        let v: serde_json::Value = serde_json::from_slice(bytes).ok()?;
        let pairs: Vec<&serde_json::Value> = v["pairs"].as_array()?.iter().collect();
        Some(VenueStatus {
            venue: v["venue"].as_str()?.to_string(),
            reported_at_micros: v["observed_at_micros"].as_i64()?,
            connection: match &v["connection"] {
                serde_json::Value::String(s) => s.clone(),
                other => other.as_object()?.keys().next()?.clone(),
            },
            subs_declared: v["subs_declared"].as_u64()?,
            subs_held: v["subs_held"].as_u64()?,
            last_event_micros: pairs
                .iter()
                .filter_map(|p| p["last_recv_micros"].as_i64())
                .max(),
            pairs: pairs
                .iter()
                .filter_map(|p| {
                    Some((
                        p["ticker"].as_str()?.to_string(),
                        p["series"].as_str()?.to_string(),
                    ))
                })
                .collect(),
            pair_reports: pairs
                .iter()
                .filter_map(|p| {
                    Some(PairReport {
                        ticker: p["ticker"].as_str()?.to_string(),
                        series: p["series"].as_str()?.to_string(),
                        state: p["state"].as_str().map(str::to_string),
                        last_recv_micros: p["last_recv_micros"].as_i64(),
                    })
                })
                .collect(),
            started_at_micros: v["started_at_micros"].as_i64(),
            session_age_secs: v.get("session_age_secs").map(serde_json::Value::as_u64),
        })
    }

    /// This status as a report for the deployment's health to judge
    /// ([`galata_datawatch::health`]): what is held of it, under capture's own
    /// names.
    pub fn as_report(&self) -> serde_json::Value {
        let mut report = serde_json::json!({
            "venue": self.venue,
            "observed_at_micros": self.reported_at_micros,
            "connection": self.connection,
            "subs_held": self.subs_held,
            "subs_declared": self.subs_declared,
            "pairs": [{ "last_recv_micros": self.last_event_micros }],
        });
        // Both only where capture stated them: an absent one is judged as an
        // older capture's, a `null` connection age as no connection open.
        if let Some(started) = self.started_at_micros {
            report["started_at_micros"] = serde_json::json!(started);
        }
        if let Some(age) = self.session_age_secs {
            report["session_age_secs"] = serde_json::json!(age);
        }
        // Each pair as stated, where capture stated them: the newest receipt
        // across them is the one above, and each state is the judge's.
        if !self.pair_reports.is_empty() {
            report["pairs"] = self
                .pair_reports
                .iter()
                .map(|p| {
                    serde_json::json!({
                        "ticker": p.ticker, "series": p.series,
                        "state": p.state, "last_recv_micros": p.last_recv_micros,
                    })
                })
                .collect();
        }
        report
    }

    /// Whether it is recent enough, at `now`, to call current.
    pub fn is_current(&self, now_micros: i64) -> bool {
        now_micros - self.reported_at_micros <= CURRENT_FOR_MICROS
    }
}

/// The latest status of every venue, shared.
#[derive(Debug, Clone, Default)]
pub struct Statuses(Arc<RwLock<BTreeMap<String, VenueStatus>>>);

impl Statuses {
    /// Take a status, where it is newer than the one held for its venue.
    pub fn take(&self, status: VenueStatus) {
        if let Ok(mut held) = self.0.write() {
            let newer = held
                .get(&status.venue)
                .is_none_or(|seen| seen.reported_at_micros <= status.reported_at_micros);
            if newer {
                held.insert(status.venue.clone(), status);
            }
        }
    }

    /// Every venue's latest, by venue.
    pub fn latest(&self) -> Vec<VenueStatus> {
        self.0
            .read()
            .map(|held| held.values().cloned().collect())
            .unwrap_or_default()
    }

    /// One venue's latest.
    pub fn of(&self, venue: &str) -> Option<VenueStatus> {
        self.0.read().ok()?.get(venue).cloned()
    }
}

/// **Follow the STATUS stream** into `statuses`, for as long as the process
/// runs: a consumer that ends or cannot be made is made again a second later,
/// and a broker that is away is waited for, never a reason to stop serving.
pub async fn follow(client: async_nats::Client, statuses: Statuses) {
    let js = jetstream::new(client);
    let mut said = false;
    loop {
        match consume(&js, &statuses).await {
            Ok(()) => said = false,
            Err(error) if !said => {
                tracing::warn!(%error, "capture's status is not being read; asking again");
                said = true;
            }
            Err(_) => {}
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

async fn consume(js: &jetstream::Context, statuses: &Statuses) -> Result<(), String> {
    let stream = js
        .get_stream(galata_broker::streams::STATUS_STREAM)
        .await
        .map_err(|e| e.to_string())?;
    let mut messages = stream
        .create_consumer(OrderedConfig {
            filter_subject: "status.>".into(),
            deliver_policy: DeliverPolicy::All,
            ..Default::default()
        })
        .await
        .map_err(|e| e.to_string())?
        .messages()
        .await
        .map_err(|e| e.to_string())?;
    while let Some(message) = messages.next().await {
        let message = message.map_err(|e| e.to_string())?;
        if let Some(status) = VenueStatus::parse(&message.payload) {
            statuses.take(status);
        }
    }
    Ok(())
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    /// A status message as capture renders it, cut to the fields read here.
    pub(crate) fn report(venue: &str, at: i64, pairs: &[(&str, &str, i64)]) -> Vec<u8> {
        serde_json::json!({
            "venue": venue,
            "observed_at_micros": at,
            "connection": "connected",
            "subs_declared": pairs.len(),
            "subs_held": pairs.len(),
            "pairs": pairs.iter().map(|(t, s, recv)| serde_json::json!({
                "ticker": t, "series": s, "state": "live", "last_recv_micros": recv
            })).collect::<Vec<_>>(),
        })
        .to_string()
        .into_bytes()
    }

    #[test]
    fn a_status_message_is_read_for_what_is_shown() {
        let status = VenueStatus::parse(&report(
            "hyperliquid",
            1_000,
            &[("BTC", "quotes", 900), ("BTC", "candles", 950)],
        ))
        .unwrap();
        assert_eq!(status.connection, "connected");
        assert_eq!((status.subs_declared, status.subs_held), (2, 2));
        assert_eq!(status.last_event_micros, Some(950));
        assert_eq!(status.pairs[1], ("BTC".to_string(), "candles".to_string()));
        assert!(VenueStatus::parse(b"{\"not\":\"a status\"}").is_none());
    }

    #[test]
    fn an_older_report_does_not_replace_a_newer_one() {
        let statuses = Statuses::default();
        statuses.take(VenueStatus::parse(&report("hyperliquid", 2_000, &[])).unwrap());
        statuses.take(VenueStatus::parse(&report("hyperliquid", 1_000, &[])).unwrap());
        assert_eq!(
            statuses.of("hyperliquid").unwrap().reported_at_micros,
            2_000
        );
    }

    #[test]
    fn a_report_older_than_a_minute_is_not_current() {
        let status = VenueStatus::parse(&report("hyperliquid", 0, &[])).unwrap();
        assert!(status.is_current(CURRENT_FOR_MICROS));
        assert!(!status.is_current(10 * 60_000_000));
    }

    /// A status capture published on the deployment, 2026-11-28: 27 of 27
    /// held, its connection 435 s old.
    const RECORDED: &str = include_str!("../tests/fixtures/capture-status-hyperliquid.json");

    /// Whether the deployment's health, judging what galata-markets holds of
    /// `status`, states the subscriptions finding.
    fn judged_short(status: &serde_json::Value) -> bool {
        let held = VenueStatus::parse(status.to_string().as_bytes()).unwrap();
        let observed = held.reported_at_micros;
        let reports = galata_datawatch::health::Reports {
            captures: vec![("hyperliquid".into(), Some(held.as_report()))],
            ledgers: Vec::new(),
            starts: Vec::new(),
            record: None,
            snapshot_secs: 300,
            events_secs: None,
        };
        serde_json::to_string(&galata_datawatch::health::judge(
            &reports,
            observed + 1_000_000,
        ))
        .unwrap()
        .contains("capture:hyperliquid/subscriptions")
    }

    /// `judge-subscriptions-per-connection`, design D4: the judge saw neither
    /// opening through what galata-markets kept, so every handover read as a
    /// shortfall on the deployment.
    #[test]
    fn the_health_judged_from_what_is_held_graces_a_handover_and_a_restart() {
        let recorded: serde_json::Value = serde_json::from_str(RECORDED).unwrap();
        assert!(!judged_short(&recorded), "as recorded: 27 of 27");
        let observed = recorded["observed_at_micros"].as_i64().unwrap();
        let mut status = recorded.clone();
        status["subs_held"] = serde_json::json!(7);
        status["session_age_secs"] = serde_json::json!(5);
        assert!(
            !judged_short(&status),
            "7 of 27, five seconds after a handover"
        );
        status["session_age_secs"] = serde_json::json!(4 * 60);
        assert!(judged_short(&status), "the same, four minutes on");
        let mut restarted = recorded.clone();
        restarted["subs_held"] = serde_json::json!(4);
        restarted["started_at_micros"] = serde_json::json!(observed - 14_000_000);
        restarted["session_age_secs"] = serde_json::json!(14);
        assert!(
            !judged_short(&restarted),
            "4 of 27, fourteen seconds after a restart"
        );
        let mut down = status.clone();
        down["connection"] = serde_json::json!("down");
        down["session_age_secs"] = serde_json::Value::Null;
        assert!(
            !judged_short(&down),
            "no connection: the disconnected finding's to say"
        );
        // An older capture, which never wrote either: as before this change.
        let mut older = status.clone();
        older.as_object_mut().unwrap().remove("session_age_secs");
        older.as_object_mut().unwrap().remove("started_at_micros");
        assert!(judged_short(&older));
    }

    #[test]
    fn what_venues_shows_of_a_status_is_unchanged() {
        let held = VenueStatus::parse(RECORDED.as_bytes()).unwrap();
        assert_eq!(held.session_age_secs, Some(Some(435)), "read as recorded");
        let shown = serde_json::to_value(&held).unwrap();
        let mut keys: Vec<&str> = shown
            .as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect();
        keys.sort_unstable();
        assert_eq!(
            keys,
            [
                "connection",
                "last_event_micros",
                "pairs",
                "reported_at_micros",
                "subs_declared",
                "subs_held",
                "venue"
            ]
        );
    }

    /// The recorded status with BABA's three pairs added, in the states given,
    /// and its counts following them.
    fn with_baba(states: [&str; 3]) -> serde_json::Value {
        let mut status: serde_json::Value = serde_json::from_str(RECORDED).unwrap();
        let observed = status["observed_at_micros"].as_i64().unwrap();
        status["session_age_secs"] = serde_json::json!(4 * 60);
        let pairs = status["pairs"].as_array_mut().unwrap();
        for (series, state) in ["quotes", "candles", "funding"].iter().zip(states) {
            pairs.push(
                serde_json::json!({"ticker": "BABA", "series": series, "state": state,
                "last_recv_micros": (state != "notsubscribed").then_some(observed - 1_000_000)}),
            );
        }
        let held = states
            .iter()
            .filter(|s| matches!(**s, "live" | "stale" | "closed"))
            .count();
        status["subs_declared"] = serde_json::json!(27 + 3);
        status["subs_held"] = serde_json::json!(27 + held);
        status
    }

    /// `judge-quiet-candles-as-held`, task 1.2: what galata-markets holds of a
    /// status carries each pair's state to the judge.
    #[test]
    fn a_quiet_markets_candles_are_judged_from_what_is_held() {
        assert!(
            !judged_short(&with_baba(["live", "notsubscribed", "live"])),
            "BABA between trades"
        );
        assert!(
            !judged_short(&with_baba(["stale", "notsubscribed", "live"])),
            "its quotes stale"
        );
        assert!(
            judged_short(&with_baba(["notsubscribed", "notsubscribed", "live"])),
            "quotes missing too"
        );
        assert!(
            judged_short(&with_baba(["live", "live", "notsubscribed"])),
            "funding missing"
        );
    }

    #[test]
    fn the_newest_receipt_reads_the_same_with_each_pair_passed() {
        let held = VenueStatus::parse(RECORDED.as_bytes()).unwrap();
        let report = held.as_report();
        let newest = report["pairs"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|p| p["last_recv_micros"].as_i64())
            .max();
        assert_eq!(newest, held.last_event_micros);
        assert_eq!(
            report["pairs"].as_array().unwrap().len(),
            held.pairs.len(),
            "one entry per pair"
        );
    }
}
