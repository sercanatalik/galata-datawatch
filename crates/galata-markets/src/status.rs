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
        })
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
}
