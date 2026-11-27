//! **`/v1/health`: the deployment's health, judged when asked**
//! (`add-service-health-monitoring`, design D1, D4 and D9).
//!
//! Each service's latest report, gathered here and judged by
//! [`galata_datawatch::health::judge`]:
//! - capture's, as galata-markets already follows it on the bus;
//! - each ledger's status and fold report, the launcher's start records and
//!   the record judge's report, from the status folder, read-only.
//!
//! **Open, as `/health` is.** galata-markets is published on the tailnet
//! only, and a finding names no address, identifier or amount. The answer is
//! the judgement alone, never a status document.

use std::path::{Path, PathBuf};

use axum::Json;
use axum::extract::State;
use galata_datawatch::config::Config;
use galata_datawatch::health::{Judgement, LedgerReports, Reports, judge};

use crate::Shared;

/// Where the reports are, and what the configuration says to expect.
#[derive(Debug, Clone, Default)]
pub struct Sources {
    /// The status folder, read-only.
    pub status: PathBuf,
    /// Each declared capture's venue.
    pub captures: Vec<String>,
    /// Each venue the ledger's accounts name.
    pub ledgers: Vec<String>,
    /// The ledger's snapshot cadence, in seconds.
    pub snapshot_secs: u64,
    /// The ledger's events cadence, where declared.
    pub events_secs: Option<u64>,
}

impl Sources {
    /// From the deployment's configuration.
    pub fn from_config(config: &Config) -> Sources {
        let mut ledgers: Vec<String> = config
            .ledger
            .iter()
            .flat_map(|l| l.account.values().map(|a| a.venue.clone()))
            .collect();
        ledgers.sort();
        ledgers.dedup();
        Sources {
            status: config.paths.status.clone(),
            captures: config.venue.keys().cloned().collect(),
            ledgers,
            snapshot_secs: config.ledger.as_ref().map_or(0, |l| l.snapshot_secs),
            events_secs: config.ledger.as_ref().and_then(|l| l.events_secs),
        }
    }
}

/// A status document in the folder, where it exists and is JSON.
fn document(folder: &Path, name: &str) -> Option<serde_json::Value> {
    let text = std::fs::read_to_string(folder.join(name)).ok()?;
    serde_json::from_str(&text).ok()
}

/// Every report, as it stands now.
pub fn reports(markets: &crate::Markets) -> Reports {
    let sources = &markets.health;
    let folder = sources.status.as_path();
    let starts = std::fs::read_dir(folder)
        .into_iter()
        .flatten()
        .filter_map(Result::ok)
        .filter_map(|entry| {
            let name = entry.file_name().to_string_lossy().to_string();
            (name.starts_with("service-") && name.ends_with(".json"))
                .then(|| document(folder, &name))
                .flatten()
        })
        .collect();
    Reports {
        captures: sources
            .captures
            .iter()
            .map(|venue| {
                let status = markets.statuses.of(venue).map(|s| s.as_report());
                (venue.clone(), status)
            })
            .collect(),
        ledgers: sources
            .ledgers
            .iter()
            .map(|venue| LedgerReports {
                venue: venue.clone(),
                status: document(folder, &format!("ledger-{venue}.json")),
                fold: document(folder, &format!("ledger-fold-{venue}.json")),
            })
            .collect(),
        starts,
        record: document(
            folder,
            &format!("{}.json", galata_datawatch::watch::REPORT_NAME),
        ),
        snapshot_secs: sources.snapshot_secs,
        events_secs: sources.events_secs,
    }
}

/// `GET /v1/health`: the judgement, as of the request.
pub async fn health_route(State(markets): State<Shared>) -> Json<Judgement> {
    Json(judge(&reports(&markets), crate::now_micros()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::status::VenueStatus;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use http_body_util::BodyExt;
    use std::sync::Arc;
    use tower::ServiceExt;

    /// A deployment of one capture and one ledger, its status folder written
    /// as the services write it.
    fn deployment() -> (tempfile::TempDir, Shared) {
        let dir = tempfile::tempdir().unwrap();
        let status = dir.path().join("status");
        std::fs::create_dir_all(&status).unwrap();
        let now = crate::now_micros();
        std::fs::write(
            status.join("ledger-derive-xyz.json"),
            serde_json::json!({
                "venue": "derive-xyz", "at_micros": now - 10_000_000,
                // A field no finding may repeat.
                "wallet": "0x0009000900090009000900090009000900090009",
                "held_until_micros": now + 60_000_000,
                "accounts": {"owner_s1": {"polled": true, "answered_micros": {"": now - 10_000_000}}}
            })
            .to_string(),
        )
        .unwrap();
        std::fs::write(
            status.join("ledger-fold-derive-xyz.json"),
            serde_json::json!({"at_micros": now - 10_000_000, "unread": []}).to_string(),
        )
        .unwrap();
        std::fs::write(
            status.join("watch.json"),
            serde_json::json!({"judged_at_micros": now - 60_000_000, "checked": 3, "findings": []})
                .to_string(),
        )
        .unwrap();
        std::fs::write(
            status.join("service-flows.json"),
            serde_json::json!({"service": "flows", "starts": [
                {"at_micros": now - 30_000_000, "outcome": "refused", "reason": "no folder at var/tape"}
            ]})
            .to_string(),
        )
        .unwrap();

        let mut markets = crate::Markets::over(dir.path().join("tape"));
        markets.statuses.take(VenueStatus {
            venue: "hyperliquid".into(),
            reported_at_micros: now - 1_000_000,
            connection: "connected".into(),
            subs_declared: 54,
            subs_held: 50,
            last_event_micros: Some(now - 1_000_000),
            pairs: vec![],
        });
        markets.health = Sources {
            status,
            captures: vec!["hyperliquid".into()],
            ledgers: vec!["derive-xyz".into()],
            snapshot_secs: 300,
            events_secs: Some(300),
        };
        (dir, Arc::new(markets))
    }

    async fn ask(markets: Shared, path: &str, token: Option<&str>) -> (StatusCode, String) {
        let app = crate::service(crate::access::tests::stand_in(), markets);
        let mut request = Request::get(path);
        if let Some(token) = token {
            request = request.header("Authorization", format!("Bearer {token}"));
        }
        let response = app
            .oneshot(request.body(Body::empty()).unwrap())
            .await
            .unwrap();
        let status = response.status();
        let body = response.into_body().collect().await.unwrap().to_bytes();
        (status, String::from_utf8(body.to_vec()).unwrap())
    }

    #[tokio::test]
    async fn health_needs_no_token_and_judges_every_declared_service() {
        let (_dir, markets) = deployment();
        let (status, body) = ask(markets, "/v1/health", None).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let judged: Judgement = serde_json::from_str(&body).unwrap();
        let names: Vec<&str> = judged.services.iter().map(|s| s.service.as_str()).collect();
        assert_eq!(
            names,
            vec![
                "capture:hyperliquid",
                "flows",
                "ledger:derive-xyz",
                "record"
            ],
            "{body}"
        );
        let ids: Vec<&str> = judged
            .services
            .iter()
            .flat_map(|s| &s.findings)
            .map(|f| f.id.as_str())
            .collect();
        for id in [
            "capture:hyperliquid/subscriptions",
            "ledger:derive-xyz/held",
            "flows/refused",
        ] {
            assert!(ids.contains(&id), "{id} in {ids:?}");
        }
    }

    #[tokio::test]
    async fn the_answer_holds_the_judgement_and_no_status_document() {
        let (_dir, markets) = deployment();
        let (_, body) = ask(markets, "/v1/health", None).await;
        assert!(!body.contains("0x0009"), "a status field leaked: {body}");
        let value: serde_json::Value = serde_json::from_str(&body).unwrap();
        let keys: Vec<&str> = value
            .as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect();
        assert_eq!(keys, vec!["at_micros", "services", "state"]);
    }

    #[tokio::test]
    async fn every_other_route_still_needs_a_token_and_health_is_unchanged() {
        let (_dir, markets) = deployment();
        let (refused, _) = ask(markets.clone(), "/v1/venues", None).await;
        assert_eq!(refused, StatusCode::UNAUTHORIZED);
        let (served, _) = ask(
            markets.clone(),
            "/v1/venues",
            Some("stand-in-token-for-operator"),
        )
        .await;
        assert_eq!(served, StatusCode::OK);
        let (status, body) = ask(markets, "/health", None).await;
        assert_eq!((status, body.as_str()), (StatusCode::OK, "ok\n"));
    }
}
