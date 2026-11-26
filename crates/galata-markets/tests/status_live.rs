//! Capture's status, followed from a real `nats-server` as `markets`
//! (`market-metadata`): the broker's own test harness, shared by path, so the
//! identity is held to the grants the deployment generates.

#[path = "../../galata-broker/tests/support/mod.rs"]
mod support;

use std::time::Duration;

use galata_broker::streams;
use galata_broker::{BUS_ADMIN, BUS_ADMIN_INBOX, GALATA_MARKETS, GALATA_MARKETS_INBOX};
use galata_markets::status::{Statuses, follow};
use support::{Broker, connect, connect_with};

fn report(at: i64, connection: &str) -> String {
    serde_json::json!({
        "venue": "hyperliquid",
        "observed_at_micros": at,
        "connection": connection,
        "subs_declared": 2,
        "subs_held": 2,
        "pairs": [
            {"ticker": "BTC", "series": "quotes", "state": "live", "last_recv_micros": at - 5},
            {"ticker": "GOLD", "series": "quotes", "state": "live", "last_recv_micros": at - 9},
        ],
    })
    .to_string()
}

async fn wait_for(statuses: &Statuses, at: i64) -> bool {
    for _ in 0..100 {
        if statuses
            .of("hyperliquid")
            .is_some_and(|s| s.reported_at_micros == at)
        {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    false
}

#[tokio::test]
async fn galata_markets_follows_each_venues_latest_status() {
    let broker = Broker::start();
    let admin = connect_with(
        &broker,
        BUS_ADMIN,
        async_nats::ConnectOptions::new().custom_inbox_prefix(BUS_ADMIN_INBOX),
    )
    .await;
    streams::apply(&admin).await;

    // A report before galata-markets started: the stream kept it.
    let capture = connect(&broker, "datawatch-hyperliquid").await;
    capture
        .publish("status.hyperliquid", report(1_000, "connected").into())
        .await
        .unwrap();
    capture.flush().await.unwrap();

    let markets = connect_with(
        &broker,
        GALATA_MARKETS,
        async_nats::ConnectOptions::new().custom_inbox_prefix(GALATA_MARKETS_INBOX),
    )
    .await;
    let statuses = Statuses::default();
    tokio::spawn(follow(markets, statuses.clone()));
    assert!(
        wait_for(&statuses, 1_000).await,
        "the kept report is read at the start"
    );
    let status = statuses.of("hyperliquid").unwrap();
    assert_eq!(status.connection, "connected");
    assert_eq!(status.last_event_micros, Some(995));
    assert_eq!(status.pairs.len(), 2);

    // And each new one, as capture sends it.
    capture
        .publish("status.hyperliquid", report(2_000, "rotating").into())
        .await
        .unwrap();
    capture.flush().await.unwrap();
    assert!(wait_for(&statuses, 2_000).await, "a new report replaces it");
    assert_eq!(statuses.of("hyperliquid").unwrap().connection, "rotating");
}
