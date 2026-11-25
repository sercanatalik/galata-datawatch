//! The generated permissions, loaded into a real `nats-server` (tests/support).

mod support;

use support::{Broker, connect};

#[tokio::test]
async fn the_reader_is_served_by_a_broker_running_the_generated_table() {
    let broker = Broker::start();
    let client = connect(&broker, "reader").await;
    let mut status = client
        .subscribe("status.>")
        .await
        .expect("reader may subscribe status.>");
    client.flush().await.expect("the broker answers");

    // And it really is the table that governs: capture's identity publishes
    // its own status, which the reader receives.
    let capture = connect(&broker, "datawatch-hyperliquid").await;
    capture
        .publish("status.hyperliquid", "{}".into())
        .await
        .expect("capture publishes its status");
    capture.flush().await.unwrap();
    let message = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        futures_util::StreamExt::next(&mut status),
    )
    .await
    .expect("the reader hears capture's status within 5 s")
    .expect("a message");
    assert_eq!(message.subject.as_str(), "status.hyperliquid");
}
