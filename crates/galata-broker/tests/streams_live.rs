//! The stream table, applied to a real `nats-server` (tests/support).

mod support;

use std::time::Duration;

use galata_broker::streams::{self, Applied};
use galata_broker::{BUS_ADMIN, BUS_ADMIN_INBOX};
use support::{Broker, connect, connect_with};

async fn admin(broker: &Broker) -> async_nats::Client {
    connect_with(
        broker,
        BUS_ADMIN,
        async_nats::ConnectOptions::new().custom_inbox_prefix(BUS_ADMIN_INBOX),
    )
    .await
}

/// A stream's messages, once the broker has stored what was sent.
async fn messages_settle(client: &async_nats::Client, stream: &str, want: u64) -> u64 {
    let jetstream = async_nats::jetstream::new(client.clone());
    let mut seen = 0;
    for _ in 0..50 {
        let mut handle = jetstream
            .get_stream(stream)
            .await
            .expect("the stream exists");
        seen = handle.info().await.expect("its info").state.messages;
        if seen == want {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    seen
}

#[tokio::test]
async fn a_first_apply_creates_the_three_streams_as_declared() {
    let broker = Broker::start();
    let client = admin(&broker).await;
    let outcomes = streams::apply(&client).await;
    assert_eq!(outcomes.len(), 3, "{outcomes:?}");
    assert!(
        outcomes
            .iter()
            .all(|o| matches!(o, Applied::Held { messages: 0, .. })),
        "{outcomes:?}"
    );

    let jetstream = async_nats::jetstream::new(client.clone());
    for declared in streams::table() {
        let mut stream = jetstream.get_stream(declared.name).await.unwrap();
        let config = stream.info().await.unwrap().config.clone();
        assert_eq!(config.subjects, declared.subjects, "{}", declared.name);
        assert_eq!(config.max_age, declared.max_age, "{}", declared.name);
        assert_eq!(config.max_bytes, declared.max_bytes, "{}", declared.name);
        assert_eq!(
            config.max_messages_per_subject, declared.max_messages_per_subject,
            "{}",
            declared.name
        );
    }
}

#[tokio::test]
async fn a_changed_retention_is_applied_with_nothing_lost() {
    let broker = Broker::start();
    let client = admin(&broker).await;
    let jetstream = async_nats::jetstream::new(client.clone());

    // As an older table had it: one hour.
    let mut older = streams::table()
        .into_iter()
        .find(|s| s.name == "MARKETS")
        .unwrap();
    older.max_age = Duration::from_secs(3_600);
    jetstream.create_stream(older.config()).await.unwrap();

    let capture = connect(&broker, "datawatch-hyperliquid").await;
    for n in 0..5 {
        capture
            .publish("markets.hyperliquid.BTC.quotes", format!("{n}").into())
            .await
            .unwrap();
    }
    capture.flush().await.unwrap();
    assert_eq!(messages_settle(&client, "MARKETS", 5).await, 5);

    let outcomes = streams::apply(&client).await;
    assert!(
        outcomes.iter().any(|o| matches!(
            o,
            Applied::Held {
                name: "MARKETS",
                messages: 5,
                ..
            }
        )),
        "{outcomes:?}"
    );
    let mut markets = jetstream.get_stream("MARKETS").await.unwrap();
    assert_eq!(
        markets.info().await.unwrap().config.max_age,
        streams::MARKET_WINDOW,
        "the window is the table's two hours now"
    );
}
