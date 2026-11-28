//! `bus-streams`, scenario by scenario, against a real `nats-server`
//! (tests/support). Retentions are shortened only where a test would
//! otherwise wait for them.

mod support;

use std::time::Duration;

use async_nats::jetstream::{self, consumer::DeliverPolicy, consumer::pull::OrderedConfig};
use futures_util::StreamExt;
use galata_broker::streams::{self, Applied};
use galata_broker::{BUS_ADMIN, BUS_ADMIN_INBOX, GALATA_MARKETS, GALATA_MARKETS_INBOX};
use galata_wire::{Envelope, Event, Kind, Quote, Ticker, Venue};
use support::{Broker, TEST_CONSUMER, TEST_LEDGER, connect, connect_with};

const BTC_QUOTES: &str = "markets.hyperliquid.BTC.quotes";

async fn admin(broker: &Broker) -> async_nats::Client {
    connect_with(
        broker,
        BUS_ADMIN,
        async_nats::ConnectOptions::new().custom_inbox_prefix(BUS_ADMIN_INBOX),
    )
    .await
}

/// A broker with the table applied.
async fn applied() -> (Broker, async_nats::Client) {
    let broker = Broker::start();
    let client = admin(&broker).await;
    let outcomes = streams::apply(&client).await;
    assert!(
        outcomes.iter().all(|o| matches!(o, Applied::Held { .. })),
        "{outcomes:?}"
    );
    (broker, client)
}

/// Fast-failing requests, so a refused one is an error in a second.
async fn quick(broker: &Broker, identity: &str) -> async_nats::Client {
    connect_with(
        broker,
        identity,
        async_nats::ConnectOptions::new().request_timeout(Some(Duration::from_secs(1))),
    )
    .await
}

/// What a stream holds once the broker has stored what was sent.
async fn settled(admin: &async_nats::Client, stream: &str, want: u64) -> u64 {
    let mut seen = 0;
    for _ in 0..60 {
        seen = streams::held(admin, stream)
            .await
            .map_or(0, |(messages, _)| messages);
        if seen == want {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    seen
}

async fn publish_all(
    client: &async_nats::Client,
    subject: &str,
    bodies: impl IntoIterator<Item = String>,
) {
    for body in bodies {
        client
            .publish(subject.to_string(), body.into())
            .await
            .unwrap();
    }
    client.flush().await.unwrap();
}

/// The next `n` payloads from an ordered replay, within five seconds.
async fn next_payloads(messages: &mut jetstream::consumer::pull::Ordered, n: usize) -> Vec<String> {
    let mut out = Vec::new();
    while out.len() < n {
        let message = tokio::time::timeout(Duration::from_secs(5), messages.next())
            .await
            .expect("the replay delivers within 5 s")
            .expect("a message")
            .expect("not an error");
        out.push(String::from_utf8(message.payload.to_vec()).unwrap());
    }
    out
}

#[tokio::test]
async fn a_consumer_replays_from_a_moment_and_then_follows_live() {
    let (broker, _admin) = applied().await;
    let capture = connect(&broker, "datawatch-hyperliquid").await;
    publish_all(&capture, BTC_QUOTES, (0..5).map(|n| n.to_string())).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    // Where the tape ends, say.
    let from = time::OffsetDateTime::now_utc();
    tokio::time::sleep(Duration::from_millis(300)).await;
    publish_all(&capture, BTC_QUOTES, (5..10).map(|n| n.to_string())).await;

    let consumer = connect(&broker, TEST_CONSUMER).await;
    let stream = jetstream::new(consumer)
        .get_stream("MARKETS")
        .await
        .unwrap();
    let mut messages = stream
        .create_consumer(OrderedConfig {
            filter_subject: "markets.>".into(),
            deliver_policy: DeliverPolicy::ByStartTime { start_time: from },
            ..Default::default()
        })
        .await
        .unwrap()
        .messages()
        .await
        .unwrap();
    assert_eq!(
        next_payloads(&mut messages, 5).await,
        ["5", "6", "7", "8", "9"],
        "everything from that moment, in order"
    );

    publish_all(&capture, BTC_QUOTES, ["10".to_string()]).await;
    assert_eq!(
        next_payloads(&mut messages, 1).await,
        ["10"],
        "and then live"
    );
}

#[tokio::test]
async fn a_message_older_than_the_window_is_gone() {
    let broker = Broker::start();
    let admin = admin(&broker).await;
    // The table's MARKETS, with a window of one second in place of two hours.
    let mut short = streams::table()
        .into_iter()
        .find(|s| s.name == "MARKETS")
        .unwrap();
    short.max_age = Duration::from_secs(1);
    jetstream::new(admin.clone())
        .create_stream(short.config())
        .await
        .unwrap();

    let capture = connect(&broker, "datawatch-hyperliquid").await;
    publish_all(&capture, BTC_QUOTES, ["old".to_string()]).await;
    assert_eq!(settled(&admin, "MARKETS", 1).await, 1);
    tokio::time::sleep(Duration::from_millis(2_500)).await;
    assert_eq!(
        settled(&admin, "MARKETS", 0).await,
        0,
        "aged out of the window"
    );
}

#[tokio::test]
async fn a_publisher_cannot_delete_the_window_nor_the_reader_create_a_stream() {
    let (broker, admin) = applied().await;

    let capture = quick(&broker, "datawatch-hyperliquid").await;
    assert!(
        jetstream::new(capture)
            .delete_stream("MARKETS")
            .await
            .is_err()
    );
    assert!(
        streams::held(&admin, "MARKETS").await.is_some(),
        "the window is unchanged"
    );

    let reader = quick(&broker, "reader").await;
    let mut rogue = streams::table()
        .into_iter()
        .find(|s| s.name == "STATUS")
        .unwrap();
    rogue.name = "ROGUE";
    rogue.subjects = vec!["rogue.>".into()];
    assert!(
        jetstream::new(reader)
            .create_stream(rogue.config())
            .await
            .is_err()
    );
    assert!(streams::held(&admin, "ROGUE").await.is_none());
}

#[tokio::test]
async fn the_status_stream_gives_each_venue_once_and_its_latest() {
    let (broker, admin) = applied().await;
    let capture = connect(&broker, "datawatch-hyperliquid").await;
    publish_all(
        &capture,
        "status.hyperliquid",
        (1..=10).map(|n| n.to_string()),
    )
    .await;
    assert_eq!(settled(&admin, "STATUS", 1).await, 1);

    let consumer = connect(&broker, TEST_CONSUMER).await;
    let stream = jetstream::new(consumer).get_stream("STATUS").await.unwrap();
    let mut messages = stream
        .create_consumer(OrderedConfig {
            filter_subject: "status.>".into(),
            deliver_policy: DeliverPolicy::All,
            ..Default::default()
        })
        .await
        .unwrap()
        .messages()
        .await
        .unwrap();
    assert_eq!(next_payloads(&mut messages, 1).await, ["10"]);
    let more = tokio::time::timeout(Duration::from_millis(500), messages.next()).await;
    assert!(more.is_err(), "one message for the venue, not ten");
}

#[tokio::test]
async fn an_account_message_is_stored_before_it_is_acknowledged() {
    let broker = Broker::start();
    let admin = admin(&broker).await;
    let ledger = jetstream::new(quick(&broker, TEST_LEDGER).await);

    // No stream yet: no acknowledgement, and the publisher knows.
    let unstored = ledger
        .publish("accounts.hyperliquid.main.margin", "{}".into())
        .await;
    let acknowledged = match unstored {
        Ok(ack) => ack.await.is_ok(),
        Err(_) => false,
    };
    assert!(
        !acknowledged,
        "nothing stores it, so nothing acknowledges it"
    );

    streams::apply(&admin).await;
    ledger
        .publish("accounts.hyperliquid.main.margin", "{}".into())
        .await
        .unwrap()
        .await
        .expect("acknowledged");
    assert_eq!(
        streams::held(&admin, "ACCOUNTS").await.map(|(m, _)| m),
        Some(1),
        "stored by the time it was acknowledged"
    );
}

#[tokio::test]
async fn the_window_survives_a_restart_of_the_broker() {
    let dir = tempfile::tempdir().unwrap();
    let broker = Broker::start_in(dir.path());
    let admin_client = admin(&broker).await;
    streams::apply(&admin_client).await;
    let capture = connect(&broker, "datawatch-hyperliquid").await;
    publish_all(&capture, BTC_QUOTES, (0..3).map(|n| n.to_string())).await;
    assert_eq!(settled(&admin_client, "MARKETS", 3).await, 3);
    drop((capture, admin_client));
    broker.stop();

    let again = Broker::start_in(dir.path());
    let admin_client = admin(&again).await;
    assert_eq!(
        streams::held(&admin_client, "MARKETS")
            .await
            .map(|(m, _)| m),
        Some(3)
    );
}

#[tokio::test]
async fn joining_the_tape_to_the_window_counts_nothing_twice() {
    // The tape holds envelopes 1 to 4; the window holds 1 to 6, and the
    // consumer replays it from envelope 3's time. What it already has is told
    // by seq, address and kind together: one archived payload can yield
    // several envelopes, so seq alone is not the key.
    let (broker, _admin) = applied().await;
    let capture = connect(&broker, "datawatch-hyperliquid").await;
    let venue = Venue::new("hyperliquid").unwrap();
    let envelopes: Vec<Envelope> = (1..=6u64)
        .map(|seq| {
            let mut envelope = Envelope::new(
                venue.clone(),
                Ticker::new("BTC").unwrap(),
                Some(seq as i64),
                seq as i64,
                Event::Quote(Quote {
                    bid_px: None,
                    ask_px: None,
                    bid_sz: None,
                    ask_sz: None,
                    bid_spread: None,
                    ask_spread: None,
                }),
            );
            envelope.seq = seq;
            envelope
        })
        .collect();
    let mut from = None;
    for envelope in &envelopes {
        if envelope.seq == 3 {
            tokio::time::sleep(Duration::from_millis(300)).await;
            from = Some(time::OffsetDateTime::now_utc());
            tokio::time::sleep(Duration::from_millis(300)).await;
        }
        capture
            .publish(BTC_QUOTES, galata_broker::encode(envelope).unwrap().into())
            .await
            .unwrap();
    }
    capture.flush().await.unwrap();

    let mut have: Vec<Envelope> = envelopes[..4].to_vec();
    let consumer = connect(&broker, TEST_CONSUMER).await;
    let stream = jetstream::new(consumer)
        .get_stream("MARKETS")
        .await
        .unwrap();
    let mut messages = stream
        .create_consumer(OrderedConfig {
            filter_subject: "markets.>".into(),
            deliver_policy: DeliverPolicy::ByStartTime {
                start_time: from.unwrap(),
            },
            ..Default::default()
        })
        .await
        .unwrap()
        .messages()
        .await
        .unwrap();
    for _ in 0..4 {
        let message = tokio::time::timeout(Duration::from_secs(5), messages.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let envelope = galata_broker::decode(&message.payload).unwrap();
        let seen = have.iter().any(|h| {
            h.seq == envelope.seq && h.address == envelope.address && h.kind() == envelope.kind()
        });
        if !seen {
            have.push(envelope);
        }
    }
    let seqs: Vec<u64> = have.iter().map(|e| e.seq).collect();
    assert_eq!(seqs, [1, 2, 3, 4, 5, 6], "every envelope once, in order");
    assert!(have.iter().all(|e| e.kind() == Kind::Quotes));
}

// ---- galata-markets' identity (`add-galata-markets`) -----------------------

/// Connected as galata-markets, on the one inbox prefix it may hear on, with
/// requests that fail in a second.
async fn galata_markets(broker: &Broker) -> async_nats::Client {
    connect_with(
        broker,
        GALATA_MARKETS,
        async_nats::ConnectOptions::new()
            .custom_inbox_prefix(GALATA_MARKETS_INBOX)
            .request_timeout(Some(Duration::from_secs(1))),
    )
    .await
}

#[tokio::test]
async fn galata_markets_replays_the_window_and_reads_each_venues_latest_status() {
    let (broker, admin) = applied().await;
    let capture = connect(&broker, "datawatch-hyperliquid").await;
    publish_all(&capture, BTC_QUOTES, (0..3).map(|n| n.to_string())).await;
    publish_all(&capture, "status.hyperliquid", ["up".to_string()]).await;
    assert_eq!(settled(&admin, "MARKETS", 3).await, 3);
    assert_eq!(settled(&admin, "STATUS", 1).await, 1);

    let markets = jetstream::new(galata_markets(&broker).await);
    let mut window = markets.get_stream("MARKETS").await.unwrap();
    assert_eq!(window.info().await.unwrap().state.messages, 3);
    let mut replay = window
        .create_consumer(OrderedConfig {
            filter_subject: "markets.hyperliquid.BTC.>".into(),
            deliver_policy: DeliverPolicy::All,
            ..Default::default()
        })
        .await
        .unwrap()
        .messages()
        .await
        .unwrap();
    assert_eq!(next_payloads(&mut replay, 3).await, ["0", "1", "2"]);

    let mut latest = markets
        .get_stream("STATUS")
        .await
        .unwrap()
        .create_consumer(OrderedConfig {
            filter_subject: "status.>".into(),
            deliver_policy: DeliverPolicy::All,
            ..Default::default()
        })
        .await
        .unwrap()
        .messages()
        .await
        .unwrap();
    assert_eq!(next_payloads(&mut latest, 1).await, ["up"]);
}

#[tokio::test]
async fn galata_markets_publishes_nothing_and_changes_no_stream() {
    let (broker, admin) = applied().await;
    let client = galata_markets(&broker).await;

    // A publish it is not granted: the server drops it, so the window holds
    // nothing it sent.
    let _ = client
        .publish(BTC_QUOTES.to_string(), "forged".into())
        .await;
    let _ = client.flush().await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(settled(&admin, "MARKETS", 0).await, 0);

    let markets = jetstream::new(client);
    assert!(markets.delete_stream("MARKETS").await.is_err());
    assert!(
        streams::held(&admin, "MARKETS").await.is_some(),
        "the window is unchanged"
    );
    assert!(markets.delete_stream("ACCOUNTS").await.is_err());
    assert!(
        streams::held(&admin, "ACCOUNTS").await.is_some(),
        "the account stream is unchanged"
    );
}

/// galata-markets reads the account stream it serves (`add-markets-accounts`,
/// design D6): a replay of one account's subject, and the last message on a
/// subject, which is an account's newest set. And it publishes nothing under
/// `accounts.`, which is each ledger's own.
#[tokio::test]
async fn galata_markets_reads_the_account_stream_and_publishes_nothing_under_it() {
    let (broker, admin) = applied().await;
    let ledger = jetstream::new(quick(&broker, TEST_LEDGER).await);
    for (subject, body) in [
        ("accounts.hyperliquid.main.positions", "first"),
        ("accounts.hyperliquid.main.fills", "fill"),
        ("accounts.hyperliquid.main.positions", "second"),
    ] {
        ledger
            .publish(subject, body.into())
            .await
            .unwrap()
            .await
            .expect("acknowledged");
    }
    assert_eq!(settled(&admin, "ACCOUNTS", 3).await, 3);

    let client = galata_markets(&broker).await;
    let markets = jetstream::new(client.clone());
    let stream = markets.get_stream("ACCOUNTS").await.unwrap();
    let mut replay = stream
        .create_consumer(OrderedConfig {
            filter_subject: "accounts.hyperliquid.main.positions".into(),
            deliver_policy: DeliverPolicy::All,
            ..Default::default()
        })
        .await
        .unwrap()
        .messages()
        .await
        .unwrap();
    assert_eq!(next_payloads(&mut replay, 2).await, ["first", "second"]);
    let newest = stream
        .get_last_raw_message_by_subject("accounts.hyperliquid.main.positions")
        .await
        .expect("the newest set, by its subject");
    assert_eq!(&newest.payload[..], b"second");

    // A publish under `accounts.` it is not granted: the server drops it, so
    // the stream holds nothing it sent.
    let _ = client
        .publish("accounts.hyperliquid.main.positions", "forged".into())
        .await;
    let _ = client.flush().await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(settled(&admin, "ACCOUNTS", 3).await, 3);
    let acknowledged = match markets
        .publish("accounts.hyperliquid.main.positions", "forged".into())
        .await
    {
        Ok(ack) => ack.await.is_ok(),
        Err(_) => false,
    };
    assert!(
        !acknowledged,
        "nor through JetStream: a publish it may not make is never acknowledged"
    );
    assert_eq!(settled(&admin, "ACCOUNTS", 3).await, 3);
}
