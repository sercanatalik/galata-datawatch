//! A ledger's grant, held by a real `nats-server` running the generated table
//! (tests/support): it publishes its own venue's accounts, acknowledged, and is
//! refused another venue's (`add-realtime-account-ledger`, design D6).

mod support;

use std::time::Duration;

use galata_broker::streams;
use galata_broker::{BUS_ADMIN, BUS_ADMIN_INBOX, ledger_identity, ledger_inbox};
use support::{Broker, connect_with};

async fn as_ledger(broker: &Broker, venue: &str) -> async_nats::Client {
    connect_with(
        broker,
        &ledger_identity(venue),
        async_nats::ConnectOptions::new().custom_inbox_prefix(ledger_inbox(venue)),
    )
    .await
}

#[tokio::test]
async fn a_ledger_publishes_its_own_venues_accounts_and_is_refused_anothers() {
    let broker = Broker::start();
    let admin = connect_with(
        &broker,
        BUS_ADMIN,
        async_nats::ConnectOptions::new().custom_inbox_prefix(BUS_ADMIN_INBOX),
    )
    .await;
    streams::apply(&admin).await;

    let ledger = as_ledger(&broker, "hyperliquid").await;
    let jetstream = async_nats::jetstream::new(ledger.clone());

    // Its own venue: stored, and the acknowledgement heard on its own inbox.
    let ack = jetstream
        .publish("accounts.hyperliquid.x", "{}".into())
        .await
        .expect("published")
        .await
        .expect("acknowledged by the stream");
    assert_eq!(ack.stream, streams::ACCOUNT_STREAM);

    // Another venue's: the server refuses the publish, so no acknowledgement
    // arrives and nothing is stored.
    let refused = tokio::time::timeout(Duration::from_secs(3), async {
        jetstream
            .publish("accounts.derive-xyz.x", "{}".into())
            .await?
            .await
    })
    .await;
    assert!(
        !matches!(refused, Ok(Ok(_))),
        "another venue's account was acknowledged: {refused:?}"
    );

    // As read by the stream's administrator: one on its own subject, none on
    // the other's.
    let jetstream_admin = async_nats::jetstream::new(admin.clone());
    let mut stream = jetstream_admin
        .get_stream(streams::ACCOUNT_STREAM)
        .await
        .unwrap();
    assert_eq!(
        stream.info().await.unwrap().state.messages,
        1,
        "its own account only"
    );
}

#[tokio::test]
async fn a_ledger_hears_no_account_it_did_not_ask_for() {
    // It subscribes to nothing but its inbox: a subscription to the accounts
    // root delivers nothing, however much is published there.
    let broker = Broker::start();
    let admin = connect_with(
        &broker,
        BUS_ADMIN,
        async_nats::ConnectOptions::new().custom_inbox_prefix(BUS_ADMIN_INBOX),
    )
    .await;
    streams::apply(&admin).await;
    let derive = as_ledger(&broker, "derive-xyz").await;
    let mut listening = derive
        .subscribe("accounts.>")
        .await
        .expect("subscribing is asynchronous; the question is what arrives");
    derive.flush().await.unwrap();

    let hyperliquid = as_ledger(&broker, "hyperliquid").await;
    async_nats::jetstream::new(hyperliquid)
        .publish("accounts.hyperliquid.y", "{}".into())
        .await
        .unwrap()
        .await
        .expect("acknowledged");
    let heard = tokio::time::timeout(
        Duration::from_secs(2),
        futures_util::StreamExt::next(&mut listening),
    )
    .await;
    assert!(
        !matches!(heard, Ok(Some(_))),
        "a ledger heard an account under accounts.>"
    );
}

#[tokio::test]
async fn an_account_message_is_acknowledged_once_by_its_id() {
    use galata_broker::{AccountPublisher, BrokerIdentity, JetStreamPublisher, Subject};
    use galata_wire::{Account, Envelope, Event, Kind, StateSet, Venue};

    let broker = Broker::start();
    let admin = connect_with(
        &broker,
        BUS_ADMIN,
        async_nats::ConnectOptions::new().custom_inbox_prefix(BUS_ADMIN_INBOX),
    )
    .await;
    streams::apply(&admin).await;

    let identity = BrokerIdentity::new(
        ledger_identity("hyperliquid"),
        support::password(&ledger_identity("hyperliquid")),
        "GALATA_BROKER_PASSWORD_LEDGER_HYPERLIQUID",
    );
    let publisher =
        JetStreamPublisher::connect(&broker.url(), &identity, &ledger_inbox("hyperliquid"))
            .await
            .expect("the ledger connects as itself");
    let venue = Venue::new("hyperliquid").unwrap();
    let main = Account::new("main").unwrap();
    let set = Envelope::for_account(
        venue.clone(),
        main.clone(),
        Some(5),
        5,
        Event::StateSet(StateSet::of(Kind::Positions, Vec::new()).unwrap()),
    );
    let subject = Subject::account(&venue, &main, Kind::Positions).unwrap();
    let first = publisher
        .publish_acked(&subject, "hyperliquid:main:positions:set:5", &set)
        .await
        .expect("acknowledged");
    assert!(!first.duplicate);
    // The same id again, as a resend after a lost acknowledgement is: the
    // stream keeps the one it has.
    let again = publisher
        .publish_acked(&subject, "hyperliquid:main:positions:set:5", &set)
        .await
        .expect("acknowledged");
    assert!(again.duplicate);
    assert_eq!(again.stream_seq, first.stream_seq);
}
