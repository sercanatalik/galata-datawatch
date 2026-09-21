//! Does the server actually enforce the table?
use galata_broker::{BrokerIdentity, NatsSubscriber};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let url = "nats://127.0.0.1:4223";
    let capture = BrokerIdentity::new(
        "datawatch-hyperliquid",
        std::env::var("HL")?,
        "GALATA_BROKER_PASSWORD_DATAWATCH_HYPERLIQUID",
    );

    // Capture is granted `subscribe: { deny: [">"] }`. Subscribing is
    // asynchronous in NATS, so the call may succeed locally — the question is
    // whether anything ever arrives.
    let mut denied = NatsSubscriber::connect(url, &capture, "markets.>").await?;
    println!("capture subscribed locally (this proves nothing on its own)");

    // **Publish something real**, so "heard nothing" cannot be a false negative
    // from nothing having been sent. Capture IS granted publish on its own
    // venue under both tables, so this half always works.
    let publisher = galata_broker::NatsPublisher::connect(url, &capture).await?;
    let subject = galata_broker::Subject::market(
        &galata_wire::Venue::new("hyperliquid")?,
        &galata_wire::Ticker::new("BTC")?,
        galata_wire::Kind::Quotes,
    );
    galata_broker::Publisher::publish_status(&publisher, &subject, b"{\"probe\":1}").await?;
    publisher.flush().await?;

    let heard =
        tokio::time::timeout(std::time::Duration::from_secs(3), denied.next_addressed()).await;
    match heard {
        Err(_) => println!(
            "VERDICT: a message WAS published and capture heard nothing — the denial holds"
        ),
        Ok(None) => println!("VERDICT: the subscription was closed by the server — denied"),
        Ok(Some((subject, _))) => {
            println!("VERDICT: FAILED — capture received {subject}, which it is denied")
        }
    }
    Ok(())
}
