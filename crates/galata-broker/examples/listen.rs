//! Subscribe, and print what arrives. **Links no parquet.**
//!
//! ```text
//! cargo run -p galata-broker --example listen -- <url> <user> <subject> [count]
//! ```
//! The password comes from `GALATA_PW`. Nothing connects anonymously.
use galata_broker::{BrokerIdentity, NatsSubscriber};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let url = args
        .next()
        .ok_or("usage: listen <url> <user> <subject> [count]")?;
    let user = args.next().ok_or("need a user")?;
    let subject = args.next().ok_or("need a subject")?;
    let want: usize = args.next().unwrap_or_else(|| "5".into()).parse()?;
    let password = std::env::var("GALATA_PW")?;

    let identity = BrokerIdentity::new(user, password, "GALATA_PW");
    let mut subscriber = NatsSubscriber::connect(&url, &identity, &subject).await?;
    println!("listening on {subject}");

    for _ in 0..want {
        match subscriber.next().await {
            None => break,
            // A message that will not decode is an error, never a skip.
            Some(Err(error)) => return Err(Box::from(error)),
            Some(Ok(envelope)) => println!(
                "seq={:<7} {:>12} {:<7} at={:?} {}",
                envelope.seq,
                envelope.venue().map(|v| v.as_str()).unwrap_or("-"),
                envelope.ticker().map(|t| t.as_str()).unwrap_or("-"),
                envelope.at_micros,
                envelope.kind(),
            ),
        }
    }
    Ok(())
}
