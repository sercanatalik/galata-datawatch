//! **What would `l2Book` have cost, against the `bbo` that replaced it?**
//!
//! ```text
//! cargo run --release --example book-volume -- <seconds> <coin>...
//! ```
//!
//! The roadmap chose `bbo` over `l2Book` for two stated reasons — it carries
//! sizes, and it is emitted only when the top of book changes on a block,
//! where the public `l2Book` is a throttled snapshot on a timer whatever
//! happened. It then recorded the volume as **unmeasured and possibly larger**.
//!
//! This subscribes to both, for the same coins, at the same time, and counts
//! bytes. Same socket, same window, so a quiet market cannot flatter one.

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::tungstenite::Message;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let seconds: u64 = args.next().unwrap_or_else(|| "60".into()).parse()?;
    let coins: Vec<String> = args.collect();
    let coins = if coins.is_empty() {
        vec!["BTC".into(), "ETH".into(), "HYPE".into()]
    } else {
        coins
    };

    let (mut socket, _) = tokio_tungstenite::connect_async("wss://api.hyperliquid.xyz/ws").await?;
    for coin in &coins {
        for channel in ["bbo", "l2Book"] {
            let subscribe = serde_json::json!({
                "method": "subscribe",
                "subscription": { "type": channel, "coin": coin }
            });
            socket
                .send(Message::Text(subscribe.to_string().into()))
                .await?;
        }
    }

    // `(messages, bytes)` per channel.
    let mut tally: BTreeMap<String, (u64, u64)> = BTreeMap::new();
    let started = Instant::now();
    let deadline = Duration::from_secs(seconds);

    while started.elapsed() < deadline {
        let remaining = deadline.saturating_sub(started.elapsed());
        let Ok(Some(Ok(message))) = tokio::time::timeout(remaining, socket.next()).await else {
            break;
        };
        let Message::Text(text) = message else {
            continue;
        };
        let Ok(value) = serde_json::from_str::<serde_json::Value>(&text) else {
            continue;
        };
        let channel = value
            .get("channel")
            .and_then(|c| c.as_str())
            .unwrap_or("other")
            .to_string();
        let entry = tally.entry(channel).or_default();
        entry.0 += 1;
        // **The frame as it arrived**, which is what capture archives — not the
        // parsed size, which would flatter whichever channel is more verbose.
        entry.1 += text.len() as u64;
    }

    let elapsed = started.elapsed().as_secs_f64();
    println!("{} coins, {elapsed:.1}s, one socket", coins.len());
    println!();
    println!("  channel      messages    bytes   msg/s    KiB/s   mean B");
    println!("  ────────────────────────────────────────────────────────");
    for (channel, (messages, bytes)) in &tally {
        println!(
            "  {channel:<12} {messages:>8} {bytes:>8} {:>7.2} {:>8.2} {:>8.0}",
            *messages as f64 / elapsed,
            *bytes as f64 / elapsed / 1024.0,
            *bytes as f64 / (*messages).max(1) as f64
        );
    }
    if let (Some(bbo), Some(book)) = (tally.get("bbo"), tally.get("l2Book")) {
        println!();
        println!(
            "  l2Book is {:.1}x the bytes of bbo, and {:.1}x the messages",
            book.1 as f64 / bbo.1.max(1) as f64,
            book.0 as f64 / bbo.0.max(1) as f64
        );
    }
    Ok(())
}
