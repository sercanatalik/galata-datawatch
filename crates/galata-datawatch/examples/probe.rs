//! A throwaway probe: subscribe to one thing and print exactly what the venue
//! says back, including a close frame's reason.
//!
//! Exists because the shipped loop maps a close to `Frame::Closed` and drops
//! the reason with it — which is fine for capture and useless for diagnosis.

use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::tungstenite::Message;

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let sub = args.first().cloned().unwrap_or_default();
    println!("--> {sub}");

    let (mut socket, _) = tokio_tungstenite::connect_async("wss://api.hyperliquid.xyz/ws")
        .await
        .expect("connect");
    socket.send(Message::Text(sub.into())).await.expect("send");

    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(6);
    while tokio::time::Instant::now() < deadline {
        match tokio::time::timeout(std::time::Duration::from_secs(2), socket.next()).await {
            Ok(Some(Ok(Message::Text(t)))) => {
                let s = t.to_string();
                println!("<-- {}", &s[..s.len().min(220)]);
            }
            Ok(Some(Ok(Message::Close(frame)))) => {
                println!("<-- CLOSE {frame:?}");
                break;
            }
            Ok(Some(Ok(other))) => println!("<-- {other:?}"),
            Ok(Some(Err(e))) => {
                println!("<-- ERROR {e}");
                break;
            }
            Ok(None) => {
                println!("<-- stream ended with no close frame");
                break;
            }
            Err(_) => println!("    (quiet)"),
        }
    }
}
