//! **The ledger names no request that acts** (`add-realtime-account-ledger`,
//! task 5.3), over REST **or its websockets**: nothing that places, amends or
//! cancels an order, or that transfers or withdraws funds, on either venue.
//! Derive's session key can trade (`add-derive-account-ledger`, design D4),
//! and the stream logs in with it, so this holds the code, not the key.
//!
//! Each source is read up to its first `#[cfg(test)]`: a test may name what it
//! asserts about. The guard is proven able to fail on a planted request.
#![cfg(all(feature = "derive-xyz", feature = "capture", feature = "ledger"))]

use galata_datawatch::adapters::derive_xyz::accounts::{
    STREAM_KEEPALIVE, STREAM_LOGIN, STREAM_SUBSCRIBE,
};

/// Derive's methods that act, by the words their names carry, as
/// galata-margin's guard lists them; and Hyperliquid's doors for every
/// action: `/exchange` over REST, and a websocket `post` of an action.
const ACTS: [&str; 19] = [
    "private/order",
    "private/replace",
    "private/cancel",
    "private/transfer",
    "private/withdraw",
    "private/liquidate",
    "set_session_key",
    "edit_session_key",
    "send_rfq",
    "send_quote",
    "execute_quote",
    "block_proposal",
    "vault_shares",
    "vault_deposit",
    "private/deposit",
    "/exchange",
    "\"method\": \"post\"",
    "\"method\":\"post\"",
    "\"type\": \"action\"",
];

/// The websocket methods the ledger may name: a login, a subscription and a
/// keepalive, each reading and none acting.
const STREAM_ASKS: [&str; 3] = ["public/login", "subscribe", "public/get_time"];

/// The sources the ledger runs: its loop, its publication and both venues'
/// account code, REST and stream alike.
fn shipped() -> Vec<(String, String)> {
    let root = concat!(env!("CARGO_MANIFEST_DIR"), "/src");
    let mut out = Vec::new();
    let mut stack: Vec<std::path::PathBuf> = ["ledger", "adapters/derive_xyz"]
        .iter()
        .map(|dir| std::path::Path::new(root).join(dir))
        .collect();
    for file in [
        "adapters/hyperliquid/accounts.rs",
        "adapters/hyperliquid/ledger.rs",
        "adapters/hyperliquid/events.rs",
        "adapters/hyperliquid/client.rs",
    ] {
        stack.push(std::path::Path::new(root).join(file));
    }
    while let Some(path) = stack.pop() {
        if path.is_dir() {
            for entry in std::fs::read_dir(&path).unwrap().flatten() {
                stack.push(entry.path());
            }
        } else if path.extension().is_some_and(|e| e == "rs") {
            let text = std::fs::read_to_string(&path).unwrap();
            let shipped = text
                .split("#[cfg(test)]")
                .next()
                .unwrap_or_default()
                .to_string();
            out.push((path.display().to_string(), shipped));
        }
    }
    out
}

/// Every acting request a set of sources names, by file.
fn acting(sources: &[(String, String)]) -> Vec<String> {
    let mut found = Vec::new();
    for (path, text) in sources {
        for word in ACTS {
            if text.contains(word) {
                found.push(format!("{path}: {word}"));
            }
        }
    }
    found
}

#[test]
fn no_request_that_acts_is_named_over_rest_or_a_websocket() {
    let sources = shipped();
    assert!(sources.len() > 10, "the ledger's sources were not found");
    let found = acting(&sources);
    assert!(
        found.is_empty(),
        "the ledger names a request that acts: {found:?}"
    );
}

#[test]
fn the_guard_fails_on_a_planted_order_and_passes_without_it() {
    let mut sources = shipped();
    assert!(acting(&sources).is_empty());
    sources.push((
        "planted.rs".into(),
        r#"self.client.history("private/order", &key, wallet, now, window)"#.into(),
    ));
    assert_eq!(acting(&sources), ["planted.rs: private/order"]);
}

#[test]
fn the_stream_names_only_a_login_a_subscription_and_a_keepalive() {
    for method in [STREAM_LOGIN, STREAM_SUBSCRIBE, STREAM_KEEPALIVE] {
        assert!(STREAM_ASKS.contains(&method), "{method}");
        assert!(!ACTS.iter().any(|a| method.contains(a)), "{method}");
    }
}
