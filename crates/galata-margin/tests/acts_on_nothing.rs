//! **galata-margin names no request that acts** (task 7.4; the spec's
//! "Advice acts on nothing"): nothing that places, amends or cancels an
//! order, or that transfers or withdraws funds, on either venue. Its key can
//! trade (design D11, revised 2026-11-27), so this holds the code, not the
//! key.
//!
//! Each source is read up to its first `#[cfg(test)]`: a test may name what it
//! asserts about.

/// Derive's methods that act, by the words their names carry
/// (`docs.derive.xyz/rate-limits`: the `matching` and `proven` classes, and
/// the RFQ and vault writes), and Hyperliquid's one door for every action,
/// `/exchange`.
const ACTS: [&str; 16] = [
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
];

/// The requests the crate is allowed to name, and every one it does.
const ASKS: [&str; 6] = [
    "private/get_margin",
    "private/session_keys",
    "private/get_subaccounts",
    "public/get_ticker",
    "metaAndAssetCtxs",
    "l2Book",
];

fn shipped() -> Vec<(String, String)> {
    let mut out = Vec::new();
    let mut stack = vec![std::path::PathBuf::from(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/src"
    ))];
    while let Some(dir) = stack.pop() {
        for entry in std::fs::read_dir(&dir).unwrap().flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
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
    }
    out
}

#[test]
fn no_request_that_acts_is_named() {
    let mut found = Vec::new();
    for (path, text) in shipped() {
        for word in ACTS {
            if text.contains(word) {
                found.push(format!("{path}: {word}"));
            }
        }
    }
    assert!(
        found.is_empty(),
        "galata-margin names a request that acts: {found:?}"
    );
}

#[test]
fn the_requests_it_names_are_reads_and_the_simulation() {
    let all: String = shipped().into_iter().map(|(_, t)| t).collect();
    // Every Derive method named, by its prefix.
    let mut named: Vec<String> = Vec::new();
    for prefix in ["private/", "public/"] {
        let mut rest = all.as_str();
        while let Some(at) = rest.find(prefix) {
            let method: String = rest[at..]
                .chars()
                .take_while(|c| c.is_ascii_alphanumeric() || *c == '_' || *c == '/')
                .collect();
            // A doc path such as `adapters/derive_xyz/private.rs` is not a method.
            if !method.ends_with('/') && method.len() > prefix.len() {
                named.push(method);
            }
            rest = &rest[at + prefix.len()..];
        }
    }
    named.sort();
    named.dedup();
    for method in &named {
        assert!(
            ASKS.contains(&method.as_str()),
            "`{method}` is named and is not a read the crate is allowed"
        );
    }
    for ask in [
        "private/get_margin",
        "private/session_keys",
        "private/get_subaccounts",
        "public/get_ticker",
    ] {
        assert!(
            named.iter().any(|m| m == ask),
            "{ask} should be named: {named:?}"
        );
    }
}
