//! Refusing, at boot, a configuration the venue cannot serve.
//!
//! # Why this exists
//!
//! **Measured 2026-09-20.** A configuration naming a coin the venue does not
//! list is answered with a **hang-up, not a refusal** — and every other
//! subscription on that socket dies with it. One wrong ticker produced
//! seventeen resets in eighteen seconds, six instruments with no data, and
//! nothing in the log but `Connection reset without closing handshake`.
//!
//! It looks exactly like a network problem and is not one. The venue will say
//! which coins exist, in one request per dex, and a refusal at boot costs a
//! restart where the alternative costs every instrument's coverage and gives no
//! reason.
//!
//! Asking again later would not help: the failure is not transient, the
//! subscription will never succeed, and every attempt costs everything else.

/// Why a declared instrument cannot be captured.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum UniverseError {
    /// The venue does not list this coin.
    #[error(
        "{venue}: {symbol:?} is not listed{on_dex}. The venue answers an unlisted coin by CLOSING \
         THE CONNECTION rather than refusing the subscription, which would take every other \
         instrument down with it. Did you mean: {candidates}?"
    )]
    NotListed {
        /// Which venue.
        venue: String,
        /// The venue symbol that was asked for.
        symbol: String,
        /// Rendered as ` on dex "xyz"`, or empty for the main dex.
        on_dex: String,
        /// The closest things the venue does list.
        candidates: String,
    },
    /// The universe could not be read at all.
    #[error("{venue}: the listed coins could not be read, so nothing can be checked: {detail}")]
    Unreadable {
        /// Which venue.
        venue: String,
        /// Why.
        detail: String,
    },
}

/// Check declared venue symbols against what a venue lists.
///
/// `listed` is one dex's universe, in the venue's own spelling.
pub fn check(
    venue: &str,
    dex: &str,
    declared: &[String],
    listed: &[String],
) -> Result<(), UniverseError> {
    for symbol in declared {
        if listed.iter().any(|l| l == symbol) {
            continue;
        }
        return Err(UniverseError::NotListed {
            venue: venue.to_string(),
            symbol: symbol.clone(),
            on_dex: if dex.is_empty() {
                String::new()
            } else {
                format!(" on dex {dex:?}")
            },
            candidates: candidates(symbol, listed),
        });
    }
    Ok(())
}

/// The closest listed names to something that is not listed.
///
/// **A refusal saying only "unknown" leaves an operator exactly where the
/// hang-up did.** `WTIOIL` is not listed and `CL` is; a message that says so is
/// the difference between a fixed configuration and an afternoon.
fn candidates(symbol: &str, listed: &[String]) -> String {
    let bare = symbol.rsplit(':').next().unwrap_or(symbol).to_uppercase();
    let mut near: Vec<&String> = listed
        .iter()
        .filter(|l| {
            let other = l.rsplit(':').next().unwrap_or(l).to_uppercase();
            // Anything sharing a leading run of three, either way round. Crude,
            // and it finds BRENTOIL for WTIOIL and SILVER for SILV.
            shares_run(&bare, &other, 3) || other.contains(&bare) || bare.contains(&other)
        })
        .collect();
    near.sort();
    near.truncate(6);
    if near.is_empty() {
        format!("nothing similar among {} listed coins", listed.len())
    } else {
        near.iter()
            .map(|s| s.as_str())
            .collect::<Vec<_>>()
            .join(", ")
    }
}

/// Whether two names share a run of `n` characters anywhere.
fn shares_run(a: &str, b: &str, n: usize) -> bool {
    if a.len() < n || b.len() < n {
        return false;
    }
    let b_bytes = b.as_bytes();
    a.as_bytes()
        .windows(n)
        .any(|w| b_bytes.windows(n).any(|x| x == w))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn xyz() -> Vec<String> {
        [
            "xyz:XYZ100",
            "xyz:GOLD",
            "xyz:CL",
            "xyz:BRENTOIL",
            "xyz:SILVER",
            "xyz:TSLA",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect()
    }

    #[test]
    fn a_listed_coin_on_a_builder_dex_passes() {
        let declared = vec!["xyz:CL".to_string(), "xyz:GOLD".to_string()];
        assert!(check("hyperliquid", "xyz", &declared, &xyz()).is_ok());
    }

    #[test]
    fn an_unlisted_coin_is_refused() {
        // The exact case that cost six instruments their coverage.
        let declared = vec!["xyz:WTIOIL".to_string()];
        let err = check("hyperliquid", "xyz", &declared, &xyz()).unwrap_err();
        let message = err.to_string();
        assert!(message.contains("WTIOIL"), "{message}");
        assert!(message.contains("dex \"xyz\""), "{message}");
    }

    #[test]
    fn the_refusal_names_what_is_listed() {
        // A refusal saying only "unknown" leaves an operator where the hang-up
        // did. WTIOIL is not listed; BRENTOIL and CL are.
        let declared = vec!["xyz:WTIOIL".to_string()];
        let message = check("hyperliquid", "xyz", &declared, &xyz())
            .unwrap_err()
            .to_string();
        assert!(message.contains("BRENTOIL"), "{message}");
    }

    #[test]
    fn the_refusal_says_the_venue_hangs_up() {
        // Because the symptom an operator has already seen is a connection
        // reset, and nothing else connects the two.
        let declared = vec!["xyz:NOPE".to_string()];
        let message = check("hyperliquid", "xyz", &declared, &xyz())
            .unwrap_err()
            .to_string();
        assert!(message.contains("CLOSING THE CONNECTION"), "{message}");
    }

    #[test]
    fn nothing_similar_still_says_how_many_were_checked() {
        let declared = vec!["xyz:QQQQQQQ".to_string()];
        let message = check("hyperliquid", "xyz", &declared, &xyz())
            .unwrap_err()
            .to_string();
        assert!(message.contains("6 listed coins"), "{message}");
    }

    #[test]
    fn the_main_dex_names_no_dex() {
        let listed = vec!["BTC".to_string(), "ETH".to_string()];
        let message = check("hyperliquid", "", &["DOGECOIN".to_string()], &listed)
            .unwrap_err()
            .to_string();
        assert!(!message.contains("dex"), "{message}");
    }

    #[test]
    fn an_empty_declaration_passes() {
        assert!(check("hyperliquid", "xyz", &[], &xyz()).is_ok());
    }
}
