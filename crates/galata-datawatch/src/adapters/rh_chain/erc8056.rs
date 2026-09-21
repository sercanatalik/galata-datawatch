//! **A raw token amount is not a share count**, and the gap is already
//! non-zero.
//!
//! ERC-8056 lets a corporate action — a split, a reverse split, a reinvested
//! dividend — be applied by updating a **display multiplier** rather than
//! minting, burning or migrating tokens:
//!
//! ```text
//!   underlying shares = raw amount × uiMultiplier ÷ 1e18
//! ```
//!
//! **Measured on the live chain, 2026-09-21:**
//!
//! ```text
//!   NVDA  uiMultiplier = 1000775159164630595 = 1.0007751591646306
//!   1000 raw tokens → 1000.775 underlying shares, understated by 0.0775%
//! ```
//!
//! So every raw amount this system records is correct as *tokens* and wrong as
//! *shares*, by a factor that changes and has already changed once.
//!
//! **Nothing here applies it.** The multiplier is recorded as its own dated
//! fact and the join is the consumer's — because the multiplier *changes*, and
//! rows written before and after a corporate action would otherwise carry
//! amounts computed under different multipliers with nothing saying which.

use galata_wire::Num;

/// `keccak256("uiMultiplier()")[..4]` — the ERC-8056 accessor.
pub const UI_MULTIPLIER_SELECTOR: &str = "a60bf13d";

/// `keccak256("symbol()")[..4]`.
pub const SYMBOL_SELECTOR: &str = "95d89b41";

/// `keccak256("decimals()")[..4]`.
pub const DECIMALS_SELECTOR: &str = "313ce567";

/// The multiplier's own scale: fixed point with eighteen decimals, so `1e18`
/// is exactly 1.0.
pub const MULTIPLIER_SCALE: u32 = 18;

/// What a contract says about itself.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Metadata {
    /// The venue's own symbol, where it states one.
    pub symbol: Option<String>,
    /// How the contract counts.
    pub decimals: Option<u32>,
    /// The display multiplier.
    ///
    /// **`None` means the contract does not implement ERC-8056** — not that it
    /// implements it and is currently unscaled. Most contracts on this chain
    /// revert on the call, and *this is not a stock token* is a different fact
    /// from *this multiplier is one*.
    pub ui_multiplier: Option<Num>,
}

/// Read a `uiMultiplier()` answer.
///
/// `None` for a reverted call, which is how a contract without the extension
/// answers — and which must not become `1.0`.
pub fn multiplier(returned: Option<&str>) -> Option<Num> {
    let hex = returned?.strip_prefix("0x").unwrap_or(returned?);
    if hex.is_empty() {
        return None;
    }
    let raw = u128::from_str_radix(hex.trim_start_matches('0'), 16)
        .ok()
        .or({
            // All zeros is a multiplier of zero, which is a real answer — a token
            // scaled to nothing — and not the same as an absent one.
            if hex.chars().all(|c| c == '0') {
                Some(0)
            } else {
                None
            }
        })?;
    let mut value = Num::from(raw);
    value.set_scale(MULTIPLIER_SCALE).ok()?;
    Some(value)
}

/// Read a `decimals()` answer.
pub fn decimals(returned: Option<&str>) -> Option<u32> {
    let hex = returned?.strip_prefix("0x").unwrap_or(returned?);
    u32::from_str_radix(hex.trim_start_matches('0'), 16)
        .ok()
        .or((!hex.is_empty() && hex.chars().all(|c| c == '0')).then_some(0))
}

/// Read a `symbol()` answer — an ABI-encoded dynamic string.
///
/// Offset, then length, then the bytes. A short or malformed answer is `None`
/// rather than a guess: a symbol invented here would be a ticker nobody chose.
pub fn symbol(returned: Option<&str>) -> Option<String> {
    let hex = returned?.strip_prefix("0x").unwrap_or(returned?);
    let bytes = (0..hex.len() / 2)
        .map(|i| u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16).ok())
        .collect::<Option<Vec<u8>>>()?;
    if bytes.len() < 64 {
        return None;
    }
    let length = u32::from_be_bytes(bytes[60..64].try_into().ok()?) as usize;
    let text = bytes.get(64..64 + length)?;
    Some(
        String::from_utf8_lossy(text)
            .trim_end_matches('\0')
            .to_string(),
    )
}

/// The three answers, exactly as the node returned them.
///
/// **This is what gets archived** — raw hex, before anything decodes it. A
/// decoder corrected next month can be re-run over the record; a decoder's
/// output cannot be un-decoded.
#[derive(Debug, Clone, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
pub struct Answers {
    /// `symbol()`, or `None` where the call reverted.
    pub symbol: Option<String>,
    /// `decimals()`, or `None` where the call reverted.
    pub decimals: Option<String>,
    /// `uiMultiplier()`, or `None` where the call reverted — which is how a
    /// contract without the extension answers.
    #[serde(rename = "uiMultiplier")]
    pub ui_multiplier: Option<String>,
}

impl Answers {
    /// Decode all three.
    pub fn read(&self) -> Metadata {
        Metadata {
            symbol: symbol(self.symbol.as_deref()),
            decimals: decimals(self.decimals.as_deref()),
            ui_multiplier: multiplier(self.ui_multiplier.as_deref()),
        }
    }
}

/// What the chain can honestly say about one instrument.
///
/// Several of [`galata_wire::Instrument`]'s fields describe an order book, and
/// **this venue has none** — so each is filled from what a chain actually
/// knows, and the reasoning is written down here rather than left for a
/// consumer to guess at:
///
/// - `tick_size`, `lot_size`, `min_size` — all three are one base unit,
///   `10^-decimals`, because that is the only increment a token contract has.
///   `contract_type: "token"` is what tells a reader that `tick_size` is a
///   quantity increment here and not a price one.
/// - `quote` — **empty, because a token contract prices nothing.** Trades are
///   reconstructed by pairing a stock-token transfer with a USDG one inside a
///   transaction, which makes USDG a fact about *that trade*, not about this
///   instrument.
/// - `hours` — `continuous`. **Measured 2026-09-21:** the record carries
///   transfers outside the Sun 18:00–Fri 17:00 ET window the roadmap assumed,
///   so the assumed calendar is disproved and the chain settles whenever a
///   block is produced.
/// - `active` — `true` only where the contract answered at all. A contract
///   that answers nothing is not one we can say is listed.
pub fn instrument(
    answers: &Answers,
    ticker: &str,
    declared_decimals: u32,
    observed_at: i64,
) -> galata_wire::Instrument {
    let read = answers.read();
    // The contract's own answer where it gave one; the declared value
    // otherwise. They disagreeing is a finding, not something to average.
    let scale = read.decimals.unwrap_or(declared_decimals);
    let unit = Num::new(1, 0) / Num::new(10i64.pow(scale.min(18)), 0);
    galata_wire::Instrument {
        tick_size: unit,
        lot_size: unit,
        min_size: unit,
        contract_type: "token".to_string(),
        base: read.symbol.unwrap_or_else(|| ticker.to_string()),
        quote: String::new(),
        hours: "continuous".to_string(),
        active: answers.symbol.is_some() || answers.decimals.is_some(),
        // A chain addresses by contract, never by position in a list.
        venue_index: None,
        ui_multiplier: read.ui_multiplier,
        observed_at,
    }
}

/// How many underlying shares a raw amount is.
///
/// **Offered for a consumer, never used at capture.** Provided here so the
/// arithmetic is written once, beside the constant it depends on, rather than
/// re-derived by whoever needs it.
pub fn underlying(raw: Num, multiplier: Option<Num>) -> Num {
    match multiplier {
        Some(m) => raw * m,
        // A token with no multiplier is a token, and its amount is itself.
        None => raw,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    /// **Captured from the live chain, 2026-09-21**: NVDA at
    /// `0xd0601ce157db5bdc3162bbac2a2c8af5320d9eec`, whose `uiMultiplier()`
    /// returned `1000775159164630595`.
    ///
    /// **Derived from that integer, not typed by hand.** The first version of
    /// this constant was hand-converted and decoded to 1.000838949559649155 —
    /// close enough to look right and wrong by 64 parts per million. A fixture
    /// whose value was guessed is a fixture that tests the guess, and this is
    /// the second time in this tree.
    const NVDA_MULTIPLIER: &str =
        "0x0000000000000000000000000000000000000000000000000de377b4760af643";

    #[test]
    fn the_real_nvda_multiplier_is_not_one() {
        // The measurement that justifies the whole column. A corporate action
        // has already been applied.
        let m = multiplier(Some(NVDA_MULTIPLIER)).unwrap();
        assert_eq!(m, Num::from_str("1.000775159164630595").unwrap());
        assert_ne!(
            m,
            Num::from(1),
            "a multiplier of exactly one proves nothing"
        );
    }

    #[test]
    fn a_thousand_tokens_is_not_a_thousand_shares() {
        // 0.0775% today, and more after the next corporate action.
        let m = multiplier(Some(NVDA_MULTIPLIER));
        let shares = underlying(Num::from(1000), m);
        assert_eq!(shares, Num::from_str("1000.775159164630595000").unwrap());
        assert!(shares > Num::from(1000));
    }

    #[test]
    fn a_reverting_contract_records_absence_and_never_one() {
        // Most contracts on this chain revert. Recording 1.0 would claim they
        // implement the standard and are currently unscaled.
        assert_eq!(multiplier(None), None);
        assert_eq!(multiplier(Some("0x")), None);
    }

    #[test]
    fn a_multiplier_of_zero_is_a_real_answer() {
        // A token scaled to nothing is a fact; it is not the same as a contract
        // that has no multiplier at all.
        let zero = format!("0x{}", "0".repeat(64));
        assert_eq!(multiplier(Some(&zero)), Some(Num::from(0)));
        assert_ne!(multiplier(Some(&zero)), None);
    }

    #[test]
    fn an_unscaled_token_keeps_its_amount() {
        assert_eq!(underlying(Num::from(1000), None), Num::from(1000));
    }

    #[test]
    fn a_real_symbol_and_decimals_decode() {
        // `symbol()` returning "NVDA": offset, length, then the bytes.
        let encoded = format!(
            "0x{}{}{}",
            format_args!("{:064x}", 32),
            format_args!("{:064x}", 4),
            "4e56444100000000000000000000000000000000000000000000000000000000"
        );
        assert_eq!(symbol(Some(&encoded)).as_deref(), Some("NVDA"));
        assert_eq!(decimals(Some(&format!("{:#066x}", 18))), Some(18));
    }

    #[test]
    fn nvda_becomes_an_instrument_carrying_its_multiplier() {
        let answers = Answers {
            symbol: Some(encoded_symbol("NVDA")),
            decimals: Some(format!("{:#066x}", 18)),
            ui_multiplier: Some(NVDA_MULTIPLIER.to_string()),
        };
        let i = instrument(&answers, "nvda", 18, 1_700_000_000_000_000);
        assert_eq!(i.base, "NVDA");
        assert_eq!(i.contract_type, "token");
        assert_eq!(
            i.ui_multiplier,
            Some(Num::from_str("1.000775159164630595").unwrap())
        );
        assert_eq!(i.tick_size, Num::from_str("0.000000000000000001").unwrap());
        assert!(i.active);
        assert_eq!(i.observed_at, 1_700_000_000_000_000);
    }

    #[test]
    fn a_plain_token_is_an_instrument_with_no_multiplier_rather_than_one() {
        // WETH on this chain: answers `symbol()` and `decimals()`, reverts on
        // `uiMultiplier()`. **Measured 2026-09-21.**
        let answers = Answers {
            symbol: Some(encoded_symbol("WETH")),
            decimals: Some(format!("{:#066x}", 18)),
            ui_multiplier: None,
        };
        let i = instrument(&answers, "weth", 18, 7);
        assert_eq!(i.base, "WETH");
        // The distinction the whole module exists for.
        assert_eq!(i.ui_multiplier, None);
        assert!(i.active);
    }

    #[test]
    fn a_contract_that_answers_nothing_is_not_called_listed() {
        let i = instrument(&Answers::default(), "ghost", 6, 7);
        // Our name, since the contract stated none.
        assert_eq!(i.base, "ghost");
        assert!(!i.active);
        // The declared decimals, since the contract stated none.
        assert_eq!(i.min_size, Num::from_str("0.000001").unwrap());
    }

    #[test]
    fn the_raw_hex_round_trips_through_the_record() {
        // What is archived is the node's answer, not our reading of it.
        let answers = Answers {
            symbol: Some(encoded_symbol("NVDA")),
            decimals: None,
            ui_multiplier: Some(NVDA_MULTIPLIER.to_string()),
        };
        let bytes = serde_json::to_vec(&answers).unwrap();
        assert_eq!(serde_json::from_slice::<Answers>(&bytes).unwrap(), answers);
        // The wire spelling, so a record written today reads tomorrow.
        assert!(String::from_utf8_lossy(&bytes).contains("uiMultiplier"));
    }

    fn encoded_symbol(text: &str) -> String {
        let mut padded = text.as_bytes().to_vec();
        padded.resize(32, 0);
        format!(
            "0x{}{}{}",
            format_args!("{:064x}", 32),
            format_args!("{:064x}", text.len()),
            padded
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect::<String>()
        )
    }

    #[test]
    fn a_malformed_symbol_is_absent_rather_than_guessed() {
        // A symbol invented here would be a ticker nobody chose.
        assert_eq!(symbol(Some("0xdeadbeef")), None);
        assert_eq!(symbol(None), None);
    }

    #[test]
    fn the_selectors_are_the_ones_the_chain_answers_to() {
        // Verified by calling them against the live chain: `uiMultiplier()`
        // returned NVDA's value and reverted on WETH.
        assert_eq!(UI_MULTIPLIER_SELECTOR, "a60bf13d");
        assert_eq!(SYMBOL_SELECTOR, "95d89b41");
        assert_eq!(DECIMALS_SELECTOR, "313ce567");
    }
}
