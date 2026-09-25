//! Hyperliquid's account history: fills, funding payments and ledger
//! updates, **pure**, bytes in and rows out.
//!
//! Shapes measured on mainnet on 2026-09-25 (`design/measured.md`, *what
//! Hyperliquid holds of an account's history*): every kind carries every dex
//! in one answer, a coin on a HIP-3 dex comes back prefixed (`xyz:GOLD`), and
//! each answer is a list, oldest first.
//!
//! **No row carries an address or a transaction hash.** The hash resolves to
//! the address on any explorer; both stay in the archived bytes, under the
//! owner-only root. A counterparty is named by the keyed fingerprint of its
//! address, and the reader turns our own accounts' fingerprints into aliases.

use galata_wire::{
    Account, Counterparty, DexEffect, Effect, Envelope, Event, Fill, FundingPayment, LedgerUpdate,
    Num, Side, Ticker, Token, Venue, millis_to_micros,
};
use serde::Deserialize;

use crate::ledger::{FingerprintKey, fingerprint};
use crate::normalise::NormaliseError;

/// The channel a fills page is recorded under.
pub const FILLS_CHANNEL: &str = "userFillsByTime";
/// The channel a funding page is recorded under.
pub const FUNDING_CHANNEL: &str = "userFunding";
/// The channel a ledger updates page is recorded under.
pub const UPDATES_CHANNEL: &str = "userNonFundingLedgerUpdates";

/// A page's size, per kind, as measured: a full page asks for the next.
pub fn page_size(channel: &str) -> Option<usize> {
    match channel {
        FILLS_CHANNEL | UPDATES_CHANNEL => Some(2_000),
        FUNDING_CHANNEL => Some(500),
        _ => None,
    }
}

fn json<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T, NormaliseError> {
    serde_json::from_slice(bytes).map_err(|e| NormaliseError::Json(e.to_string()))
}

/// `(dex, ticker)` from a venue coin: the prefix composed away and kept apart.
fn dex_and_ticker(coin: &str) -> Result<(Option<String>, Ticker), NormaliseError> {
    Ok(match coin.split_once(':') {
        Some((dex, bare)) => (Some(dex.to_string()), Ticker::new(bare)?),
        None => (None, Ticker::new(coin)?),
    })
}

/// The time every row of a page is stamped with: each row's own.
#[derive(Deserialize)]
struct Timed {
    time: i64,
}

/// The newest venue time on a page, and how many rows it held: where the next
/// page starts, and whether there is one. Read without building the rows.
pub fn page_end(bytes: &[u8]) -> Option<(i64, usize)> {
    let rows: Vec<Timed> = serde_json::from_slice(bytes).ok()?;
    Some((millis_to_micros(rows.last()?.time), rows.len()))
}

/// The earliest venue time on a page.
pub fn page_start(bytes: &[u8]) -> Option<i64> {
    let rows: Vec<Timed> = serde_json::from_slice(bytes).ok()?;
    Some(millis_to_micros(rows.first()?.time))
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct WireFill {
    coin: String,
    px: Num,
    sz: Num,
    side: String,
    time: i64,
    start_position: Option<Num>,
    dir: Option<String>,
    closed_pnl: Option<Num>,
    oid: u64,
    crossed: Option<bool>,
    fee: Option<Num>,
    fee_token: Option<String>,
    builder_fee: Option<Num>,
    tid: u64,
    twap_id: Option<u64>,
}

/// A fills page to its rows.
pub fn fill_rows(
    venue: &Venue,
    account: &Account,
    recv_micros: i64,
    bytes: &[u8],
) -> Result<Vec<Envelope>, NormaliseError> {
    let fills: Vec<WireFill> = json(bytes)?;
    fills
        .into_iter()
        .map(|f| {
            let (dex, ticker) = dex_and_ticker(&f.coin)?;
            let side = match f.side.as_str() {
                "B" => Side::Bid,
                "A" => Side::Ask,
                other => {
                    return Err(NormaliseError::Shape {
                        kind: "userFillsByTime",
                        detail: format!("side {other:?} is neither B nor A"),
                    });
                }
            };
            Ok(Envelope::for_account(
                venue.clone(),
                account.clone(),
                Some(millis_to_micros(f.time)),
                recv_micros,
                Event::Fill(Fill {
                    dex,
                    ticker,
                    side,
                    price: f.px,
                    size: f.sz,
                    start_position: f.start_position,
                    direction: f.dir,
                    closed_pnl: f.closed_pnl,
                    fee: f.fee,
                    fee_token: f.fee_token,
                    builder_fee: f.builder_fee,
                    crossed: f.crossed,
                    order_id: f.oid,
                    trade_id: f.tid,
                    twap_id: f.twap_id,
                }),
            ))
        })
        .collect()
}

#[derive(Deserialize)]
struct WireFunding {
    time: i64,
    delta: FundingDelta,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct FundingDelta {
    coin: String,
    usdc: Num,
    szi: Num,
    funding_rate: Num,
    n_samples: Option<u32>,
}

/// A funding page to its rows. The sign is the venue's: negative when paid.
pub fn funding_rows(
    venue: &Venue,
    account: &Account,
    recv_micros: i64,
    bytes: &[u8],
) -> Result<Vec<Envelope>, NormaliseError> {
    let rows: Vec<WireFunding> = json(bytes)?;
    rows.into_iter()
        .map(|r| {
            let (dex, ticker) = dex_and_ticker(&r.delta.coin)?;
            Ok(Envelope::for_account(
                venue.clone(),
                account.clone(),
                Some(millis_to_micros(r.time)),
                recv_micros,
                Event::FundingPayment(FundingPayment {
                    dex,
                    ticker,
                    usdc: r.delta.usdc,
                    size: r.delta.szi,
                    rate: r.delta.funding_rate,
                    samples: r.delta.n_samples,
                }),
            ))
        })
        .collect()
}

#[derive(Deserialize)]
struct WireUpdate {
    time: i64,
    delta: serde_json::Value,
}

/// Who an update's addresses belong to: this account, or someone named by
/// fingerprint. **Never** a string that could be an address.
struct Parties<'a> {
    key: &'a FingerprintKey,
    own: &'a str,
}

impl Parties<'_> {
    fn fp(&self, address: &str) -> String {
        fingerprint(self.key, address)
    }
    fn ours(&self, address: Option<&str>) -> bool {
        address.is_some_and(|a| self.fp(a) == self.own)
    }
    fn other(&self, address: Option<&str>) -> Option<Counterparty> {
        address.map(|a| Counterparty::Fingerprint(self.fp(a)))
    }
}

fn text<'a>(delta: &'a serde_json::Value, field: &str) -> Option<&'a str> {
    delta.get(field).and_then(|v| v.as_str())
}

fn number(delta: &serde_json::Value, field: &'static str) -> Result<Option<Num>, NormaliseError> {
    match delta.get(field) {
        None | Some(serde_json::Value::Null) => Ok(None),
        Some(serde_json::Value::String(s)) => Ok(Some(Token::new(s.clone()).require(field)?)),
        Some(serde_json::Value::Number(n)) => Ok(Some(Token::new(n.to_string()).require(field)?)),
        Some(other) => Err(NormaliseError::Shape {
            kind: "userNonFundingLedgerUpdates",
            detail: format!("{field} is {other}, not a number"),
        }),
    }
}

/// A dex as the venue spells it in a `send`: `""` is the main perp dex, and
/// `spot` is not a perp dex at all.
fn perp_dex(spelled: Option<&str>) -> Option<Option<String>> {
    match spelled {
        Some("spot") => None,
        Some("") | None => Some(None),
        Some(dex) => Some(Some(dex.to_string())),
    }
}

fn main_dex(usdc: Num) -> Effect {
    Effect::Known(vec![DexEffect { dex: None, usdc }])
}

/// One update's row: its type verbatim, its effect on perp margin per dex,
/// and its counterparty by fingerprint.
///
/// **The table covers exactly the seventeen types measured on 2026-09-25.**
/// A type it does not know, or whose answer does not state what it did to
/// margin, is `Effect::Unknown`: recorded, and reported, never guessed.
fn update_row(
    delta: &serde_json::Value,
    parties: &Parties<'_>,
) -> Result<LedgerUpdate, NormaliseError> {
    let kind = text(delta, "type").unwrap_or("").to_string();
    let usdc = || number(delta, "usdc");
    let fee = number(delta, "fee")?;
    let mut row = LedgerUpdate {
        kind: kind.clone(),
        effect: Effect::Unknown,
        counterparty: None,
        token: None,
        amount: None,
        fee,
    };
    match kind.as_str() {
        "deposit" => row.effect = usdc()?.map_or(Effect::Unknown, main_dex),
        "withdraw" => row.effect = usdc()?.map_or(Effect::Unknown, |u| main_dex(-u)),
        "accountClassTransfer" => {
            let to_perp = delta.get("toPerp").and_then(|v| v.as_bool());
            row.effect = match (usdc()?, to_perp) {
                (Some(u), Some(true)) => main_dex(u),
                (Some(u), Some(false)) => main_dex(-u),
                _ => Effect::Unknown,
            };
        }
        "internalTransfer" | "subAccountTransfer" => {
            let (from, to) = (text(delta, "user"), text(delta, "destination"));
            if let Some(u) = usdc()? {
                if parties.ours(from) {
                    row.effect = main_dex(-u);
                    row.counterparty = parties.other(to);
                } else {
                    row.effect = main_dex(u);
                    row.counterparty = parties.other(from);
                }
            }
        }
        "send" => {
            let (from, to) = (text(delta, "user"), text(delta, "destination"));
            let token = text(delta, "token").unwrap_or("");
            let amount = number(delta, "amount")?;
            row.token = (token != "USDC").then(|| token.to_string());
            row.amount = amount;
            let (sent, received) = (parties.ours(from), parties.ours(to));
            row.counterparty = match (sent, received) {
                (true, true) => None,
                (true, false) => parties.other(to),
                _ => parties.other(from),
            };
            let mut effects = Vec::new();
            if token == "USDC"
                && let Some(amount) = amount
            {
                if sent && let Some(dex) = perp_dex(text(delta, "sourceDex")) {
                    effects.push(DexEffect { dex, usdc: -amount });
                }
                if received && let Some(dex) = perp_dex(text(delta, "destinationDex")) {
                    effects.push(DexEffect { dex, usdc: amount });
                }
            }
            row.effect = Effect::Known(effects);
        }
        "spotTransfer" => {
            let (from, to) = (text(delta, "user"), text(delta, "destination"));
            row.token = text(delta, "token").map(str::to_string);
            row.amount = number(delta, "amount")?;
            row.counterparty = if parties.ours(from) {
                parties.other(to)
            } else {
                parties.other(from)
            };
            row.effect = Effect::Known(Vec::new());
        }
        // Spot, staking and lending: nothing in perp margin moves.
        "spotGenesis" | "cStakingTransfer" | "borrowLend" | "rewardsClaim" => {
            row.token = text(delta, "token").map(str::to_string);
            row.amount = number(delta, "amount")?;
            row.effect = Effect::Known(Vec::new());
        }
        "vaultDeposit" | "vaultCreate" => {
            row.counterparty = parties.other(text(delta, "vault"));
            row.effect = usdc()?.map_or(Effect::Unknown, |u| main_dex(-u));
        }
        "vaultWithdraw" => {
            row.counterparty = parties.other(text(delta, "vault"));
            row.effect = number(delta, "netWithdrawnUsd")?.map_or(Effect::Unknown, main_dex);
        }
        // Measured, and their answers do not say what they did to perp
        // margin: a liquidation states the notional taken, not the margin
        // lost; a vault distribution and a dex abstraction's activation name
        // an amount without a direction this build can read. Unknown, stated.
        "liquidation" | "vaultDistribution" | "activateDexAbstraction" => {
            row.counterparty = parties.other(text(delta, "vault"));
        }
        _ => {}
    }
    Ok(row)
}

/// A ledger updates page to its rows. `own` is this account's fingerprint,
/// which decides which side of a transfer it was on.
pub fn update_rows(
    venue: &Venue,
    account: &Account,
    key: &FingerprintKey,
    own: &str,
    recv_micros: i64,
    bytes: &[u8],
) -> Result<Vec<Envelope>, NormaliseError> {
    let rows: Vec<WireUpdate> = json(bytes)?;
    let parties = Parties { key, own };
    rows.into_iter()
        .map(|r| {
            Ok(Envelope::for_account(
                venue.clone(),
                account.clone(),
                Some(millis_to_micros(r.time)),
                recv_micros,
                Event::LedgerUpdate(update_row(&r.delta, &parties)?),
            ))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Secret;
    use std::str::FromStr;

    const OURS: &str = "0x3f9aa0c1d2e3f4a5b6c7d8e9f0a1b2c3d4e5f6a7";
    const THEIRS: &str = "0x00000000000000000000000000000000000000aa";

    fn key() -> FingerprintKey {
        FingerprintKey::new(Secret::new("k"))
    }
    fn venue() -> Venue {
        Venue::new("hyperliquid").unwrap()
    }
    fn main() -> Account {
        Account::new("main").unwrap()
    }
    fn n(s: &str) -> Num {
        Num::from_str(s).unwrap()
    }
    fn update(delta: &str) -> LedgerUpdate {
        let own = fingerprint(&key(), OURS);
        let page = format!(r#"[{{"time":1,"hash":"0xabc","delta":{delta}}}]"#);
        let rows = update_rows(&venue(), &main(), &key(), &own, 2, page.as_bytes()).unwrap();
        let Event::LedgerUpdate(u) = &rows[0].event else {
            panic!()
        };
        let json = serde_json::to_string(&rows[0]).unwrap();
        assert!(!json.contains("0xabc"), "the hash reached a row: {json}");
        for address in [OURS, THEIRS] {
            assert!(
                !json.contains(&address[2..]),
                "an address reached a row: {json}"
            );
        }
        u.clone()
    }

    #[test]
    fn a_fill_keeps_its_venue_ids_and_drops_its_hash() {
        let page = br#"[{"coin":"xyz:GOLD","px":"4301.5","sz":"0.5","side":"A","time":1790332213000,"startPosition":"1.0","dir":"Close Long","closedPnl":"12.5","hash":"0xdeadbeef","oid":77,"crossed":true,"fee":"0.9","builderFee":"0.0","tid":88,"feeToken":"USDC","twapId":null}]"#;
        let rows = fill_rows(&venue(), &main(), 5, page).unwrap();
        let Event::Fill(f) = &rows[0].event else {
            panic!()
        };
        assert_eq!((f.trade_id, f.order_id), (88, 77));
        assert_eq!(f.dex.as_deref(), Some("xyz"));
        assert_eq!(f.ticker.as_str(), "GOLD");
        assert_eq!(f.side, Side::Ask);
        assert_eq!(rows[0].at_micros, Some(1_790_332_213_000_000));
        assert!(
            !serde_json::to_string(&rows[0])
                .unwrap()
                .contains("deadbeef")
        );
    }

    #[test]
    fn a_funding_payment_keeps_the_venues_sign() {
        let page = br#"[{"time":1,"hash":"0x0","delta":{"type":"funding","coin":"BTC","usdc":"-0.1239","szi":"0.01","fundingRate":"0.0000125","nSamples":null}}]"#;
        let rows = funding_rows(&venue(), &main(), 5, page).unwrap();
        let Event::FundingPayment(p) = &rows[0].event else {
            panic!()
        };
        assert_eq!(
            p.usdc,
            n("-0.1239"),
            "negative when paid, as the venue signs it"
        );
        assert_eq!(p.dex, None);
    }

    #[test]
    fn a_send_between_dexes_moves_margin_out_of_one_and_into_the_other() {
        let u = update(&format!(
            r#"{{"type":"send","user":"{OURS}","destination":"{OURS}","sourceDex":"","destinationDex":"xyz","token":"USDC","amount":"25.77","usdcValue":"25.77","fee":"0.0"}}"#
        ));
        assert_eq!(
            u.effect,
            Effect::Known(vec![
                DexEffect {
                    dex: None,
                    usdc: n("-25.77")
                },
                DexEffect {
                    dex: Some("xyz".into()),
                    usdc: n("25.77")
                },
            ])
        );
        assert_eq!(u.counterparty, None, "a send to itself has no other side");
    }

    #[test]
    fn a_spot_transfer_leaves_perp_margin_alone() {
        let u = update(&format!(
            r#"{{"type":"spotTransfer","token":"FATCAT","amount":"10.0","usdcValue":"4.7766","user":"{OURS}","destination":"{THEIRS}","fee":"0.0"}}"#
        ));
        assert_eq!(u.effect, Effect::Known(Vec::new()));
        assert_eq!(u.token.as_deref(), Some("FATCAT"));
        assert_eq!(
            u.counterparty,
            Some(Counterparty::Fingerprint(fingerprint(&key(), THEIRS)))
        );
    }

    #[test]
    fn a_withdrawal_elsewhere_names_a_fingerprint_not_an_address() {
        let u = update(&format!(
            r#"{{"type":"internalTransfer","usdc":"8.85","user":"{OURS}","destination":"{THEIRS}","fee":"1.0"}}"#
        ));
        assert_eq!(u.effect, main_dex(n("-8.85")));
        assert_eq!(
            u.fee,
            Some(n("1.0")),
            "the fee is stated beside the effect, not in it"
        );
        assert!(matches!(u.counterparty, Some(Counterparty::Fingerprint(_))));
    }

    #[test]
    fn a_transfer_in_is_positive_and_names_the_sender() {
        let u = update(&format!(
            r#"{{"type":"subAccountTransfer","usdc":"1000.0","user":"{THEIRS}","destination":"{OURS}"}}"#
        ));
        assert_eq!(u.effect, main_dex(n("1000.0")));
        assert_eq!(
            u.counterparty,
            Some(Counterparty::Fingerprint(fingerprint(&key(), THEIRS)))
        );
    }

    #[test]
    fn every_observed_type_has_its_stated_effect() {
        let cases: [(&str, Effect); 9] = [
            (
                r#"{"type":"deposit","usdc":"3000.0"}"#,
                main_dex(n("3000.0")),
            ),
            (
                r#"{"type":"withdraw","usdc":"3000.0","nonce":0,"fee":"0.0"}"#,
                main_dex(n("-3000.0")),
            ),
            (
                r#"{"type":"accountClassTransfer","usdc":"5.0","toPerp":true}"#,
                main_dex(n("5.0")),
            ),
            (
                r#"{"type":"accountClassTransfer","usdc":"5.0","toPerp":false}"#,
                main_dex(n("-5.0")),
            ),
            (
                r#"{"type":"spotGenesis","token":"PURR","amount":"402349.0"}"#,
                Effect::Known(vec![]),
            ),
            (
                r#"{"type":"cStakingTransfer","token":"HYPE","amount":"3000.0","isDeposit":false}"#,
                Effect::Known(vec![]),
            ),
            (
                r#"{"type":"borrowLend","token":"USDC","operation":"supply","amount":"500.0","interestAmount":"0.0"}"#,
                Effect::Known(vec![]),
            ),
            (
                r#"{"type":"rewardsClaim","amount":"2.48","token":"USDC"}"#,
                Effect::Known(vec![]),
            ),
            (
                r#"{"type":"liquidation","liquidatedNtlPos":"36657.0","accountValue":"365.89","leverageType":"Isolated","liquidatedPositions":[{"coin":"ETH","szi":"22.5"}]}"#,
                Effect::Unknown,
            ),
        ];
        for (delta, expected) in cases {
            assert_eq!(update(delta).effect, expected, "{delta}");
        }
        let vault = update(&format!(
            r#"{{"type":"vaultDeposit","vault":"{THEIRS}","usdc":"212.46"}}"#
        ));
        assert_eq!(vault.effect, main_dex(n("-212.46")));
        let back = update(&format!(
            r#"{{"type":"vaultWithdraw","vault":"{THEIRS}","user":"{OURS}","requestedUsd":"770.6","commission":"0.0","closingCost":"0.0","basis":"684.4","netWithdrawnUsd":"770.6"}}"#
        ));
        assert_eq!(back.effect, main_dex(n("770.6")));
    }

    #[test]
    fn an_unknown_ledger_update_type_is_kept_with_an_unknown_effect() {
        let u = update(r#"{"type":"somethingNew","usdc":"1.0"}"#);
        assert_eq!(u.kind, "somethingNew");
        assert_eq!(u.effect, Effect::Unknown);
    }

    #[test]
    fn a_page_says_where_it_ends() {
        let page = br#"[{"time":1000},{"time":2000}]"#;
        assert_eq!(page_end(page), Some((2_000_000, 2)));
        assert_eq!(page_start(page), Some(1_000_000));
        assert_eq!(page_end(b"[]"), None);
        assert_eq!(page_size(FUNDING_CHANNEL), Some(500));
    }
}
