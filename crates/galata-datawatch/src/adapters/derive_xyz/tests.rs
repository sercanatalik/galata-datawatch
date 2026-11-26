//! The adapter, against payloads the venue sent on 2026-11-24.
//!
//! Every fixture under `tests/fixtures/derive_xyz` is verbatim: one frame off
//! the socket or one answer to a request, with nothing edited.

use std::path::Path;
use std::str::FromStr;

use galata_wire::{Event, Instrument, Kind, Mark, Num, OptionPricing, Quote, Series, Ticker};

use super::*;
use crate::venue::Adapter;

/// Read at run time, as `signals.rs` reads its own: the crate ships `/src`
/// only, and a fixture compiled in would stop the shipped crate's tests
/// compiling at all.
fn fixture(name: &str) -> Vec<u8> {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/derive_xyz")
        .join(name);
    std::fs::read(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()))
}

fn num(s: &str) -> Option<Num> {
    Some(Num::from_str(s).unwrap())
}

const ETH_CALL: &str = "ETH-20261225-2200-C";
const BTC_PUT: &str = "BTC-20261127-50000-P";
const LIVE: &str = "BTC-20261125-56000-C";
const EXPIRED: &str = "BTC-20261124-75000-P";

fn adapter(names: &[&str]) -> DeriveXyz {
    DeriveXyz::new(
        Config {
            market: Market::Mainnet,
            instruments: names.iter().map(|n| n.to_string()).collect(),
        },
        None,
    )
    .unwrap()
}

fn shipped() -> DeriveXyz {
    adapter(&[ETH_CALL, BTC_PUT, LIVE, EXPIRED])
}

fn events(adapter: &DeriveXyz, payload: &Payload) -> Vec<(Ticker, Option<i64>, Event)> {
    adapter
        .normalise(payload)
        .unwrap()
        .into_iter()
        .map(|e| (e.ticker().unwrap().clone(), e.at_micros, e.event))
        .collect()
}

// ---- names ---------------------------------------------------------------

#[test]
fn a_name_is_read_into_its_four_parts() {
    let name = OptionName::parse("BTC-20261225-60000-P").unwrap();
    assert_eq!(name.currency, "BTC");
    assert_eq!(name.date, "20261225");
    assert_eq!(name.strike, Num::from_str("60000").unwrap());
    assert_eq!(name.right, Right::Put);
    assert_eq!(name.universe, None);
    assert_eq!(name.index_ticker(), "BTC-USD");
}

#[test]
fn a_fractional_strike_is_spelled_with_an_underscore() {
    let name = OptionName::parse("ADA-20261127-0_15-C").unwrap();
    assert_eq!(name.strike, Num::from_str("0.15").unwrap());
    assert_eq!(name.right, Right::Call);
    // And the venue's name is already a legal ticker, underscore included.
    assert!(Ticker::new("ADA-20261127-0_15-C").is_ok());
}

#[test]
fn a_risk_universe_suffix_is_read_and_not_required() {
    let name = OptionName::parse("ETH-20261225-2000-C-3").unwrap();
    assert_eq!(name.universe, Some(3));
}

#[test]
fn what_is_not_an_option_name_is_refused_by_name_and_shown_the_form() {
    for bad in [
        "BTC-PERP",
        "",
        "BTC-20261225-60000-X",
        "btc-20261225-60000-P",
        "BTC-2026122-60000-P",
        "BTC-20261225-60k-P",
        "BTC-20261225-0_-P",
        "BTC-20261225-60000-P-x",
    ] {
        let refusal = OptionName::parse(bad).unwrap_err();
        assert!(refusal.contains(&format!("{bad:?}")), "{refusal}");
        assert!(refusal.contains("BTC-20261225-60000-P"), "{refusal}");
    }
}

// ---- construction --------------------------------------------------------

#[test]
fn a_credential_is_refused_rather_than_ignored() {
    let refusal = DeriveXyz::new(
        Config {
            market: Market::Mainnet,
            instruments: vec![LIVE.into()],
        },
        Some(Credential::Signed {
            key: "k".into(),
            secret: "s".into(),
        }),
    )
    .unwrap_err();
    assert!(refusal.to_string().contains("public"), "{refusal}");
}

#[test]
fn fifty_options_are_taken_and_fifty_one_are_refused() {
    let names = |n: usize| -> Vec<String> {
        (0..n)
            .map(|i| format!("BTC-20261225-{}-C", 50_000 + i * 500))
            .collect()
    };
    let config = |n| Config {
        market: Market::Mainnet,
        instruments: names(n),
    };
    let fifty = DeriveXyz::new(config(50), None).unwrap();
    assert_eq!(fifty.options().len(), 50);
    assert_eq!(fifty.declaration().max_instruments, Some(50));

    let refusal = DeriveXyz::new(config(51), None).unwrap_err().to_string();
    assert!(refusal.contains("51"), "{refusal}");
    assert!(refusal.contains("50"), "{refusal}");
}

#[test]
fn a_name_declared_twice_is_refused() {
    let refusal = DeriveXyz::new(
        Config {
            market: Market::Mainnet,
            instruments: vec![LIVE.into(), LIVE.into()],
        },
        None,
    )
    .unwrap_err()
    .to_string();
    assert!(
        refusal.contains(LIVE) && refusal.contains("twice"),
        "{refusal}"
    );
}

#[test]
fn the_venue_supplies_quotes_and_not_trades() {
    let declaration = shipped().declaration().clone();
    assert!(declaration.supplies(Series::Quotes));
    // The venue has a trades channel. This adapter does not name it, so a
    // configuration declaring `trades` is refused at load rather than
    // accepted and ignored.
    assert!(!declaration.supplies(Series::Trades));
    assert!(!declaration.supplies(Series::Candles));
    assert!(declaration.historical.is_empty());
}

// ---- the set, and what follows from it ------------------------------------

#[test]
fn an_index_and_a_perpetual_follow_from_each_underlying_and_none_is_counted() {
    // Four BTC options and one ETH option: two indexes and two perpetuals,
    // and no perpetual of an underlying with no option in the set.
    let adapter = adapter(&[
        "BTC-20261225-60000-P",
        "BTC-20261225-60000-C",
        "BTC-20270129-50000-P",
        "BTC-20270129-75000-C",
        "ETH-20261225-2000-C",
    ]);
    let implied = adapter.implied_subscriptions();
    let tickers: Vec<&str> = implied.iter().map(|s| s.ticker.as_str()).collect();
    assert_eq!(tickers, ["BTC-USD", "ETH-USD", "BTC-PERP", "ETH-PERP"]);
    assert_eq!(adapter.perpetuals(), ["BTC-PERP", "ETH-PERP"]);
    assert!(implied.iter().all(|s| s.series == Series::Quotes));
    // The cap counts what was declared.
    assert_eq!(adapter.options().len(), 5);
}

#[test]
fn fifty_options_and_their_indexes_are_one_frame() {
    let names: Vec<String> = (0..50)
        .map(|i| {
            let ccy = ["BTC", "ETH", "SOL"][i % 3];
            format!("{ccy}-20261225-{}-C", 1_000 + i * 10)
        })
        .collect();
    let adapter = DeriveXyz::new(
        Config {
            market: Market::Mainnet,
            instruments: names.clone(),
        },
        None,
    )
    .unwrap();
    let mut subscriptions: Vec<Subscription> = names
        .iter()
        .map(|n| Subscription {
            ticker: Ticker::new(n.as_str()).unwrap(),
            series: Series::Quotes,
        })
        .collect();
    subscriptions.extend(adapter.implied_subscriptions());
    let frames = adapter.subscribe_frames(&subscriptions);
    assert_eq!(frames.len(), 1);
    let json: serde_json::Value = serde_json::from_str(&frames[0]).unwrap();
    // Fifty options, and an index and a perpetual for each of three
    // underlyings: far inside the venue's limit of 10,000 a frame.
    assert_eq!(json["params"]["channels"].as_array().unwrap().len(), 56);
}

#[test]
fn the_subscribe_frame_is_the_set_sorted_and_names_no_trades_channel() {
    let adapter = adapter(&["ETH-20261225-2000-C", "BTC-20261225-60000-P"]);
    let mut subscriptions: Vec<Subscription> = adapter
        .options()
        .iter()
        .map(|n| Subscription {
            ticker: Ticker::new(n.as_str()).unwrap(),
            series: Series::Quotes,
        })
        .collect();
    subscriptions.extend(adapter.implied_subscriptions());

    let frames = adapter.subscribe_frames(&subscriptions);
    assert_eq!(
        frames,
        vec![
            serde_json::json!({
                "method": "subscribe",
                "params": { "channels": [
                    "spot_feed.BTC",
                    "spot_feed.ETH",
                    "ticker_slim.BTC-20261225-60000-P.1000",
                    "ticker_slim.BTC-PERP.1000",
                    "ticker_slim.ETH-20261225-2000-C.1000",
                    "ticker_slim.ETH-PERP.1000",
                ]},
                "id": 1,
            })
            .to_string()
        ]
    );
    assert!(!frames[0].contains("trades"));
    // Nothing to ask for is no frame, not an empty one.
    assert!(adapter.subscribe_frames(&[]).is_empty());
}

#[test]
fn a_subscription_to_another_series_is_not_sent() {
    let adapter = adapter(&[LIVE]);
    let frames = adapter.subscribe_frames(&[Subscription {
        ticker: Ticker::new(LIVE).unwrap(),
        series: Series::Trades,
    }]);
    assert!(frames.is_empty());
}

// ---- classification ------------------------------------------------------

#[test]
fn the_bytes_are_unchanged_by_classification() {
    let adapter = shipped();
    for name in [
        "ticker_two_sides.json",
        "ticker_empty_side.json",
        "spot_feed.json",
        "subscribe_ack.json",
    ] {
        let bytes = fixture(name);
        let payload = adapter.classify(&bytes, 7);
        assert_eq!(payload.payload, bytes, "{name}");
        assert_eq!(payload.recv_micros, 7);
        assert_eq!(payload.origin, Origin::Streamed);
    }
}

#[test]
fn a_ticker_is_classified_by_its_instrument_and_an_index_by_its_currency() {
    let adapter = shipped();
    let ticker = adapter.classify(&fixture("ticker_two_sides.json"), 0);
    assert_eq!(ticker.channel, wire::TICKER);
    assert_eq!(ticker.symbol.as_deref(), Some(ETH_CALL));
    assert_eq!(ticker.kind, "quotes");
    assert_eq!(
        adapter.venue_ticker(&ticker.channel, ETH_CALL),
        Some(Ticker::new(ETH_CALL).unwrap())
    );

    let index = adapter.classify(&fixture("spot_feed.json"), 0);
    assert_eq!(index.channel, wire::INDEX);
    assert_eq!(index.symbol.as_deref(), Some("BTC"));
    assert_eq!(
        adapter.venue_ticker(&index.channel, "BTC"),
        Some(Ticker::new("BTC-USD").unwrap())
    );
    // And the way back: what a subscription to the index is asked as.
    assert_eq!(
        adapter
            .venue_symbol(&Ticker::new("BTC-USD").unwrap())
            .as_deref(),
        Some("BTC")
    );
}

#[test]
fn an_acknowledgement_partitions_under_its_own_name_and_covers_nothing() {
    let adapter = shipped();
    let ack = adapter.classify(&fixture("subscribe_ack.json"), 0);
    assert_eq!(ack.channel, wire::ANSWER);
    assert_eq!(ack.kind, wire::ANSWER);
    assert_eq!(ack.symbol, None);
    assert_eq!(adapter.series_of_channel(&ack.channel), None);
    assert!(adapter.normalise(&ack).unwrap().is_empty());
}

// ---- normalisation -------------------------------------------------------

#[test]
fn a_two_sided_ticker_is_a_quote_a_mark_and_the_venues_pricing() {
    let adapter = shipped();
    let payload = adapter.classify(&fixture("ticker_two_sides.json"), 99);
    let events = events(&adapter, &payload);
    assert_eq!(events.len(), 3);
    for (ticker, at, _) in &events {
        assert_eq!(ticker.as_str(), ETH_CALL);
        // The venue's milliseconds, as microseconds.
        assert_eq!(*at, Some(1_795_561_677_430_000));
    }
    assert_eq!(
        events[0].2,
        Event::Quote(Quote {
            bid_px: num("9.8"),
            bid_sz: num("2.74"),
            ask_px: num("11.3"),
            ask_sz: num("103.15"),
            bid_spread: None,
            ask_spread: None,
        })
    );
    assert_eq!(
        events[1].2,
        Event::Mark(Mark {
            mark: num("10.1"),
            index: num("1682.9"),
            open_interest: num("1581.491"),
            ..Mark::default()
        })
    );
    assert_eq!(
        events[2].2,
        Event::OptionPricing(OptionPricing {
            iv: num("0.61817"),
            bid_iv: num("0.61274"),
            ask_iv: num("0.63313"),
            delta: num("0.08025"),
            gamma: num("0.00049569"),
            vega: num("0.72468"),
            theta: num("-0.73755"),
            rho: num("0.84657"),
            forward: num("1685.9"),
            discount_factor: num("0.998"),
        })
    );
    let kinds: Vec<Kind> = events.iter().map(|e| e.2.kind()).collect();
    assert_eq!(kinds, [Kind::Quotes, Kind::Marks, Kind::OptionPricing]);
}

#[test]
fn an_empty_side_is_absent_and_so_is_its_volatility() {
    // The venue printed bid "0" for "0" and a bid volatility of "0".
    let adapter = shipped();
    let payload = adapter.classify(&fixture("ticker_empty_side.json"), 0);
    let events = events(&adapter, &payload);
    let Event::Quote(quote) = &events[0].2 else {
        panic!("{:?}", events[0].2)
    };
    assert_eq!((quote.bid_px, quote.bid_sz), (None, None));
    assert_eq!((quote.ask_px, quote.ask_sz), (num("1035"), num("5.5")));

    let Event::OptionPricing(pricing) = &events[2].2 else {
        panic!("{:?}", events[2].2)
    };
    assert_eq!(pricing.bid_iv, None);
    assert_eq!(pricing.ask_iv, num("2.44373"));
    // Small, and not zero: a greek is kept as the venue printed it.
    assert_eq!(pricing.gamma, num("0.00000437"));
    assert_eq!(pricing.delta, num("-0.00710"));
}

fn ticker_with(pricing: &str, bid: (&str, &str)) -> Vec<u8> {
    format!(
        r#"{{"method":"subscription","params":{{"channel":"ticker_slim.{LIVE}.1000","data":{{"timestamp":5,"instrument_ticker":{{"t":5,"A":"1","a":"7","B":"{}","b":"{}","f":null,"option_pricing":{pricing},"I":"60000","M":"3","stats":{{"oi":"0"}}}}}}}}}}"#,
        bid.1, bid.0
    )
    .into_bytes()
}

#[test]
fn a_greek_of_zero_is_a_value() {
    let adapter = shipped();
    let bytes = ticker_with(
        r#"{"d":"0","t":"0","g":"0","v":"0","i":"1.05065","r":"0","f":"60000","m":"0","df":"0.999","bi":"0","ai":"0.9"}"#,
        ("0", "0"),
    );
    let events = events(&adapter, &adapter.classify(&bytes, 0));
    let Event::OptionPricing(pricing) = &events[2].2 else {
        panic!("{:?}", events[2].2)
    };
    assert_eq!(pricing.delta, num("0"));
    assert_eq!(pricing.gamma, num("0"));
    assert_eq!(pricing.theta, num("0"));
    // Nobody bids, so the zero beside the bid is an absence.
    assert_eq!(pricing.bid_iv, None);
    assert_eq!(pricing.ask_iv, num("0.9"));
}

#[test]
fn a_zero_volatility_beside_a_real_quote_is_the_venues_claim_and_is_kept() {
    let adapter = shipped();
    let bytes = ticker_with(r#"{"d":"0.5","i":"0.6","bi":"0","ai":"0.7"}"#, ("5", "2"));
    let events = events(&adapter, &adapter.classify(&bytes, 0));
    let Event::OptionPricing(pricing) = &events[2].2 else {
        panic!("{:?}", events[2].2)
    };
    assert_eq!(pricing.bid_iv, num("0"));
    // A key the venue did not send is a field that reads absent.
    assert_eq!(pricing.gamma, None);
}

#[test]
fn a_ticker_with_no_pricing_is_still_a_quote_and_a_mark() {
    let adapter = shipped();
    let events = events(
        &adapter,
        &adapter.classify(&ticker_with("null", ("5", "2")), 0),
    );
    let kinds: Vec<Kind> = events.iter().map(|e| e.2.kind()).collect();
    assert_eq!(kinds, [Kind::Quotes, Kind::Marks]);
}

#[test]
fn an_index_frame_is_a_mark_holding_only_the_index() {
    let adapter = shipped();
    let payload = adapter.classify(&fixture("spot_feed.json"), 0);
    let events = events(&adapter, &payload);
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].0.as_str(), "BTC-USD");
    assert_eq!(events[0].1, Some(1_795_561_679_143_000));
    assert_eq!(
        events[0].2,
        Event::Mark(Mark {
            index: num("59962.839271709774"),
            ..Mark::default()
        })
    );
}

#[test]
fn a_truncated_ticker_is_refused_and_does_not_panic() {
    let adapter = shipped();
    let mut bytes = fixture("ticker_two_sides.json");
    bytes.truncate(bytes.len() / 2);
    let payload = Payload {
        channel: wire::TICKER.into(),
        symbol: Some(ETH_CALL.into()),
        ..adapter.classify(&fixture("ticker_two_sides.json"), 0)
    };
    let truncated = Payload {
        payload: bytes,
        ..payload
    };
    assert!(adapter.normalise(&truncated).is_err());
}

#[test]
fn a_frame_for_an_instrument_nobody_declared_is_refused_by_name() {
    let adapter = adapter(&[LIVE]);
    let payload = adapter.classify(&fixture("ticker_two_sides.json"), 0);
    let refusal = adapter.normalise(&payload).unwrap_err().to_string();
    assert!(refusal.contains(ETH_CALL), "{refusal}");
}

// ---- reference data ------------------------------------------------------

fn instrument(adapter: &DeriveXyz, name: &str, file: &str) -> Instrument {
    let payload = reference_page(name, fixture(file), 1_000);
    let mut events = events(adapter, &payload);
    assert_eq!(events.len(), 1);
    let (ticker, at, event) = events.remove(0);
    assert_eq!(ticker.as_str(), name);
    // The venue states no time for reference data.
    assert_eq!(at, None);
    match event {
        Event::Instrument(instrument) => instrument,
        other => panic!("{other:?}"),
    }
}

#[test]
fn a_live_option_states_its_terms_and_the_venues_own_expiry() {
    let adapter = shipped();
    let live = instrument(&adapter, LIVE, "get_instrument_live.json");
    assert_eq!(live.contract_type, "option");
    assert_eq!(live.base, "BTC");
    assert_eq!(live.quote, "USDC");
    assert!(live.active);
    assert_eq!(live.strike, num("56000"));
    assert_eq!(live.right.as_deref(), Some("call"));
    // 1795593600 s: 2026-11-25T08:00:00Z, as the venue stated it.
    assert_eq!(live.expiry_micros, Some(1_795_593_600_000_000));
    assert_eq!(live.tick_size, Num::from_str("1").unwrap());
    assert_eq!(live.min_size, Num::from_str("0.01").unwrap());
    assert_eq!(live.lot_size, Num::from_str("0.00001").unwrap());
    assert_eq!(live.observed_at, 1_000);
}

#[test]
fn an_expired_option_is_still_answered_for_and_reads_inactive() {
    let adapter = shipped();
    let expired = instrument(&adapter, EXPIRED, "get_instrument_expired.json");
    assert!(!expired.active);
    assert_eq!(expired.right.as_deref(), Some("put"));
    assert_eq!(expired.strike, num("75000"));
    assert_eq!(expired.expiry_micros, Some(1_795_507_200_000_000));
}

#[test]
fn the_expiry_is_the_venues_and_not_the_names() {
    // An equity option under a name of the same shape settles at 21:00 UTC,
    // not 08:00 (measured 2026-11-24: NVDA-20261125-210-C, expiry 1795640400).
    let adapter = adapter(&["NVDA-20261125-210-C"]);
    let bytes = String::from_utf8(fixture("get_instrument_live.json"))
        .unwrap()
        .replace("BTC-20261125-56000-C", "NVDA-20261125-210-C")
        .replace("\"expiry\":1795593600", "\"expiry\":1795640400")
        .into_bytes();
    let payload = reference_page("NVDA-20261125-210-C", bytes, 0);
    let events = events(&adapter, &payload);
    let Event::Instrument(equity) = &events[0].2 else {
        panic!("{:?}", events[0].2)
    };
    let eight_utc = 1_795_593_600_000_000_i64;
    assert_eq!(equity.expiry_micros, Some(1_795_640_400_000_000));
    assert_eq!(
        equity.expiry_micros.unwrap() - eight_utc,
        13 * 3_600 * 1_000_000
    );
}

#[test]
fn a_name_the_venue_does_not_know_is_refused_with_the_venues_words() {
    let adapter = adapter(&[LIVE]);
    let payload = reference_page(LIVE, fixture("get_instrument_unknown.json"), 0);
    let refusal = adapter.normalise(&payload).unwrap_err().to_string();
    assert!(refusal.contains("Instrument not found"), "{refusal}");
    assert!(refusal.contains("12001"), "{refusal}");
}

#[test]
fn the_reference_asks_about_every_option_then_each_perpetual_and_no_index() {
    let adapter = shipped();
    let reference = adapter.reference().unwrap();
    assert_eq!(reference.interval_micros, REFERENCE_INTERVAL_MICROS);
    let mut expected = adapter.options().to_vec();
    expected.extend(["BTC-PERP".to_string(), "ETH-PERP".to_string()]);
    assert_eq!(reference.symbols, expected);
    assert!(reference.symbols.iter().all(|s| !s.ends_with("-USD")));
    // And each is resolvable from the channel a reference answer is filed
    // under, which is how the loop learns whose answer it holds.
    for symbol in &reference.symbols {
        assert!(adapter.venue_ticker(REFERENCE_CHANNEL, symbol).is_some());
    }
}

#[test]
fn a_reference_refresh_of_a_full_set_fits_far_inside_its_interval() {
    // Fifty requests, paced at the declared budget, against a five-minute
    // refresh: the refresh can never overlap its own next turn.
    let adapter = shipped();
    let pace_ms = adapter.declaration().budget.interval_ms(1, 1.0);
    let whole_refresh_micros = i64::from(MAX_INSTRUMENTS) * pace_ms as i64 * 1_000;
    assert!(
        pace_ms >= 200,
        "{pace_ms} ms outruns five requests a second"
    );
    assert!(
        whole_refresh_micros * 10 <= REFERENCE_INTERVAL_MICROS,
        "{whole_refresh_micros} µs of requests in a {REFERENCE_INTERVAL_MICROS} µs interval"
    );
}

#[test]
fn every_event_is_reproduced_by_a_second_normalisation() {
    // No clock and no state: the same bytes are the same events, which is
    // what lets a rebuild write the tape again byte for byte.
    let adapter = shipped();
    for name in [
        "ticker_two_sides.json",
        "ticker_empty_side.json",
        "spot_feed.json",
    ] {
        let payload = adapter.classify(&fixture(name), 11);
        assert_eq!(
            adapter.normalise(&payload).unwrap(),
            adapter.normalise(&payload).unwrap(),
            "{name}"
        );
    }
}

// ---- the boot check ------------------------------------------------------

#[test]
fn a_live_option_stands() {
    assert_eq!(
        standing(LIVE, &fixture("get_instrument_live.json")),
        Ok(Standing::Live)
    );
}

#[test]
fn an_expired_option_has_left_and_says_when() {
    assert_eq!(
        standing(EXPIRED, &fixture("get_instrument_expired.json")),
        Ok(Standing::Left {
            expiry_micros: 1_795_507_200_000_000
        })
    );
}

#[test]
fn a_name_the_venue_never_listed_refuses_the_start_by_name() {
    let refusal = standing(
        "BTC-20261225-61234-P",
        &fixture("get_instrument_unknown.json"),
    )
    .unwrap_err();
    assert!(refusal.contains("BTC-20261225-61234-P"), "{refusal}");
    assert!(refusal.contains("has ever listed"), "{refusal}");
}

#[test]
fn an_answer_that_cannot_be_read_refuses_the_start() {
    let refusal = standing(LIVE, b"<html>502</html>").unwrap_err();
    assert!(refusal.contains(LIVE), "{refusal}");
}

#[test]
fn an_index_that_is_not_the_one_composed_refuses_the_start() {
    let bytes = String::from_utf8(fixture("get_instrument_live.json"))
        .unwrap()
        .replace("\"index\":\"BTC-USD\"", "\"index\":\"XBT-USD\"")
        .into_bytes();
    let refusal = standing(LIVE, &bytes).unwrap_err();
    assert!(
        refusal.contains("XBT-USD") && refusal.contains("BTC-USD"),
        "{refusal}"
    );
}

#[test]
fn something_that_is_not_an_option_refuses_the_start() {
    let bytes = String::from_utf8(fixture("get_instrument_live.json"))
        .unwrap()
        .replace(
            "\"instrument_type\":\"option\"",
            "\"instrument_type\":\"perp\"",
        );
    let cut = bytes.find("\"option_details\"").unwrap();
    let end = bytes[cut..].find("},").unwrap() + cut + 2;
    let bytes = format!("{}\"option_details\":null,{}", &bytes[..cut], &bytes[end..]);
    let refusal = standing(LIVE, bytes.as_bytes()).unwrap_err();
    assert!(refusal.contains("perp"), "{refusal}");
}

#[test]
fn one_live_option_among_expired_ones_starts() {
    let left = Standing::Left {
        expiry_micros: 1_795_507_200_000_000,
    };
    assert_eq!(
        judge_set(&[
            (EXPIRED.to_string(), left),
            (LIVE.to_string(), Standing::Live),
            ("BTC-20261124-70000-P".to_string(), left),
        ]),
        Ok(())
    );
}

#[test]
fn a_set_where_every_option_has_expired_refuses_naming_each() {
    let left = Standing::Left {
        expiry_micros: 1_795_507_200_000_000,
    };
    let refusal = judge_set(&[
        (EXPIRED.to_string(), left),
        ("BTC-20261124-70000-P".to_string(), left),
    ])
    .unwrap_err();
    assert!(refusal.contains(EXPIRED), "{refusal}");
    assert!(refusal.contains("BTC-20261124-70000-P"), "{refusal}");
    assert!(refusal.contains("2026-11-24"), "{refusal}");
}

// ---- leaving the set, in the capture loop ---------------------------------

/// The loop's half of an expiry. Here rather than beside the loop because the
/// loop's own tests are driven by a venue whose instruments never expire, and
/// this is the only module permitted to name the one whose do.
#[cfg(feature = "capture")]
mod leaving {
    use std::sync::Arc;
    use std::sync::Mutex;

    use galata_wire::{Clipped, Event, Gap};

    use super::*;
    use crate::capture::{Capture, Clock, PairState, TestClock, Wiring};
    use crate::sink::testing::RecordingSink;

    const SEC: i64 = 1_000_000;
    /// After `EXPIRED`'s expiry (1795507200 s) and before `LIVE`'s
    /// (1795593600 s).
    const BETWEEN: i64 = 1_795_560_000 * SEC;
    const LIVE_EXPIRY: i64 = 1_795_593_600 * SEC;

    struct Fixture {
        capture: Capture,
        clock: Arc<TestClock>,
        sink: Arc<RecordingSink>,
        _root: tempfile::TempDir,
    }

    fn subscription(ticker: &str) -> Subscription {
        Subscription {
            ticker: Ticker::new(ticker).unwrap(),
            series: Series::Quotes,
        }
    }

    /// `LIVE`, `EXPIRED` and `ETH_CALL` declared, with the indexes that follow.
    fn fixture() -> Fixture {
        let root = tempfile::tempdir().unwrap();
        let clock = Arc::new(TestClock::at(BETWEEN));
        let sink = Arc::new(RecordingSink::default());
        let adapter = adapter(&[LIVE, EXPIRED, ETH_CALL]);
        let mut declared: Vec<Subscription> = [LIVE, EXPIRED, ETH_CALL].map(subscription).to_vec();
        declared.extend(adapter.implied_subscriptions());
        let capture = Capture::new(Wiring {
            adapter: Box::new(adapter),
            sink: sink.clone(),
            clock: clock.clone(),
            archive_root: root.path().join("archive"),
            status_dir: root.path().join("status"),
            flush_secs: 2,
            status_secs: 1,
            declared,
            clipped: Clipped::Continuous,
            config_hash: "test".into(),
        });
        Fixture {
            capture,
            clock,
            sink,
            _root: root,
        }
    }

    fn take_reference(f: &mut Fixture, name: &str, file: &str) {
        let now = f.clock.now_micros();
        f.capture
            .take(reference_page(name, fixture_bytes(file), now))
            .unwrap();
    }

    fn fixture_bytes(file: &str) -> Vec<u8> {
        super::fixture(file)
    }

    fn declared(f: &mut Fixture) -> Vec<String> {
        f.capture
            .held_mut()
            .declared()
            .iter()
            .map(|s| s.ticker.to_string())
            .collect()
    }

    fn gaps(f: &Fixture) -> Vec<(String, Gap)> {
        f.sink
            .emitted()
            .into_iter()
            .filter_map(|e| match &e.event {
                Event::Gap(g) => Some((e.ticker().unwrap().to_string(), g.clone())),
                _ => None,
            })
            .collect()
    }

    #[test]
    fn an_option_that_expired_before_the_start_leaves_before_anything_is_asked() {
        let mut f = fixture();
        // The boot takes the reference answers before the first subscribe.
        take_reference(&mut f, LIVE, "get_instrument_live.json");
        take_reference(&mut f, EXPIRED, "get_instrument_expired.json");

        // It is not in the set the first subscribe is built from.
        let asked: Vec<String> = f
            .capture
            .held_mut()
            .converge()
            .to_subscribe
            .iter()
            .map(|s| s.ticker.to_string())
            .collect();
        assert!(!asked.contains(&EXPIRED.to_string()), "{asked:?}");
        assert!(asked.contains(&LIVE.to_string()));
        assert!(asked.contains(&ETH_CALL.to_string()));

        // Status says which left, why and when; and the others are declared.
        let status = f.capture.status(f.clock.now_micros());
        assert_eq!(status.left.len(), 1);
        assert_eq!(status.left[0].ticker.as_str(), EXPIRED);
        assert_eq!(status.left[0].reason, "expired");
        assert_eq!(status.left[0].at_micros, BETWEEN);
        assert_eq!(
            status.subs_declared, 6,
            "two options, two indexes and two perpetuals"
        );
        assert!(status.pairs.iter().all(|p| p.ticker.as_str() != EXPIRED));

        // Its reference data is in the record, stating it inactive.
        let recorded: Vec<bool> = f
            .sink
            .emitted()
            .into_iter()
            .filter(|e| e.ticker().is_some_and(|t| t.as_str() == EXPIRED))
            .filter_map(|e| match e.event {
                Event::Instrument(i) => Some(i.active),
                _ => None,
            })
            .collect();
        assert_eq!(recorded, [false]);
    }

    #[test]
    fn an_option_expires_while_running_and_the_others_keep_recording() {
        let mut f = fixture();
        take_reference(&mut f, LIVE, "get_instrument_live.json");
        f.capture
            .take_frame(&super::ticker_with("null", ("5", "2")))
            .unwrap();
        f.capture
            .take_frame(&fixture_bytes("ticker_two_sides.json"))
            .unwrap();

        // One second short of the venue's stated expiry: still in the set.
        f.clock.set(LIVE_EXPIRY - SEC);
        f.capture.retire_step(f.clock.now_micros());
        assert!(declared(&mut f).contains(&LIVE.to_string()));

        // At it: gone, on the loop's own clock, with no reference refresh.
        f.clock.set(LIVE_EXPIRY);
        f.capture.retire_step(f.clock.now_micros());
        assert!(!declared(&mut f).contains(&LIVE.to_string()));
        let status = f.capture.status(f.clock.now_micros());
        let left: Vec<(&str, &str, i64)> = status
            .left
            .iter()
            .map(|l| (l.ticker.as_str(), l.reason.as_str(), l.at_micros))
            .collect();
        assert_eq!(left, [(LIVE, "expired", LIVE_EXPIRY)]);

        // The other option is untouched, and still takes frames.
        f.capture
            .take_frame(&fixture_bytes("ticker_two_sides.json"))
            .unwrap();
        let status = f.capture.status(f.clock.now_micros());
        let eth = status
            .pairs
            .iter()
            .find(|p| p.ticker.as_str() == ETH_CALL)
            .unwrap();
        assert_eq!(eth.state, PairState::Live);
        assert_eq!(eth.count, 2);

        // Nothing was lost, so no gap was recorded for the one that left.
        assert!(gaps(&f).is_empty(), "{:?}", gaps(&f));
    }

    #[test]
    fn a_lost_connection_after_an_expiry_gaps_only_what_is_still_in_the_set() {
        let mut f = fixture();
        take_reference(&mut f, LIVE, "get_instrument_live.json");
        f.capture
            .take_frame(&super::ticker_with("null", ("5", "2")))
            .unwrap();
        f.capture
            .take_frame(&fixture_bytes("ticker_two_sides.json"))
            .unwrap();
        f.capture
            .take_frame(&fixture_bytes("spot_feed.json"))
            .unwrap();

        f.clock.set(LIVE_EXPIRY + SEC);
        f.capture.retire_step(f.clock.now_micros());
        f.clock.advance_secs(5);
        f.capture.session_lost(f.clock.now_micros());

        let gapped: Vec<String> = gaps(&f).into_iter().map(|(ticker, _)| ticker).collect();
        assert!(!gapped.contains(&LIVE.to_string()), "{gapped:?}");
        assert!(gapped.contains(&ETH_CALL.to_string()), "{gapped:?}");
        assert!(gapped.contains(&"BTC-USD".to_string()), "{gapped:?}");

        // And the reconnect asks for everything still in the set, and not it.
        f.capture.held_mut().connection_lost();
        let asked: Vec<String> = f
            .capture
            .held_mut()
            .converge()
            .to_subscribe
            .iter()
            .map(|s| s.ticker.to_string())
            .collect();
        assert!(!asked.contains(&LIVE.to_string()), "{asked:?}");
        assert!(asked.contains(&ETH_CALL.to_string()), "{asked:?}");
        assert!(asked.contains(&"BTC-USD".to_string()), "{asked:?}");
    }

    #[test]
    fn a_frame_for_an_option_that_left_is_recorded_and_is_not_coverage() {
        let mut f = fixture();
        take_reference(&mut f, LIVE, "get_instrument_live.json");
        f.clock.set(LIVE_EXPIRY);
        f.capture.retire_step(f.clock.now_micros());

        // The venue may keep sending. The bytes are archived as everything is.
        let before = f.capture.archive_next_seq();
        f.capture
            .take_frame(&super::ticker_with("null", ("5", "2")))
            .unwrap();
        assert!(f.capture.archive_next_seq() > before);

        let status = f.capture.status(f.clock.now_micros());
        assert!(status.pairs.iter().all(|p| p.ticker.as_str() != LIVE));
        assert_eq!(status.subs_held, 0);
    }

    #[test]
    fn an_option_the_venue_delists_before_its_expiry_is_no_longer_listed() {
        let mut f = fixture();
        // Inactive, and its stated expiry is still ahead of the clock.
        let bytes = String::from_utf8(fixture_bytes("get_instrument_live.json"))
            .unwrap()
            .replace("\"is_active\":true", "\"is_active\":false")
            .into_bytes();
        let now = f.clock.now_micros();
        f.capture.take(reference_page(LIVE, bytes, now)).unwrap();
        let status = f.capture.status(now);
        assert_eq!(status.left[0].reason, "no longer listed");
    }

    #[test]
    fn a_perpetual_the_venue_does_not_list_is_retired_before_anything_is_asked() {
        // What the boot does with a ticker the check handed back as absent.
        let mut f = fixture();
        let perpetual = Ticker::new("ETH-PERP").unwrap();
        let now = f.clock.now_micros();
        assert!(f.capture.retire(&perpetual, "not listed", now));

        let asked: Vec<String> = f
            .capture
            .held_mut()
            .converge()
            .to_subscribe
            .iter()
            .map(|s| s.ticker.to_string())
            .collect();
        assert!(!asked.contains(&"ETH-PERP".to_string()), "{asked:?}");
        // The options, the indexes and the other perpetual are all asked for.
        for kept in [LIVE, ETH_CALL, "ETH-USD", "BTC-USD", "BTC-PERP"] {
            assert!(asked.contains(&kept.to_string()), "{kept} in {asked:?}");
        }
        let status = f.capture.status(now);
        assert_eq!(status.left.len(), 1);
        assert_eq!(status.left[0].ticker.as_str(), "ETH-PERP");
        assert_eq!(status.left[0].reason, "not listed");
    }

    #[test]
    fn a_perpetual_that_goes_inactive_while_running_leaves_and_the_options_carry_on() {
        let mut f = fixture();
        f.capture
            .take_frame(&fixture_bytes("ticker_perp.json"))
            .unwrap();
        f.capture
            .take_frame(&fixture_bytes("ticker_two_sides.json"))
            .unwrap();

        let inactive = String::from_utf8(fixture_bytes("get_instrument_perp.json"))
            .unwrap()
            .replace("\"is_active\":true", "\"is_active\":false")
            .into_bytes();
        let now = f.clock.now_micros();
        f.capture
            .take(reference_page("BTC-PERP", inactive, now))
            .unwrap();

        let status = f.capture.status(now);
        let left: Vec<(&str, &str)> = status
            .left
            .iter()
            .map(|l| (l.ticker.as_str(), l.reason.as_str()))
            .collect();
        // It has no expiry to have passed, so it is not `expired`.
        assert_eq!(left, [("BTC-PERP", "no longer listed")]);

        f.capture
            .take_frame(&fixture_bytes("ticker_two_sides.json"))
            .unwrap();
        let status = f.capture.status(f.clock.now_micros());
        let eth = status
            .pairs
            .iter()
            .find(|p| p.ticker.as_str() == ETH_CALL)
            .unwrap();
        assert_eq!(eth.count, 2);
        assert!(gaps(&f).is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn a_reference_refresh_retires_what_the_venue_now_says_has_gone() {
        let mut f = fixture();
        take_reference(&mut f, LIVE, "get_instrument_live.json");
        take_reference(&mut f, EXPIRED, "get_instrument_expired.json");

        let asked: Arc<Mutex<Vec<Vec<String>>>> = Arc::default();
        let seen = asked.clone();
        let every = REFERENCE_INTERVAL_MICROS;
        f.capture.refresh_reference_with(every, move |symbols| {
            seen.lock().unwrap().push(symbols.clone());
            async move {
                // The venue now says the live one is inactive too.
                symbols
                    .into_iter()
                    .map(|name| {
                        let bytes = String::from_utf8(super::fixture("get_instrument_live.json"))
                            .unwrap()
                            .replace(LIVE, &name)
                            .replace("\"is_active\":true", "\"is_active\":false")
                            .into_bytes();
                        Ok(reference_page(&name, bytes, BETWEEN + every))
                    })
                    .collect()
            }
        });

        // Not due yet: nothing is asked.
        f.capture.reference_step(f.clock.now_micros()).unwrap();
        assert!(asked.lock().unwrap().is_empty());

        // Due: asked about what is still in the set — never the one that left.
        f.clock.advance(every);
        f.capture.reference_step(f.clock.now_micros()).unwrap();
        for _ in 0..4 {
            tokio::task::yield_now().await;
        }
        assert_eq!(
            asked.lock().unwrap().as_slice(),
            [vec![
                LIVE.to_string(),
                ETH_CALL.to_string(),
                "BTC-PERP".to_string(),
                "ETH-PERP".to_string()
            ]]
        );

        // Taken through the one path on the next step.
        f.capture.reference_step(f.clock.now_micros()).unwrap();
        let status = f.capture.status(f.clock.now_micros());
        let left: Vec<&str> = status.left.iter().map(|l| l.ticker.as_str()).collect();
        // The option that had expired, the two the venue now says are gone,
        // and the two perpetuals it said the same of.
        assert_eq!(left.len(), 5, "{left:?}");
        assert!(left.contains(&LIVE) && left.contains(&ETH_CALL));
        assert!(left.contains(&"BTC-PERP") && left.contains(&"ETH-PERP"));
        // The indexes stay: an index is not an instrument that can leave.
        assert_eq!(status.subs_declared, 2);
    }
}

// ---- the underlying's perpetual --------------------------------------------

#[test]
fn a_perpetuals_ticker_is_a_quote_a_mark_and_a_funding_rate() {
    let adapter = shipped();
    let payload = adapter.classify(&fixture("ticker_perp.json"), 3);
    assert_eq!(payload.symbol.as_deref(), Some("BTC-PERP"));
    let events = events(&adapter, &payload);
    let kinds: Vec<Kind> = events.iter().map(|e| e.2.kind()).collect();
    // No option pricing: the venue states `null` for a perpetual.
    assert_eq!(kinds, [Kind::Quotes, Kind::Marks, Kind::Funding]);
    for (ticker, at, _) in &events {
        assert_eq!(ticker.as_str(), "BTC-PERP");
        assert_eq!(*at, Some(1_795_564_705_600_000));
    }
    assert_eq!(
        events[0].2,
        Event::Quote(Quote {
            bid_px: num("59734.2"),
            bid_sz: num("1.0835"),
            ask_px: num("59734.3"),
            ask_sz: num("0.0169"),
            bid_spread: None,
            ask_spread: None,
        })
    );
    assert_eq!(
        events[1].2,
        Event::Mark(Mark {
            mark: num("59725.9"),
            index: num("59735.9"),
            open_interest: num("580.248"),
            ..Mark::default()
        })
    );
    // The rate as the venue printed it. The ticker states no next funding
    // time and no premium, and neither is invented.
    assert_eq!(
        events[2].2,
        Event::Funding(galata_wire::Funding {
            rate: Num::from_str("0.000012500").unwrap(),
            next_micros: None,
            premium: None,
        })
    );
}

#[test]
fn an_options_ticker_states_no_funding_rate_and_writes_none() {
    let adapter = shipped();
    for name in ["ticker_two_sides.json", "ticker_empty_side.json"] {
        let events = events(&adapter, &adapter.classify(&fixture(name), 0));
        assert!(events.iter().all(|e| e.2.kind() != Kind::Funding), "{name}");
    }
}

#[test]
fn a_perpetuals_reference_data_states_a_perpetual_and_no_option_terms() {
    let adapter = shipped();
    let perpetual = instrument(&adapter, "BTC-PERP", "get_instrument_perp.json");
    assert_eq!(perpetual.contract_type, "perp");
    assert_eq!(perpetual.base, "BTC");
    assert_eq!(perpetual.quote, "USDC");
    assert!(perpetual.active);
    assert_eq!(perpetual.tick_size, Num::from_str("0.1").unwrap());
    assert_eq!(perpetual.min_size, Num::from_str("0.01").unwrap());
    assert_eq!(perpetual.strike, None);
    assert_eq!(perpetual.expiry_micros, None);
    assert_eq!(perpetual.right, None);
}

#[test]
fn derives_reference_data_names_itself_and_states_no_more_than_the_venue_does() {
    // `normalise-instrument-reference`: the venue's own name beside the
    // ticker; a fixed tick and a size minimum, so no figure limit, no value
    // minimum, no leverage and no dex.
    let adapter = shipped();
    for (name, file) in [
        ("BTC-PERP", "get_instrument_perp.json"),
        (LIVE, "get_instrument_live.json"),
    ] {
        let row = instrument(&adapter, name, file);
        assert_eq!(row.venue_symbol.as_deref(), Some(name));
        assert_eq!(row.dex, None, "{name}");
        assert_eq!(row.price_figures, None, "{name}");
        assert_eq!(row.min_notional, None, "{name}");
        assert_eq!(row.max_leverage, None, "{name}");
        assert_eq!(row.isolated_only, None, "{name}");
        assert!(row.price_is_valid(row.tick_size * Num::from(7)), "{name}");
    }
}

#[test]
fn a_perpetual_is_not_a_name_an_operator_may_declare() {
    // It follows from the set. Declared, it is refused as any name that is
    // not an option is, at load and at construction.
    assert!(OptionName::parse("BTC-PERP").is_err());
    assert!(
        DeriveXyz::new(
            Config {
                market: Market::Mainnet,
                instruments: vec!["BTC-PERP".into()],
            },
            None,
        )
        .is_err()
    );
}

#[test]
fn a_perpetual_at_boot_is_live_inactive_or_absent_and_only_silence_refuses() {
    assert_eq!(
        perpetual_standing("BTC-PERP", &fixture("get_instrument_perp.json")),
        Ok(PerpetualStanding::Live)
    );
    let inactive = String::from_utf8(fixture("get_instrument_perp.json"))
        .unwrap()
        .replace("\"is_active\":true", "\"is_active\":false");
    assert_eq!(
        perpetual_standing("BTC-PERP", inactive.as_bytes()),
        Ok(PerpetualStanding::Inactive)
    );
    // The venue has no perpetual of this name: the options start without it.
    assert_eq!(
        perpetual_standing("XYZ-PERP", &fixture("get_instrument_unknown.json")),
        Ok(PerpetualStanding::Absent)
    );
    // The venue did not answer at all: that is not an answer of no.
    let refusal = perpetual_standing("BTC-PERP", b"<html>502</html>").unwrap_err();
    assert!(refusal.contains("BTC-PERP"), "{refusal}");
}

// ---- the one path, and the tape -------------------------------------------

/// Through `ingest`, as every payload goes, and from the record back onto the
/// tape, as a rebuild goes.
mod one_path {
    use galata_wire::Event;

    use super::*;
    use crate::ingest::ingest;
    use crate::record::Archive;
    use crate::sink::testing::RecordingSink;
    use crate::tape::rebuild;

    const DAY: i64 = 86_400_000_000;

    fn frames() -> Vec<Vec<u8>> {
        vec![
            fixture("ticker_two_sides.json"),
            fixture("ticker_empty_side.json"),
            fixture("spot_feed.json"),
            fixture("subscribe_ack.json"),
        ]
    }

    /// Every fixture through the one path, into a fresh record.
    fn ingested(dir: &Path) -> Vec<galata_wire::Envelope> {
        let adapter = shipped();
        let sink = RecordingSink::default();
        let mut archive = Archive::open(dir.join("archive"));
        for (i, bytes) in frames().iter().enumerate() {
            let payload = adapter.classify(bytes, 20_000 * DAY + i as i64);
            ingest(&mut archive, &adapter, &sink, payload).unwrap();
        }
        let now = 20_000 * DAY + 10;
        ingest(
            &mut archive,
            &adapter,
            &sink,
            reference_page(LIVE, fixture("get_instrument_live.json"), now),
        )
        .unwrap();
        archive.flush().unwrap();
        sink.emitted()
    }

    #[test]
    fn the_same_payloads_are_the_same_envelopes_on_every_run() {
        let (a, b) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
        let (first, second) = (ingested(a.path()), ingested(b.path()));
        assert_eq!(first, second);
        // Two tickers of three events, one index, one instrument, and nothing
        // for the acknowledgement.
        assert_eq!(first.len(), 3 + 3 + 1 + 1);
        assert!(first.iter().all(|e| e.venue().unwrap().as_str() == VENUE));
    }

    #[test]
    fn a_payload_that_does_not_parse_is_an_unparsed_row_and_the_rest_carry_on() {
        let dir = tempfile::tempdir().unwrap();
        let adapter = shipped();
        let sink = RecordingSink::default();
        let mut archive = Archive::open(dir.path().join("archive"));

        let whole = fixture("ticker_two_sides.json");
        let mut payload = adapter.classify(&whole, 1);
        payload.payload.truncate(whole.len() / 2);
        let result = ingest(&mut archive, &adapter, &sink, payload).unwrap();
        assert!(result.unparsed);

        // The next payload is taken as though nothing had happened.
        let good = adapter.classify(&fixture("ticker_empty_side.json"), 2);
        let result = ingest(&mut archive, &adapter, &sink, good).unwrap();
        assert!(!result.unparsed);
        assert_eq!(result.emitted, 3);

        let unparsed: Vec<_> = sink
            .emitted()
            .into_iter()
            .filter_map(|e| match e.event {
                Event::Unparsed(u) => Some(u),
                _ => None,
            })
            .collect();
        assert_eq!(unparsed.len(), 1);
        assert_eq!(unparsed[0].channel, wire::TICKER);
    }

    fn tape_files(root: &Path) -> Vec<(String, Vec<u8>)> {
        fn walk(dir: &Path, root: &Path, out: &mut Vec<(String, Vec<u8>)>) {
            let Ok(entries) = std::fs::read_dir(dir) else {
                return;
            };
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    walk(&path, root, out);
                } else if path.extension().is_some_and(|e| e == "parquet") {
                    out.push((
                        path.strip_prefix(root)
                            .unwrap()
                            .to_string_lossy()
                            .to_string(),
                        std::fs::read(&path).unwrap(),
                    ));
                }
            }
        }
        let mut out = Vec::new();
        walk(root, root, &mut out);
        out.sort();
        out
    }

    #[test]
    fn a_rebuild_writes_the_option_datasets_and_writes_them_the_same_twice() {
        let dir = tempfile::tempdir().unwrap();
        ingested(dir.path());
        let adapter = shipped();
        let archive = dir.path().join("archive");

        let report = rebuild(
            &archive,
            &dir.path().join("tape"),
            &adapter,
            None,
            i64::MIN,
            i64::MAX,
        )
        .unwrap();
        assert_eq!(report.unparsed, 0, "{}", report.report());
        assert_eq!(report.rows, 8, "{}", report.report());

        let first = tape_files(&dir.path().join("tape"));
        let datasets: std::collections::BTreeSet<&str> = first
            .iter()
            .map(|(path, _)| path.split('/').next().unwrap())
            .collect();
        assert_eq!(
            datasets.into_iter().collect::<Vec<_>>(),
            [
                "kind=instruments",
                "kind=marks",
                "kind=option_pricing",
                "kind=quotes"
            ]
        );

        // The one format reaches the tape through the rebuild: every
        // instrument row names the venue's own symbol, which is its ticker
        // here (`normalise-instrument-reference`).
        {
            use arrow::array::{Array, StringArray};
            let reader =
                crate::tape::Reader::open(dir.path().join("tape"), &["kind=instruments"]).unwrap();
            let batches = reader
                .view(crate::tape::Window {
                    kind: galata_wire::Kind::Instruments,
                    from_micros: i64::MIN,
                    to_micros: i64::MAX,
                    ticker: None,
                })
                .unwrap();
            let mut rows = 0;
            for batch in &batches {
                let text = |name: &str| {
                    batch
                        .column_by_name(name)
                        .unwrap()
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .unwrap()
                        .clone()
                };
                let (tickers, symbols) = (text("ticker"), text("venue_symbol"));
                for i in 0..batch.num_rows() {
                    assert!(!symbols.is_null(i));
                    assert_eq!(symbols.value(i), tickers.value(i));
                    rows += 1;
                }
            }
            assert!(rows > 0, "the rebuild wrote no instrument row");
        }

        // The tape is a cache: a second rebuild from the same record, into an
        // empty tape, is the same files with the same bytes.
        rebuild(
            &archive,
            &dir.path().join("again"),
            &adapter,
            None,
            i64::MIN,
            i64::MAX,
        )
        .unwrap();
        assert_eq!(first, tape_files(&dir.path().join("again")));

        // The layout check knows the new dataset, and still refuses one that
        // is not a dataset at all.
        assert_eq!(
            crate::tape::check_layout(&dir.path().join("tape")),
            Vec::new()
        );
        std::fs::create_dir_all(dir.path().join("tape/kind=option_greeks/date=2026-11-24"))
            .unwrap();
        let problems = crate::tape::check_layout(&dir.path().join("tape"));
        assert!(
            problems
                .iter()
                .any(|p| matches!(p, crate::tape::LayoutProblem::UnknownDataset { name, .. } if name == "option_greeks")),
            "{problems:?}"
        );
    }
}
