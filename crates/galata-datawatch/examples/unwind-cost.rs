//! **What does the panic boundary cost the one path?**
//!
//! ```text
//! cargo run --release --example unwind-cost -- <archive-root>
//! ```
//!
//! `ingest` normalises inside `catch_unwind` so an adapter meeting a shape it
//! was not written for costs a parse and never the bytes. That boundary sits on
//! the hottest path in the system, and the roadmap has carried it as an open
//! question since Tier 1 on the strength of a Servo profile — which predates
//! the change that let LLVM inline the try closure into the happy path.
//!
//! **A profile of another program is a hypothesis here, not a result.** So this
//! normalises real archived payloads, the same payloads, with and without the
//! boundary, and reports the difference.

use std::hint::black_box;
use std::path::Path;
use std::time::Instant;

use galata_datawatch::adapters::rh_chain::{self, RhChain};
use galata_datawatch::normalise::Normalise;
use galata_datawatch::record::Payload;
use galata_wire::Origin;

/// The same boundary `ingest::catch_normalise` puts round a normalise.
fn caught(normaliser: &dyn Normalise, payload: &Payload) -> usize {
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        normaliser.normalise(payload)
    })) {
        Ok(Ok(events)) => events.len(),
        _ => 0,
    }
}

/// The same work with nothing round it.
fn bare(normaliser: &dyn Normalise, payload: &Payload) -> usize {
    normaliser.normalise(payload).map(|e| e.len()).unwrap_or(0)
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let root = std::env::args()
        .nth(1)
        .ok_or("usage: unwind-cost <archive-root>")?;

    let archived = archived(Path::new(&root))?;
    if archived.is_empty() {
        return Err(format!("no transfer payloads under {root}").into());
    }
    // **Two shapes, because the question is about a per-CALL cost.**
    //
    // A chain response is one huge payload — 26 MB of JSON — so the boundary's
    // fixed cost is divided by an enormous amount of work and vanishes whether
    // it exists or not. The concern the roadmap recorded is the opposite shape:
    // Hyperliquid pushes a candle on EVERY update, so the one path is called
    // constantly with a small frame. That is where a per-call cost would show,
    // and it is the shape that has to be measured.
    let split = into_single_log_responses(&archived);
    let shapes: Vec<(&str, Vec<Payload>)> = vec![
        ("as archived (one huge response per call)", archived),
        ("split (one log per call)", split),
    ];

    let chain = RhChain::new(rh_chain::Config {
        rpc_url: galata_datawatch::venue::Endpoint::public(rh_chain::PUBLIC_RPC),
        instruments: vec![
            rh_chain::Instrument {
                ticker: "NVDA".into(),
                contract: "0xd0601ce157db5bdc3162bbac2a2c8af5320d9eec".into(),
                decimals: 18,
            },
            rh_chain::Instrument {
                ticker: "WETH".into(),
                contract: "0x0bd7d308f8e1639fab988df18a8011f41eacad73".into(),
                decimals: 18,
            },
        ],
    })?;

    for (label, payloads) in &shapes {
        if payloads.is_empty() {
            continue;
        }
        let bytes: usize = payloads.iter().map(|p| p.payload.len()).sum();

        // Warm: the first pass pays for page faults and branch prediction, and
        // whichever arm ran first would otherwise look slower than it is.
        for payload in payloads {
            black_box(caught(&chain, payload));
            black_box(bare(&chain, payload));
        }

        let rounds = if payloads.len() > 1_000 { 5 } else { 20 };
        let mut events = 0usize;
        println!();
        println!("{label}");
        println!(
            "  {} payloads, {:.2} MiB, {rounds} rounds each",
            payloads.len(),
            bytes as f64 / 1_048_576.0
        );

        // **The arms ALTERNATE which goes first.**
        //
        // Interleaving within a round is not enough, and getting this wrong
        // produced a false result here: running `caught` first every round
        // leaves the allocator's free lists warm for `bare`, which then looks
        // 30% faster. An isolated boundary measured 0–17 ns per call, so a
        // 400 ns difference could only have been the harness. Alternating
        // gives each arm the cold half of the rounds.
        let (mut caught_ns, mut bare_ns) = (0u128, 0u128);
        for round in 0..rounds {
            let mut run = |first: bool| {
                let started = Instant::now();
                for payload in payloads {
                    events += black_box(if first {
                        caught(&chain, payload)
                    } else {
                        bare(&chain, payload)
                    });
                }
                started.elapsed().as_nanos()
            };
            if round % 2 == 0 {
                caught_ns += run(true);
                bare_ns += run(false);
            } else {
                bare_ns += run(false);
                caught_ns += run(true);
            }
        }

        let per = |ns: u128| ns as f64 / rounds as f64 / 1_000_000.0;
        let (with, without) = (per(caught_ns), per(bare_ns));
        println!("  with catch_unwind : {with:>9.2} ms per pass");
        println!("  without           : {without:>9.2} ms per pass");
        println!(
            "  difference        : {:>9.2} ms ({:+.2}%)",
            with - without,
            (with - without) * 100.0 / without
        );
        println!(
            "  per call          : {:>9.2} ns",
            (with - without) * 1_000_000.0 / payloads.len() as f64
        );
        let _ = events;
    }
    Ok(())
}

/// One archived response split into one response per log.
///
/// **The frame shape a pushing venue has**, built from real bytes rather than
/// invented: each is a legal `eth_getLogs` result carrying a single entry, so
/// the normaliser does the same work it always does, just far less of it per
/// call.
fn into_single_log_responses(payloads: &[Payload]) -> Vec<Payload> {
    let mut out = Vec::new();
    for payload in payloads {
        let Ok(serde_json::Value::Array(logs)) =
            serde_json::from_slice::<serde_json::Value>(&payload.payload)
        else {
            continue;
        };
        for log in logs {
            let Ok(bytes) = serde_json::to_vec(&vec![log]) else {
                continue;
            };
            out.push(Payload {
                payload: bytes,
                ..payload.clone()
            });
        }
        // Enough to measure a per-call cost without turning a measurement into
        // a soak.
        if out.len() >= 20_000 {
            break;
        }
    }
    out
}

/// Real archived payloads, read back out of the record.
fn archived(root: &Path) -> Result<Vec<Payload>, Box<dyn std::error::Error>> {
    let mut files = Vec::new();
    collect(root, &mut files);
    files.sort();
    let mut out = Vec::new();
    for path in files {
        if !path.to_string_lossy().contains("kind=transfers") {
            continue;
        }
        for batch in galata_segments::read_segment(&path)? {
            out.extend(payloads_of(&batch));
        }
    }
    Ok(out)
}

fn payloads_of(batch: &arrow::record_batch::RecordBatch) -> Vec<Payload> {
    use arrow::array::{Array, BinaryArray, Int64Array};
    let Some(column) = batch
        .column_by_name("payload")
        .and_then(|c| c.as_any().downcast_ref::<BinaryArray>())
    else {
        return Vec::new();
    };
    let recv = batch
        .column_by_name("recv_micros")
        .and_then(|c| c.as_any().downcast_ref::<Int64Array>());
    (0..batch.num_rows())
        .filter(|i| !column.is_null(*i))
        .map(|i| Payload {
            seq: 0,
            recv_micros: recv.map(|r| r.value(i)).unwrap_or(0),
            address: galata_datawatch::record::PayloadAddress::Venue(rh_chain::VENUE.into()),
            channel: "eth_getLogs".into(),
            kind: galata_wire::Series::Transfers.as_str().to_string(),
            symbol: None,
            origin: Origin::Fetched,
            payload: column.value(i).to_vec(),
        })
        .collect()
}

fn collect(dir: &Path, out: &mut Vec<std::path::PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            collect(&path, out);
        } else if path.extension().is_some_and(|e| e == "parquet") {
            out.push(path);
        }
    }
}
