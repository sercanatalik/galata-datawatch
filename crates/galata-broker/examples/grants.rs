//! Generate the server's authorization fragment.
//!
//! ```text
//! cargo run -p galata-broker --example grants -- hyperliquid rh-chain derive-xyz \
//!     --ledgers hyperliquid derive-xyz > config/nats-authorization.conf
//! ```
//!
//! The capturing venues first, then after `--ledgers` the venues whose ledger
//! publishes its accounts. The output holds **no secret** — every password is
//! an environment reference — so it is committed and read by anyone with the
//! repository.
fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let split = args.iter().position(|a| a == "--ledgers");
    let (venues, ledgers) = match split {
        Some(i) => (&args[..i], &args[i + 1..]),
        None => (&args[..], &args[..0]),
    };
    let venues: Vec<&str> = venues.iter().map(String::as_str).collect();
    let ledgers: Vec<&str> = ledgers.iter().map(String::as_str).collect();
    if venues.is_empty() {
        eprintln!("usage: grants <venue>... [--ledgers <venue>...]");
        std::process::exit(2);
    }
    print!(
        "{}",
        galata_broker::to_nats_config(&galata_broker::grants::table(&venues, &ledgers))
    );
}
