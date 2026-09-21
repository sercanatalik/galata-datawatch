//! Generate the server's authorization fragment.
//!
//! ```text
//! cargo run -p galata-broker --example grants -- hyperliquid rh-chain > config/nats-authorization.conf
//! ```
//!
//! The output holds **no secret** — every password is an environment
//! reference — so it is committed and read by anyone with the repository.
fn main() {
    let venues: Vec<String> = std::env::args().skip(1).collect();
    let venues: Vec<&str> = venues.iter().map(String::as_str).collect();
    if venues.is_empty() {
        eprintln!("usage: grants <venue>...");
        std::process::exit(2);
    }
    print!(
        "{}",
        galata_broker::to_nats_config(&galata_broker::grants::table(&venues))
    );
}
