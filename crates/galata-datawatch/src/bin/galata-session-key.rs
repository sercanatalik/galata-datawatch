//! Make a Derive session key for the ledger.
//!
//! **Its private half goes to standard output, which must not be a terminal**:
//! `scripts/make-derive-session-key.sh` pipes it straight into `gv set`, so
//! no argument, file or screen ever holds it. Its address goes to standard
//! error, for the owner to register on the wallet with no scope.
use std::io::IsTerminal;
use std::process::ExitCode;

fn main() -> ExitCode {
    if std::env::args().len() > 1 {
        eprintln!("usage: galata-session-key | gv set <NAME> --env <project/section>");
        return ExitCode::FAILURE;
    }
    if std::io::stdout().is_terminal() {
        eprintln!(
            "galata-session-key: refusing to write a private key to a terminal; pipe it into \
             `gv set` (scripts/make-derive-session-key.sh)"
        );
        return ExitCode::FAILURE;
    }
    // The private half goes from the key's maker to the pipe; this file
    // never holds it.
    let address = match galata_datawatch::adapters::derive_xyz::sign::generate_into(
        &mut std::io::stdout().lock(),
    ) {
        Ok(address) => address,
        Err(error) => {
            eprintln!("galata-session-key: no key was handed on ({error}); nothing was kept");
            return ExitCode::FAILURE;
        }
    };
    eprintln!("the session key's address: {address}");
    ExitCode::SUCCESS
}
