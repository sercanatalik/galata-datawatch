//! Say when this service's vault token expires, and what that means now.
//!
//! ```text
//!   galata-token-expiry
//! ```
//!
//! Prints `<unix> <YYYY-MM-DD> <days> <state>` and starts nothing. `<state>`
//! is `ok`, `WARN` inside [`galata_datawatch_vault::TOKEN_WARNING_DAYS`], or `EXPIRED`. The token is
//! the one the vault's own environment contract names; this never says how.
//!
//! **What is left of `galata-vault-exec`.** That binary also started the
//! service with its secrets in the environment, because `gv run` could not be
//! trusted to: given a token file it read through the owner key instead. From
//! galata-vault 0.5.0 `gv run` uses the token or refuses, so
//! `scripts/run-service.sh` execs it, and the half that was a second
//! implementation of `gv run` is gone. This half stays because the judgement
//! is this deployment's and not the vault's: the vault knows when a token
//! lapses, and only an operator who re-mints by hand, once a year, knows that
//! thirty days is when to start saying so.
//!
//! **Tokens are minted at the vault's 365-day maximum; one that lapses is a
//! service that cannot restart.** `run-service.sh` asks this before every
//! start — refusing a lapsed token by name, and logging one line for one
//! inside the window — and `install-services.sh --status` asks it for every
//! token.
//!
//! Exit codes: `0` with the line, `2` bad arguments, `1` the vault would not
//! open or the handle is not a service token's.

use std::process::ExitCode;
use std::time::{SystemTime, UNIX_EPOCH};

use galata_datawatch_vault::{token_notice, utc_date};

fn main() -> ExitCode {
    if std::env::args().len() != 1 {
        eprintln!("usage: galata-token-expiry");
        return ExitCode::from(2);
    }
    let vault = match galata_vault::Vault::from_env() {
        Ok(vault) => vault,
        Err(error) => {
            eprintln!("galata-token-expiry: the vault would not open: {error}");
            return ExitCode::from(1);
        }
    };
    // Refused by name rather than guessed: a token-opened handle always knows
    // its expiry, so its absence means something other than a service token.
    let Some(expires_at) = vault.expiry().token_expires_at else {
        eprintln!("galata-token-expiry: this handle carries no token expiry — not a service token");
        return ExitCode::from(1);
    };
    let notice = token_notice(expires_at, now());
    println!(
        "{expires_at} {} {} {}",
        utc_date(expires_at),
        notice.days(),
        notice.word()
    );
    ExitCode::SUCCESS
}

/// The one clock this binary reads.
fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_secs()).unwrap_or(i64::MAX))
}
