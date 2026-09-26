//! Run a command with named secrets from the vault in its environment —
//! **read through a token**.
//!
//! ```text
//!   galata-vault-exec --only NAME [--only NAME ...] -- <command> [args ...]
//!   galata-vault-exec --expiry
//! ```
//!
//! **Why this exists beside `gv run`.** `gv run` is the owner's tool: it reads
//! through the owner key and the local project registry. On a machine that
//! holds the owner key — the operator's own, where these services run — a
//! service started with `gv run` could be handed every secret in the vault,
//! whatever token it was meant to use. Measured on 2026-09-25: the tower's
//! token, restricted to the reader's password, "read" capture's through
//! `gv run`, because the owner key answered instead. This reads through the
//! SDK, which uses the token alone, so the vault's own restriction is the
//! one that holds.
//!
//! **`exec`**, so the command replaces this process: a service manager's
//! SIGTERM reaches the service itself, and nothing resident holds the values.
//!
//! **The token's expiry is said before it bites.** Tokens are minted at the
//! vault's 365-day maximum; one that lapses is a service that cannot restart.
//! Inside [`TOKEN_WARNING_DAYS`] this writes one line to stderr — the
//! service's log under launchd — and starts the service anyway: a valid token
//! is never refused. `--expiry` prints `<unix> <YYYY-MM-DD> <days> <state>`
//! and starts nothing, for `install-services.sh --status`.
//!
//! Exit codes: `2` bad arguments, `1` the vault refused a name or could not be
//! reached, and otherwise whatever the command exits with.

use std::os::unix::process::CommandExt;
use std::process::{Command, ExitCode};
use std::time::{SystemTime, UNIX_EPOCH};

use galata_datawatch_vault::{TOKEN_WARNING_DAYS, TokenNotice, token_notice, utc_date};

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args == ["--expiry"] {
        return expiry();
    }
    let Some(split) = args.iter().position(|a| a == "--") else {
        return usage();
    };
    let (flags, command) = (&args[..split], &args[split + 1..]);
    let mut names = Vec::new();
    let mut flags = flags.iter();
    while let Some(flag) = flags.next() {
        match (flag.as_str(), flags.next()) {
            ("--only", Some(name)) => names.push(name.clone()),
            _ => return usage(),
        }
    }
    if names.is_empty() || command.is_empty() {
        return usage();
    }

    let vault = match galata_vault::Vault::from_env() {
        Ok(vault) => vault,
        Err(error) => {
            eprintln!("galata-vault-exec: the vault would not open: {error}");
            return ExitCode::from(1);
        }
    };
    if let Some(expires_at) = vault.expiry().token_expires_at
        && let TokenNotice::Soon { days } = token_notice(expires_at, now())
    {
        eprintln!(
            "galata-vault-exec: WARNING this service's token expires on {} ({days} days, inside \
             {TOKEN_WARNING_DAYS}); after that the service cannot restart. Re-mint with \
             scripts/mint-service-tokens.sh, then scripts/install-services.sh",
            utc_date(expires_at)
        );
    }
    let environment = match galata_datawatch_vault::environment_for(&vault, &names) {
        Ok(environment) => environment,
        Err(error) => {
            eprintln!("galata-vault-exec: {error}");
            return ExitCode::from(1);
        }
    };
    // The vault's own variables pass through unnamed: naming how a vault
    // client authenticates is the vault's rule (check-secret-reach.sh). They
    // point at this service's own token, which can read nothing else.
    let error = Command::new(&command[0])
        .args(&command[1..])
        .envs(environment)
        .exec();
    eprintln!("galata-vault-exec: could not run {}: {error}", command[0]);
    ExitCode::from(1)
}

/// `--expiry`: the token's expiry as one machine line, or the vault's reason.
fn expiry() -> ExitCode {
    let vault = match galata_vault::Vault::from_env() {
        Ok(vault) => vault,
        Err(error) => {
            eprintln!("galata-vault-exec: the vault would not open: {error}");
            return ExitCode::from(1);
        }
    };
    // Refused by name rather than guessed: a token-opened handle always knows
    // its expiry, so its absence means something other than a service token.
    let Some(expires_at) = vault.expiry().token_expires_at else {
        eprintln!("galata-vault-exec: this handle carries no token expiry — not a service token");
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

fn usage() -> ExitCode {
    eprintln!(
        "usage: galata-vault-exec --only NAME [--only NAME ...] -- <command> [args ...]\n       \
         galata-vault-exec --expiry"
    );
    ExitCode::from(2)
}
