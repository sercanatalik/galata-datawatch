//! Run a command with named secrets from the vault in its environment —
//! **read through a token**.
//!
//! ```text
//!   galata-vault-exec --only NAME [--only NAME ...] -- <command> [args ...]
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
//! Exit codes: `2` bad arguments, `1` the vault refused a name or could not be
//! reached, and otherwise whatever the command exits with.

use std::os::unix::process::CommandExt;
use std::process::{Command, ExitCode};

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
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

fn usage() -> ExitCode {
    eprintln!("usage: galata-vault-exec --only NAME [--only NAME ...] -- <command> [args ...]");
    ExitCode::from(2)
}
