"""The vault names one venue's ledger may read, one per line.

    python3 scripts/lib/ledger_vars.py <config.toml> <venue>
    python3 scripts/lib/ledger_vars.py <config.toml> --venues

Each declared account's `address_var` on that venue, and its `session_key_var`
where it has one, then the fingerprint key, then — where the configuration
declares a `[broker]` — the ledger's own broker password, with which it
publishes the venue's accounts (add-realtime-account-ledger, design D6).
Read from the deployment's configuration so the token minted for the ledger
(`mint-service-tokens.sh`) and the names the service asks for
(`run-service.sh`) come from one place and cannot disagree. Names only: the
configuration holds no address, and `Config::validate` refuses one.

`--venues` prints instead every venue a declared account names, once each: the
ledgers the deployment keeps, whether or not the venue has a `[venue.…]` table.
A ledger-only venue has none (add-derive-account-ledger, design D7), so a
script that took its venues from those tables would miss its ledger.

Exits 3, printing nothing, when the configuration declares no ledger for the
venue, or none at all — a clean *nothing to mint*, as the maintenance tools
use 3.
"""

import pathlib
import sys
import tomllib

config = tomllib.loads(pathlib.Path(sys.argv[1]).read_text())
venue = sys.argv[2]
ledger = config.get("ledger")
if not ledger:
    sys.exit(3)
if venue == "--venues":
    venues = sorted({account["venue"] for account in ledger.get("account", {}).values()})
    if not venues:
        sys.exit(3)
    print("\n".join(venues))
    sys.exit(0)
names = [
    var
    for account in ledger.get("account", {}).values()
    if account.get("venue") == venue
    for var in (account["address_var"], account.get("session_key_var"))
    if var
]
if not names:
    sys.exit(3)
names = sorted(set(names)) + [ledger["fingerprint_key_var"]]
if config.get("broker"):
    # As `galata_broker::password_var(&ledger_identity(venue))` spells it.
    names.append("GALATA_BROKER_PASSWORD_LEDGER_" + venue.replace("-", "_").upper())
print("\n".join(names))
