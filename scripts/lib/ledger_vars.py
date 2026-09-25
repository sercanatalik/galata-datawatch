"""The vault names one venue's ledger may read, one per line.

    python3 scripts/lib/ledger_vars.py <config.toml> <venue>

Each declared account's `address_var` on that venue, then the fingerprint key.
Read from the deployment's configuration so the token minted for the ledger
(`mint-service-tokens.sh`) and the names the service asks for
(`run-service.sh`) come from one place and cannot disagree. Names only: the
configuration holds no address, and `Config::validate` refuses one.

Exits 3, printing nothing, when the configuration declares no ledger for the
venue — a clean *nothing to mint*, as the maintenance tools use 3.
"""

import pathlib
import sys
import tomllib

config = tomllib.loads(pathlib.Path(sys.argv[1]).read_text())
venue = sys.argv[2]
ledger = config.get("ledger")
if not ledger:
    sys.exit(3)
names = [
    account["address_var"]
    for account in ledger.get("account", {}).values()
    if account.get("venue") == venue
]
if not names:
    sys.exit(3)
print("\n".join(sorted(set(names)) + [ledger["fingerprint_key_var"]]))
