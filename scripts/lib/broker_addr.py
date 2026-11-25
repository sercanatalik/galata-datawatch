"""Where the deployment's configuration says the broker answers, as `host:port`.

    python3 scripts/lib/broker_addr.py <config.toml>

Read from `[broker] url`, the one place a deployment states it, so that the
tower is pointed at the broker capture publishes to and the two cannot
disagree. The address was once fixed here as `127.0.0.1:4222`; a broker
reached by the host's tailnet name answers nowhere on loopback.

Exits 3, printing nothing, when the configuration declares no broker: a clean
*no broker here*, as the maintenance tools use 3.
"""

import pathlib
import sys
import tomllib
import urllib.parse

config = tomllib.loads(pathlib.Path(sys.argv[1]).read_text())
url = (config.get("broker") or {}).get("url")
if not url:
    sys.exit(3)
parts = urllib.parse.urlsplit(url if "://" in url else f"nats://{url}")
if not parts.hostname:
    sys.exit(f"{sys.argv[1]}: [broker] url = {url!r} names no host")
print(f"{parts.hostname}:{parts.port or 4222}")
