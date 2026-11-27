"""Record one start of a service in the status folder.

    python3 scripts/lib/start_record.py <status folder> <service> started
    python3 scripts/lib/start_record.py <status folder> <service> refused <reason>

Appends the start, with its time and outcome and, for a refusal, its reason, to
`<status folder>/service-<service>.json` (a `:` in the service becomes `-`),
and keeps the ten most recent: enough to tell a service that is restarting in
a loop, and nothing else (add-service-health-monitoring, design D5). Written
through a temporary file and a rename, as every status file is, so a reader
never sees half of one.

The launcher, scripts/run-service.sh, calls it just before it hands over to
the service, and in `refuse`; never under `--check`.
"""

import json
import os
import pathlib
import sys
import time

KEPT = 10

folder, service, outcome = pathlib.Path(sys.argv[1]), sys.argv[2], sys.argv[3]
if outcome not in ("started", "refused"):
    sys.exit(f"start_record: no outcome {outcome!r} (started, refused)")
start = {"at_micros": time.time_ns() // 1_000, "outcome": outcome}
if outcome == "refused":
    start["reason"] = sys.argv[4] if len(sys.argv) > 4 else ""

folder.mkdir(parents=True, exist_ok=True)
path = folder / f"service-{service.replace(':', '-')}.json"
try:
    starts = json.loads(path.read_text()).get("starts", [])
    if not isinstance(starts, list):
        starts = []
except (OSError, ValueError, AttributeError):
    starts = []
starts = (starts + [start])[-KEPT:]
temporary = path.with_suffix(".json.writing")
temporary.write_text(json.dumps({"service": service, "starts": starts}, indent=2) + "\n")
os.replace(temporary, path)
