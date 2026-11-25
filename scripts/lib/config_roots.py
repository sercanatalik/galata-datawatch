"""Where the deployment's configuration puts a root: on this machine, and in a container.

    python3 scripts/lib/config_roots.py <config.toml> <repo root> <key> [<key> ...]

A key is `<table>.<name>`: `paths.archive`, `paths.status`, `paths.tape`,
`ledger.root`, `ledger.tape`. One line per key, `<host path>\t<path inside>`.

Read from the deployment's configuration so the folders a service is given
(`run-service.sh`) and the folders it then opens come from one place and
cannot disagree. A container that mounted `var/archive` while the
configuration said `/Volumes/record/archive` would write a record nobody
reads, into the image's own filesystem, and lose it when it stopped.

**The same path on both sides.** In the image, `/app` stands where the
checkout does. A relative root is under the repository on the host and under
`/app` inside; an absolute one is at its own path inside too. So the
configuration a container is handed is the deployment's, unedited.

Exits 3, printing nothing, when a key is not declared: a clean *no such
root*, as the maintenance tools use 3. `ledger.tape` is optional, and a
deployment with no ledger declares neither.
"""

import pathlib
import sys
import tomllib

INSIDE = pathlib.PurePosixPath("/app")

config = tomllib.loads(pathlib.Path(sys.argv[1]).read_text())
root = pathlib.PurePosixPath(sys.argv[2])
lines = []
for key in sys.argv[3:]:
    table, _, name = key.partition(".")
    value = (config.get(table) or {}).get(name)
    if value is None:
        sys.exit(3)
    path = pathlib.PurePosixPath(value)
    if ".." in path.parts:
        sys.exit(f"{sys.argv[1]}: [{table}] {name} = {value!r} climbs out of its root with '..'")
    if path.is_absolute():
        lines.append(f"{path}\t{path}")
    else:
        lines.append(f"{root / path}\t{INSIDE / path}")
print("\n".join(lines))
