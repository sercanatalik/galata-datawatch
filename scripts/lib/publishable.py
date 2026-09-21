"""The workspace's publishable member names, sorted, space-separated.

Computed from the manifests so that no list in a shell script is the thing
that decides which crates publish.
"""

import pathlib
import tomllib

ws = tomllib.loads(pathlib.Path("Cargo.toml").read_text())
names = []
for member in ws["workspace"]["members"]:
    pkg = tomllib.loads(pathlib.Path(member, "Cargo.toml").read_text())["package"]
    if pkg.get("publish") is not False:
        names.append(pkg["name"])
print(" ".join(sorted(names)))
