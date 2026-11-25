"""Carry named secrets from one vault environment to another, value by value.

    python3 scripts/lib/carry_secrets.py <gv> --from ENV [--from-home DIR]
        --to ENV [--to-home DIR] [--pause] NAME [NAME ...]

Run through `scripts/carry-secrets.sh`, which finds a `gv` new enough to trust.

Reads each NAME as the owner from the first environment, holds the values in
this process's memory, sets each in the second with the value on `gv`'s
standard input, then reads each back and compares it byte for byte.

**A value is never an argument and never a file.** It leaves `gv get` on a
pipe and enters `gv set` on a pipe. Nothing this prints holds one: a secret is
named, and its length is not said either.

**Bytes, not text.** `gv set` stores its standard input as given and `gv get`
prints what was stored, with no newline added or removed (measured against
gv 0.5.1, 2026-11-24). So a value that ends in a newline, or that is not
text at all, arrives as it left. A shell variable could not promise that:
`$(...)` drops trailing newlines.

**Every read before any write.** If one name cannot be read, nothing is set.

**`--pause`, for two vaults that cannot be up at once.** The vault this
deployment ran itself and the hosted one both answer on this machine's
loopback port 8750. With `--pause` the values are read, then this waits for a
line on standard input while the first vault is stopped and the second
started, and only then writes.

**`--from-home` and `--to-home`, for two projects of one name.** `gv` keeps a
project's pin and its owner key under its home (GV_HOME), and refuses a
second project of the same name there. Each side may name its own.

**As the owner, whatever the caller's environment says.** GV_TOKEN,
GV_TOKEN_FILE, GV_SERVER and GV_ENV are removed for every `gv` this starts: a
token set in the shell would otherwise turn an owner's read into a token's.
"""

import argparse
import os
import subprocess
import sys

ME = "carry-secrets"
STRIPPED = ("GV_TOKEN", "GV_TOKEN_FILE", "GV_SERVER", "GV_ENV")


def refuse(message: str, code: int = 1) -> "None":
    print(f"{ME}: REFUSED — {message}", file=sys.stderr)
    sys.exit(code)


def side_env(home: "str | None") -> "dict[str, str]":
    """The environment for a `gv` acting on one side: the owner, in that home."""
    env = {key: value for key, value in os.environ.items() if key not in STRIPPED}
    if home is not None:
        env["GV_HOME"] = home
    return env


def said(stderr: bytes, values: "list[bytes]") -> str:
    """The first line of what `gv` said, with any value it might echo removed."""
    for value in values:
        if value:
            stderr = stderr.replace(value, b"<value>")
    lines = stderr.decode("utf-8", "replace").strip().splitlines()
    return lines[-1][:200] if lines else "it said nothing"


def read(gv: str, env_path: str, home: "str | None", name: str) -> "tuple[bytes | None, str]":
    done = subprocess.run(
        [gv, "get", name, "--env", env_path],
        stdin=subprocess.DEVNULL,
        capture_output=True,
        env=side_env(home),
        check=False,
    )
    if done.returncode != 0:
        return None, said(done.stderr, [])
    return done.stdout, ""


def write(gv: str, env_path: str, home: "str | None", name: str, value: bytes) -> str:
    done = subprocess.run(
        [gv, "set", name, "--env", env_path],
        input=value,
        capture_output=True,
        env=side_env(home),
        check=False,
    )
    return "" if done.returncode == 0 else said(done.stderr, [value])


def main() -> None:
    parser = argparse.ArgumentParser(prog=ME, add_help=True)
    parser.add_argument("gv")
    parser.add_argument("--from", dest="source", required=True, metavar="ENV")
    parser.add_argument("--from-home", metavar="DIR")
    parser.add_argument("--to", dest="target", required=True, metavar="ENV")
    parser.add_argument("--to-home", metavar="DIR")
    parser.add_argument("--pause", action="store_true")
    parser.add_argument("names", nargs="+", metavar="NAME")
    args = parser.parse_args()

    if len(set(args.names)) != len(args.names):
        refuse("a name is given twice", 2)
    same_home = (args.from_home or os.environ.get("GV_HOME")) == (args.to_home or os.environ.get("GV_HOME"))
    if args.source == args.target and same_home:
        refuse(f"{args.source} would be carried onto itself: name a different environment, or a different home for one side", 2)

    # Every read, before any write.
    values: "dict[str, bytes]" = {}
    for name in args.names:
        value, why = read(args.gv, args.source, args.from_home, name)
        if value is None:
            refuse(f"{name} could not be read from {args.source}, so nothing was set anywhere: {why}")
        if not value:
            refuse(f"{name} is empty in {args.source}, so nothing was set anywhere: an empty secret is a service started without one")
        values[name] = value
    print(f"{ME}: read {len(values)} from {args.source}: {' '.join(args.names)}", file=sys.stderr)

    if args.pause:
        print(
            f"{ME}: the values are held in memory, and nowhere else. Stop the vault they came from, start the one\n"
            f"{ME}: they go to, and press Enter. Anything else, or closing this, sets nothing.",
            file=sys.stderr,
        )
        line = sys.stdin.readline()
        if line.strip() != "" or line == "":
            refuse("not confirmed, so nothing was set")

    carried, failed = [], []
    for name in args.names:
        why = write(args.gv, args.target, args.to_home, name, values[name])
        if why:
            failed.append((name, f"could not be set in {args.target}: {why}"))
            continue
        back, why = read(args.gv, args.target, args.to_home, name)
        if back is None:
            failed.append((name, f"was set in {args.target} and could not be read back: {why}"))
        elif back != values[name]:
            failed.append((name, f"was set in {args.target} and reads back differently"))
        else:
            carried.append(name)
            print(f"{ME}: carried {name}: {args.target} holds it, byte for byte", file=sys.stderr)

    if failed:
        for name, why in failed:
            print(f"{ME}: FAILED — {name} {why}", file=sys.stderr)
        refuse(f"{len(carried)} of {len(args.names)} carried; the rest are named above, and {args.source} still holds every one")
    print(f"{ME}: ok — {len(carried)} carried from {args.source} to {args.target}")


if __name__ == "__main__":
    main()
