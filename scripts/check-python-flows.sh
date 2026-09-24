#!/usr/bin/env bash
#
# The scheduling lane's boundary, read from its import graph.
#
# The Cargo guards read a resolved dependency graph, and a Python file is
# invisible to all of them. Legacy conceded as much — "a Python file has no
# Cargo.toml", so its python wall was "honestly the weaker one". With a
# package, Python has an import graph, and the graph is what this reads, with
# `ast` rather than grep, so a name in a comment is not an import and an
# import split across lines is still one.
#
#   1. IMPORTS ARE A SHORT LIST. cereyan, and the stdlib a flow needs to name
#      a date, read a configuration's table names and spawn a tool. Anything
#      else — `os` included, because `os.environ` is how an environment gets
#      inherited — is a diff that has to argue for itself here.
#   2. ONE MODULE SPAWNS. Only `_runner.py` imports `subprocess`, so there is
#      one place a job's environment and exit code are decided.
#   3. EVERY SPAWN BUILDS ITS ENVIRONMENT. A `subprocess.run` or `Popen`
#      without `env=` inherits the scheduler's, credentials and all.
#   4. NOTHING DELETES. `--delete` appears in no argument. Registration is
#      the exposure boundary of cereyan's `run_flow`; a flow that must not be
#      triggerable must not exist.
#   5. NO CREDENTIAL IS NAMED. PASSWORD, GV_TOKEN, SECRET, CEREYAN_TOKEN or a
#      `_KEY` in an identifier or a string.
#
# Docstrings are exempt from 4 and 5: they say why the rule exists, and
# saying so needs the words. A docstring cannot pass an argument.
#
# WHAT THIS CANNOT CHECK: that a tool invoked with a clean environment does
# not go and read a credential from a file of its own. That is the Rust
# guards' side of the wall (`check-secret-reach.sh`), and why both exist.
#
# Usage: check-python-flows.sh [root]

set -euo pipefail
ROOT="${1:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"

python3 - "$ROOT/py/flows" <<'PY'
import ast, pathlib, re, sys

flows = pathlib.Path(sys.argv[1])
ALLOWED = {"cereyan", "subprocess", "pathlib", "datetime", "tomllib", "__future__", "typing"}
SPAWNER = "_runner.py"
CREDENTIAL = re.compile(r"PASSWORD|GV_TOKEN|SECRET|CEREYAN_TOKEN|_KEY\b")

def docstrings(tree):
    ids = set()
    for node in ast.walk(tree):
        if isinstance(node, (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
            body = node.body
            if body and isinstance(body[0], ast.Expr) and isinstance(body[0].value, ast.Constant) \
                    and isinstance(body[0].value.value, str):
                ids.add(id(body[0].value))
    return ids

problems = []
files = sorted(flows.glob("*.py"))
if not files:
    problems.append(f"{flows}: no flows to check — a guard over nothing is green for the wrong reason")

for path in files:
    rel = path.relative_to(flows.parents[1])
    tree = ast.parse(path.read_text(), filename=str(path))
    docs = docstrings(tree)
    for node in ast.walk(tree):
        at = f"{rel}:{getattr(node, 'lineno', '?')}"
        if isinstance(node, ast.Import):
            roots = [a.name.split(".")[0] for a in node.names]
        elif isinstance(node, ast.ImportFrom):
            roots = [] if node.level else [(node.module or "").split(".")[0]]
        else:
            roots = []
        for root in roots:
            if root not in ALLOWED:
                problems.append(f"{at}: imports {root!r}, which the lane does not allow")
            if root == "subprocess" and path.name != SPAWNER:
                problems.append(f"{at}: imports subprocess outside {SPAWNER}")

        if isinstance(node, ast.Call):
            f = node.func
            name = f.attr if isinstance(f, ast.Attribute) else getattr(f, "id", "")
            if name in {"run", "Popen", "call", "check_call", "check_output"} and \
                    isinstance(f, ast.Attribute) and getattr(f.value, "id", "") == "subprocess":
                if not any(k.arg == "env" for k in node.keywords):
                    problems.append(f"{at}: subprocess.{name} without env= inherits the scheduler's environment")

        if isinstance(node, ast.Constant) and isinstance(node.value, str) and id(node) not in docs:
            if "--delete" in node.value:
                problems.append(f"{at}: passes --delete; deletion is never a flow")
            if CREDENTIAL.search(node.value):
                problems.append(f"{at}: names a credential in {node.value!r}")
        if isinstance(node, ast.Name) and CREDENTIAL.search(node.id):
            problems.append(f"{at}: names a credential: {node.id}")
        if isinstance(node, ast.Attribute) and CREDENTIAL.search(node.attr):
            problems.append(f"{at}: names a credential: {node.attr}")

if problems:
    print("check-python-flows:", file=sys.stderr)
    for p in problems:
        print(f"  {p}", file=sys.stderr)
    sys.exit(1)
print(f"python flows: ok. {len(files)} modules; one spawner, a built environment, no delete, no credential")
PY
