"""Verify that a generated plan round-trips as a no-op, in a fresh subprocess.

Both sides of the process hop live here.  :func:`run_verification` is the parent
side: it spawns the child and parses its report.  :func:`main` is the child entry
point, run as
`python -m lamindb_setup.evolve.introspect._verify <plan> <var> <instance>`.

Introspection writes the plan in the parent process (which has imported lamindb
to read the instance).  A plan, however, must be *constructed* before connecting
to an instance (see :func:`lamindb_setup.evolve.authoring._plan._ensure_lamindb_not_loaded`),
so verification runs in a fresh process: it imports the generated plan first, then
connects and computes the changes that applying it would make against the
instance, emitting a JSON report on stdout.  A faithful introspection yields no
create / update / delete actions.

The module entry point loads declarations before connecting. The existing
connection context keeps dtype resolution independent of the selected instance.
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path
from typing import Any

__all__ = ["main", "run_verification", "verify"]


def run_verification(output: Path, func: str, instance: str) -> dict[str, Any]:
    """Verify in a subprocess that applying the plan would make no changes.

    Returns the child's report, or `{"error": ...}` if it could not be run or
    its output could not be parsed.
    """
    proc = subprocess.run(
        [
            sys.executable,
            "-m",
            "lamindb_setup.evolve.introspect._verify",
            str(output),
            func,
            instance,
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    if proc.returncode != 0:
        message = proc.stderr.strip() or proc.stdout.strip() or "verification failed"
        return {"error": message}
    for line in reversed(proc.stdout.splitlines()):
        line = line.strip()
        if not line:
            continue
        try:
            return json.loads(line)
        except json.JSONDecodeError:
            break
    return {"error": "could not parse verification output"}


def _load_plan(path: Path, var: str) -> Any:
    """Import `path` and return its `var` plan before connecting."""
    from ..engine import import_plan_module

    return getattr(import_plan_module(path), var)


def verify(plan_path: Path, var: str, instance: str) -> dict[str, Any]:
    """Return a JSON-serializable report of the plan's changes against `instance`."""
    plan_obj = _load_plan(plan_path, var)

    from ..engine import safe_connect

    safe_connect(instance)

    from ..core import Action, ApplyMode
    from ..engine import compute_changes

    ctx = compute_changes(plan_obj, mode=ApplyMode.PLAN)

    counts = {action.value: 0 for action in Action}
    changes: list[dict[str, str]] = []
    for change in ctx.changes:
        counts[change.action.value] += 1
        if change.action is not Action.NO_CHANGE:
            label = (
                change.kind if change.name is None else f"{change.kind} {change.name!r}"
            )
            changes.append({"action": change.action.value, "object": label})
    return {"counts": counts, "changes": changes}


def main(argv: list[str]) -> int:
    if len(argv) != 3:
        print("usage: _verify <plan_path> <plan_var> <instance>", file=sys.stderr)
        return 2
    plan_path, var, instance = argv
    report = verify(Path(plan_path), var, instance)
    print(json.dumps(report))
    return 0


if __name__ == "__main__":
    from ..engine import no_default_instance

    with no_default_instance():
        raise SystemExit(main(sys.argv[1:]))
