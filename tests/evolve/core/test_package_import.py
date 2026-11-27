"""The evolution package is independent of LaminDB's import and reload lifecycle."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

_SUPPORT = Path(__file__).resolve().parents[1] / "_support" / "network.py"


def _python(code: str, tmp_path: Path, **env: str) -> subprocess.CompletedProcess[str]:
    guard = f"""import runpy, sys
helpers = runpy.run_path({str(_SUPPORT)!r})
sys.addaudithook(helpers["guard_network"])
"""
    return subprocess.run(
        [sys.executable, "-c", guard + code],
        cwd=tmp_path,
        env={**os.environ, **env},
        capture_output=True,
        text=True,
        timeout=120,
    )


def test_import_and_declaration_leave_selected_instance_untouched(tmp_path):
    result = _python(
        """import os
import lamindb_setup as setup
from lamindb_setup.core import django
before = setup.settings._instance_settings
assert not django.IS_SETUP
class NoORM:
    def find_spec(self, fullname, path=None, target=None):
        if fullname == "lamindb" or fullname.startswith("lamindb."):
            raise AssertionError("Evolution import must not import LaminDB")
sys.meta_path.insert(0, NoORM())
from lamindb_setup import evolve
@evolve.create_plan
def demo(ctx: evolve.PlanContext, exports: None) -> None:
    evolve.FeatureType("Measurements")
assert isinstance(demo, evolve.Plan)
assert setup.settings._instance_settings is before
assert os.environ["LAMIN_CURRENT_INSTANCE"] == "unconfigured/default"
assert not django.IS_SETUP
assert "lamindb" not in sys.modules
from lamindb_setup.evolve.engine import LockFile
assert LockFile(plan="demo").to_json()["lamindb_version"]
assert "lamindb" not in sys.modules
""",
        tmp_path,
        LAMIN_CURRENT_INSTANCE="unconfigured/default",
    )
    assert result.returncode == 0, result.stderr


def test_setup_import_does_not_load_optional_evolve_dependencies(tmp_path):
    result = _python(
        """import lamindb_setup
assert "lamindb_setup.evolve" not in sys.modules
assert "typer" not in sys.modules
assert "lamindb" not in sys.modules
""",
        tmp_path,
        LAMIN_CURRENT_INSTANCE="unconfigured/default",
    )
    assert result.returncode == 0, result.stderr


def test_declaration_identities_survive_direct_setup_connections(
    instance_factory, lamin_hub, tmp_path
):
    first = instance_factory("identity-first")
    second = instance_factory("identity-second")
    result = _python(
        f"""import lamindb_setup as setup
from lamindb_setup import evolve
from lamindb_setup.evolve.core import Action, ApplyMode
from lamindb_setup.evolve.engine import compute_changes
@evolve.create_plan
def demo(ctx: evolve.PlanContext, exports: None) -> None:
    evolve.FeatureType("IdentityMeasurements")
before = {{name: module for name, module in sys.modules.copy().items()
          if name == "lamindb_setup.evolve" or name.startswith("lamindb_setup.evolve.")}}
for slug in ({first.slug!r}, {second.slug!r}):
    setup.connect(slug)
    from lamindb_setup.evolve import Plan
    assert isinstance(demo, Plan)
    assert all(sys.modules[name] is module for name, module in before.items())
    changes = compute_changes(demo, mode=ApplyMode.PLAN)
    assert any(change.action is Action.CREATE and change.name == "IdentityMeasurements"
               for change in changes.changes)
""",
        tmp_path,
        SUPABASE_API_URL=lamin_hub,
        SUPABASE_ANON_KEY="pytest",
    )
    assert result.returncode == 0, result.stderr
