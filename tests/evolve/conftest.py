from __future__ import annotations

import http.server
import json
import os
import subprocess
import sys
import threading
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from collections.abc import Iterator


# Isolate settings before the native package is imported during collection.
import shutil
import tempfile
from types import SimpleNamespace
from uuid import UUID

_settings_dir = tempfile.TemporaryDirectory(prefix="schema-evolution-tests-")
os.environ["LAMIN_SETTINGS_DIR"] = _settings_dir.name
os.environ.pop("LAMIN_CURRENT_INSTANCE", None)
os.environ.pop("LAMIN_API_KEY", None)
os.environ["LAMIN_ENV"] = "pytest"

from lamindb_setup import settings as _settings
from lamindb_setup.core import InstanceSettings

_offline = InstanceSettings(
    id=UUID(int=0), owner="none", name="none", storage=None, modules="bionty,pertdb"
)
_settings._instance_settings = _offline

# Keep database tests offline, including the process that creates the instance.
# Child processes independently install the same audit guard.
import runpy

_test_child = runpy.run_path(str(Path(__file__).parent / "_support" / "network.py"))
sys.addaudithook(_test_child["guard_network"])


@pytest.fixture(scope="session")
def _database(tmp_path_factory, lamin_hub):
    import lamindb_setup as setup

    root = tmp_path_factory.mktemp("schema-evolution-instance")
    os.environ["SUPABASE_API_URL"] = lamin_hub
    os.environ["SUPABASE_ANON_KEY"] = "pytest"
    os.environ["LAMIN_CACHE_DIR"] = str(root / "cache")
    previous = Path.cwd()
    try:
        os.chdir(root)
        setup.init(
            storage=root / "storage",
            name="schema-evolution-tests",
            modules="bionty,pertdb",
        )
    finally:
        os.chdir(previous)
    import lamindb as ln
    from django.db import connections

    isettings = setup.settings.instance
    path = Path(connections["default"].settings_dict["NAME"])
    connections.close_all()
    backup = root / "baseline.sqlite"
    shutil.copy2(path, backup)
    return SimpleNamespace(settings=isettings, path=path, backup=backup, ln=ln)


@pytest.fixture
def instance(_database):
    from django.db import connections

    connections.close_all()
    shutil.copy2(_database.backup, _database.path)
    _settings._instance_settings = _database.settings
    try:
        yield SimpleNamespace(slug=_database.settings.slug, path=_database.path)
    finally:
        connections.close_all()
        _settings._instance_settings = _offline


@pytest.fixture
def ln(instance, _database):
    return _database.ln


@pytest.fixture
def bt(instance):
    import bionty

    return bionty


@pytest.fixture
def pt(instance):
    import pertdb

    return pertdb


class _HubHandler(http.server.BaseHTTPRequestHandler):
    """Answer every hub query with an empty result: nothing is on the hub."""

    def _empty(self) -> None:
        body = b"[]"
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    do_GET = do_POST = do_PATCH = do_DELETE = _empty

    def log_message(self, format: str, *args: object) -> None:
        """Silence the per-request logging to stderr."""


@pytest.fixture(scope="session")
def lamin_hub() -> Iterator[str]:
    """Return the URL of a stand-in for the Lamin hub.

    lamindb asks the hub whether an instance is registered there before it
    saves a run's artifacts -- which `apply` does when it tracks the run -- so
    a child would otherwise need the network.  A local, anonymous test instance
    never is registered, and this answers exactly that.
    """
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _HubHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


@pytest.fixture
def instance_factory(instance, tmp_path):
    """Create additional local instances without changing the parent's connection."""

    def create(name: str):
        storage = tmp_path / name / "storage"
        script = """
import json
import runpy
import sys
from pathlib import Path

support, storage, name = sys.argv[1:]
helpers = runpy.run_path(support)
sys.addaudithook(helpers["guard_network"])
import lamindb_setup as setup
setup.init(storage=Path(storage), name=name, modules="bionty,pertdb")
from django.db import connections
print(json.dumps({"slug": setup.settings.instance.slug,
                  "path": str(connections["default"].settings_dict["NAME"])}))
"""
        result = subprocess.run(
            [
                sys.executable,
                "-c",
                script,
                str(Path(__file__).resolve().parent / "_support" / "network.py"),
                str(storage),
                f"schema-evolution-{name}",
            ],
            cwd=tmp_path,
            capture_output=True,
            text=True,
            check=True,
        )
        info = json.loads(result.stdout.strip().splitlines()[-1])
        return SimpleNamespace(slug=info["slug"], path=Path(info["path"]))

    return create
