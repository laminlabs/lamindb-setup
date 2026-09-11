from __future__ import annotations

import os
import subprocess
import sys
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pathlib import Path


def test_worktree_setting_moves_dev_dir_content(tmp_path: Path):
    settings_dir = tmp_path / "settings"
    dev_dir = tmp_path / "dev"
    dev_dir.mkdir()
    script = """
import sys
from pathlib import Path

from lamindb_setup import settings

dev_dir = Path(sys.argv[1])
assert settings.dev_dir is None
assert settings.worktree is False

settings.dev_dir = dev_dir
(dev_dir / "analysis.py").write_text("print('hello')\\n")
config = dev_dir / ".agents" / "instructions.md"
config.parent.mkdir()
config.write_text("shared")

try:
    settings.worktree = True
    assert settings.worktree is True
    assert (dev_dir / "main" / "analysis.py").exists()
    assert config.read_text() == "shared"

    settings.worktree = False
    assert settings.worktree is False
    assert (dev_dir / "analysis.py").exists()
    assert not (dev_dir / "main").exists()

    settings.worktree = True
    assert settings.worktree is True
    assert (dev_dir / "main" / "analysis.py").exists()
finally:
    if settings.worktree:
        settings.worktree = False
    settings.dev_dir = None
"""
    env = os.environ.copy()
    env["LAMIN_SETTINGS_DIR"] = str(settings_dir)

    result = subprocess.run(
        [sys.executable, "-c", script, str(dev_dir)],
        input="y\ny\ny\ny\n",
        capture_output=True,
        text=True,
        env=env,
    )

    assert result.returncode == 0, f"stdout: {result.stdout}\nstderr: {result.stderr}"
    assert (dev_dir / "analysis.py").exists()
    assert (dev_dir / ".agents" / "instructions.md").read_text() == "shared"


def test_worktree_setting_preserves_lamindb_storage(tmp_path: Path):
    settings_dir = tmp_path / "settings"
    dev_dir = tmp_path / "dev"
    dev_dir.mkdir()
    script = """
import sys
from pathlib import Path

from lamindb_setup import settings

dev_dir = Path(sys.argv[1])
settings.dev_dir = dev_dir
storage = dev_dir / "storage"
marker = storage / ".lamindb" / "storage_uid.txt"
marker.parent.mkdir(parents=True)
marker.write_text("storageuid")
(storage / "artifact.txt").write_text("data")
(dev_dir / "analysis.py").write_text("print('hello')\\n")

try:
    settings.worktree = True
    assert marker.read_text() == "storageuid"
    assert (storage / "artifact.txt").read_text() == "data"
    assert not (dev_dir / "main" / "storage").exists()
    assert (dev_dir / "main" / "analysis.py").exists()
finally:
    if settings.worktree:
        settings.worktree = False
    settings.dev_dir = None
"""
    env = os.environ.copy()
    env["LAMIN_SETTINGS_DIR"] = str(settings_dir)

    result = subprocess.run(
        [sys.executable, "-c", script, str(dev_dir)],
        input="y\ny\n",
        capture_output=True,
        text=True,
        env=env,
    )

    assert result.returncode == 0, f"stdout: {result.stdout}\nstderr: {result.stderr}"
    assert (dev_dir / "storage" / ".lamindb" / "storage_uid.txt").exists()
    assert (dev_dir / "storage" / "artifact.txt").read_text() == "data"
