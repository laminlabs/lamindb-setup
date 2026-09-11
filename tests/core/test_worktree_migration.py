from __future__ import annotations

import os
import subprocess
import sys
from typing import TYPE_CHECKING

from lamindb_setup.core._settings import _rollback_moved_entries

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


def test_disable_refuses_ambiguous_or_colliding_dev_dir(tmp_path: Path):
    settings_dir = tmp_path / "settings"
    dev_dir = tmp_path / "dev"
    dev_dir.mkdir()
    script = """
import shutil
import sys
from pathlib import Path

from lamindb_setup import settings
from lamindb_setup.core._settings_store import local_current_branch_file

dev_dir = Path(sys.argv[1])
settings.dev_dir = dev_dir
settings.worktree = True

main = dev_dir / "main"
feature = dev_dir / "feature"
for branch_dir, uid in ((main, "uid-main"), (feature, "uid-feature")):
    marker = local_current_branch_file(branch_dir)
    marker.parent.mkdir(parents=True)
    marker.write_text(f"{uid}\\n{branch_dir.name}")
(main / "analysis.py").write_text("new")
(feature / "feature.py").write_text("feature")

try:
    try:
        settings.worktree = False
    except RuntimeError as error:
        assert "multiple branch directories" in str(error)
    else:
        raise AssertionError("disabling multiple branches should fail")
    assert settings.worktree is True
    assert (feature / "feature.py").read_text() == "feature"

    shutil.rmtree(feature)
    (dev_dir / "analysis.py").write_text("old")
    try:
        settings.worktree = False
    except RuntimeError as error:
        assert "already exist" in str(error)
    else:
        raise AssertionError("disabling with a collision should fail")
    assert settings.worktree is True
    assert (main / "analysis.py").read_text() == "new"
    assert (dev_dir / "analysis.py").read_text() == "old"
finally:
    (dev_dir / "analysis.py").unlink(missing_ok=True)
    if settings.worktree:
        settings.worktree = False
    settings.dev_dir = None
"""
    env = os.environ.copy()
    env["LAMIN_SETTINGS_DIR"] = str(settings_dir)

    result = subprocess.run(
        [sys.executable, "-c", script, str(dev_dir)],
        input="y\n",
        capture_output=True,
        text=True,
        env=env,
    )

    assert result.returncode == 0, f"stdout: {result.stdout}\nstderr: {result.stderr}"
    assert (dev_dir / "analysis.py").read_text() == "new"
    assert not (dev_dir / "main").exists()


def test_enable_refuses_storage_dev_dir_and_nested_storage(tmp_path: Path):
    settings_dir = tmp_path / "settings"
    dev_dir = tmp_path / "dev"
    dev_dir.mkdir()
    script = """
import shutil
import sys
from pathlib import Path

from lamindb_setup import settings

dev_dir = Path(sys.argv[1])
settings.dev_dir = dev_dir
root_marker = dev_dir / ".lamindb" / "storage_uid.txt"
root_marker.parent.mkdir()
root_marker.write_text("root-storage")

try:
    try:
        settings.worktree = True
    except RuntimeError as error:
        assert "dev-dir is a LaminDB storage" in str(error)
    else:
        raise AssertionError("a storage-root dev-dir must be rejected")
    assert settings.worktree is False

    root_marker.unlink()
    nested_marker = dev_dir / "project" / "storage" / ".lamindb" / "storage_uid.txt"
    nested_marker.parent.mkdir(parents=True)
    nested_marker.write_text("nested-storage")
    try:
        settings.worktree = True
    except RuntimeError as error:
        assert "would relocate LaminDB storage" in str(error)
    else:
        raise AssertionError("a nested storage location must not move")
    assert settings.worktree is False
    assert nested_marker.read_text() == "nested-storage"
finally:
    shutil.rmtree(dev_dir / "project", ignore_errors=True)
    settings.dev_dir = None
"""
    env = os.environ.copy()
    env["LAMIN_SETTINGS_DIR"] = str(settings_dir)

    result = subprocess.run(
        [sys.executable, "-c", script, str(dev_dir)],
        capture_output=True,
        text=True,
        env=env,
    )

    assert result.returncode == 0, f"stdout: {result.stdout}\nstderr: {result.stderr}"


def test_disable_refuses_unmarked_symlinked_and_storage_workspaces(tmp_path: Path):
    settings_dir = tmp_path / "settings"
    dev_dir = tmp_path / "dev"
    dev_dir.mkdir()
    external = tmp_path / "external"
    external.mkdir()
    script = """
import shutil
import sys
from pathlib import Path

from lamindb_setup import settings
from lamindb_setup.core._settings_store import local_current_branch_file

dev_dir = Path(sys.argv[1])
external = Path(sys.argv[2])
settings.dev_dir = dev_dir
settings.worktree = True

try:
    unmarked = dev_dir / "unmarked"
    unmarked.mkdir()
    (unmarked / "analysis.py").write_text("data")
    try:
        settings.worktree = False
    except RuntimeError as error:
        assert "unrecognized paths: unmarked" in str(error)
    else:
        raise AssertionError("an unmarked workspace must not be stranded")
    assert settings.worktree is True
    shutil.rmtree(unmarked)

    external_marker = local_current_branch_file(external)
    external_marker.parent.mkdir()
    external_marker.write_text("uid-external\\nexternal")
    symlink = dev_dir / "linked"
    symlink.symlink_to(external, target_is_directory=True)
    try:
        settings.worktree = False
    except RuntimeError as error:
        assert "symlinked paths: linked" in str(error)
    else:
        raise AssertionError("a symlinked workspace must be rejected")
    assert settings.worktree is True
    assert external_marker.exists()
    symlink.unlink()

    branch = dev_dir / "main"
    branch_marker = local_current_branch_file(branch)
    branch_marker.parent.mkdir(parents=True)
    branch_marker.write_text("uid-main\\nmain")
    storage_marker = branch / "storage" / ".lamindb" / "storage_uid.txt"
    storage_marker.parent.mkdir(parents=True)
    storage_marker.write_text("branch-storage")
    try:
        settings.worktree = False
    except RuntimeError as error:
        assert "branch directory contains a LaminDB storage" in str(error)
    else:
        raise AssertionError("storage inside a branch must not move")
    assert settings.worktree is True
    assert storage_marker.read_text() == "branch-storage"
finally:
    shutil.rmtree(dev_dir / "main", ignore_errors=True)
    if settings.worktree:
        settings.worktree = False
    settings.dev_dir = None
"""
    env = os.environ.copy()
    env["LAMIN_SETTINGS_DIR"] = str(settings_dir)

    result = subprocess.run(
        [sys.executable, "-c", script, str(dev_dir), str(external)],
        capture_output=True,
        text=True,
        env=env,
    )

    assert result.returncode == 0, f"stdout: {result.stdout}\nstderr: {result.stderr}"
    assert (external / ".lamin" / "current_branch").exists()


def test_disable_restores_surviving_branch_marker(tmp_path: Path):
    settings_dir = tmp_path / "settings"
    dev_dir = tmp_path / "dev"
    dev_dir.mkdir()
    script = """
import sys
from pathlib import Path

from lamindb_setup import settings
from lamindb_setup.core._settings_store import local_current_branch_file

dev_dir = Path(sys.argv[1])
settings.dev_dir = dev_dir
root_marker = local_current_branch_file(dev_dir)
root_marker.parent.mkdir()
root_marker.write_text("uid-feature\\nfeature")
(dev_dir / "analysis.py").write_text("feature data")

try:
    settings.worktree = True
    branch_marker = local_current_branch_file(dev_dir / "feature")
    assert branch_marker.read_text() == "uid-feature\\nfeature"
    root_marker.write_text("uid-main\\nmain")
    settings._branch = object()

    settings.worktree = False
    assert settings.worktree is False
    assert settings._branch is None
    assert root_marker.read_text() == "uid-feature\\nfeature"
    assert (dev_dir / "analysis.py").read_text() == "feature data"
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


def test_disable_rechecks_workspace_after_confirmation(tmp_path: Path):
    settings_dir = tmp_path / "settings"
    dev_dir = tmp_path / "dev"
    dev_dir.mkdir()
    script = """
import sys
from pathlib import Path

from lamindb_setup import settings
from lamindb_setup.core import _settings

dev_dir = Path(sys.argv[1])
settings.dev_dir = dev_dir
(dev_dir / "analysis.py").write_text("data")
settings.worktree = True

def confirm_and_add_file():
    (dev_dir / "main" / "created-during-confirmation.txt").write_text("new")
    return True

_settings._confirm_worktree_migration = confirm_and_add_file
settings.worktree = False
assert (dev_dir / "analysis.py").read_text() == "data"
assert (dev_dir / "created-during-confirmation.txt").read_text() == "new"
settings.dev_dir = None
"""
    env = os.environ.copy()
    env["LAMIN_SETTINGS_DIR"] = str(settings_dir)

    result = subprocess.run(
        [sys.executable, "-c", script, str(dev_dir)],
        input="y\n",
        capture_output=True,
        text=True,
        env=env,
    )

    assert result.returncode == 0, f"stdout: {result.stdout}\nstderr: {result.stderr}"


def test_rollback_restores_broken_symlink(tmp_path: Path):
    source = tmp_path / "source-link"
    destination = tmp_path / "destination-link"
    destination.symlink_to(tmp_path / "missing-target")

    _rollback_moved_entries([(source, destination)])

    assert source.is_symlink()
    assert not destination.is_symlink()


def test_disable_refuses_active_workspace_and_recovers_missing_dev_dir(
    tmp_path: Path,
):
    settings_dir = tmp_path / "settings"
    dev_dir = tmp_path / "dev"
    dev_dir.mkdir()
    script = """
import os
import shutil
import sys
from pathlib import Path

from lamindb_setup import settings

dev_dir = Path(sys.argv[1])
settings.dev_dir = dev_dir
(dev_dir / "analysis.py").write_text("data")

try:
    settings.worktree = True
    branch = dev_dir / "main"
    os.chdir(branch)
    try:
        settings.worktree = False
    except RuntimeError as error:
        assert "current working directory" in str(error)
    else:
        raise AssertionError("the active workspace directory must not be removed")
    assert settings.worktree is True
    assert Path.cwd() == branch

    os.chdir(dev_dir)
    settings.worktree = False
    assert (dev_dir / "analysis.py").read_text() == "data"

    (dev_dir / "analysis.py").unlink()
    settings.worktree = True
    shutil.rmtree(dev_dir)
    settings.worktree = False
    assert settings.worktree is False
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
