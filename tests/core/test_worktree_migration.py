from __future__ import annotations

import shutil
from types import SimpleNamespace
from typing import TYPE_CHECKING

import pytest
from lamindb_setup.core import _settings as settings_module
from lamindb_setup.core._settings import SetupSettings, _rollback_moved_entries
from lamindb_setup.core._settings_store import local_current_branch_file

if TYPE_CHECKING:
    from pathlib import Path


@pytest.fixture
def settings(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> SetupSettings:
    settings_dir = tmp_path / "settings"
    settings_dir.mkdir()
    monkeypatch.setattr(settings_module, "settings_dir", settings_dir)
    monkeypatch.setattr(settings_module, "_confirm_worktree_migration", lambda: True)

    result = SetupSettings()
    result._instance_settings = SimpleNamespace(
        owner="none", name="none", slug="none/none"
    )
    return result


def test_worktree_setting_moves_dev_dir_content(
    settings: SetupSettings, tmp_path: Path
) -> None:
    dev_dir = tmp_path / "dev"
    dev_dir.mkdir()
    settings.dev_dir = dev_dir
    (dev_dir / "analysis.py").write_text("print('hello')\n")
    config = dev_dir / ".agents" / "instructions.md"
    config.parent.mkdir()
    config.write_text("shared")

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


def test_worktree_setting_preserves_lamindb_storage(
    settings: SetupSettings, tmp_path: Path
) -> None:
    dev_dir = tmp_path / "dev"
    dev_dir.mkdir()
    settings.dev_dir = dev_dir
    storage = dev_dir / "storage"
    marker = storage / ".lamindb" / "storage_uid.txt"
    marker.parent.mkdir(parents=True)
    marker.write_text("storageuid")
    (storage / "artifact.txt").write_text("data")
    (dev_dir / "analysis.py").write_text("print('hello')\n")

    settings.worktree = True

    assert marker.read_text() == "storageuid"
    assert (storage / "artifact.txt").read_text() == "data"
    assert not (dev_dir / "main" / "storage").exists()
    assert (dev_dir / "main" / "analysis.py").exists()


def test_disable_refuses_ambiguous_or_colliding_dev_dir(
    settings: SetupSettings, tmp_path: Path
) -> None:
    dev_dir = tmp_path / "dev"
    dev_dir.mkdir()
    settings.dev_dir = dev_dir
    settings.worktree = True

    main = dev_dir / "main"
    feature = dev_dir / "feature"
    for branch_dir, uid in ((main, "uid-main"), (feature, "uid-feature")):
        marker = local_current_branch_file(branch_dir)
        marker.parent.mkdir(parents=True)
        marker.write_text(f"{uid}\n{branch_dir.name}")
    (main / "analysis.py").write_text("new")
    (feature / "feature.py").write_text("feature")

    with pytest.raises(RuntimeError, match="multiple branch directories"):
        settings.worktree = False
    assert settings.worktree is True
    assert (feature / "feature.py").read_text() == "feature"

    shutil.rmtree(feature)
    (dev_dir / "analysis.py").write_text("old")
    with pytest.raises(RuntimeError, match="already exist"):
        settings.worktree = False
    assert settings.worktree is True
    assert (main / "analysis.py").read_text() == "new"
    assert (dev_dir / "analysis.py").read_text() == "old"


def test_enable_refuses_storage_dev_dir_and_nested_storage(
    settings: SetupSettings, tmp_path: Path
) -> None:
    dev_dir = tmp_path / "dev"
    dev_dir.mkdir()
    settings.dev_dir = dev_dir
    root_marker = dev_dir / ".lamindb" / "storage_uid.txt"
    root_marker.parent.mkdir()
    root_marker.write_text("root-storage")

    with pytest.raises(RuntimeError, match="dev-dir is a LaminDB storage"):
        settings.worktree = True
    assert settings.worktree is False

    root_marker.unlink()
    nested_marker = dev_dir / "project" / "storage" / ".lamindb" / "storage_uid.txt"
    nested_marker.parent.mkdir(parents=True)
    nested_marker.write_text("nested-storage")
    with pytest.raises(RuntimeError, match="would relocate LaminDB storage"):
        settings.worktree = True
    assert settings.worktree is False
    assert nested_marker.read_text() == "nested-storage"


def test_disable_refuses_unmarked_symlinked_and_storage_workspaces(
    settings: SetupSettings, tmp_path: Path
) -> None:
    dev_dir = tmp_path / "dev"
    dev_dir.mkdir()
    external = tmp_path / "external"
    external.mkdir()
    settings.dev_dir = dev_dir
    settings.worktree = True

    unmarked = dev_dir / "unmarked"
    unmarked.mkdir()
    (unmarked / "analysis.py").write_text("data")
    with pytest.raises(RuntimeError, match="unrecognized paths: unmarked"):
        settings.worktree = False
    assert settings.worktree is True
    shutil.rmtree(unmarked)

    external_marker = local_current_branch_file(external)
    external_marker.parent.mkdir()
    external_marker.write_text("uid-external\nexternal")
    symlink = dev_dir / "linked"
    symlink.symlink_to(external, target_is_directory=True)
    with pytest.raises(RuntimeError, match="symlinked paths: linked"):
        settings.worktree = False
    assert settings.worktree is True
    assert external_marker.exists()
    symlink.unlink()

    branch = dev_dir / "main"
    branch_marker = local_current_branch_file(branch)
    branch_marker.parent.mkdir(parents=True)
    branch_marker.write_text("uid-main\nmain")
    storage_marker = branch / "storage" / ".lamindb" / "storage_uid.txt"
    storage_marker.parent.mkdir(parents=True)
    storage_marker.write_text("branch-storage")
    with pytest.raises(
        RuntimeError, match="branch directory contains a LaminDB storage"
    ):
        settings.worktree = False
    assert settings.worktree is True
    assert storage_marker.read_text() == "branch-storage"


def test_disable_restores_surviving_branch_marker(
    settings: SetupSettings, tmp_path: Path
) -> None:
    dev_dir = tmp_path / "dev"
    dev_dir.mkdir()
    settings.dev_dir = dev_dir
    root_marker = local_current_branch_file(dev_dir)
    root_marker.parent.mkdir()
    root_marker.write_text("uid-feature\nfeature")
    (dev_dir / "analysis.py").write_text("feature data")

    settings.worktree = True
    branch_marker = local_current_branch_file(dev_dir / "feature")
    assert branch_marker.read_text() == "uid-feature\nfeature"
    root_marker.write_text("uid-main\nmain")
    settings._branch = object()

    settings.worktree = False

    assert settings.worktree is False
    assert settings._branch is None
    assert root_marker.read_text() == "uid-feature\nfeature"
    assert (dev_dir / "analysis.py").read_text() == "feature data"


def test_disable_rechecks_workspace_after_confirmation(
    settings: SetupSettings, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    dev_dir = tmp_path / "dev"
    dev_dir.mkdir()
    settings.dev_dir = dev_dir
    (dev_dir / "analysis.py").write_text("data")
    settings.worktree = True

    def confirm_and_add_file() -> bool:
        (dev_dir / "main" / "created-during-confirmation.txt").write_text("new")
        return True

    monkeypatch.setattr(
        settings_module, "_confirm_worktree_migration", confirm_and_add_file
    )
    settings.worktree = False

    assert (dev_dir / "analysis.py").read_text() == "data"
    assert (dev_dir / "created-during-confirmation.txt").read_text() == "new"


def test_enable_rechecks_dev_dir_after_confirmation(
    settings: SetupSettings, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    dev_dir = tmp_path / "dev"
    dev_dir.mkdir()
    settings.dev_dir = dev_dir
    (dev_dir / "analysis.py").write_text("data")

    def confirm_and_add_file() -> bool:
        (dev_dir / "created-during-confirmation.txt").write_text("new")
        return True

    monkeypatch.setattr(
        settings_module, "_confirm_worktree_migration", confirm_and_add_file
    )
    settings.worktree = True

    assert (dev_dir / "main" / "analysis.py").read_text() == "data"
    assert (dev_dir / "main" / "created-during-confirmation.txt").read_text() == "new"


def test_rollback_restores_broken_symlink(tmp_path: Path) -> None:
    source = tmp_path / "source-link"
    destination = tmp_path / "destination-link"
    destination.symlink_to(tmp_path / "missing-target")

    _rollback_moved_entries([(source, destination)])

    assert source.is_symlink()
    assert not destination.is_symlink()


def test_disable_refuses_active_workspace_and_recovers_missing_dev_dir(
    settings: SetupSettings, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    dev_dir = tmp_path / "dev"
    dev_dir.mkdir()
    settings.dev_dir = dev_dir
    (dev_dir / "analysis.py").write_text("data")

    settings.worktree = True
    branch = dev_dir / "main"
    monkeypatch.chdir(branch)
    with pytest.raises(RuntimeError, match="current working directory"):
        settings.worktree = False
    assert settings.worktree is True

    monkeypatch.chdir(dev_dir)
    settings.worktree = False
    assert (dev_dir / "analysis.py").read_text() == "data"

    (dev_dir / "analysis.py").unlink()
    settings.worktree = True
    shutil.rmtree(dev_dir)
    settings.worktree = False
    assert settings.worktree is False
