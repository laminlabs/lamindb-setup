from __future__ import annotations

import os
import shutil
import sys
from pathlib import Path

from lamindb_setup import settings
from lamindb_setup.core._settings import _rollback_moved_entries
from lamindb_setup.core._settings_store import local_current_branch_file


def moves_dev_dir_content(dev_dir: Path) -> None:
    dev_dir.mkdir()
    settings.dev_dir = dev_dir
    (dev_dir / "analysis.py").write_text("print('hello')\n")
    config = dev_dir / ".agents/instructions.md"
    config.parent.mkdir()
    config.write_text("shared")
    settings.worktree = True
    assert settings.worktree
    assert (dev_dir / "main/analysis.py").exists()
    assert config.read_text() == "shared"
    settings.worktree = False
    assert not settings.worktree
    assert (dev_dir / "analysis.py").exists()
    assert not (dev_dir / "main").exists()


def preserves_lamindb_storage(dev_dir: Path) -> None:
    dev_dir.mkdir()
    settings.dev_dir = dev_dir
    storage = dev_dir / "storage"
    marker = storage / ".lamindb/storage_uid.txt"
    marker.parent.mkdir(parents=True)
    marker.write_text("storageuid")
    (storage / "artifact.txt").write_text("data")
    (dev_dir / "analysis.py").write_text("data")
    settings.worktree = True
    assert marker.read_text() == "storageuid"
    assert (storage / "artifact.txt").read_text() == "data"
    assert not (dev_dir / "main/storage").exists()
    assert (dev_dir / "main/analysis.py").exists()


def refuses_ambiguous_or_colliding_dev_dir(dev_dir: Path) -> None:
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
    try:
        settings.worktree = False
    except RuntimeError as error:
        assert "multiple branch directories" in str(error)
    else:
        raise AssertionError("multiple branch directories should be rejected")
    shutil.rmtree(feature)
    (dev_dir / "analysis.py").write_text("old")
    try:
        settings.worktree = False
    except RuntimeError as error:
        assert "already exist" in str(error)
    else:
        raise AssertionError("collisions should be rejected")


def refuses_storage_dev_dir_and_nested_storage(dev_dir: Path) -> None:
    dev_dir.mkdir()
    settings.dev_dir = dev_dir
    root_marker = dev_dir / ".lamindb/storage_uid.txt"
    root_marker.parent.mkdir()
    root_marker.write_text("root-storage")
    try:
        settings.worktree = True
    except RuntimeError as error:
        assert "dev-dir is a LaminDB storage" in str(error)
    else:
        raise AssertionError("a storage-root dev-dir should be rejected")
    root_marker.unlink()
    nested_marker = dev_dir / "project/storage/.lamindb/storage_uid.txt"
    nested_marker.parent.mkdir(parents=True)
    nested_marker.write_text("nested-storage")
    try:
        settings.worktree = True
    except RuntimeError as error:
        assert "would relocate LaminDB storage" in str(error)
    else:
        raise AssertionError("nested storage should be rejected")


def refuses_unsafe_workspaces(dev_dir: Path) -> None:
    dev_dir.mkdir()
    external = dev_dir.parent / "external"
    external.mkdir()
    settings.dev_dir = dev_dir
    settings.worktree = True
    unmarked = dev_dir / "unmarked"
    unmarked.mkdir()
    try:
        settings.worktree = False
    except RuntimeError as error:
        assert "unrecognized paths: unmarked" in str(error)
    else:
        raise AssertionError("an unmarked workspace should be rejected")
    unmarked.rmdir()
    marker = local_current_branch_file(external)
    marker.parent.mkdir()
    marker.write_text("uid-external\nexternal")
    symlink = dev_dir / "linked"
    symlink.symlink_to(external, target_is_directory=True)
    try:
        settings.worktree = False
    except RuntimeError as error:
        assert "symlinked paths: linked" in str(error)
    else:
        raise AssertionError("a symlinked workspace should be rejected")
    symlink.unlink()
    branch = dev_dir / "main"
    branch_marker = local_current_branch_file(branch)
    branch_marker.parent.mkdir(parents=True)
    branch_marker.write_text("uid-main\nmain")
    storage_marker = branch / "storage/.lamindb/storage_uid.txt"
    storage_marker.parent.mkdir(parents=True)
    storage_marker.write_text("branch-storage")
    try:
        settings.worktree = False
    except RuntimeError as error:
        assert "branch directory contains a LaminDB storage" in str(error)
    else:
        raise AssertionError("branch storage should be rejected")


def restores_surviving_branch_marker(dev_dir: Path) -> None:
    dev_dir.mkdir()
    settings.dev_dir = dev_dir
    root_marker = local_current_branch_file(dev_dir)
    root_marker.parent.mkdir()
    root_marker.write_text("uid-feature\nfeature")
    (dev_dir / "analysis.py").write_text("feature data")
    settings.worktree = True
    root_marker.write_text("uid-main\nmain")
    settings._branch = object()
    settings.worktree = False
    assert settings._branch is None
    assert root_marker.read_text() == "uid-feature\nfeature"
    assert (dev_dir / "analysis.py").read_text() == "feature data"


def rechecks_workspace_after_confirmation(dev_dir: Path) -> None:
    dev_dir.mkdir()
    settings.dev_dir = dev_dir
    settings.worktree = True
    branch = dev_dir / "main"
    branch_marker = local_current_branch_file(branch)
    branch_marker.parent.mkdir(parents=True)
    branch_marker.write_text("1\nmain")
    (branch / "analysis.py").write_text("data")
    settings.worktree = False
    assert (dev_dir / "analysis.py").read_text() == "data"
    assert (dev_dir / "created-during-confirmation.txt").read_text() == "new"


def rechecks_dev_dir_after_confirmation(dev_dir: Path) -> None:
    dev_dir.mkdir()
    settings.dev_dir = dev_dir
    (dev_dir / "analysis.py").write_text("data")
    settings.worktree = True
    assert (dev_dir / "main/analysis.py").read_text() == "data"
    assert (dev_dir / "main/created-during-confirmation.txt").read_text() == "new"


def refuses_unsafe_branch_name(dev_dir: Path) -> None:
    dev_dir.mkdir()
    settings.dev_dir = dev_dir
    marker = local_current_branch_file(dev_dir)
    marker.parent.mkdir()
    marker.write_text("uid-unsafe\n../outside")
    (dev_dir / "analysis.py").write_text("data")
    try:
        settings.worktree = True
    except RuntimeError as error:
        assert "not a safe branch directory name" in str(error)
    else:
        raise AssertionError("an unsafe branch name should be rejected")
    assert (dev_dir / "analysis.py").read_text() == "data"
    assert not (dev_dir.parent / "outside").exists()


def refuses_relative_symlinks(dev_dir: Path) -> None:
    dev_dir.mkdir()
    settings.dev_dir = dev_dir
    relative_link = dev_dir / "relative-link"
    relative_link.symlink_to("../shared")
    try:
        settings.worktree = True
    except RuntimeError as error:
        assert "Cannot migrate relative symlinks" in str(error)
    else:
        raise AssertionError("a relative symlink should be rejected")
    relative_link.unlink()

    settings.worktree = True
    branch = dev_dir / "main"
    marker = local_current_branch_file(branch)
    marker.parent.mkdir(parents=True)
    marker.write_text("1\nmain")
    (branch / "relative-link").symlink_to("../shared")
    try:
        settings.worktree = False
    except RuntimeError as error:
        assert "Cannot migrate relative symlinks" in str(error)
    else:
        raise AssertionError("a relative symlink should be rejected")


def rolls_back_cleanup_failure(dev_dir: Path) -> None:
    dev_dir.mkdir()
    settings.dev_dir = dev_dir
    settings.worktree = True
    branch = dev_dir / "main"
    marker = local_current_branch_file(branch)
    marker.parent.mkdir(parents=True)
    marker.write_text("1\nmain")
    (branch / "analysis.py").write_text("data")
    marker.parent.chmod(0o500)
    try:
        try:
            settings.worktree = False
        except PermissionError:
            pass
        else:
            raise AssertionError("cleanup should fail for read-only metadata")
    finally:
        marker.parent.chmod(0o700)
    assert settings.worktree
    assert (branch / "analysis.py").read_text() == "data"
    assert marker.read_text() == "1\nmain"


def restores_broken_symlink(dev_dir: Path) -> None:
    dev_dir.mkdir()
    source = dev_dir / "source-link"
    destination = dev_dir / "destination-link"
    destination.symlink_to(dev_dir / "missing-target")
    _rollback_moved_entries([(source, destination)])
    assert source.is_symlink()
    assert not destination.is_symlink()


def refuses_active_workspace_and_recovers_missing_dev_dir(dev_dir: Path) -> None:
    dev_dir.mkdir()
    settings.dev_dir = dev_dir
    (dev_dir / "analysis.py").write_text("data")
    settings.worktree = True
    branch = dev_dir / "main"
    os.chdir(branch)
    try:
        settings.worktree = False
    except RuntimeError as error:
        assert "current working directory" in str(error)
    else:
        raise AssertionError("an active workspace should be rejected")
    os.chdir(dev_dir)
    settings.worktree = False
    (dev_dir / "analysis.py").unlink()
    settings.worktree = True
    shutil.rmtree(dev_dir)
    settings.worktree = False
    assert not settings.worktree


if __name__ == "__main__":
    case_name, path = sys.argv[1:]
    globals()[case_name](Path(path))
