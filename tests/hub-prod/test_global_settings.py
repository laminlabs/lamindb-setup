from __future__ import annotations

import os
import subprocess
from pathlib import Path

import lamindb_setup as ln_setup
import pytest
from lamindb_setup.core._settings_store import local_current_branch_file
from lamindb_setup.core.hashing import hash_dir
from lamindb_setup.errors import NoDevDirConfigured, WorktreePathError


def test_auto_connect():
    current_state = ln_setup.settings.auto_connect
    ln_setup.settings.auto_connect = True
    assert ln_setup.settings._auto_connect_path.exists()
    ln_setup.settings.auto_connect = False
    assert not ln_setup.settings._auto_connect_path.exists()
    ln_setup.settings.auto_connect = current_state


def test_branch():
    import lamindb as ln

    ln_setup.settings._branch_path.unlink(missing_ok=True)
    assert ln_setup.settings.branch.uid == 12 * "m"
    ln_setup.settings.branch = "archive"
    assert ln_setup.settings._branch_path.read_text() == f"{12 * 'a'}\narchive"
    ln_setup.settings.branch = "main"
    assert ln_setup.settings._branch_path.read_text() == f"{12 * 'm'}\nmain"
    with pytest.raises(ln.errors.DoesNotExist):
        ln_setup.settings.branch = "not_exists"


def test_space():
    import lamindb as ln

    ln_setup.settings._space_path.unlink(missing_ok=True)
    assert ln_setup.settings.space.uid == 12 * "a"
    ln_setup.settings.space = "all"
    assert ln_setup.settings._space_path.read_text() == f"{12 * 'a'}\nall"
    with pytest.raises(ln.errors.DoesNotExist):
        ln_setup.settings.space = "not_exists"


def _restore_worktree_settings(
    previous_dev_dir: Path | None, previous_worktree: bool
) -> None:
    ln_setup.settings._worktree_path.unlink(missing_ok=True)
    ln_setup.settings.dev_dir = previous_dev_dir
    if previous_worktree:
        ln_setup.settings._worktree_path.write_text("true")


def test_worktree_setting_roundtrip(tmp_path: Path):
    previous_dev_dir = ln_setup.settings.dev_dir
    previous_worktree = ln_setup.settings.worktree
    try:
        ln_setup.settings.dev_dir = tmp_path
        ln_setup.settings.worktree = True
        assert ln_setup.settings.worktree is True
        ln_setup.settings.worktree = False
        assert ln_setup.settings.worktree is False
    finally:
        _restore_worktree_settings(previous_dev_dir, previous_worktree)


def test_resolve_active_worktree_root_and_branch_path(tmp_path: Path):
    previous_dev_dir = ln_setup.settings.dev_dir
    previous_worktree = ln_setup.settings.worktree
    previous_cwd = Path.cwd()
    root = tmp_path / "worktrees"
    root.mkdir()
    child = root / "feature-a"
    try:
        ln_setup.settings.dev_dir = root
        ln_setup.settings.worktree = True
        child.mkdir()
        os.chdir(child)
        assert ln_setup.settings.effective_dev_dir == child.resolve()
        assert ln_setup.settings._branch_path == local_current_branch_file(
            child.resolve()
        )
    finally:
        os.chdir(previous_cwd)
        _restore_worktree_settings(previous_dev_dir, previous_worktree)


def test_resolve_active_worktree_root_errors_at_dev_dir_root(tmp_path: Path):
    previous_dev_dir = ln_setup.settings.dev_dir
    previous_worktree = ln_setup.settings.worktree
    root = tmp_path / "worktrees"
    root.mkdir()
    try:
        ln_setup.settings.dev_dir = root
        ln_setup.settings.worktree = True
        with pytest.raises(WorktreePathError, match="inside a child directory"):
            ln_setup.settings._resolve_active_worktree_root(
                cwd=root, raise_on_error=True
            )
    finally:
        _restore_worktree_settings(previous_dev_dir, previous_worktree)


def test_resolve_active_worktree_root_non_worktree_mode(tmp_path: Path):
    previous_dev_dir = ln_setup.settings.dev_dir
    previous_worktree = ln_setup.settings.worktree
    try:
        ln_setup.settings._worktree_path.unlink(missing_ok=True)
        ln_setup.settings.dev_dir = tmp_path
        assert (
            ln_setup.settings._resolve_active_worktree_root(raise_on_error=True)
            == tmp_path.resolve()
        )
        ln_setup.settings.dev_dir = None
        assert ln_setup.settings._resolve_active_worktree_root() is None
    finally:
        _restore_worktree_settings(previous_dev_dir, previous_worktree)


def test_resolve_active_worktree_root_without_dev_dir():
    previous_dev_dir = ln_setup.settings.dev_dir
    previous_worktree = ln_setup.settings.worktree
    try:
        ln_setup.settings.dev_dir = None
        ln_setup.settings._worktree_path.unlink(missing_ok=True)
        with pytest.raises(
            NoDevDirConfigured,
            match="Please set it using: lamin settings dev-dir set path/to/directory",
        ):
            ln_setup.settings.worktree = True
        ln_setup.settings._worktree_path.write_text("true")
        assert ln_setup.settings._resolve_active_worktree_root() is None
        with pytest.raises(
            NoDevDirConfigured,
            match="Please set it using: lamin settings dev-dir set path/to/directory",
        ):
            ln_setup.settings._resolve_active_worktree_root(raise_on_error=True)
    finally:
        _restore_worktree_settings(previous_dev_dir, previous_worktree)


def test_resolve_active_worktree_root_invalid_location_returns_none(tmp_path: Path):
    previous_dev_dir = ln_setup.settings.dev_dir
    previous_worktree = ln_setup.settings.worktree
    root = tmp_path / "worktrees"
    outside = tmp_path / "outside"
    root.mkdir()
    outside.mkdir()
    try:
        ln_setup.settings.dev_dir = root
        ln_setup.settings.worktree = True
        assert ln_setup.settings._resolve_active_worktree_root(cwd=root) is None
        assert ln_setup.settings._resolve_active_worktree_root(cwd=outside) is None
    finally:
        _restore_worktree_settings(previous_dev_dir, previous_worktree)


def test_dev_dir_unset_removes_local_branch_marker(tmp_path: Path):
    previous_dev_dir = ln_setup.settings.dev_dir
    previous_worktree = ln_setup.settings.worktree
    try:
        ln_setup.settings.worktree = False
        ln_setup.settings.dev_dir = tmp_path
        ln_setup.settings.branch = "archive"
        local_branch_marker = local_current_branch_file(tmp_path.resolve())
        assert local_branch_marker.exists()
        ln_setup.settings.dev_dir = None
        assert not local_branch_marker.exists()
    finally:
        ln_setup.settings.worktree = previous_worktree
        ln_setup.settings.dev_dir = previous_dev_dir
        ln_setup.settings._branch = None
        ln_setup.settings.branch = "main"


def test_branch_falls_back_to_main_for_stale_local_marker(tmp_path: Path):
    previous_dev_dir = ln_setup.settings.dev_dir
    previous_worktree = ln_setup.settings.worktree
    try:
        ln_setup.settings.worktree = False
        ln_setup.settings.dev_dir = tmp_path
        local_branch_marker = local_current_branch_file(tmp_path.resolve())
        local_branch_marker.parent.mkdir(parents=True, exist_ok=True)
        local_branch_marker.write_text("nonexistentuid123\nstale-branch")
        ln_setup.settings._branch = None
        branch = ln_setup.settings.branch
        assert branch.name == "main"
        assert local_branch_marker.read_text() == f"{branch.uid}\nmain"
    finally:
        ln_setup.settings.worktree = previous_worktree
        ln_setup.settings.dev_dir = previous_dev_dir
        ln_setup.settings._branch = None
        ln_setup.settings.branch = "main"


def test_private_django_api():
    from django import db

    django_dir = Path(db.__file__).parent.parent

    # below, we're checking whether a repo is clean via the internal hashing
    # function
    # def is_repo_clean() -> bool:
    #     from django import db

    #     django_dir = Path(db.__file__).parent.parent
    #     print(django_dir)
    #     result = subprocess.run(
    #         ["git", "diff"],
    #         capture_output=True,
    #         text=True,
    #         cwd=django_dir,
    #     )
    #     print(result.stdout)
    #     print(result.stderr)
    #     return result.stdout.strip() == "" and result.stderr.strip() == ""

    _, orig_hash, _, _ = hash_dir(django_dir)
    current_state = ln_setup.settings.private_django_api
    ln_setup.settings.private_django_api = True
    # do not run below on CI, but only locally
    # installing django via git didn't succeed
    # assert not is_repo_clean()
    _, hash, _, _ = hash_dir(django_dir)
    assert hash != orig_hash
    assert ln_setup.settings._private_django_api_path.exists()
    ln_setup.settings.private_django_api = False
    # assert is_repo_clean()
    _, hash, _, _ = hash_dir(django_dir)
    assert hash == orig_hash
    assert not ln_setup.settings._private_django_api_path.exists()
    ln_setup.settings.private_django_api = current_state
