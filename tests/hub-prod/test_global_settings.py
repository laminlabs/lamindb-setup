from __future__ import annotations

import os
import subprocess
from pathlib import Path

import lamindb_setup as ln_setup
import pytest
from lamindb_setup.core._settings_store import local_current_branch_file
from lamindb_setup.core.hashing import hash_dir


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
