from __future__ import annotations

import os
from pathlib import Path

import lamindb_setup as ln_setup
import pytest
from lamindb_setup.core._settings_store import (
    local_current_branch_file,
    write_local_current_instance,
)
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


def _branch_name() -> str:
    return ln_setup.settings.branch.name


def test_two_dev_dirs_are_independent(tmp_path: Path):
    previous_cwd = Path.cwd()
    first = tmp_path / "first"
    second = tmp_path / "second"
    outside = tmp_path / "outside"
    first.mkdir()
    second.mkdir()
    outside.mkdir()
    try:
        os.chdir(first)
        ln_setup.settings.dev_dir = first
        ln_setup.settings.branch = "archive"
        ln_setup.settings.space = "all"
        os.chdir(second)
        ln_setup.settings._branch = None
        ln_setup.settings._space = None
        ln_setup.settings.dev_dir = second
        ln_setup.settings.branch = "main"
        ln_setup.settings.space = "all"
        from lamindb_setup.core._settings_store import local_current_space_file

        assert local_current_branch_file(first.resolve()).exists()
        assert local_current_branch_file(second.resolve()).exists()
        assert local_current_space_file(first.resolve()).exists()
        assert local_current_space_file(second.resolve()).exists()
        os.chdir(first)
        ln_setup.settings._branch = None
        ln_setup.settings._space = None
        assert ln_setup.settings.dev_dir == first.resolve()
        assert _branch_name() == "archive"
        os.chdir(second)
        ln_setup.settings._branch = None
        ln_setup.settings._space = None
        assert ln_setup.settings.dev_dir == second.resolve()
        assert _branch_name() == "main"
        os.chdir(outside)
        assert ln_setup.settings.dev_dir is None
        os.chdir(first)
        ln_setup.settings.dev_dir = None
        assert ln_setup.settings.dev_dir is None
        assert not local_current_branch_file(first.resolve()).exists()
        os.chdir(second)
        assert ln_setup.settings.dev_dir == second.resolve()
    finally:
        os.chdir(second)
        ln_setup.settings.dev_dir = None
        os.chdir(first)
        ln_setup.settings.dev_dir = None
        os.chdir(previous_cwd)
        ln_setup.settings._branch = None
        ln_setup.settings._space = None
        ln_setup.settings.branch = "main"
        ln_setup.settings.space = "all"


def test_home_marker_is_not_a_dev_dir(tmp_path: Path, monkeypatch):
    home = tmp_path / "home"
    child = home / "project"
    home.mkdir()
    child.mkdir()
    write_local_current_instance(home, ln_setup.settings.instance.slug)
    monkeypatch.setattr(
        "lamindb_setup.core._settings.is_home_directory",
        lambda path: Path(path).resolve() == home.resolve(),
    )
    previous_cwd = Path.cwd()
    try:
        os.chdir(child)
        assert ln_setup.settings.dev_dir is None
        with pytest.raises(ValueError, match="home directory"):
            ln_setup.settings.dev_dir = home
    finally:
        os.chdir(previous_cwd)


def test_dev_dir_unset_removes_local_branch_and_space(tmp_path: Path):
    previous_cwd = Path.cwd()
    try:
        os.chdir(tmp_path)
        ln_setup.settings.dev_dir = tmp_path
        ln_setup.settings.branch = "archive"
        ln_setup.settings.space = "all"
        local_branch_marker = local_current_branch_file(tmp_path.resolve())
        from lamindb_setup.core._settings_store import local_current_space_file

        local_space_marker = local_current_space_file(tmp_path.resolve())
        assert local_branch_marker.exists()
        assert local_space_marker.exists()
        ln_setup.settings.dev_dir = None
        assert not local_branch_marker.exists()
        assert not local_space_marker.exists()
        assert ln_setup.settings.dev_dir is None
    finally:
        os.chdir(previous_cwd)
        ln_setup.settings._branch = None
        ln_setup.settings._space = None
        ln_setup.settings.branch = "main"
        ln_setup.settings.space = "all"


def test_space_reads_home_file_when_local_file_is_missing(tmp_path: Path):
    from lamindb_setup.core._settings_store import local_current_space_file

    previous_cwd = Path.cwd()
    home_space = ln_setup.settings._home_space_path
    original = home_space.read_text() if home_space.exists() else None
    try:
        os.chdir(tmp_path)
        ln_setup.settings.dev_dir = tmp_path
        home_space.parent.mkdir(parents=True, exist_ok=True)
        home_space.write_text(f"{12 * 'a'}\nall")
        local_space = local_current_space_file(tmp_path.resolve())
        local_space.unlink(missing_ok=True)
        ln_setup.settings._space = None
        assert ln_setup.settings.space.name == "all"
        ln_setup.settings.space = "all"
        assert local_space.read_text() == f"{12 * 'a'}\nall"
        assert home_space.read_text() == f"{12 * 'a'}\nall"
    finally:
        os.chdir(tmp_path)
        ln_setup.settings.dev_dir = None
        os.chdir(previous_cwd)
        if original is None:
            home_space.unlink(missing_ok=True)
        else:
            home_space.write_text(original)
        ln_setup.settings._space = None
        ln_setup.settings.space = "all"


def test_branch_falls_back_to_main_for_stale_local_marker(tmp_path: Path):
    previous_cwd = Path.cwd()
    try:
        os.chdir(tmp_path)
        ln_setup.settings.dev_dir = tmp_path
        local_branch_marker = local_current_branch_file(tmp_path.resolve())
        local_branch_marker.parent.mkdir(parents=True, exist_ok=True)
        local_branch_marker.write_text("nonexistentuid123\nstale-branch")
        ln_setup.settings._branch = None
        branch = ln_setup.settings.branch
        assert branch.name == "main"
        assert local_branch_marker.read_text() == f"{branch.uid}\nmain"
    finally:
        os.chdir(tmp_path)
        ln_setup.settings.dev_dir = None
        os.chdir(previous_cwd)
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
