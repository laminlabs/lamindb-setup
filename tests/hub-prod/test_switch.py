"""Tests for lamindb_setup.switch."""

import os
import shutil
import time
from pathlib import Path

import lamindb as ln
import lamindb_setup as ln_setup
import pytest
from lamindb_setup.core._settings_store import local_current_branch_file


def test_switch_create_existing_branch_raises():
    """Switch with create=True and existing branch raises BranchAlreadyExists with hint."""
    with pytest.raises(ln.errors.BranchAlreadyExists) as exc_info:
        ln_setup.switch("main", create=True)
    msg = str(exc_info.value)
    assert "already exists" in msg
    assert "-c/--create" in msg or "Omit" in msg


def test_switch_nonexistent_branch_suggests_create():
    missing = "nonexistent_branch_xyz_for_create_hint"
    with pytest.raises(ln.errors.DoesNotExist) as exc_info:
        ln_setup.switch(missing)
    msg = str(exc_info.value)
    assert "does not exist" in msg
    assert f"lamin switch -c {missing}" in msg


def test_switch_space_does_not_depend_on_worktree_bootstrap_state():
    ln_setup.switch("all", space=True)
    assert ln_setup.settings.space.name == "all"


def test_switch_create_worktree_from_dev_dir_root(tmp_path: Path):
    previous_dev_dir = ln_setup.settings.dev_dir
    previous_worktree = ln_setup.settings.worktree
    previous_cwd = Path.cwd()
    worktree_parent = tmp_path / "worktrees"
    worktree_parent.mkdir(parents=True, exist_ok=True)
    branch_name = f"wt-{time.time_ns()}"
    child = worktree_parent / branch_name
    try:
        ln_setup.settings.dev_dir = worktree_parent
        ln_setup.settings.worktree = True
        os.chdir(worktree_parent)
        ln_setup.switch(branch_name, create=True)
        assert child.exists()
        assert (
            local_current_branch_file(child).read_text().split("\n")[1] == branch_name
        )
    finally:
        os.chdir(previous_cwd)
        ln.Branch.filter(name=branch_name).delete(permanent=True)
        if child.exists():
            shutil.rmtree(child)
        ln_setup.settings.worktree = False
        ln_setup.settings.dev_dir = previous_dev_dir
        if previous_worktree:
            ln_setup.settings._worktree_path.write_text("true")


def test_switch_worktree_from_root_requires_cd_instruction(tmp_path: Path):
    previous_dev_dir = ln_setup.settings.dev_dir
    previous_worktree = ln_setup.settings.worktree
    previous_cwd = Path.cwd()
    worktree_parent = tmp_path / "worktrees"
    worktree_parent.mkdir(parents=True, exist_ok=True)
    try:
        ln_setup.settings.dev_dir = worktree_parent
        ln_setup.settings.worktree = True
        os.chdir(worktree_parent)
        with pytest.raises(ValueError, match=r"To switch, run: mkdir main && cd main"):
            ln_setup.switch("main")
    finally:
        os.chdir(previous_cwd)
        ln_setup.settings.worktree = previous_worktree
        ln_setup.settings.dev_dir = previous_dev_dir


def test_switch_worktree_from_child_requires_cd_sibling_instruction(tmp_path: Path):
    previous_dev_dir = ln_setup.settings.dev_dir
    previous_worktree = ln_setup.settings.worktree
    previous_cwd = Path.cwd()
    worktree_parent = tmp_path / "worktrees"
    child_main = worktree_parent / "main"
    child_target = worktree_parent / "testcontrib"
    worktree_parent.mkdir(parents=True, exist_ok=True)
    branch_created = False
    try:
        ln_setup.settings.dev_dir = worktree_parent
        ln_setup.settings.worktree = True
        child_main.mkdir(parents=True, exist_ok=True)
        child_target.mkdir(parents=True, exist_ok=True)
        if ln.Branch.filter(name="testcontrib").one_or_none() is None:
            ln.Branch(name="testcontrib").save()
            branch_created = True
        os.chdir(child_main)
        with pytest.raises(ValueError, match=r"To switch, run: cd \.\./testcontrib"):
            ln_setup.switch("testcontrib")
    finally:
        os.chdir(previous_cwd)
        if branch_created:
            ln.Branch.filter(name="testcontrib").delete(permanent=True)
        if child_target.exists():
            shutil.rmtree(child_target)
        if child_main.exists():
            shutil.rmtree(child_main)
        ln_setup.settings.worktree = previous_worktree
        ln_setup.settings.dev_dir = previous_dev_dir


def test_switch_worktree_sequence_missing_then_create_requires_navigation(
    tmp_path: Path,
):
    """Equivalent to: switch missing -> switch -c missing from wrong child."""
    previous_dev_dir = ln_setup.settings.dev_dir
    previous_worktree = ln_setup.settings.worktree
    previous_cwd = Path.cwd()
    worktree_parent = tmp_path / "worktrees"
    child_main = worktree_parent / "main"
    branch_name = f"seq-{time.time_ns()}"
    child_target = worktree_parent / branch_name
    worktree_parent.mkdir(parents=True, exist_ok=True)
    branch_created = False
    branch_registered = False
    try:
        ln_setup.settings.dev_dir = worktree_parent
        ln_setup.settings.worktree = True
        child_main.mkdir(parents=True, exist_ok=True)
        os.chdir(child_main)

        with pytest.raises(ln.errors.DoesNotExist) as exc_info:
            ln_setup.switch(branch_name)
        assert f"lamin switch -c {branch_name}" in str(exc_info.value)
        assert ln_setup.settings.branch.name == "main"

        with pytest.raises(
            ValueError,
        ) as exc_info:
            ln_setup.switch(branch_name, create=True)
        msg = str(exc_info.value)
        assert f"lamin create branch {branch_name}" in msg
        assert f"mkdir ../{branch_name} && cd ../{branch_name}" in msg
        assert f"lamin switch {branch_name}" in msg
        assert ln_setup.settings.branch.name == "main"

        child_target.mkdir(parents=True, exist_ok=True)
        ln.Branch(name=branch_name).save()
        branch_registered = True
        with pytest.raises(
            ValueError,
            match=rf"To switch, run: cd \.\./{branch_name}",
        ):
            ln_setup.switch(branch_name, create=True)
        assert ln_setup.settings.branch.name == "main"

        os.chdir(child_target)
        ln_setup.switch(branch_name)
        assert ln_setup.settings.branch.name == branch_name
    finally:
        os.chdir(previous_cwd)
        if branch_created or branch_registered:
            ln.Branch.filter(name=branch_name).delete(permanent=True)
        if child_target.exists():
            shutil.rmtree(child_target)
        if child_main.exists():
            shutil.rmtree(child_main)
        ln_setup.settings.worktree = previous_worktree
        ln_setup.settings.dev_dir = previous_dev_dir
