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
    child_main.mkdir(parents=True, exist_ok=True)
    child_target.mkdir(parents=True, exist_ok=True)
    try:
        ln_setup.settings.dev_dir = worktree_parent
        ln_setup.settings.worktree = True
        os.chdir(child_main)
        with pytest.raises(ValueError, match=r"To switch, run: cd \.\./testcontrib"):
            ln_setup.switch("testcontrib")
    finally:
        os.chdir(previous_cwd)
        ln_setup.settings.worktree = previous_worktree
        ln_setup.settings.dev_dir = previous_dev_dir
