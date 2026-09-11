from __future__ import annotations

import os
from pathlib import Path

import lamindb_setup as ln_setup
import pytest
from lamindb_setup.core._settings_store import local_current_branch_file
from lamindb_setup.errors import WorktreePathError


def restore_settings(previous_dev_dir: Path | None, previous_worktree: bool) -> None:
    ln_setup.settings._worktree_path.unlink(missing_ok=True)
    ln_setup.settings.dev_dir = previous_dev_dir
    if previous_worktree:
        ln_setup.settings._worktree_path.write_text("true")


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
        restore_settings(previous_dev_dir, previous_worktree)


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
        restore_settings(previous_dev_dir, previous_worktree)


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
        restore_settings(previous_dev_dir, previous_worktree)


def test_resolve_active_worktree_root_without_dev_dir():
    previous_dev_dir = ln_setup.settings.dev_dir
    previous_worktree = ln_setup.settings.worktree
    try:
        ln_setup.settings.dev_dir = None
        # Cover stale state that an older version could have persisted.
        ln_setup.settings._worktree_path.write_text("true")
        assert ln_setup.settings._resolve_active_worktree_root() is None
        with pytest.raises(WorktreePathError, match="requires a configured dev-dir"):
            ln_setup.settings._resolve_active_worktree_root(raise_on_error=True)
    finally:
        restore_settings(previous_dev_dir, previous_worktree)


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
        restore_settings(previous_dev_dir, previous_worktree)
