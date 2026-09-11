from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

CASES = (
    "refuses_ambiguous_or_colliding_dev_dir",
    "refuses_storage_dev_dir_and_nested_storage",
    "refuses_unsafe_workspaces",
    "restores_surviving_branch_marker",
    "rechecks_workspace_after_confirmation",
    "refuses_unsafe_branch_name",
    "refuses_relative_symlinks",
    "rolls_back_cleanup_failure",
    "refuses_active_workspace",
)


@pytest.mark.parametrize("case", CASES)
def test_worktree_migration(case: str, tmp_path: Path) -> None:
    env = os.environ.copy()
    env["LAMIN_SETTINGS_DIR"] = str(tmp_path / "settings")
    env["PYTHONUNBUFFERED"] = "1"
    runner = Path(__file__).with_name("worktree_migration_cases.py")
    process = subprocess.Popen(
        [sys.executable, str(runner), case, str(tmp_path / "dev")],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=env,
    )
    assert process.stdout is not None
    stdout_prefix = ""
    if case == "rechecks_workspace_after_confirmation":
        prompt = "Continue? [Y/n]: "
        while not stdout_prefix.endswith(prompt):
            character = process.stdout.read(1)
            assert character, "migration process exited before requesting confirmation"
            stdout_prefix += character
        (tmp_path / "dev/main/created-during-confirmation.txt").write_text("new")
    stdout, stderr = process.communicate(input="y\n" * 5)
    stdout = stdout_prefix + stdout

    assert process.returncode == 0, f"stdout: {stdout}\nstderr: {stderr}"
