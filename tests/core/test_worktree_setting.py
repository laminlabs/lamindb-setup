from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest


@pytest.mark.parametrize("case", ["toggle", "enable_rejects", "disable_rejects"])
def test_worktree_setting(case: str, tmp_path: Path) -> None:
    env = os.environ.copy()
    env["LAMIN_SETTINGS_DIR"] = str(tmp_path / "settings")
    runner = Path(__file__).with_name("worktree_setting_cases.py")
    result = subprocess.run(
        [sys.executable, str(runner), case, str(tmp_path / "dev")],
        capture_output=True,
        text=True,
        env=env,
    )
    assert result.returncode == 0, f"stdout: {result.stdout}\nstderr: {result.stderr}"
