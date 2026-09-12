from __future__ import annotations

import sys
from pathlib import Path

from lamindb_setup import settings
from lamindb_setup.errors import DevDirNonEmpty


def toggle(dev_dir: Path) -> None:
    dev_dir.mkdir()
    settings.dev_dir = dev_dir
    (dev_dir / ".agents").mkdir()
    storage_marker = dev_dir / "storage/.lamindb/storage_uid.txt"
    storage_marker.parent.mkdir(parents=True)
    storage_marker.write_text("uid")
    settings.worktree = True
    assert settings.worktree
    settings.worktree = False
    assert not settings.worktree


def enable_rejects(dev_dir: Path) -> None:
    dev_dir.mkdir()
    settings.dev_dir = dev_dir
    (dev_dir / "analysis.py").write_text("data")
    try:
        settings.worktree = True
    except DevDirNonEmpty as error:
        assert "analysis.py" in str(error)
    else:
        raise AssertionError("a non-empty dev-dir should be rejected")


def disable_rejects(dev_dir: Path) -> None:
    dev_dir.mkdir()
    settings.dev_dir = dev_dir
    settings.worktree = True
    (dev_dir / "main").mkdir()
    try:
        settings.worktree = False
    except DevDirNonEmpty as error:
        assert "main" in str(error)
    else:
        raise AssertionError("existing worktrees should be rejected")


if __name__ == "__main__":
    case_name, path = sys.argv[1:]
    globals()[case_name](Path(path))
