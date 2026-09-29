from __future__ import annotations

from typing import TYPE_CHECKING

from lamindb_setup.core._settings_store import (
    dev_dir_containing,
    find_dev_dirs,
    local_current_branch_file,
    write_local_current_instance,
)

if TYPE_CHECKING:
    from pathlib import Path


def test_find_dev_dirs_lists_nested_and_skips_home(tmp_path: Path, monkeypatch):
    home = tmp_path / "home"
    workspace = home / "work"
    first = workspace / "one"
    second = workspace / "two"
    hidden = workspace / ".hidden" / "skip"
    for path in (home, first, second, hidden):
        path.mkdir(parents=True)
    write_local_current_instance(home, "owner/global")
    write_local_current_instance(first, "owner/one")
    write_local_current_instance(second, "owner/two")
    write_local_current_instance(hidden, "owner/hidden")
    branch_file = local_current_branch_file(first.resolve())
    branch_file.write_text("uid\narchive")
    monkeypatch.setattr(
        "lamindb_setup.core._settings_store.Path.home",
        classmethod(lambda cls: home),
    )

    assert find_dev_dirs(home) == []
    found = {
        directory: (slug, branch)
        for directory, slug, branch in find_dev_dirs(workspace)
    }
    assert found[first.resolve()] == ("owner/one", "archive")
    assert found[second.resolve()] == ("owner/two", "main")
    assert home.resolve() not in found
    assert hidden.resolve() not in found

    script = first / "analysis" / "script.py"
    script.parent.mkdir()
    script.write_text("print('hello')\n")
    assert dev_dir_containing(script, instance_slug="owner/one") == first.resolve()
    assert dev_dir_containing(script, instance_slug="other/missing") is None
