from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from lamindb_setup.core._settings_store import (
    local_current_branch_file,
    local_current_instance_file,
    local_current_space_file,
    remove_local_current_instance,
)

if TYPE_CHECKING:
    import pytest


def pytest_sessionfinish(session: pytest.Session) -> None:
    """Drop the dev-dir marker ``init()`` writes in the checkout root.

    ``delete()`` leaves directory markers in place. The hub-cloud notebooks
    then init ``mydata`` again, and a parent marker with that slug resolves to
    the new instance.
    """
    root = Path(session.config.rootpath)
    marker = local_current_instance_file(root)
    if not marker.is_file():
        return
    local_current_branch_file(root).unlink(missing_ok=True)
    local_current_space_file(root).unlink(missing_ok=True)
    remove_local_current_instance(marker=marker)
