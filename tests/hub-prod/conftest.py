from __future__ import annotations

from typing import TYPE_CHECKING

import lamindb_setup
from lamin_utils import logger

if TYPE_CHECKING:
    import pytest


def pytest_sessionstart(session: pytest.Session):
    lamindb_setup.init(
        storage="./default_storage",
        schema="bionty",
        name="lamindb-setup-unit-tests",
    )


def pytest_sessionfinish(session: pytest.Session):
    logger.set_verbosity(1)
    lamindb_setup.delete("lamindb-setup-unit-tests", force=True)
