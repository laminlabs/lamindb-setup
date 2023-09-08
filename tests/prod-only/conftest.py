import shutil

import pytest
from lamin_utils import logger

import lamindb_setup


def pytest_sessionstart(session: pytest.Session):
    lamindb_setup.init(
        storage="./default_storage",
        schema="bionty",
        name="lamindb-setup-unit-tests",
    )


def pytest_sessionfinish(session: pytest.Session):
    logger.set_verbosity(1)
    lamindb_setup.delete("lamindb-setup-unit-tests", force=True)
    shutil.rmtree("./default_storage")
