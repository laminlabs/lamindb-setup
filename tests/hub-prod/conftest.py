import os
from uuid import UUID

import lamindb_setup
import pytest
from lamin_utils import logger


def pytest_sessionstart(session: pytest.Session):
    lamindb_instance_id = UUID("e1a2d3ab762e4592af5a1e53f288284e")
    os.environ["LAMINDB_INSTANCE_ID_INIT"] = lamindb_instance_id.hex
    assert lamindb_setup.settings.user.handle == "testuser2"
    lamindb_setup.init(
        storage="./default_storage",
        schema="bionty",
        name="lamindb-setup-unit-tests",
    )
    assert lamindb_setup.settings.instance._id == lamindb_instance_id
    assert lamindb_setup.settings.instance.uid == "7cIQBFhUg8ok"


def pytest_sessionfinish(session: pytest.Session):
    logger.set_verbosity(1)
    lamindb_setup.delete("testuser2/lamindb-setup-unit-tests", force=True)
