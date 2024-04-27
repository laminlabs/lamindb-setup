import os
from typing import TYPE_CHECKING
from uuid import UUID

import lamindb_setup
import pytest
from lamin_utils import logger


def pytest_sessionstart(session: pytest.Session):
    lamindb_instance_id = UUID("10075f070b0b48b0900618724fb3be62")
    os.environ["LAMINDB_INSTANCE_ID_INIT"] = lamindb_instance_id.hex
    lamindb_setup.init(
        storage="./default_storage",
        schema="bionty",
        name="lamindb-setup-unit-tests",
    )
    assert lamindb_setup.settings.instance._id == lamindb_instance_id
    assert lamindb_setup.settings.instance.uid == "7clAMMtTbqlK"


def pytest_sessionfinish(session: pytest.Session):
    logger.set_verbosity(1)
    lamindb_setup.delete("lamindb-setup-unit-tests", force=True)
