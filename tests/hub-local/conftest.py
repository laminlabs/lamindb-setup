from __future__ import annotations

import os

from lamin_utils import logger
from laminhub_rest.dev._setup_local_hub import setup_local_hub

pytest_plugins = [
    "laminhub_rest.core.account.user.test.fixtures",
    "laminhub_rest.test.fixtures.run_id",
]

local_setup_state = setup_local_hub()


def pytest_configure():
    if os.environ["LAMIN_ENV"] == "local":
        local_setup_state.__enter__()
    else:
        logger.warning("you're running non-local tests")


def pytest_unconfigure():
    if os.environ["LAMIN_ENV"] == "local":
        local_setup_state.__exit__(None, None, None)
