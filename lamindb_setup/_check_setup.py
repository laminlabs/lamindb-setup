from __future__ import annotations

import os
from typing import TYPE_CHECKING, Optional

from lamin_utils import logger

from ._silence_loggers import silence_loggers
from .core import django
from .core._settings import settings
from .core._settings_store import current_instance_settings_file
from .core.exceptions import DefaultMessageException

if TYPE_CHECKING:
    from .core._settings_instance import InstanceSettings


class InstanceNotSetupError(DefaultMessageException):
    default_message = """\
To use lamindb, you need to connect to an instance.

Connect to an instance: `ln.connect()`. Init an instance: `ln.setup.init()`.

If you used the CLI to set up lamindb in a notebook, restart the Python session.
"""


CURRENT_ISETTINGS: InstanceSettings | None = None


def _get_current_instance_settings() -> InstanceSettings | None:
    global CURRENT_ISETTINGS

    if CURRENT_ISETTINGS is not None:
        return CURRENT_ISETTINGS
    if current_instance_settings_file().exists():
        from .core._settings_load import load_instance_settings

        try:
            isettings = load_instance_settings()
        except Exception as e:
            # user will get more detailed traceback once they run the CLI
            logger.error(
                "Current instance cannot be reached, close it: `lamin close`\n"
                "Alternatively, init or load a connectable instance on the"
                " command line: `lamin load <instance>` or `lamin init <...>`"
            )
            raise e
        return isettings
    else:
        return None


# we make this a private function because in all the places it's used,
# users should see it
def _check_instance_setup(from_lamindb: bool = False) -> bool:
    if django.IS_SETUP:
        return True
    silence_loggers()
    if os.environ.get("LAMINDB_MULTI_INSTANCE") == "true":
        logger.warning(
            "running LaminDB in multi-instance mode; you'll experience "
            "errors in regular lamindb usage"
        )
        return True
    isettings = _get_current_instance_settings()
    if isettings is not None:
        if from_lamindb and settings.auto_connect:
            if not django.IS_SETUP:
                from ._init_instance import reload_schema_modules

                django.setup_django(isettings)
                reload_schema_modules(isettings)
                logger.important(f"connected lamindb: {isettings.slug}")
        return django.IS_SETUP
    else:
        if from_lamindb and settings.auto_connect:
            logger.warning(InstanceNotSetupError.default_message)
        return False
