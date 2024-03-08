from lamin_utils import logger
from typing import Optional
import os
from ._init_instance import reload_lamindb, reload_schema_modules
from ._silence_loggers import silence_loggers
from .core._settings_store import current_instance_settings_file
from .core.exceptions import DefaultMessageException
from .core._settings import settings

_LAMINDB_CONNECTED_TO: Optional[str] = None


class InstanceNotSetupError(DefaultMessageException):
    default_message = """\
To use lamindb, you need to connect to an instance.

Connect to an instance: `ln.connect()`. Init an instance: `ln.setup.init()`.

If you used the CLI to set up lamindb in a notebook, restart the Python session.
"""


# we make this a private function because in all the places it's used,
# users should see it
def _check_instance_setup(from_lamindb: bool = False) -> bool:
    from .core.django import IS_SETUP, setup_django

    if _LAMINDB_CONNECTED_TO is not None:
        return True
    if IS_SETUP:
        return True
    silence_loggers()
    if os.environ.get("LAMINDB_MULTI_INSTANCE") == "true":
        logger.warning(
            "running LaminDB in multi-instance mode; you'll experience "
            "errors in regular lamindb usage"
        )
        return True
    if current_instance_settings_file().exists():
        try:
            # attempt loading the settings file
            from .core._settings_load import load_instance_settings

            isettings = load_instance_settings()

            # this flag should probably be renamed to `from_user`
            # it will typically be invoked if lamindb is imported for use
            # but users might also import their schema modules first
            # and then want lamindb be to be available
            if from_lamindb and settings.auto_connect:
                # this guarantees that ths is called exactly once
                # prior to django being setup!
                if not IS_SETUP:
                    setup_django(isettings)
                    reload_schema_modules(isettings)
                    # only now we can import lamindb
                    reload_lamindb(isettings)
                return True
            else:
                return IS_SETUP
        except Exception as e:
            # user will get more detailed traceback once they run the CLI
            logger.error(
                "Current instance cannot be reached, close it: `lamin close`\n"
                "Alternatively, init or load a connectable instance on the"
                " command line: `lamin load <instance>` or `lamin init <...>`"
            )
            raise e
    else:
        if from_lamindb:
            logger.warning(InstanceNotSetupError.default_message)
        return False
