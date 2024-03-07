from lamin_utils import logger
from typing import Optional
import os
from ._init_instance import reload_lamindb, reload_schema_modules
from ._silence_loggers import silence_loggers
from .core._settings_store import current_instance_settings_file

_LAMINDB_CONNECTED_TO: Optional[str] = None

_INSTANCE_NOT_SETUP_WARNING = """\
To use lamindb, you need to connect to an instance.

Connect to an instance: `ln.connect()`. Init an instance: `ln.setup.init()`.
"""


def check_instance_setup(from_lamindb: bool = False) -> bool:
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
            if from_lamindb:
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
            logger.warning(_INSTANCE_NOT_SETUP_WARNING)
        return False
