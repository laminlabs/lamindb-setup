from lamin_logger import logger

from ._init_instance import reload_schema_modules
from ._silence_loggers import silence_loggers
from .dev._settings_store import current_instance_settings_file


def check_instance_setup(from_lamindb: bool = False):
    if current_instance_settings_file().exists():
        silence_loggers()
        try:
            # attempt loading the settings file
            from .dev._settings_load import load_instance_settings

            isettings = load_instance_settings()

            from .dev._django import IS_SETUP, setup_django

            if from_lamindb:
                setup_django(isettings)
                reload_schema_modules(isettings)
            else:
                return IS_SETUP

            # set the check to true
            return True
        except Exception:
            # user will get more detailed traceback once they run the CLI
            raise RuntimeError(
                "Current instance cannot be reached, close it: `lamin close`\n"
                "Alternatively, init or load a connectable instance on the"
                " command line: `lamin load <instance>` or `lamin init <...>`"
            )
    else:
        if from_lamindb:
            logger.warning(
                "You haven't yet setup an instance using the CLI: Please call"
                " `ln.setup.init()` or `ln.setup.load()`"
            )
        return False
