from lamin_utils import logger

from ._init_instance import reload_lamindb, reload_schema_modules
from ._silence_loggers import silence_loggers
from .dev._settings_store import current_instance_settings_file

_INSTANCE_NOT_SETUP_WARNING = """\
You haven't yet setup an instance: Please call `ln.setup.init()` or `ln.setup.load()`
"""


def check_instance_setup(from_lamindb: bool = False):
    if current_instance_settings_file().exists():
        silence_loggers()
        try:
            # attempt loading the settings file
            from .dev._settings_load import load_instance_settings

            isettings = load_instance_settings()

            from .dev.django import IS_SETUP, setup_django

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
