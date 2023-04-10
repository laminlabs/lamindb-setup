from lamin_logger import logger

from .dev._settings_store import current_instance_settings_file


def check_instance_setup(from_lamindb: bool = False):
    if current_instance_settings_file().exists():
        try:
            # attempt loading the settings file
            from .dev._settings_load import load_instance_settings

            load_instance_settings()

            # if importing from lamindb, also ensure migrations are correct
            if from_lamindb:
                # attempt accessing settings and migrating the instance
                from . import settings
                from ._migrate import check_deploy_migration

                check_deploy_migration(
                    usettings=settings.user, isettings=settings.instance
                )
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
        logger.warning(
            "You haven't yet setup an instance using the CLI: Please call"
            " `ln.setup.init()` or `ln.setup.load()`"
        )
        return False
