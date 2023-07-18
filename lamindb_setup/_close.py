from lamin_logger import logger

from ._settings import settings
from .dev._settings_store import current_instance_settings_file
from .dev._setup_bionty_sources import delete_bionty_sources_yaml


def close(mute: bool = False) -> None:
    """Close existing instance.

    Returns `None` if succeeds, otherwise an exception is raised.
    """
    if current_instance_settings_file().exists():
        instance = settings.instance.identifier
        try:
            settings.instance._update_cloud_sqlite_file()
        except FileNotFoundError:
            logger.warning("Did not find local cache file")
        current_instance_settings_file().unlink()
        delete_bionty_sources_yaml()
        logger.success(f"Closed instance: {instance}")
    else:
        if not mute:
            logger.info("No instance loaded")
