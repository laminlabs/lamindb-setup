from __future__ import annotations

from lamin_utils import logger

from .core._settings import settings
from .core._settings_store import current_instance_settings_file
from .core.cloud_sqlite_locker import clear_locker


def disconnect(mute: bool = False) -> None:
    """Disconnect an instance.

    Returns `None` if succeeds, otherwise an exception is raised.
    """
    if current_instance_settings_file().exists():
        instance = settings.instance.slug
        try:
            settings.instance._update_cloud_sqlite_file()
        except Exception as e:
            if isinstance(e, FileNotFoundError):
                logger.warning("did not find local cache file")
            elif isinstance(e, PermissionError):
                logger.warning("did not upload cache file - not enough permissions")
            else:
                raise e
        current_instance_settings_file().unlink()
        clear_locker()
        if not mute:
            logger.success(f"disconnected instance: {instance}")
    else:
        if not mute:
            logger.info("no instance loaded")
