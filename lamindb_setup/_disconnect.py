from __future__ import annotations

from lamin_utils import logger

from .core._settings import settings
from .core._settings_load import load_instance_settings
from .core.cloud_sqlite_locker import clear_locker


def disconnect(mute: bool = False) -> None:
    """Clear default instance configuration.

    Returns `None` if succeeds, otherwise an exception is raised.

    See Also:
        Clear default instance configuration via the CLI, see `here <https://docs.lamin.ai/cli#disconnect>`__.
    """
    # settings._instance_exists can be true due to connect even without having a file
    if settings._instance_exists:
        instance = settings.instance
        try:
            instance._update_cloud_sqlite_file()
        except Exception as e:
            if isinstance(e, FileNotFoundError):
                logger.warning("did not find local cache file")
            elif isinstance(e, PermissionError):
                logger.warning("did not upload cache file - not enough permissions")
            else:
                raise e
        clear_locker()
        # instance in current instance file can differ from instance in settings
        if load_instance_settings().slug == instance.slug:
            settings._instance_settings_path.unlink(missing_ok=True)
        settings._instance_settings = None
        if not mute:
            logger.success(f"disconnected instance: {instance.slug}")
    elif not mute:
        logger.info("no instance loaded")
