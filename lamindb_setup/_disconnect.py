from __future__ import annotations

from lamin_utils import logger

from .core._settings import settings
from .core._settings_load import load_instance_settings
from .core._settings_store import (
    find_local_current_instance_file,
    remove_local_current_instance,
)
from .core.cloud_sqlite_locker import clear_locker


def disconnect(mute: bool = False, here: bool = False) -> None:
    """Clear default instance configuration.

    Returns `None` if succeeds, otherwise an exception is raised.

    Args:
        mute: If `True`, mute logging output.
        here: If `True`, disconnect local directory context by removing the
            nearest local marker and unsetting the mapped dev-dir.

    See Also:
        Clear default instance configuration via the CLI, see `here <https://docs.lamin.ai/cli#disconnect>`__.
    """
    if here:
        marker = find_local_current_instance_file()
        if marker is None:
            if not mute:
                logger.info("no local instance marker found")
            return None

        instance_slug = marker.read_text().strip()
        from ._connect_instance import _connect_cli

        try:
            _connect_cli(instance_slug, here=True)
            settings.dev_dir = None
            if not mute:
                logger.success(
                    f"disconnected local instance context: {instance_slug} and unset dev-dir"
                )
        except Exception:
            removed_marker = remove_local_current_instance(marker=marker)
            if not mute:
                if removed_marker is not None:
                    logger.warning(
                        "removed local instance marker, but could not resolve instance settings to unset dev-dir"
                    )
                else:
                    logger.info("no local instance marker found")
        return None

    # settings.is_configured can be true due to connect even without having a file
    if settings.is_configured:
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
        # if django is set up, reconnect to the in-memory none/none instance
        # to avoid leaking the previous default DB connection after disconnect
        from .core import django as django_lamin

        if django_lamin.IS_SETUP:
            django_lamin.reconnect_django(settings.instance)
        if not mute:
            logger.success(f"disconnected instance: {instance.slug}")
    elif not mute:
        logger.info("no instance loaded")
