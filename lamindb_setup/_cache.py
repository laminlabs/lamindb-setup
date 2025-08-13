from __future__ import annotations

import shutil
from pathlib import Path

from dotenv import dotenv_values
from lamin_utils import logger

from .core._settings_save import save_platform_user_storage_settings
from .core._settings_store import system_settings_file
from .errors import CurrentInstanceNotConfigured


def clear_cache_dir():
    from lamindb_setup import disconnect, settings

    try:
        if settings.instance._is_cloud_sqlite:
            logger.warning(
                "disconnecting the current instance to update the cloud sqlite database."
            )
            disconnect()
    except CurrentInstanceNotConfigured:
        pass

    cache_dir = settings.cache_dir
    if cache_dir.exists():
        shutil.rmtree(cache_dir)
        cache_dir.mkdir()
        logger.success("the cache directory was cleared")
    else:
        logger.warning("the cache directory doesn't exist")


def get_cache_dir():
    from lamindb_setup import settings

    return settings.cache_dir.as_posix()


def set_cache_dir(cache_dir: str):
    from lamindb_setup.core._settings import (
        DEFAULT_CACHE_DIR,
        _process_cache_path,
        settings,
    )

    old_cache_dir = settings.cache_dir
    new_cache_dir = _process_cache_path(cache_dir)

    system_cache_dir = None
    if (system_settings := system_settings_file()).exists():
        system_cache_dir = dotenv_values(system_settings).get(
            "lamindb_cache_path", None
        )
        system_cache_dir = (
            Path(system_cache_dir) if system_cache_dir is not None else None
        )

    need_reset = False
    if new_cache_dir is None:
        need_reset = True
        new_cache_dir = (
            DEFAULT_CACHE_DIR if system_cache_dir is None else system_cache_dir
        )

    if new_cache_dir != old_cache_dir:
        if old_cache_dir.exists():
            shutil.copytree(old_cache_dir, new_cache_dir, dirs_exist_ok=True)
            logger.info(
                f"the current cache directory was copied to {new_cache_dir.as_posix()} "
            )
            if old_cache_dir != system_cache_dir:
                shutil.rmtree(old_cache_dir)
                logger.info(
                    f"cleared the old cache directory {old_cache_dir.as_posix()}"
                )
            else:
                logger.info(
                    f"didn't clear the system cache directory {system_cache_dir.as_posix()}, "
                    "please clear it manually if you need"
                )
        else:
            new_cache_dir.mkdir(parents=True, exist_ok=True)
        new_cache_dir = new_cache_dir.resolve()
    save_platform_user_storage_settings(None if need_reset else new_cache_dir)
    settings._cache_dir = new_cache_dir
