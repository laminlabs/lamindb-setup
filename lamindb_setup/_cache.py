from __future__ import annotations

import shutil

from lamin_utils import logger

from .core._settings_save import save_system_storage_settings


def clear_cache_dir():
    from lamindb_setup import disconnect, settings

    try:
        if settings.instance._is_cloud_sqlite:
            logger.warning(
                "Disconnecting the current instance to update the cloud sqlite database."
            )
            disconnect()
    except SystemExit as e:
        if str(e) != "No instance connected! Call `lamin connect` or `lamin init`":
            raise e

    cache_dir = settings.cache_dir
    shutil.rmtree(cache_dir)
    cache_dir.mkdir()
    logger.success("The cache directory was cleared.")


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
    if new_cache_dir is None:
        new_cache_dir = DEFAULT_CACHE_DIR
    if new_cache_dir != old_cache_dir:
        shutil.copytree(old_cache_dir, new_cache_dir, dirs_exist_ok=True)
        shutil.rmtree(old_cache_dir)
        logger.info("The current cache directory was moved to the specified location")
    new_cache_dir = new_cache_dir.resolve()
    save_system_storage_settings(new_cache_dir)
    settings._cache_dir = new_cache_dir
