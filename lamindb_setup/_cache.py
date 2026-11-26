from __future__ import annotations

import shutil
from typing import TYPE_CHECKING

from .core._logger import logger
from .core._settings_load import load_cache_path_from_settings
from .core._settings_save import save_platform_user_storage_settings
from .core._settings_store import system_settings_file
from .errors import CurrentInstanceNotConfigured

if TYPE_CHECKING:
    from .types import AnyPathStr


def clear_cache_dir():
    from lamindb_setup import disconnect, settings

    from .core._settings import _resolve_cache_dir

    try:
        if settings.instance._is_cloud_sqlite:
            logger.warning(
                "disconnecting the current instance to update the cloud sqlite database."
            )
            disconnect()
    except CurrentInstanceNotConfigured:
        pass

    cache_dir = _resolve_cache_dir(settings.cache_dir)
    if cache_dir.exists():
        shutil.rmtree(cache_dir)
        cache_dir.mkdir()
        logger.success("the cache directory was cleared")
    else:
        logger.warning("the cache directory doesn't exist")


def get_cache_dir():
    from lamindb_setup import settings

    return settings.cache_dir.as_posix()


def set_cache_dir(cache_dir: AnyPathStr | None) -> None:
    from lamindb_setup.core._settings import (
        _default_cache_dir,
        _process_cache_path,
        _resolve_cache_dir,
        settings,
    )
    from lamindb_setup.core.upath import UPath

    old_cache_dir = _resolve_cache_dir(settings.cache_dir)
    requested_cache_dir = _process_cache_path(cache_dir)
    system_cache_path = load_cache_path_from_settings(system_settings_file())
    default_cache_dir = _resolve_cache_dir(
        _process_cache_path(system_cache_path) or _default_cache_dir()
    )

    new_cache_dir = (
        default_cache_dir
        if requested_cache_dir is None
        else _resolve_cache_dir(requested_cache_dir)
    )

    migrating = new_cache_dir != old_cache_dir and old_cache_dir.exists()
    if migrating:
        shutil.copytree(old_cache_dir, new_cache_dir, dirs_exist_ok=True)
        logger.info(
            f"the current cache directory was copied to {new_cache_dir.as_posix()}"
        )
    elif new_cache_dir != old_cache_dir:
        new_cache_dir.mkdir(parents=True, exist_ok=True)

    save_platform_user_storage_settings(
        None if requested_cache_dir is None else new_cache_dir
    )
    settings._cache_dir = UPath(new_cache_dir)

    if migrating:
        if old_cache_dir == default_cache_dir:
            logger.info(
                f"preserved the default cache directory {old_cache_dir.as_posix()}"
            )
        else:
            shutil.rmtree(old_cache_dir)
            logger.info(f"cleared the old cache directory {old_cache_dir.as_posix()}")
