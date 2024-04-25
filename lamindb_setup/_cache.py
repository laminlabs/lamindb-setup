from __future__ import annotations

import shutil

from lamin_utils import logger


def clear_cache_dir():
    from lamindb_setup import close, settings

    if settings.instance._is_cloud_sqlite:
        logger.warning(
            "Closing the current instance to update the cloud sqlite database."
        )
        close()

    cache_dir = settings.storage.cache_dir
    shutil.rmtree(cache_dir)
    cache_dir.mkdir()
    logger.success("The cache directory was cleared.")


def get_cache_dir():
    from lamindb_setup import settings

    return settings.storage.cache_dir.as_posix()


def set_cache_dir(cache_dir: str):
    from lamindb_setup import settings

    settings.storage.cache_dir = cache_dir
    cache_str = settings.storage.cache_dir.as_posix()  # type: ignore
    logger.success(f"The cache directory was set to {cache_str}.")
