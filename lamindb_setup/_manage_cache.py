import shutil
from lamin_utils import logger


def clear_cache_dir():
    from lamindb_setup import settings, close

    if settings.instance._is_cloud_sqlite:
        logger.warning(
            "Closing the current instance to update the cloud sqlite database."
        )
        close()

    cache_dir = settings.storage.cache_dir
    shutil.rmtree(cache_dir)
    cache_dir.mkdir()


def set_cache_dir(cache_dir: str):
    from lamindb_setup import settings

    settings.storage.cache_dir = cache_dir
