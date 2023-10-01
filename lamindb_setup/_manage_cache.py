import shutil
from lamin_utils import logger


def clear_cache_folder():
    from lamindb_setup import settings, close

    if settings.instance._is_cloud_sqlite:
        logger.warning(
            "Closing the current instance to update the cloud sqlite database."
        )
        close()

    cache_folder = settings.storage.cache_dir
    shutil.rmtree(cache_folder)
    cache_folder.mkdir()
