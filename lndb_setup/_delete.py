import shutil
from pathlib import Path

from lamin_logger import logger

from ._settings import settings
from ._settings_load import load_instance_settings
from ._settings_store import instance_settings_file


def delete(instance_name: str):
    """Delete an instance."""
    settings_file = instance_settings_file(instance_name, settings.user.handle)
    isettings = load_instance_settings(settings_file)

    if isettings.is_remote:
        logger.info("Please delete your remote instance on lamin.ai.")
    else:
        if isettings.storage.type == "local":
            delete_storage(isettings.storage.root)
        else:
            delete_cache(isettings.cache_dir)
            logger.info("Storage won't be deleted as it is a cloud storage.")

    delete_settings(settings_file)


def delete_storage(storage_root: Path):
    if storage_root.exists():
        shutil.rmtree(storage_root)
        logger.info("Instance default storage root deleted.")


def delete_cache(cache_dir: Path):
    if cache_dir:
        if cache_dir.exists():
            shutil.rmtree(cache_dir)
            logger.info("Instance cache deleted.")


def delete_settings(settings_file: Path):
    settings_file.unlink()
    logger.info("Instance settings deleted.")
