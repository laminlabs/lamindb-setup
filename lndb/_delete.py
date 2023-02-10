import shutil
from pathlib import Path

from lamin_logger import logger

from ._settings import settings
from ._settings_load import load_instance_settings
from ._settings_store import instance_settings_file


def delete(instance_name: str):
    """Delete an instance."""
    logger.info(f"Deleting instance {settings.user.handle}/{instance_name}")
    settings_file = instance_settings_file(instance_name, settings.user.handle)
    isettings = load_instance_settings(settings_file)

    delete_settings(settings_file)
    delete_cache(isettings.storage.cache_dir)
    logger.info(
        f"    consider deleting your stored data manually: {isettings.storage.root}"
    )
    if isettings.is_remote:
        logger.info("    please manually delete your remote instance on lamin.ai")
    else:
        if isettings.dialect == "sqlite":
            isettings._sqlite_file.unlink()
            logger.info("    deleted sqlite file")


def delete_cache(cache_dir: Path):
    if cache_dir is not None and cache_dir.exists():
        shutil.rmtree(cache_dir)
        logger.info("    instance cache deleted")


def delete_settings(settings_file: Path):
    settings_file.unlink()
    logger.info("    instance settings deleted")
