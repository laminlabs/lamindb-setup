import shutil
from pathlib import Path

from lamin_logger import logger

from ._settings import settings
from .dev._settings_load import load_instance_settings
from .dev._settings_store import instance_settings_file


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
    if isettings.dialect == "sqlite":
        if isettings._sqlite_file.exists():
            isettings._sqlite_file.unlink()
            logger.info("    deleted '.lndb' sqlite file")
        else:
            logger.info("    '.lndb' sqlite file does not exist")
    if isettings.is_remote:
        logger.info("    please manually delete your remote instance on lamin.ai")


def delete_cache(cache_dir: Path):
    if cache_dir is not None and cache_dir.exists():
        shutil.rmtree(cache_dir)
        logger.info("    instance cache deleted")


def delete_settings(settings_file: Path):
    if settings_file.exists():
        settings_file.unlink()
        logger.info("    instance settings '.env' deleted")
    else:
        logger.info("    instance settings '.env' do not exist locally")
