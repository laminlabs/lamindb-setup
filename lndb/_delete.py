import shutil
from pathlib import Path

from lamin_logger import logger

from ._settings import settings
from .dev._settings_load import load_instance_settings
from .dev._settings_store import current_instance_settings_file, instance_settings_file


def delete(instance_name: str):
    """Delete an instance."""
    instance_identifier = f"{settings.user.handle}/{instance_name}"
    logger.info(f"Deleting instance {instance_identifier}")
    settings_file = instance_settings_file(instance_name, settings.user.handle)
    if not settings_file.exists():
        raise RuntimeError(
            "Instance settings do not exist locally. Did you provide a wrong instance"
            " name? Could you try loading it?"
        )
    isettings = load_instance_settings(settings_file)

    delete_settings(settings_file)
    if settings._instance_exists:
        if instance_identifier == settings.instance.identifier:
            current_settings_file = current_instance_settings_file()
            logger.info(
                f"    current instance settings {current_settings_file} deleted"
            )
            current_settings_file.unlink()
            settings._instance_settings = None
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
