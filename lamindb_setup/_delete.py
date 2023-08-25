import shutil
from pathlib import Path

from lamin_utils import logger

from ._close import close
from ._settings import settings
from .dev._settings_load import load_instance_settings
from .dev._settings_store import instance_settings_file


def delete(instance_name: str, force: bool = False):
    """Delete an instance."""
    if "/" in instance_name:
        logger.warning(
            "Deleting the instance of another user is currently not supported with the"
            " CLI. Please provide only the instance name when deleting an instance ('/'"
            " delimiter not allowed)."
        )
        raise ValueError("Invalid instance name: '/' delimiter not allowed.")

    if not force:
        valid_responses = ["y", "yes"]
        user_input = (
            input("Are you sure you want to delete this instance? (y/n): ")
            .strip()
            .lower()
        )
        if user_input not in valid_responses:
            return -1

    instance_identifier = f"{settings.user.handle}/{instance_name}"
    logger.info(f"deleting instance {instance_identifier}")
    settings_file = instance_settings_file(instance_name, settings.user.handle)
    if not settings_file.exists():
        logger.warning(
            "could not delete as instance settings do not exist locally. did you"
            " provide a wrong instance name? could you try loading it?"
        )
        return None
    isettings = load_instance_settings(settings_file)

    delete_settings(settings_file)
    if settings._instance_exists:
        if instance_identifier == settings.instance.identifier:
            close(mute=True)  # close() does further operations, unlocking...
            settings._instance_settings = None
    delete_cache(isettings.storage.cache_dir)
    if isettings.dialect == "sqlite":
        if isettings._sqlite_file.exists():
            isettings._sqlite_file.unlink()
            logger.success("    deleted '.lndb' sqlite file")
        else:
            logger.warning("    '.lndb' sqlite file does not exist")
    if isettings.is_remote:
        logger.warning(
            "    consider manually deleting your remote instance on lamin.ai"
        )
    logger.warning(
        f"    consider manually deleting your stored data: {isettings.storage.root}"
    )


def delete_cache(cache_dir: Path):
    if cache_dir is not None and cache_dir.exists():
        shutil.rmtree(cache_dir)
        logger.success("    instance cache deleted")


def delete_settings(settings_file: Path):
    if settings_file.exists():
        settings_file.unlink()
        logger.success(f"    deleted instance settings file: {settings_file}")
    else:
        logger.warning(f"    instance settings file doesn't exist: {settings_file}")
