import shutil
from pathlib import Path
from lamin_utils import logger
from typing import Optional
from ._close import close
from .core._settings_instance import InstanceSettings
from .core._settings import settings
from .core._settings_load import connect_instance_settings
from .core._settings_store import instance_settings_file


def delete_cache(cache_dir: Path):
    if cache_dir is not None and cache_dir.exists():
        shutil.rmtree(cache_dir)


def delete_by_isettings(isettings: InstanceSettings) -> None:
    if settings._instance_exists:
        if isettings.slug == settings.instance.slug:
            close(mute=True)  # close() also unlocks, that's why we need it
            settings._instance_settings = None
    settings_file = isettings._get_settings_file()
    if settings_file.exists():
        settings_file.unlink()
    delete_cache(isettings.storage.cache_dir)
    if isettings.dialect == "sqlite":
        if isettings._sqlite_file.exists():
            isettings._sqlite_file.unlink()
        exclusion_dir = isettings.storage.root / ".lamindb/_exclusion"
        if exclusion_dir.exists():
            exclusion_dir.rmdir()
    if isettings.is_remote:
        logger.warning("manually delete your remote instance on lamin.ai")
    logger.warning(f"manually delete your stored data: {isettings.storage.root}")


def delete(instance_name: str, force: bool = False) -> Optional[int]:
    """Delete an instance."""
    if "/" in instance_name:
        logger.warning(
            "Deleting the instance of another user is currently not supported with the"
            " CLI. Please provide only the instance name when deleting an instance ('/'"
            " delimiter not allowed)."
        )
        raise ValueError("Invalid instance name: '/' delimiter not allowed.")
    instance_slug = f"{settings.user.handle}/{instance_name}"
    if not force:
        valid_responses = ["y", "yes"]
        user_input = (
            input(f"Are you sure you want to delete instance {instance_slug}? (y/n) ")
            .strip()
            .lower()
        )
        if user_input not in valid_responses:
            return -1
    logger.info(f"deleting instance {instance_slug}")
    if settings.instance.name == instance_name:
        settings_file = instance_settings_file(instance_name, settings.user.handle)
        if not settings_file.exists():
            logger.warning(
                "could not delete as instance settings do not exist locally. did you"
                " provide a wrong instance name? could you try loading it?"
            )
            return None
        isettings = connect_instance_settings(settings_file)
    else:
        isettings = settings.instance
    delete_by_isettings(isettings)
    return None
