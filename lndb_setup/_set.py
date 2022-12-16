from pathlib import Path
from typing import Optional, Union

from cloudpathlib import CloudPath
from lamin_logger import logger

from ._settings import settings
from ._settings_load import load_instance_settings
from ._settings_store import current_instance_settings_file, instance_settings_file
from ._setup_instance import get_storage_region, register, setup_storage_root


def set_storage(
    storage: Union[str, Path, CloudPath], instance_name: Optional[str] = None
):
    """Set storage."""
    if settings.instance.owner != settings.user.handle:
        logger.error("Can only set storage if current user is instance owner.")
        return "only-owner-can-set-storage"
    settings_file = (
        instance_settings_file(instance_name, settings.instance.owner)
        if instance_name
        else current_instance_settings_file()
    )
    isettings = load_instance_settings(settings_file)

    if isettings.dialect == "sqlite":
        logger.error("Can't set storage for sqlite instance.")
        return "set-storage-failed"

    storage_root = setup_storage_root(storage)
    storage_region = get_storage_region(storage_root)
    isettings.storage_root = storage_root
    isettings.storage_region = storage_region
    isettings._persist()
    register(isettings, settings.user)
    logger.info(
        f"Set storage {storage_root} for instance {isettings.owner}/{isettings.name}"
    )
