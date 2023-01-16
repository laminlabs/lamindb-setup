from pathlib import Path
from typing import Optional, Union

from cloudpathlib import CloudPath
from lamin_logger import logger
from lnhub_rest._add_storage_sbclient import add_storage as add_storage_hub

from ._init_instance import register
from ._settings import settings
from ._settings_instance import InstanceSettings
from ._settings_load import load_instance_settings
from ._settings_store import current_instance_settings_file, instance_settings_file


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

    new_isettings = InstanceSettings(
        owner=isettings.owner,
        name=isettings.name,
        storage_root=storage,
        db=isettings.db,
        schema=isettings._schema_str,
    )

    new_isettings._persist()
    register(new_isettings, settings.user)
    logger.info(
        f"Set storage {storage} for instance {isettings.owner}/{isettings.name}"
    )
    if isettings.is_remote:
        add_storage_hub(storage, account_handle=isettings.owner)
