from pathlib import Path
from typing import Optional

from lamin_logger import logger

from ._settings import settings
from ._settings_instance import InstanceSettings
from ._settings_load import setup_storage_root


def load(
    instance_name: str, owner: Optional[str] = None, migrate: Optional[bool] = None
) -> Optional[str]:
    """Load existing instance.

    Returns `None` if succeeds, otherwise a string error code.

    Args:
        instance_name: Instance name.
        owner: Owner handle (default: current user).
        migrate: Whether to auto-migrate or not.
    """
    from ._migrate import check_migrate
    from ._setup_instance import persist_check_reload_schema, register
    from ._setup_knowledge import load_bionty_versions

    owner_handle = owner if owner is not None else settings.user.handle
    isettings, message = load_isettings(instance_name, owner_handle)

    if message is not None:
        return message

    persist_check_reload_schema(isettings)
    logger.info(f"Loading instance: {owner}/{isettings.name}")
    message = check_migrate(
        usettings=settings.user, isettings=isettings, migrate_confirmed=migrate
    )
    if message == "migrate-failed":
        return message
    register(isettings, settings.user)
    load_bionty_versions(isettings)
    return message


def load_isettings(instance_name: str, owner_handle: str):
    message, instance, storage = get_instance_metadata_required_for_loading(
        instance_name, owner_handle
    )

    if message is not None:
        return None, message

    if storage["type"] == "local":
        if not Path(storage["root"]).exists():
            logger.error("Instance local storage does not exists.")
            return None, "instance-local-storage-does-not-exists"

    url = None if instance["dbconfig"] == "sqlite" else instance["db"]

    isettings = InstanceSettings(
        storage_root=setup_storage_root(storage["root"]),
        storage_region=storage["region"],
        url=url,
        _schema=instance["schema"],
        name=instance["name"],
        owner=owner_handle,
    )

    return isettings, None


def get_instance_metadata_required_for_loading(instance_name: str, owner_handle: str):
    from ._hub import (
        connect_hub_with_auth,
        get_instance,
        get_storage_by_id,
        get_user_by_handle,
    )

    hub = connect_hub_with_auth()

    try:
        user = get_user_by_handle(hub, owner_handle)

        if user is None:
            logger.error(f"User {owner_handle} does not exists.")
            return "user-does-not-exists", None, None

        instance = get_instance(hub, instance_name, user["id"])

        if instance is None:
            logger.error(f"Instance {owner_handle}/{instance_name} does not exists.")
            return "instance-does-not-exists", None, None

        storage = get_storage_by_id(hub, instance["storage_id"])

        return None, instance, storage
    finally:
        hub.auth.sign_out()


def is_local_db(url: str):
    if "@localhost:" in url:
        return True
    if "@0.0.0.0:" in url:
        return True
    if "@127.0.0.1" in url:
        return True
