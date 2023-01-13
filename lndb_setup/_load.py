import warnings
from typing import Optional

from lamin_logger import logger

from ._settings import InstanceSettings, settings
from ._settings_instance import is_instance_remote
from ._settings_load import load_instance_settings, setup_storage_root
from ._settings_store import instance_settings_file


def load(
    instance_name: str,
    owner: Optional[str] = None,
    migrate: Optional[bool] = None,
    _log_error_message: bool = True,
) -> Optional[str]:
    """Load existing instance.

    Returns `None` if succeeds, otherwise a string error code.

    Args:
        instance_name: Instance name.
        owner: Owner handle (default: current user).
        migrate: Whether to auto-migrate or not.
    """
    from ._setup_instance import is_instance_db_setup

    owner_handle = owner if owner is not None else settings.user.handle

    message, isettings = load_isettings(instance_name, owner_handle, _log_error_message)
    if message is not None:
        return message

    if not is_instance_db_setup(isettings):
        logger.warning("Instance db is not setup")
        return "db-is-not-setup"

    message = load_from_isettings(isettings, migrate)
    return message


def load_from_isettings(
    isettings: InstanceSettings,
    migrate: Optional[bool] = None,
):
    from ._migrate import check_migrate
    from ._setup_instance import persist_check_reload_schema, register
    from ._setup_knowledge import load_bionty_versions

    persist_check_reload_schema(isettings)
    logger.info(f"Loading instance: {isettings.owner}/{isettings.name}")
    message = check_migrate(
        usettings=settings.user, isettings=isettings, migrate_confirmed=migrate
    )
    if message == "migrate-failed":
        return message
    register(isettings, settings.user)
    load_bionty_versions(isettings)
    return message


def load_isettings(
    instance_name: str, owner_handle: str, log_error_message: bool = True
):
    isettings = load_isettings_from_file(instance_name, owner_handle)

    if isettings is None:
        message, isettings = load_isettings_from_hub(instance_name, owner_handle)
        if isettings:
            logger.info("Use settings from hub")
            return None, isettings
        elif log_error_message:
            if message == "user-does-not-exists":
                logger.error(f"User {owner_handle} does not exists.")
                return message, None
            if message == "instance-does-not-exists":
                logger.error(
                    f"Instance {owner_handle}/{instance_name} does not exists."
                )
                return message, None
            else:
                return message, None
        else:
            return message, None

    else:
        if not isettings.is_remote:
            logger.info("Use settings from local file")
            return None, isettings
        else:
            message, isettings = load_isettings_from_hub(instance_name, owner_handle)
            if isettings:
                logger.info("Use settings from hub")
                return None, isettings
            elif log_error_message:
                if message == "user-does-not-exists":
                    logger.error(
                        "Local settings file refer to a user that not exists in the"
                        " hub.\nPlease use delete command to clean up your environment."
                    )
                    return message, None
                if message == "instance-does-not-exists":
                    logger.error(
                        "Local settings file refer to an instance that not exists in"
                        " the hub.\nPlease use delete command to clean up your"
                        " environment."
                    )
                    return message, None
                else:
                    return message, None
            else:
                return message, None


def load_isettings_from_file(instance_name: str, owner_handle: str):
    settings_file = instance_settings_file(instance_name, owner_handle)
    if settings_file.exists():
        isettings = load_instance_settings(settings_file)
        return isettings
    return None


def load_isettings_from_hub(instance_name: str, owner_handle: str):
    message, instance, storage = get_instance_metadata_required_for_loading(
        instance_name, owner_handle
    )

    if message is not None:
        return message, None

    url = None if instance["dbconfig"] == "sqlite" else instance["db"]

    schema = instance["schema"]
    if schema is None:
        schema = ""

    if not is_instance_remote(storage["root"], url):
        warnings.warn(
            "Trying to load a non remote instance from the hub."
            "\nIgnoring settings from hub."
        )
        return "instance-does-not-exists", None

    isettings = InstanceSettings(
        storage_root=setup_storage_root(storage["root"]),
        storage_region=storage["region"],
        url=url,
        _schema=schema,
        name=instance["name"],
        owner=owner_handle,
    )

    return None, isettings


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
            return "user-does-not-exists", None, None

        instance = get_instance(hub, instance_name, user["id"])
        if instance is None:
            return "instance-does-not-exists", None, None

        storage = get_storage_by_id(hub, instance["storage_id"])

        return None, instance, storage
    finally:
        hub.auth.sign_out()
