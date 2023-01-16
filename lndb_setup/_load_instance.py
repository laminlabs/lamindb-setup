from typing import Optional

from lamin_logger import logger
from lnhub_rest._init_instance_sbclient import init_instance as init_instance_hub
from lnhub_rest._load_instance_sbclient import load_instance as load_instance_from_hub

from ._settings import InstanceSettings, settings
from ._settings_load import load_instance_settings
from ._settings_store import instance_settings_file


def load(
    identifier: str,
    *,
    migrate: Optional[bool] = None,
    _log_error_message: bool = True,
) -> Optional[str]:
    """Load existing instance.

    Returns `None` if succeeds, otherwise a string error code.

    Args:
        identifier: The instance identifier can the instance name (owner is
            current user), handle/name, or the URL: https://lamin.ai/handle/name.
        migrate: Whether to auto-migrate or not.
    """
    owner, name = get_owner_name_from_identifier(identifier)

    hub_result = load_instance_from_hub(owner=owner, name=name)
    if not isinstance(hub_result, str):
        instance, storage = hub_result
        isettings = InstanceSettings(
            owner=owner,
            name=name,
            storage_root=storage.root,
            storage_region=storage.region,
            db=instance.db,
            schema=instance.schema_str,
        )
    else:
        settings_file = instance_settings_file(name, owner)
        if settings_file.exists():
            isettings = load_instance_settings(settings_file)
        else:
            if _log_error_message:
                logger.error("Instance neither exists on hub nor locally.")
            return "instance-not-exists"

    check, msg = isettings._is_db_setup()
    if not check:
        if _log_error_message:
            raise RuntimeError(msg)
        else:
            logger.warning(
                "Instance metadata exists, but DB might have been corrupted or deleted."
                " Re-initializing the DB."
            )
            return "instance-not-exists"

    # register legacy instances on hub if they aren't yet!
    if isettings.is_remote and isinstance(hub_result, str):
        logger.info("Registering instance on hub.")
        init_instance_hub(
            owner=isettings.owner,
            name=isettings.name,
            storage=str(isettings.storage.root),
            db=isettings._db,
            schema=isettings._schema_str,
        )

    message = load_from_isettings(isettings, migrate)
    return message


def get_owner_name_from_identifier(identifier: str):
    if "/" in identifier:
        if identifier.startswith("https://lamin.ai/"):
            identifier = identifier.replace("https://lamin.ai/", "")
        split = identifier.split("/")
        if len(split) > 2:
            raise ValueError(
                "The instance identifier needs to be 'owner/name', the instance name"
                " (owner is current user) or the URL: https://lamin.ai/owner/name."
            )
        owner, name = split
    else:
        owner = settings.user.handle
        name = identifier
    return owner, name


def load_from_isettings(
    isettings: InstanceSettings,
    migrate: Optional[bool] = None,
):
    from ._init_instance import persist_check_reload_schema, register
    from ._migrate import check_migrate
    from ._setup_knowledge import load_bionty_versions

    logger.info(f"Loading instance: {isettings.owner}/{isettings.name}")
    persist_check_reload_schema(isettings)
    message = check_migrate(
        usettings=settings.user, isettings=isettings, migrate_confirmed=migrate
    )
    if message == "migrate-failed":
        return message
    register(isettings, settings.user)
    load_bionty_versions(isettings)
    return message
