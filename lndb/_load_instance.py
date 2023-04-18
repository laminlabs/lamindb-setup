import os
from typing import Optional

from lamin_logger import logger
from lnhub_rest.core.instance._load_instance import (
    load_instance as load_instance_from_hub,
)

from ._settings import InstanceSettings, settings
from .dev._settings_load import load_instance_settings
from .dev._settings_store import instance_settings_file


def load(
    identifier: str,
    *,
    migrate: Optional[bool] = None,
    _log_error_message: bool = True,
    _access_token: Optional[str] = None,
) -> Optional[str]:
    """Load existing instance.

    Args:
        identifier: The instance identifier can the instance name (owner is
            current user), handle/name, or the URL: https://lamin.ai/handle/name.
        migrate: Whether to auto-migrate or not.

    Returns:
        - migrate-failed if migration failed
        - migrate-success if migration was successful
        - migrate-unnecessary if migration was not required
    """
    owner, name = get_owner_name_from_identifier(identifier)

    hub_result = load_instance_from_hub(
        owner=owner, name=name, _access_token=_access_token
    )
    # if hub_result is not a string, it means it made a request
    # that successfully returned metadata
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
            logger.info(f"Found cached instance metadata: {settings_file}")
            isettings = load_instance_settings(settings_file)
        else:
            if _log_error_message:
                logger.error(
                    f"Instance {owner}/{name} neither loadable from hub nor local"
                    " cache. Check whether instance exists and you have access:"
                    f" https://lamin.ai/{owner}/{name}?tab=collaborators"
                )
            return "instance-not-reachable"

    check, msg = isettings._is_db_setup()
    if not check:
        if _log_error_message:
            raise RuntimeError(msg)
        else:
            logger.warning(
                "Instance metadata exists, but DB might have been corrupted or deleted."
                " Re-initializing the DB."
            )
            return "instance-not-reachable"

    message = load_from_isettings(isettings, migrate)

    if not message == "migrate-failed":
        os.environ["LAMINDB_INSTANCE_LOADED"] = "1"

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
    from ._init_instance import persist_settings_load_schema, register, reload_lamindb
    from ._migrate import check_deploy_migration
    from .dev._setup_knowledge import load_bionty_versions

    logger.info(f"Loading instance: {isettings.owner}/{isettings.name}")
    persist_settings_load_schema(isettings)
    message = check_deploy_migration(
        usettings=settings.user, isettings=isettings, attempt_deploy=migrate
    )
    if message == "migrate-failed":
        return message
    register(isettings, settings.user)
    load_bionty_versions(isettings)
    reload_lamindb()
    return message
