import os
from pathlib import Path
from typing import Optional, Union

import sqlmodel as sqm
from lamin_logger import logger
from lnhub_rest.core.instance._load_instance import (
    load_instance as load_instance_from_hub,
)

from lndb.dev.upath import UPath

from . import _USE_DJANGO
from ._settings import InstanceSettings, settings
from .dev._django import setup_django
from .dev._settings_load import load_instance_settings
from .dev._settings_store import instance_settings_file
from .dev._storage import StorageSettings


def load(
    identifier: str,
    *,
    migrate: Optional[bool] = None,
    storage: Optional[Union[str, Path, UPath]] = None,
    _log_error_message: bool = True,
    _access_token: Optional[str] = None,
    _test: bool = False,
) -> Optional[str]:
    """Load existing instance.

    Args:
        identifier: `str` - The instance identifier can the instance name (owner is
            current user), handle/name, or the URL: https://lamin.ai/handle/name.
        storage: `Optional[PathLike] = None` - Load the instance with an
            updated default storage.
        migrate: `Optional[bool] = None` - Whether to auto-migrate or not.

    Returns:
        - "migrate-failed" if migration failed
        - "migrate-success" if migration was successful
        - "migrate-unnecessary" if migration was not required
    """
    owner, name = get_owner_name_from_identifier(identifier)

    hub_result = load_instance_from_hub(
        owner=owner, name=name, _access_token=_access_token
    )
    # if hub_result is not a string, it means it made a request
    # that successfully returned metadata
    if not isinstance(hub_result, str):
        instance_result, storage_result = hub_result
        isettings = InstanceSettings(
            owner=owner,
            name=name,
            storage_root=storage_result.root,
            storage_region=storage_result.region,
            db=instance_result.db,
            schema=instance_result.schema_str,
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

    if storage is not None:
        update_isettings_with_storage(isettings, storage)
    if _test:
        isettings._persist()  # this is to test the settings
        return None

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

    if _USE_DJANGO:
        setup_django(isettings)
    else:
        from ._init_instance import persist_settings_load_schema

        persist_settings_load_schema(isettings)
    if storage is not None and isettings.dialect == "sqlite":
        update_storage(isettings)

    if not isettings.storage.root.exists():
        raise RuntimeError(
            f"Storage root does not exist: {isettings.storage.root}\n"
            "Please amend by passing --storage <my-storage-root>"
        )

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
) -> Optional[str]:
    from ._init_instance import persist_settings_load_schema, register, reload_lamindb
    from ._migrate import check_deploy_migration
    from .dev._setup_knowledge import load_bionty_versions

    persist_settings_load_schema(isettings)
    message = None
    if not _USE_DJANGO:
        message = check_deploy_migration(
            usettings=settings.user, isettings=isettings, attempt_deploy=migrate
        )
        if message == "migrate-failed":
            return message
    register(isettings, settings.user)
    load_bionty_versions(isettings)
    reload_lamindb(isettings)
    return message


def update_isettings_with_storage(
    isettings: InstanceSettings, storage: Union[str, Path, UPath]
) -> None:
    ssettings = StorageSettings(storage, instance_settings=isettings)
    if ssettings.is_cloud:
        try:  # triggering ssettings.id makes a lookup in the storage table
            logger.success(f"Loaded storage: {ssettings.id} / {ssettings.root_as_str}")
        except RuntimeError:
            raise RuntimeError(
                "Storage not registered!\n"
                "Load instance without the `storage` arg and register storage root: "
                f"`lamin set storage --storage {storage}`"
            )
    else:
        # local storage
        # assumption is you want to merely update the storage location
        isettings._storage = ssettings  # need this here already
    # update isettings in place
    isettings._storage = ssettings


# this is different from register!
def update_storage(isettings: InstanceSettings):
    if _USE_DJANGO:
        from lnschema_core.models import Storage

        storages = Storage.objects.all()
        if len(storages) != 1:
            raise RuntimeError("Can't identify which storage location to update")
        storage = storages[0]
        storage.root = isettings.storage.root_as_str
        storage.save()
    else:
        from lnschema_core import Storage

        isettings._engine = sqm.create_engine(isettings.db)
        with sqm.Session(isettings.engine) as session:
            storage = session.exec(
                sqm.select(Storage).where(Storage.root == isettings.storage.root_as_str)
            ).one_or_none()
        if storage is None:
            with sqm.Session(isettings.engine) as session:
                storage_record = session.exec(sqm.select(Storage)).one()
                storage_record.root = isettings.storage.root_as_str
                session.add(storage_record)
                session.commit()
                session.refresh(storage_record)
            logger.success(
                f"Updated storage root {storage_record.id} to"
                f" {isettings.storage.root_as_str}"
            )
