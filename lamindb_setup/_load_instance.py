from pathlib import Path
from typing import Optional, Union
from uuid import UUID

from lamin_utils import logger

from lamindb_setup.dev.upath import UPath
from ._close import close as close_instance
from ._init_instance import load_from_isettings
from ._settings import InstanceSettings, settings
from ._silence_loggers import silence_loggers
from .dev._settings_load import load_instance_settings
from .dev._settings_storage import StorageSettings
from .dev._settings_store import instance_settings_file
from .dev._hub_client import call_with_fallback
from .dev._hub_crud import sb_select_account_handle_name_by_lnid
from .dev.cloud_sqlite_locker import (
    InstanceLockedException,
    unlock_cloud_sqlite_upon_exception,
    EXPIRATION_TIME,
)


# this is for testing purposes only
# set to True only to test failed load
_TEST_FAILED_LOAD = False


@unlock_cloud_sqlite_upon_exception(ignore_prev_locker=True)
def load(
    identifier: str,
    *,
    storage: Optional[Union[str, Path, UPath]] = None,
    _raise_not_reachable_error: bool = True,
    _test: bool = False,
) -> Optional[str]:
    """Load existing instance.

    Args:
        identifier: `str` - The instance identifier `owner/name`.
            You can also pass the URL: `https://lamin.ai/owner/name`.
            If the instance is owned by you,
            it suffices to pass the instance name.
        storage: `Optional[PathLike] = None` - Load the instance with an
            updated default storage.
    """
    from ._check_instance_setup import check_instance_setup
    from .dev._hub_core import load_instance as load_instance_from_hub

    owner, name = get_owner_name_from_identifier(identifier)

    if check_instance_setup() and not _test:
        raise RuntimeError(
            "Currently don't support init or load of multiple instances in the same"
            " Python session. We will bring this feature back at some point."
        )
    else:
        # compare normalized identifier with a potentially previously loaded identifier
        if (
            settings._instance_exists
            and f"{owner}/{name}" != settings.instance.identifier
        ):
            close_instance(mute=True)

    # the following will return a string if the instance does not exist
    # on the hub
    hub_result = load_instance_from_hub(owner=owner, name=name)

    # if hub_result is not a string, it means it made a request
    # that successfully returned metadata
    if not isinstance(hub_result, str):
        instance_result, storage_result = hub_result
        isettings = InstanceSettings(
            owner=owner,
            name=name,
            storage_root=storage_result["root"],
            storage_region=storage_result["region"],
            db=instance_result["db"],
            schema=instance_result["schema_str"],
            id=UUID(instance_result["id"]),
        )
    else:
        settings_file = instance_settings_file(name, owner)
        if settings_file.exists():
            isettings = load_instance_settings(settings_file)
            if isettings.is_remote:
                if _raise_not_reachable_error:
                    raise RuntimeError(
                        f"Remote instance {owner}/{name} not loadable from hub. The"
                        " instance might have been deleted or you may have lost"
                        " access."
                    )
                return "instance-not-reachable"
            logger.info(f"found cached instance metadata: {settings_file}")
        else:
            if _raise_not_reachable_error:
                raise RuntimeError(
                    f"Instance {owner}/{name} neither loadable from hub nor local"
                    " cache. check whether instance exists and you have access:"
                    f" https://lamin.ai/{owner}/{name}?tab=collaborators"
                )
            return "instance-not-reachable"

    if storage is not None:
        update_isettings_with_storage(isettings, storage)
    if _test:
        isettings._persist()  # this is to test the settings
        return None
    silence_loggers()
    check, msg = isettings._load_db()  # this also updates local SQLite
    if not check:
        local_db = isettings._is_cloud_sqlite and isettings._sqlite_file_local.exists()
        if local_db:
            logger.warning(
                "SQLite file does not exist in the cloud, but exists locally:"
                f" {isettings._sqlite_file_local}\nTo push the file to the cloud, call:"
                " lamin close"
            )
        elif _raise_not_reachable_error:
            raise RuntimeError(msg)
        else:
            logger.warning(
                "Instance metadata exists, but DB might have been corrupted or deleted."
                " Re-initializing the DB."
            )
            return "instance-not-reachable"
    # at this point the lock should be already initialized
    if not isettings._cloud_sqlite_locker.has_lock:
        locked_by = isettings._cloud_sqlite_locker._locked_by
        lock_msg = "Cannot load the instance, it is locked by "
        user_info = call_with_fallback(
            sb_select_account_handle_name_by_lnid,
            lnid=locked_by,
        )
        if user_info is None:
            lock_msg += f"id: '{locked_by}'."
        else:
            lock_msg += (
                f"'{user_info['handle']}' (id: '{locked_by}', name:"
                f" '{user_info['name']}')."
            )
        lock_msg += (
            "The instance will be automatically unlocked after"
            f" {int(EXPIRATION_TIME/3600)}h of no activity."
        )
        raise InstanceLockedException(lock_msg)

    # this is for testing purposes only
    if _TEST_FAILED_LOAD:
        raise RuntimeError("Technical testing error.")

    if storage is not None and isettings.dialect == "sqlite":
        update_root_field_in_default_storage(isettings)
    load_from_isettings(isettings)
    return None


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


def update_isettings_with_storage(
    isettings: InstanceSettings, storage: Union[str, Path, UPath]
) -> None:
    ssettings = StorageSettings(storage)
    if ssettings.is_cloud:
        try:  # triggering ssettings.id makes a lookup in the storage table
            logger.success(f"loaded storage: {ssettings.id} / {ssettings.root_as_str}")
        except RuntimeError:
            raise RuntimeError(
                "storage not registered!\n"
                "load instance without the `storage` arg and register storage root: "
                f"`lamin set storage --storage {storage}`"
            )
    else:
        # local storage
        # assumption is you want to merely update the storage location
        isettings._storage = ssettings  # need this here already
    # update isettings in place
    isettings._storage = ssettings


# this is different from register!
# register registers a new storage location
# update_root_field_in_default_storage updates the root
# field in the default storage locations
def update_root_field_in_default_storage(isettings: InstanceSettings):
    from lnschema_core.models import Storage

    storages = Storage.objects.all()
    if len(storages) != 1:
        raise RuntimeError(
            "You have several storage locations: Can't identify in which storage"
            " location the root column is to be updated!"
        )
    storage = storages[0]
    storage.root = isettings.storage.root_as_str
    storage.save()
    logger.save(f"updated storage root {storage.id} to {isettings.storage.root_as_str}")
