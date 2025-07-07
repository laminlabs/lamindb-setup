from __future__ import annotations

import importlib
import os
from typing import TYPE_CHECKING, Any
from uuid import UUID

from lamin_utils import logger

from ._check_setup import _check_instance_setup, _get_current_instance_settings
from ._disconnect import disconnect
from ._init_instance import (
    MESSAGE_CANNOT_SWITCH_DEFAULT_INSTANCE,
    load_from_isettings,
)
from ._silence_loggers import silence_loggers
from .core._hub_core import connect_instance_hub
from .core._hub_utils import (
    LaminDsn,
    LaminDsnModel,
)
from .core._settings import settings
from .core._settings_instance import InstanceSettings
from .core._settings_load import load_instance_settings
from .core._settings_storage import StorageSettings
from .core._settings_store import instance_settings_file, settings_dir
from .core.cloud_sqlite_locker import unlock_cloud_sqlite_upon_exception
from .errors import CannotSwitchDefaultInstance

if TYPE_CHECKING:
    from pathlib import Path

    from .core._settings_user import UserSettings
    from .types import UPathStr

# this is for testing purposes only
# set to True only to test failed load
_TEST_FAILED_LOAD = False


INSTANCE_NOT_FOUND_MESSAGE = (
    "'{owner}/{name}' not found:"
    " '{hub_result}'\nCheck your permissions:"
    " https://lamin.ai/{owner}/{name}"
)


class InstanceNotFoundError(SystemExit):
    pass


def check_db_dsn_equal_up_to_credentials(db_dsn_hub, db_dsn_local):
    return (
        db_dsn_hub.scheme == db_dsn_local.scheme
        and db_dsn_hub.host == db_dsn_local.host
        and db_dsn_hub.database == db_dsn_local.database
        and db_dsn_hub.port == db_dsn_local.port
    )


def update_db_using_local(
    hub_instance_result: dict[str, str],
    settings_file: Path,
    db: str | None = None,
    raise_permission_error=True,
) -> str | None:
    db_updated = None
    # check if postgres
    if hub_instance_result["db_scheme"] == "postgresql":
        if db is not None:
            # use only the provided db if it is set
            db_dsn_hub = LaminDsnModel(db=db)
            db_dsn_local = db_dsn_hub
        else:
            db_dsn_hub = LaminDsnModel(db=hub_instance_result["db"])
            # read directly from the environment
            if os.getenv("LAMINDB_INSTANCE_DB") is not None:
                logger.important("loading db URL from env variable LAMINDB_INSTANCE_DB")
                db_dsn_local = LaminDsnModel(db=os.getenv("LAMINDB_INSTANCE_DB"))
            # read from a cached settings file in case the hub result is only
            # read level or inexistent
            elif settings_file.exists() and (
                db_dsn_hub.db.user in {None, "none"} or "read" in db_dsn_hub.db.user  # type:ignore
            ):
                isettings = load_instance_settings(settings_file)
                db_dsn_local = LaminDsnModel(db=isettings.db)
            else:
                # just take the default hub result and ensure there is actually a user
                if (
                    db_dsn_hub.db.user in {None, "none"}
                    and db_dsn_hub.db.password in {None, "none"}
                    and raise_permission_error
                ):
                    raise PermissionError(
                        "No database access, please ask your admin to provide you with"
                        " a DB URL and pass it via --db <db_url>"
                    )
                db_dsn_local = db_dsn_hub
            if not check_db_dsn_equal_up_to_credentials(db_dsn_hub.db, db_dsn_local.db):
                raise ValueError(
                    "The local differs from the hub database information:\n"
                    "did your database get updated by an admin?\n"
                    "Consider deleting your cached database environment:\nrm"
                    f" {settings_file.as_posix()}"
                )
        db_updated = LaminDsn.build(
            scheme=db_dsn_hub.db.scheme,
            user=db_dsn_local.db.user,
            password=db_dsn_local.db.password,
            host=db_dsn_hub.db.host,  # type: ignore
            port=db_dsn_hub.db.port,
            database=db_dsn_hub.db.database,
        )
    return db_updated


def _connect_instance(
    owner: str,
    name: str,
    *,
    db: str | None = None,
    raise_permission_error: bool = True,
    access_token: str | None = None,
) -> InstanceSettings:
    settings_file = instance_settings_file(name, owner)
    make_hub_request = True
    if settings_file.exists():
        isettings = load_instance_settings(settings_file)
        # skip hub request for a purely local instance
        make_hub_request = isettings.is_remote
    if make_hub_request:
        # the following will return a string if the instance does not exist
        # on the hub
        # do not call hub if the user is anonymous
        if owner != "anonymous":
            hub_result = connect_instance_hub(
                owner=owner, name=name, access_token=access_token
            )
        else:
            hub_result = "anonymous-user"
        # if hub_result is not a string, it means it made a request
        # that successfully returned metadata
        if not isinstance(hub_result, str):
            instance_result, storage_result = hub_result
            db_updated = update_db_using_local(
                instance_result,
                settings_file,
                db=db,
                raise_permission_error=raise_permission_error,
            )
            ssettings = StorageSettings(
                root=storage_result["root"],
                region=storage_result["region"],
                uid=storage_result["lnid"],
                uuid=UUID(storage_result["id"]),
                instance_id=UUID(instance_result["id"]),
            )
            isettings = InstanceSettings(
                id=UUID(instance_result["id"]),
                owner=owner,
                name=instance_result["name"],
                storage=ssettings,
                db=db_updated,
                modules=instance_result["schema_str"],
                git_repo=instance_result["git_repo"],
                keep_artifacts_local=bool(instance_result["keep_artifacts_local"]),
                is_on_hub=True,
                api_url=instance_result["api_url"],
                schema_id=None
                if (schema_id := instance_result["schema_id"]) is None
                else UUID(schema_id),
                fine_grained_access=instance_result.get("fine_grained_access", False),
                db_permissions=instance_result.get("db_permissions", None),
            )
        else:
            if hub_result != "anonymous-user":
                message = INSTANCE_NOT_FOUND_MESSAGE.format(
                    owner=owner, name=name, hub_result=hub_result
                )
            else:
                message = "It is not possible to load an anonymous-owned instance from the hub"
            if settings_file.exists():
                isettings = load_instance_settings(settings_file)
                if isettings.is_remote:
                    raise InstanceNotFoundError(message)
            else:
                raise InstanceNotFoundError(message)
    return isettings


@unlock_cloud_sqlite_upon_exception(ignore_prev_locker=True)
def connect(instance: str | None = None, **kwargs: Any) -> str | tuple | None:
    """Connect to an instance.

    Args:
        instance: Pass a slug (`account/name`) or URL (`https://lamin.ai/account/name`).
            If `None`, looks for an environment variable `LAMIN_CURRENT_INSTANCE` to get the instance identifier. If it doesn't find this variable, it connects to the instance that was connected with `lamin connect` through the CLI.
    """
    # validate kwargs
    valid_kwargs = {
        "_db",
        "_write_settings",
        "_raise_not_found_error",
        "_reload_lamindb",
        "_test",
        "_user",
    }
    for kwarg in kwargs:
        if kwarg not in valid_kwargs:
            raise TypeError(f"connect() got unexpected keyword argument '{kwarg}'")
    isettings: InstanceSettings = None  # type: ignore
    # _db is still needed because it is called in init
    _db: str | None = kwargs.get("_db", None)
    _write_settings: bool = kwargs.get("_write_settings", False)
    _raise_not_found_error: bool = kwargs.get("_raise_not_found_error", True)
    _reload_lamindb: bool = kwargs.get("_reload_lamindb", True)
    _test: bool = kwargs.get("_test", False)

    access_token: str | None = None
    _user: UserSettings | None = kwargs.get("_user", None)
    if _user is not None:
        access_token = _user.access_token
    if instance is None:
        instance = os.environ.get("LAMIN_CURRENT_INSTANCE")

    try:
        if instance is None:
            isettings_or_none = _get_current_instance_settings()
            if isettings_or_none is None:
                raise ValueError(
                    "No instance was connected through the CLI, pass a value to `instance` or connect via the CLI."
                )
            isettings = isettings_or_none
        else:
            owner, name = get_owner_name_from_identifier(instance)
            if _check_instance_setup() and not _test:
                if (
                    settings._instance_exists
                    and f"{owner}/{name}" == settings.instance.slug
                ):
                    logger.important(f"connected lamindb: {settings.instance.slug}")
                    return None
                else:
                    raise CannotSwitchDefaultInstance(
                        MESSAGE_CANNOT_SWITCH_DEFAULT_INSTANCE
                    )
            elif (
                _write_settings
                and settings._instance_exists
                and f"{owner}/{name}" != settings.instance.slug
            ):
                disconnect(mute=True)

            try:
                isettings = _connect_instance(
                    owner, name, db=_db, access_token=access_token
                )
            except InstanceNotFoundError as e:
                if _raise_not_found_error:
                    raise e
                else:
                    return "instance-not-found"
            if isinstance(isettings, str):
                return isettings
        # at this point we have checked already that isettings is not a string
        # _user is passed to lock cloud sqlite for this user in isettings._load_db()
        # has no effect if _user is None or if not cloud sqlite instance
        isettings._locker_user = _user
        isettings._persist(write_to_disk=_write_settings)
        if _test:
            return None
        silence_loggers()
        check, msg = isettings._load_db()
        if not check:
            local_db = (
                isettings._is_cloud_sqlite and isettings._sqlite_file_local.exists()
            )
            if local_db:
                logger.warning(
                    "SQLite file does not exist in the cloud, but exists locally:"
                    f" {isettings._sqlite_file_local}\nTo push the file to the cloud,"
                    " call: lamin disconnect"
                )
            elif _raise_not_found_error:
                raise SystemExit(msg)
            else:
                logger.warning(
                    f"instance exists with id {isettings._id.hex}, but database is not"
                    " loadable: re-initializing"
                )
                return "instance-corrupted-or-deleted"
        # this is for testing purposes only
        if _TEST_FAILED_LOAD:
            raise RuntimeError("Technical testing error.")

        load_from_isettings(isettings, user=_user, write_settings=_write_settings)
        if _reload_lamindb:
            importlib.reload(importlib.import_module("lamindb"))
        logger.important(f"connected lamindb: {isettings.slug}")
    except Exception as e:
        if isettings is not None:
            if _write_settings:
                isettings._get_settings_file().unlink(missing_ok=True)  # type: ignore
            settings._instance_settings = None
        raise e
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
