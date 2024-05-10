from __future__ import annotations

import importlib
import os
import sys
import uuid
from typing import TYPE_CHECKING, Literal
from uuid import UUID

from django.core.exceptions import FieldError
from django.db.utils import OperationalError, ProgrammingError
from lamin_utils import logger

from ._close import close as close_instance
from ._silence_loggers import silence_loggers
from .core import InstanceSettings
from .core._settings import settings
from .core._settings_storage import StorageSettings, init_storage
from .core.upath import convert_pathlike

if TYPE_CHECKING:
    from pydantic import PostgresDsn

    from .core.types import UPathStr


def get_schema_module_name(schema_name) -> str:
    import importlib.util

    name_attempts = [f"lnschema_{schema_name.replace('-', '_')}", schema_name]
    for name in name_attempts:
        module_spec = importlib.util.find_spec(name)
        if module_spec is not None:
            return name
    raise ImportError(
        f"Python package for '{schema_name}' is not installed, tried two package names:"
        f" {name_attempts}\nHave you installed the schema package using `pip install`?"
    )


def register_storage_in_instance(ssettings: StorageSettings):
    from lnschema_core.models import Storage
    from lnschema_core.users import current_user_id

    from .core.hashing import hash_and_encode_as_b62

    if ssettings._instance_id is not None:
        instance_uid = hash_and_encode_as_b62(ssettings._instance_id.hex)[:12]
    else:
        instance_uid = None
    # how do we ensure that this function is only called passing
    # the managing instance?
    defaults = {
        "root": ssettings.root_as_str,
        "type": ssettings.type,
        "region": ssettings.region,
        "instance_uid": instance_uid,
        "created_by_id": current_user_id(),
    }
    if ssettings._uid is not None:
        defaults["uid"] = ssettings._uid
    storage, _ = Storage.objects.update_or_create(
        root=ssettings.root_as_str,
        defaults=defaults,
    )
    return storage


def register_user(usettings):
    from lnschema_core.models import User

    try:
        # need to have try except because of integer primary key migration
        user, created = User.objects.update_or_create(
            uid=usettings.uid,
            defaults={
                "handle": usettings.handle,
                "name": usettings.name,
            },
        )
    # for users with only read access, except via ProgrammingError
    # ProgrammingError: permission denied for table lnschema_core_user
    except (OperationalError, FieldError, ProgrammingError):
        pass


def register_user_and_storage_in_instance(isettings: InstanceSettings, usettings):
    """Register user & storage in DB."""
    from django.db.utils import OperationalError

    try:
        register_user(usettings)
        register_storage_in_instance(isettings.storage)
    except OperationalError as error:
        logger.warning(f"instance seems not set up ({error})")


def reload_schema_modules(isettings: InstanceSettings):
    schema_names = ["core"] + list(isettings.schema)
    schema_module_names = [get_schema_module_name(n) for n in schema_names]

    for schema_module_name in schema_module_names:
        if schema_module_name in sys.modules:
            schema_module = importlib.import_module(schema_module_name)
            importlib.reload(schema_module)


def reload_lamindb_itself(isettings) -> bool:
    reloaded = False
    if "lamindb" in sys.modules:
        import lamindb

        importlib.reload(lamindb)
        reloaded = True
    if "bionty" in isettings.schema and "bionty" in sys.modules:
        schema_module = importlib.import_module("bionty")
        importlib.reload(schema_module)
        reloaded = True
    return reloaded


def reload_lamindb(isettings: InstanceSettings):
    # only touch lamindb if we're operating from lamindb
    reload_schema_modules(isettings)
    log_message = settings.auto_connect
    if not reload_lamindb_itself(isettings):
        log_message = True
    if log_message:
        logger.important(f"connected lamindb: {isettings.slug}")


ERROR_SQLITE_CACHE = """
Your cached local SQLite file exists, while your cloud SQLite file ({}) doesn't.
Either delete your cache ({}) or add it back to the cloud (if delete was accidental).
"""


def process_connect_response(
    response: tuple | str, instance_identifier: str
) -> tuple[
    UUID,
    Literal[
        "instance-corrupted-or-deleted", "account-not-exists", "instance-not-found"
    ],
]:
    # for internal use when creating instances through CICD
    if isinstance(response, tuple) and response[0] == "instance-corrupted-or-deleted":
        hub_result = response[1]
        instance_state = response[0]
        instance_id = UUID(hub_result["id"])
    else:
        instance_id_str = os.getenv("LAMINDB_INSTANCE_ID_INIT")
        if instance_id_str is None:
            instance_id = uuid.uuid5(uuid.NAMESPACE_URL, instance_identifier)
        else:
            instance_id = UUID(instance_id_str)
        instance_state = response
    return instance_id, instance_state


def validate_init_args(
    *,
    storage: UPathStr,
    name: str | None = None,
    db: PostgresDsn | None = None,
    schema: str | None = None,
    _test: bool = False,
) -> tuple[
    str,
    UUID | None,
    Literal[
        "connected",
        "instance-corrupted-or-deleted",
        "account-not-exists",
        "instance-not-found",
    ],
    str,
]:
    from ._connect_instance import connect
    from .core._hub_utils import (
        validate_schema_arg,
    )

    # should be called as the first thing
    name_str = infer_instance_name(storage=storage, name=name, db=db)
    # test whether instance exists by trying to load it
    instance_slug = f"{settings.user.handle}/{name_str}"
    response = connect(instance_slug, db=db, _raise_not_found_error=False, _test=_test)
    instance_state: Literal[
        "connected",
        "instance-corrupted-or-deleted",
        "account-not-exists",
        "instance-not-found",
    ] = "connected"
    instance_id = None
    if response is not None:
        instance_id, instance_state = process_connect_response(response, instance_slug)
    schema = validate_schema_arg(schema)
    return name_str, instance_id, instance_state, instance_slug


MESSAGE_NO_MULTIPLE_INSTANCE = """
Currently don't support subsequent connection to different databases in the same
Python session.\n
Try running on the CLI: lamin set auto-connect false
"""


def init(
    *,
    storage: UPathStr,
    name: str | None = None,
    db: PostgresDsn | None = None,
    schema: str | None = None,
    _test: bool = False,
) -> None:
    """Create and load a LaminDB instance.

    Args:
        storage: Either ``"create-s3"``, local or
            remote folder (``"s3://..."`` or ``"gs://..."``).
        name: Instance name.
        db: Database connection url, do not pass for SQLite.
        schema: Comma-separated string of schema modules. None if not set.
    """
    isettings = None
    ssettings = None
    try:
        silence_loggers()
        from ._check_setup import _check_instance_setup

        if _check_instance_setup() and not _test:
            raise RuntimeError(MESSAGE_NO_MULTIPLE_INSTANCE)
        else:
            close_instance(mute=True)
        from .core._hub_core import init_instance as init_instance_hub

        name_str, instance_id, instance_state, _ = validate_init_args(
            storage=storage,
            name=name,
            db=db,
            schema=schema,
            _test=_test,
        )
        if instance_state == "connected":
            settings.auto_connect = True  # we can also debate this switch here
            return None
        ssettings = init_storage(storage, instance_id=instance_id)
        isettings = InstanceSettings(
            id=instance_id,  # type: ignore
            owner=settings.user.handle,
            name=name_str,
            storage=ssettings,
            db=db,
            schema=schema,
            uid=ssettings.uid,
        )
        if isettings.is_remote and instance_state != "instance-corrupted-or-deleted":
            init_instance_hub(isettings)
        validate_sqlite_state(isettings)
        isettings._persist()
        if _test:
            return None
        isettings._init_db()
        load_from_isettings(isettings, init=True)
        if isettings._is_cloud_sqlite:
            isettings._cloud_sqlite_locker.lock()
            logger.warning(
                "locked instance (to unlock and push changes to the cloud SQLite file,"
                " call: lamin close)"
            )
        # we can debate whether this is the right setting, but this is how
        # things have been and we'd like to not easily break backward compat
        settings.auto_connect = True
    except Exception as e:
        from ._delete import delete_by_isettings
        from .core._hub_core import delete_instance_record, delete_storage_record

        if isettings is not None:
            delete_by_isettings(isettings)
            if settings.user.handle != "anonymous" and isettings.is_on_hub:
                delete_instance_record(isettings._id)
            isettings._get_settings_file().unlink(missing_ok=True)  # type: ignore
        if (
            ssettings is not None
            and settings.user.handle != "anonymous"
            and ssettings.is_on_hub
        ):
            delete_storage_record(ssettings._uuid)  # type: ignore
        raise e
    return None


def load_from_isettings(
    isettings: InstanceSettings,
    *,
    init: bool = False,
) -> None:
    from .core._setup_bionty_sources import load_bionty_sources, write_bionty_sources

    if init:
        # during init both user and storage need to be registered
        register_user_and_storage_in_instance(isettings, settings.user)
        write_bionty_sources(isettings)
        isettings._update_cloud_sqlite_file(unlock_cloud_sqlite=False)
    else:
        # when loading, django is already set up
        #
        # only register user if the instance is connected
        # for the first time in an environment
        # this is our best proxy for that the user might not
        # yet be registered
        if not isettings._get_settings_file().exists():
            register_user(settings.user)
        load_bionty_sources(isettings)
    isettings._persist()
    reload_lamindb(isettings)


def validate_sqlite_state(isettings: InstanceSettings) -> None:
    if isettings._is_cloud_sqlite:
        if (
            # it's important to first evaluate the existence check
            # for the local sqlite file because it doesn't need a network
            # request
            isettings._sqlite_file_local.exists()
            and not isettings._sqlite_file.exists()
        ):
            raise RuntimeError(
                ERROR_SQLITE_CACHE.format(
                    isettings._sqlite_file, isettings._sqlite_file_local
                )
            )


def infer_instance_name(
    *,
    storage: UPathStr,
    name: str | None = None,
    db: PostgresDsn | None = None,
) -> str:
    if name is not None:
        if "/" in name:
            raise ValueError("Invalid instance name: '/' delimiter not allowed.")
        return name
    if db is not None:
        logger.warning("using the sql database name for the instance name")
        # this isn't a great way to access the db name
        # could use LaminDsn instead
        return str(db).split("/")[-1]
    if storage == "create-s3":
        raise ValueError("pass name to init if storage = 'create-s3'")
    storage_path = convert_pathlike(storage)
    if storage_path.name != "":
        name = storage_path.name
    else:
        # dedicated treatment of bucket names
        name = storage_path._url.netloc
    return name.lower()
