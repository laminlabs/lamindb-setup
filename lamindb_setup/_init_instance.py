from __future__ import annotations

import importlib
import os
import uuid
from typing import TYPE_CHECKING, Literal
from uuid import UUID

import click
from django.core.exceptions import FieldError
from django.db.utils import IntegrityError, OperationalError, ProgrammingError
from lamin_utils import logger

from ._disconnect import disconnect
from ._silence_loggers import silence_loggers
from .core import InstanceSettings
from .core._docs import doc_args
from .core._settings import settings
from .core._settings_instance import check_is_instance_remote, is_local_db_url
from .core._settings_storage import StorageSettings, init_storage
from .core.upath import UPath
from .errors import CannotSwitchDefaultInstance

if TYPE_CHECKING:
    from pydantic import PostgresDsn

    from .core._settings_user import UserSettings
    from .types import UPathStr


class InstanceNotCreated(click.ClickException):
    def show(self, file=None):
        pass


def get_schema_module_name(module_name, raise_import_error: bool = True) -> str | None:
    import importlib.util

    if module_name == "core":
        return "lamindb"
    name_attempts = [f"lnschema_{module_name.replace('-', '_')}", module_name]
    for name in name_attempts:
        module_spec = importlib.util.find_spec(name)
        if module_spec is not None:
            return name
    message = f"schema module '{module_name}' is not installed → resolve via `pip install {module_name}`"
    if raise_import_error:
        raise ImportError(message)
    return None


def register_storage_in_instance(ssettings: StorageSettings):
    from lamindb.models import Storage

    # how do we ensure that this function is only called passing
    # the managing instance?
    kwargs = {
        "root": ssettings.root_as_str,
        "type": ssettings.type,
        "region": ssettings.region,
        "instance_uid": ssettings.instance_uid,
        "run": None,
        "_skip_preparation": True,
    }
    if ssettings._uid is not None:
        kwargs["uid"] = ssettings._uid
    # this checks if the storage already exists under the hood
    storage = Storage(**kwargs).save()
    return storage


def register_user(usettings: UserSettings, update_user: bool = True):
    from lamindb.models import User

    if not update_user and User.objects.filter(uid=usettings.uid).exists():
        return

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
    # ProgrammingError: permission denied for table lamindb_user
    # IntegrityError: when trying to update a user on a fine-grained access instance
    except (OperationalError, FieldError, ProgrammingError, IntegrityError):
        pass


def register_initial_records(isettings: InstanceSettings, usettings: UserSettings):
    """Register space, user & storage in DB."""
    from django.db.utils import OperationalError
    from lamindb.models import Branch, Space

    try:
        Space.objects.get_or_create(
            uid=12 * "a",
            name="all",
            description="Every team & user with access to the instance has access.",
        )
        Branch.objects.get_or_create(
            id=-1,
            uid=12 * "t",
            name="trash",
            description="The trash.",
        )
        Branch.objects.get_or_create(
            id=0,
            uid=12 * "a",
            name="archive",
            description="The archive.",
        )
        Branch.objects.get_or_create(
            uid=12 * "m",
            name="main",
            description="The main & default branch of the instance.",
        )
        register_user(usettings)
        register_storage_in_instance(isettings.storage)
    except OperationalError as error:
        logger.warning(f"instance seems not set up ({error})")


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


def process_modules_arg(modules: str | None = None) -> str:
    if modules is None or modules == "":
        return ""
    # currently no actual validation, can add back if we see a need
    # the following just strips white spaces
    to_be_validated = [s.strip() for s in modules.split(",")]
    return ",".join(to_be_validated)


def validate_init_args(
    *,
    storage: UPathStr,
    name: str | None = None,
    db: PostgresDsn | None = None,
    modules: str | None = None,
    _test: bool = False,
    _write_settings: bool = True,
    _user: UserSettings | None = None,
) -> tuple[
    str,
    UUID,
    Literal[
        "connected",
        "instance-corrupted-or-deleted",
        "account-not-exists",
        "instance-not-found",
    ],
    str,
]:
    from ._connect_instance import connect

    if storage is None:
        raise SystemExit("✗ `storage` argument can't be `None`")
    # should be called as the first thing
    name_str = infer_instance_name(storage=storage, name=name, db=db)
    owner_str = settings.user.handle if _user is None else _user.handle
    # test whether instance exists by trying to load it
    instance_slug = f"{owner_str}/{name_str}"
    response = connect(
        instance_slug,
        _db=db,
        _raise_not_found_error=False,
        _test=_test,
        _write_settings=_write_settings,
        _user=_user,
    )
    instance_id: UUID
    instance_state: Literal[
        "connected",
        "instance-corrupted-or-deleted",
        "account-not-exists",
        "instance-not-found",
    ]
    if response is None:
        instance_state, instance_id = "connected", settings.instance._id
    else:
        instance_id, instance_state = process_connect_response(response, instance_slug)
    modules = process_modules_arg(modules)
    return name_str, instance_id, instance_state, instance_slug


DOC_STORAGE_ARG = "A local or remote folder (`'s3://...'` or `'gs://...'`). Defaults to current working directory."
DOC_INSTANCE_NAME = (
    "Instance name. If not passed, it will equal the folder name passed to `storage`."
)
DOC_DB = "Database connection URL. Defaults to `None`, which implies an SQLite file in the storage location."
DOC_MODULES = "Comma-separated string of schema modules."
DOC_LOW_LEVEL_KWARGS = "Keyword arguments for low-level control."


@doc_args(DOC_STORAGE_ARG, DOC_INSTANCE_NAME, DOC_DB, DOC_MODULES, DOC_LOW_LEVEL_KWARGS)
def init(
    *,
    storage: UPathStr = ".",
    name: str | None = None,
    db: PostgresDsn | None = None,
    modules: str | None = None,
    **kwargs,
) -> None:
    """Init a LaminDB instance.

    Args:
        storage: {}
        name: {}
        db: {}
        modules: {}
        **kwargs: {}

    See Also:
        Init an instance for via the CLI, see `here <https://docs.lamin.ai/cli#init>`__.
    """
    isettings = None
    ssettings = None

    _write_settings: bool = kwargs.get("_write_settings", True)
    if modules is None:
        modules = kwargs.get("schema", None)
    _test: bool = kwargs.get("_test", False)

    # use this user instead of settings.user
    # contains access_token
    _user: UserSettings | None = kwargs.get("_user", None)
    user_handle: str = settings.user.handle if _user is None else _user.handle
    user__uuid: UUID = settings.user._uuid if _user is None else _user._uuid  # type: ignore
    access_token: str | None = None if _user is None else _user.access_token

    try:
        silence_loggers()
        from ._check_setup import _check_instance_setup

        if _check_instance_setup() and not _test:
            from lamindb_setup.core.django import reset_django

            if settings._instance_exists:
                raise CannotSwitchDefaultInstance(
                    "Cannot init new instance after connecting to an existing instance."
                )
            reset_django()
        elif _write_settings:
            disconnect(mute=True)
        from ._connect_instance import reset_django_module_variables
        from .core._hub_core import init_instance_hub

        name_str, instance_id, instance_state, _ = validate_init_args(
            storage=storage,
            name=name,
            db=db,
            modules=modules,
            _test=_test,
            _write_settings=_write_settings,
            _user=_user,  # will get from settings.user if _user is None
        )
        if instance_state == "connected":
            return None
        isettings = InstanceSettings(
            id=instance_id,  # type: ignore
            owner=user_handle,
            name=name_str,
            db=db,
            modules=modules,
            # to lock passed user in isettings._cloud_sqlite_locker.lock()
            _locker_user=_user,  # only has effect if cloud sqlite
        )
        register_on_hub = (
            check_is_instance_remote(root=storage, db=db)
            and instance_state != "instance-corrupted-or-deleted"
        )
        if register_on_hub:
            init_instance_hub(
                isettings, account_id=user__uuid, access_token=access_token
            )
        ssettings, _ = init_storage(
            storage,
            instance_id=instance_id,
            instance_slug=f"{user_handle}/{name_str}",
            init_instance=True,
            register_hub=register_on_hub,
            created_by=user__uuid,
            access_token=access_token,
        )
        isettings._storage = ssettings
        if register_on_hub and not ssettings.is_on_hub:
            raise InstanceNotCreated(
                "Unable to create the instance because failed to register the storage."
            )
        validate_sqlite_state(isettings)
        # why call it here if it is also called in load_from_isettings?
        isettings._persist(write_to_disk=_write_settings)
        if _test:
            return None
        isettings._init_db()
        load_from_isettings(
            isettings, init=True, user=_user, write_settings=_write_settings
        )
        if _write_settings and isettings._is_cloud_sqlite:
            isettings._cloud_sqlite_locker.lock()
            logger.warning(
                "locked instance (to unlock and push changes to the cloud SQLite file,"
                " call: lamin disconnect)"
            )
        if register_on_hub and isettings.dialect != "sqlite":
            from ._schema_metadata import update_schema_in_hub

            update_schema_in_hub(access_token=access_token)
        importlib.reload(importlib.import_module("lamindb"))
        reset_django_module_variables()
        logger.important(f"initialized lamindb: {isettings.slug}")
    except Exception as e:
        from ._delete import delete_by_isettings
        from .core._hub_core import delete_instance_record, delete_storage_record

        if isettings is not None:
            if _write_settings:
                delete_by_isettings(isettings)
            else:
                settings._instance_settings = None
        if user_handle != "anonymous" or access_token is not None:
            if ssettings is not None and ssettings.is_on_hub:
                delete_storage_record(ssettings, access_token=access_token)
            if isettings is not None and isettings.is_on_hub:
                delete_instance_record(isettings._id, access_token=access_token)
        raise e
    return None


def load_from_isettings(
    isettings: InstanceSettings,
    *,
    init: bool = False,
    user: UserSettings | None = None,
    write_settings: bool = True,
) -> None:
    from .core._setup_bionty_sources import write_bionty_sources

    user = settings.user if user is None else user

    if init:
        # during init space, user and storage need to be registered
        register_initial_records(isettings, user)
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
            # do not try to update the user on fine grained access instances
            # this is blocked anyways, only select and insert are allowed
            register_user(user, update_user=not isettings._fine_grained_access)
    isettings._persist(write_to_disk=write_settings)
    # clear branch & space cache after reconnecting
    settings._branch = None
    settings._space = None


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
    storage_path = UPath(storage).resolve()
    name = storage_path.path.rstrip("/").split("/")[-1]
    return name.lower()
