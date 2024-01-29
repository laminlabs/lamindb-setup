import importlib
import sys
from pathlib import Path
from typing import Optional, Union
import uuid
from uuid import UUID
import os
from lamin_utils import logger
from typing import Tuple, Literal
from pydantic import PostgresDsn
from django.db.utils import OperationalError, ProgrammingError
from django.core.exceptions import FieldError
from lamindb_setup.dev.upath import LocalPathClasses, UPath
from ._close import close as close_instance
from ._settings import settings
from ._silence_loggers import silence_loggers
from .dev import InstanceSettings
from .dev._settings_storage import StorageSettings, init_storage
from .dev.upath import create_path
from .dev._hub_core import access_aws


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


def register_storage(ssettings: StorageSettings):
    from lnschema_core.models import Storage
    from lnschema_core.users import current_user_id

    defaults = dict(
        root=ssettings.root_as_str,
        type=ssettings.type,
        region=ssettings.region,
        created_by_id=current_user_id(),
    )
    if ssettings._uid is not None:
        defaults["uid"] = ssettings._uid

    storage, created = Storage.objects.update_or_create(
        root=ssettings.root_as_str,
        defaults=defaults,
    )
    if created:
        logger.save(f"saved: {storage}")
    return storage


def register_user(usettings):
    from lnschema_core.models import User

    if usettings.handle != "laminapp-admin":
        try:
            # need to have try except because of integer primary key migration
            user, created = User.objects.update_or_create(
                uid=usettings.uid,
                defaults=dict(
                    handle=usettings.handle,
                    name=usettings.name,
                ),
            )
            if created:
                logger.save(f"saved: {user}")
        # for users with only read access, except via ProgrammingError
        # ProgrammingError: permission denied for table lnschema_core_user
        except (OperationalError, FieldError, ProgrammingError):
            pass


def register_user_and_storage(isettings: InstanceSettings, usettings):
    """Register user & storage in DB."""
    from django.db.utils import OperationalError

    try:
        register_user(usettings)
        register_storage(isettings.storage)
    except OperationalError as error:
        logger.warning(f"instance seems not set up ({error})")


def reload_schema_modules(isettings: InstanceSettings):
    schema_names = ["core"] + list(isettings.schema)
    schema_module_names = [get_schema_module_name(n) for n in schema_names]

    for schema_module_name in schema_module_names:
        if schema_module_name in sys.modules:
            schema_module = importlib.import_module(schema_module_name)
            importlib.reload(schema_module)


def reload_lamindb(isettings: InstanceSettings):
    # only touch lamindb if we're operating from lamindb
    reload_schema_modules(isettings)
    if "lamindb" in sys.modules:
        import lamindb

        importlib.reload(lamindb)
        logger.important(f"lamindb instance: {isettings.identifier}")
    else:
        # only log if we're outside lamindb
        # lamindb itself logs upon import!
        logger.important(f"loaded instance: {isettings.owner}/{isettings.name}")


ERROR_SQLITE_CACHE = """
Your cached local SQLite file exists, while your cloud SQLite file ({}) doesn't.
Either delete your cache ({}) or add it back to the cloud (if delete was accidental).
"""


def process_load_response(
    response: Union[Tuple, str], instance_identifier: str
) -> Tuple[
    UUID,
    Literal[
        "instance-corrupted-or-deleted", "account-not-exists", "instance-not-reachable"
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
    storage: Union[str, Path, UPath],
    name: Optional[str] = None,
    db: Optional[PostgresDsn] = None,
    schema: Optional[str] = None,
    _test: bool = False,
) -> Tuple[
    str,
    Optional[UUID],
    Literal[
        "loaded",
        "instance-corrupted-or-deleted",
        "account-not-exists",
        "instance-not-reachable",
    ],
]:
    from ._load_instance import load
    from .dev._hub_utils import (
        validate_schema_arg,
    )

    # should be called as the first thing
    name_str = infer_instance_name(storage=storage, name=name, db=db)
    # test whether instance exists by trying to load it
    instance_identifier = f"{settings.user.handle}/{name_str}"
    response = load(instance_identifier, _raise_not_reachable_error=False, _test=_test)
    instance_state: Literal[
        "loaded",
        "instance-corrupted-or-deleted",
        "account-not-exists",
        "instance-not-reachable",
    ] = "loaded"
    instance_id = None
    if response is not None:
        instance_id, instance_state = process_load_response(
            response, instance_identifier
        )
    schema = validate_schema_arg(schema)
    return name_str, instance_id, instance_state


def init(
    *,
    storage: Union[str, Path, UPath],
    name: Optional[str] = None,
    db: Optional[PostgresDsn] = None,
    schema: Optional[str] = None,
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
        from ._check_instance_setup import check_instance_setup

        if check_instance_setup() and not _test:
            raise RuntimeError(
                "Currently don't support init or load of multiple instances in the same"
                " Python session. We will bring this feature back at some point."
            )
        else:
            close_instance(mute=True)
        from .dev._hub_core import init_instance as init_instance_hub

        name_str, instance_id, instance_state = validate_init_args(
            storage=storage,
            name=name,
            db=db,
            schema=schema,
            _test=_test,
        )
        if instance_state == "loaded":
            logger.important("loaded instance instead without creating it")
            return None
        ssettings = init_storage(storage)
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
        # assign permissions after init_storage to account for AWS latency
        if ssettings.is_cloud and storage == "create-s3":
            access_aws()
        if _test:
            isettings._persist()
            return None
        isettings._init_db()
        load_from_isettings(isettings, init=True)
        if isettings._is_cloud_sqlite:
            isettings._cloud_sqlite_locker.lock()
            logger.warning(
                "locked instance (to unlock and push changes to the cloud SQLite file,"
                " call: lamin close)"
            )
        if not isettings.is_remote:
            logger.important("did not register local instance on lamin.ai")
    except Exception as e:
        from .dev._hub_core import delete_storage, delete_instance

        if isettings is not None:
            delete_instance(isettings)
        if ssettings is not None:
            delete_storage(ssettings)
        raise e
    return None


def load_from_isettings(
    isettings: InstanceSettings,
    *,
    init: bool = False,
) -> None:
    from .dev._setup_bionty_sources import load_bionty_sources, write_bionty_sources

    if init:
        # during init both user and storage need to be registered
        register_user_and_storage(isettings, settings.user)
        write_bionty_sources(isettings)
        isettings._update_cloud_sqlite_file(unlock_cloud_sqlite=False)
    else:
        # when loading, django is already set up
        #
        # only register user if the instance is loaded
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
    storage: Union[str, Path, UPath],
    name: Optional[str] = None,
    db: Optional[PostgresDsn] = None,
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
    storage_path = create_path(storage)
    if isinstance(storage_path, LocalPathClasses):
        name = storage_path.stem
    else:
        name = storage_path._url.netloc
    name = name.lower()
    return name
