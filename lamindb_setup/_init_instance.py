import importlib
import sys
from pathlib import Path
from typing import Optional, Union

from lamin_logger import logger
from pydantic import PostgresDsn

from lamindb_setup.dev.upath import UPath

from ._close import close as close_instance
from ._docstrings import instance_description as description
from ._settings import settings
from ._silence_loggers import silence_loggers
from .dev import InstanceSettings
from .dev._docs import doc_args
from .dev._setup_schema import get_schema_module_name, load_schema
from .dev._storage import StorageSettings


def register_storage(ssettings: StorageSettings):
    from lnschema_core.models import Storage

    storage, created = Storage.objects.update_or_create(
        root=ssettings.root_as_str,
        defaults=dict(
            root=ssettings.root_as_str,
            type=ssettings.type,
            region=ssettings.region,
            created_by_id=settings.user.id,
        ),
    )
    if created:
        logger.success(f"Saved: {storage}")


def register_user_and_storage(isettings: InstanceSettings, usettings):
    """Register user & storage in DB."""
    from django.db.utils import OperationalError
    from lnschema_core.models import User

    try:
        user, created = User.objects.update_or_create(
            id=usettings.id,
            defaults=dict(
                handle=usettings.handle,
                name=usettings.name,
                email=usettings.email,
            ),
        )
        if created:
            logger.success(f"Saved: {user}")
        register_storage(isettings.storage)
    except OperationalError as error:
        logger.warning(f"Instance seems not set up ({error})")


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
    else:
        # only log if we're outside lamindb
        # lamindb itself logs upon import!
        logger.success(f"Loaded instance: {isettings.owner}/{isettings.name}")


ERROR_SQLITE_CACHE = """
Your cached local SQLite file exists, while your cloud SQLite file ({}) doesn't.
Either delete your cache ({}) or add it back to the cloud (if delete was accidental).
"""


@doc_args(
    description.storage_root,
    description.name,
    description.db,
    description.schema,
)
def init(
    *,
    storage: Union[str, Path, UPath],
    name: Optional[str] = None,
    db: Optional[PostgresDsn] = None,
    schema: Optional[str] = None,
    _test: bool = False,
) -> None:
    """Creating and loading a LaminDB instance.

    Args:
        storage: {}
        name: {}
        db: {}
        schema: {}
    """
    from ._check_instance_setup import check_instance_setup

    if check_instance_setup() and not _test:
        raise RuntimeError(
            "Currently don't support init or load of multiple instances in the same"
            " Python session. We will bring this feature back at some point."
        )
    else:
        close_instance(mute=True)
    # clean up in next refactor
    # avoid circular import
    from ._load_instance import load
    from .dev._hub_core import init_instance as init_instance_hub
    from .dev._hub_utils import (
        get_storage_region,
        validate_db_arg,
        validate_schema_arg,
        validate_storage_root_arg,
    )

    assert settings.user.id  # check user is logged in
    owner = settings.user.handle

    schema = validate_schema_arg(schema)
    validate_storage_root_arg(str(storage))
    validate_db_arg(db)

    name_str = infer_instance_name(storage=storage, name=name, db=db)
    # test whether instance exists by trying to load it
    response = load(f"{owner}/{name_str}", _log_error_message=False, _test=_test)
    if response is None:
        return None  # successful load!

    isettings = InstanceSettings(
        owner=owner,
        name=name_str,
        storage_root=storage,
        storage_region=get_storage_region(storage),  # type: ignore
        db=db,
        schema=schema,
    )

    if isettings._is_cloud_sqlite:
        if (
            not isettings._sqlite_file.exists()
            and isettings._sqlite_file_local.exists()
        ):
            raise RuntimeError(
                ERROR_SQLITE_CACHE.format(
                    isettings._sqlite_file, isettings._sqlite_file_local
                )
            )

    if isettings.is_remote:
        result = init_instance_hub(
            owner=owner,
            name=name_str,
            storage=str(storage),
            db=db,
            schema=schema,
        )
        if result == "instance-exists-already":
            pass  # everything is alright!
        elif isinstance(result, str):
            raise RuntimeError(f"Registering instance on hub failed:\n{result}")
        logger.success(
            f"Registered instance on hub: https://lamin.ai/{owner}/{name_str}"
        )

    if _test:
        isettings._persist()
        return None

    silence_loggers()

    also_init_bionty = True
    if isettings._is_db_setup(mute=True)[0]:
        logger.warning(
            "Your instance DB already has content, but we couldn't find settings,"
            " proceeding with setup"
        )
        # do not write the bionty tables again
        also_init_bionty = False
    load_from_isettings(isettings, init=True, also_init_bionty=also_init_bionty)
    if isettings._is_cloud_sqlite:
        isettings._cloud_sqlite_locker.lock()
        logger.warning(
            "Locked the instance. To unlock and push changes to the cloud SQLite file,"
            " call: lamin close"
        )
    if not isettings.is_remote:
        verbosity = logger._verbosity
        logger.set_verbosity(3)
        logger.hint(
            "Did not register local instance on hub (if you want to, call `lamin"
            " register`)"
        )
        logger.set_verbosity(verbosity)
    return None


def load_from_isettings(
    isettings: InstanceSettings,
    *,
    init: bool = False,
    also_init_bionty: bool = True,
) -> None:
    from .dev._setup_bionty_sources import load_bionty_sources, write_bionty_sources

    load_schema(isettings, init=init)
    register_user_and_storage(isettings, settings.user)
    if init and also_init_bionty:
        write_bionty_sources(isettings)
    else:
        load_bionty_sources(isettings)
    isettings._persist()
    reload_lamindb(isettings)


def infer_instance_name(
    *,
    storage: Union[str, Path, UPath],
    name: Optional[str] = None,
    db: Optional[PostgresDsn] = None,
):
    if name is not None:
        return name
    if db is not None:
        # better way of accessing the database name?
        return str(db).split("/")[-1]

    if isinstance(storage, str):
        storage_path = StorageSettings._str_to_path(storage)
    else:
        storage_path = storage

    if isinstance(storage_path, UPath):
        name = storage_path._url.netloc
    else:
        name = str(storage_path.stem)
    name = name.lower()

    return name
