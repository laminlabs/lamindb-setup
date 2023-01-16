from pathlib import Path
from typing import Optional, Union

from cloudpathlib import CloudPath
from lamin_logger import logger
from lnhub_rest._add_storage import get_storage_region
from lnhub_rest._init_instance_sbclient import init_instance as init_instance_hub
from pydantic import PostgresDsn

from ._db import insert_if_not_exists, upsert
from ._docs import doc_args
from ._load_instance import load, load_from_isettings
from ._settings import settings
from ._settings_instance import InstanceSettings
from ._setup_knowledge import write_bionty_versions
from ._setup_schema import load_schema, setup_schema
from ._storage import Storage


def register(isettings: InstanceSettings, usettings):
    """Register user & storage in DB."""
    upsert.user(usettings.email, usettings.id, usettings.handle, usettings.name)
    insert_if_not_exists.storage(isettings.storage.root, isettings.storage.region)


def persist_check_reload_schema(isettings: InstanceSettings):
    # check whether we're switching from sqlite to postgres or vice versa
    # if we do, we need to re-import the schema modules to account for differences
    check = False
    if settings._instance_exists:
        if settings.instance.dialect == "sqlite" and isettings.dialect != "sqlite":
            check = True
        if settings.instance.dialect != "sqlite" and isettings.dialect == "sqlite":
            check = True
    isettings._persist()
    if check:
        load_schema(isettings, reload=True)


ERROR_SQLITE_CACHE = """
Your cached local SQLite file still exists, while your cloud SQLite file was deleted.
Please add {} back to the cloud location.
"""


# This provides the doc strings for the init function on the
# CLI and the API
# It is located here as it *mostly* parallels the InstanceSettings docstrings.
# Small differences are on purpose, due to the different scope!
class description:
    storage_root = """Storage root. Either local dir, ``s3://bucket_name`` or ``gs://bucket_name``."""  # noqa
    db = """Database connection url, do not pass for SQLite."""
    name = """Instance name."""
    schema = """Comma-separated string of schema modules. None if not set."""


@doc_args(
    description.storage_root,
    description.name,
    description.db,
    description.schema,
)
def init(
    *,
    storage: Union[str, Path, CloudPath],
    name: Optional[str] = None,
    db: Optional[PostgresDsn] = None,
    schema: Optional[str] = None,
    _migrate: bool = False,  # not user-facing
) -> Optional[str]:
    """Setup LaminDB.

    Args:
        storage: {}
        name: {}
        db: {}
        schema: {}
    """
    assert settings.user.id  # check user is logged in
    owner = settings.user.handle

    name_str = infer_instance_name(storage=storage, name=name, db=db)

    # test whether instance exists by trying to load it
    message = load(f"{owner}/{name_str}", _log_error_message=False, migrate=_migrate)
    if message != "instance-not-exists":
        return message

    isettings = InstanceSettings(
        owner=owner,
        name=name_str,
        storage_root=storage,
        storage_region=get_storage_region(storage),
        db=db,
        schema=schema,
    )
    persist_check_reload_schema(isettings)
    if isettings.storage.is_cloud:
        if (
            not isettings._sqlite_file.exists()
            and isettings._sqlite_file_local.exists()
        ):
            logger.error(
                ERROR_SQLITE_CACHE.format(settings.instance._sqlite_file_local)
            )
            return "remote-sqlite-deleted"

    # some legacy instances not yet registered in hub may actually exist
    # despite being not loadable above
    message = None
    if not isettings._is_db_setup()[0]:
        setup_schema(isettings, settings.user)
        register(isettings, settings.user)
        write_bionty_versions(isettings)
    else:
        message = load_from_isettings(isettings, migrate=_migrate)
        logger.info("Loaded from existing DB, now storing metadata")

    if isettings.is_remote:
        result = init_instance_hub(
            owner=owner,
            name=name_str,
            storage=str(storage),
            db=db,
            schema=schema,
        )
        if result == "instance-exists-already-on-hub":
            logger.info(result)

    return message


def infer_instance_name(
    *,
    storage: Union[str, Path, CloudPath],
    name: Optional[str] = None,
    db: Optional[PostgresDsn] = None,
):
    if name is not None:
        return name
    if db is not None:
        # better way of accessing the database name?
        return str(db).split("/")[-1]

    if isinstance(storage, str):
        storage_path = Storage._str_to_path(storage)
    else:
        storage_path = storage
    return str(storage_path.stem).lower()
