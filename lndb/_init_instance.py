import sys
from pathlib import Path
from typing import Optional, Union

from lamin_logger import logger
from lndb_storage import UPath
from lnhub_rest._add_storage import get_storage_region
from lnhub_rest._init_instance import init_instance as init_instance_hub
from lnhub_rest._init_instance import (
    validate_db_arg,
    validate_schema_arg,
    validate_storage_arg,
)
from pydantic import PostgresDsn

from ._load_instance import load, load_from_isettings
from ._settings import settings
from .dev import InstanceSettings
from .dev._db import insert_if_not_exists, upsert
from .dev._docs import doc_args
from .dev._setup_knowledge import write_bionty_versions
from .dev._setup_schema import load_schema, setup_schema
from .dev._storage import Storage


def register(isettings: InstanceSettings, usettings):
    """Register user & storage in DB."""
    upsert.user(usettings.email, usettings.id, usettings.handle, usettings.name)
    insert_if_not_exists.storage(isettings.storage)


def import_schema_lamin_root_api():
    # only touch lamindb if we're operating from lamindb
    if "lamindb" in sys.modules:
        import lamindb

        lamindb.schema._import_schema()


def persist_settings_load_schema(isettings: InstanceSettings):
    # The reason for why the following two calls should always come together
    # is that the schema modules need information about what type of database
    # (sqlite or not) is mounted at time of importing the module!
    # hence, the schema modules look for the settings file that is generated
    # by calling isettings._persist()
    isettings._persist()
    load_schema(isettings)


ERROR_SQLITE_CACHE = """
Your cached local SQLite file exists, while your cloud SQLite file ({}) doesn't.
Either delete your cache ({}) or add it back to the cloud (if delete was accidental).
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
    storage: Union[str, Path, UPath],
    name: Optional[str] = None,
    db: Optional[PostgresDsn] = None,
    schema: Optional[str] = None,
    _migrate: bool = False,  # not user-facing
) -> Optional[str]:
    """Creating and loading a LaminDB instance.

    Args:
        storage: {}
        name: {}
        db: {}
        schema: {}
    """
    assert settings.user.id  # check user is logged in
    owner = settings.user.handle

    schema = validate_schema_arg(schema)
    validate_storage_arg(str(storage))  # needs improvement!
    validate_db_arg(db)

    name_str = infer_instance_name(storage=storage, name=name, db=db)
    # test whether instance exists by trying to load it
    message = load(f"{owner}/{name_str}", _log_error_message=False, migrate=_migrate)
    if message != "instance-not-reachable":
        return message

    isettings = InstanceSettings(
        owner=owner,
        name=name_str,
        storage_root=storage,
        storage_region=get_storage_region(storage),
        db=db,
        schema=schema,
    )

    if isettings.storage.is_cloud:
        if (
            not isettings._sqlite_file.exists()
            and isettings._sqlite_file_local.exists()
        ):
            raise RuntimeError(
                ERROR_SQLITE_CACHE.format(
                    isettings._sqlite_file, isettings._sqlite_file_local
                )
            )

    # for remote instance, a lot of validation happens in the next function
    # if this errors, the whole function errors
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
            raise RuntimeError(f"Creating instance on hub failed:\n{result}")

    persist_settings_load_schema(isettings)

    message = None
    if not isettings._is_db_setup()[0]:
        setup_schema(isettings, settings.user)
        register(isettings, settings.user)
        write_bionty_versions(isettings)
    else:
        # we're currently using this for testing migrations
        # passing connection strings of databases that need to be tested
        # for migrations
        logger.warning("Your instance seems already set up, attempt load:")
        message = load_from_isettings(isettings, migrate=_migrate)

    import_schema_lamin_root_api()
    logger.success(
        f"Created & loaded instance: {settings.user.handle}/{isettings.name}"
    )
    return message


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
        storage_path = Storage._str_to_path(storage)
    else:
        storage_path = storage

    if isinstance(storage_path, UPath):
        name = storage_path._url.netloc
    else:
        name = str(storage_path.stem)
    name = name.lower()

    return name
