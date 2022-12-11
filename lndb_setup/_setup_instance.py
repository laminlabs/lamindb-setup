from pathlib import Path
from typing import Optional, Union

from cloudpathlib import CloudPath
from lamin_logger import logger
from sqlalchemy import text

from ._assets import schemas as known_schema_names
from ._db import insert_if_not_exists, upsert
from ._docs import doc_args
from ._hub import push_instance_if_not_exists
from ._migrate import check_migrate
from ._settings import settings
from ._settings_instance import InstanceSettings
from ._settings_instance import instance_description as description
from ._settings_load import load_instance_settings, setup_storage_root
from ._settings_store import current_instance_settings_file, instance_settings_file
from ._setup_knowledge import load_bionty_versions, write_bionty_versions
from ._setup_schema import load_schema, setup_schema
from ._setup_storage import get_storage_region


def instance_exists(isettings: InstanceSettings):
    if isettings._dbconfig == "sqlite":
        if isettings._sqlite_file.exists():
            return True
        else:
            return False
    else:  # postgres
        with isettings.db_engine().connect() as conn:
            results = conn.execute(
                text(
                    """
                SELECT EXISTS (
                    SELECT FROM
                        information_schema.tables
                    WHERE
                        table_schema LIKE 'public' AND
                        table_name = 'version_yvzi'
                );
            """
                )
            ).first()  # returns tuple of boolean
            return results[0]


def register(isettings, usettings):
    """Register user & storage in DB. Register instance in hub."""
    # we should also think about updating the user name here at some point!
    # (passing user.name from cloud to the upsert as is done in setup_user.py)
    upsert.user(usettings.email, usettings.id, usettings.handle, usettings.name)

    storage = insert_if_not_exists.storage(
        isettings.storage_root, isettings.storage_region
    )
    push_instance_if_not_exists(storage)


def validate_schema_arg(schema: Optional[str] = None) -> str:
    if schema is None:
        return ""
    validated_schema = []
    for module in known_schema_names:
        if module in schema:
            validated_schema.append(module)
    if len(validated_schema) == 0:
        raise RuntimeError(f"Unknown schema modules. Only know {known_schema_names}.")
    return ",".join(validated_schema)


def persist_check_reload_schema(isettings: InstanceSettings):
    # check whether we're switching from sqlite to postgres or vice versa
    # if we do, we need to re-import the schema modules to account for differences
    check = False
    if settings._instance_exists:
        if settings.instance._dbconfig == "sqlite" and isettings._dbconfig != "sqlite":
            check = True
        if settings.instance._dbconfig != "sqlite" and isettings._dbconfig == "sqlite":
            check = True
    isettings._persist()
    if check:
        load_schema(isettings, reload=True)


def load(instance_name: str, migrate: Optional[bool] = None) -> Optional[str]:
    """Load existing instance.

    Returns `None` if succeeds, otherwise a string error code.

    Args:
        instance_name: Instance name.
        migrate: Whether to auto-migrate or not.
    """
    isettings = load_instance_settings(instance_settings_file(instance_name))
    persist_check_reload_schema(isettings)
    logger.info(f"Loading instance: {isettings.name}")
    message = check_migrate(
        usettings=settings.user, isettings=isettings, migrate_confirmed=migrate
    )
    if message == "migrate-failed":
        return message
    register(isettings, settings.user)
    load_bionty_versions(isettings)
    return message


def close() -> None:
    """Close existing instance.

    Returns `None` if succeeds, otherwise a string error code.
    """
    current_instance_settings_file().unlink()


ERROR_SQLITE_CACHE = """
Your cached local SQLite file still exists, while your cloud SQLite file was deleted.
Please delete {} or add it to the cloud location.
"""


@doc_args(
    description.storage_root,
    description._dbconfig,
    description._schema,
    description.name,
)
def init(
    *,
    storage: Union[str, Path, CloudPath],
    dbconfig: str = "sqlite",
    schema: Optional[str] = None,
    migrate: Optional[bool] = None,
    name: Optional[str] = None,
) -> Optional[str]:
    """Setup LaminDB.

    Args:
        storage: {}
        dbconfig: {}
        schema: {}
        name: {}
        migrate: Whether to auto-migrate or not.
    """
    assert settings.user.id  # check user is logged in

    storage_root = setup_storage_root(storage)
    isettings = InstanceSettings(
        storage_root=storage_root,
        storage_region=get_storage_region(storage_root),
        _dbconfig=dbconfig,
        _schema=validate_schema_arg(schema),
        _name=name,
    )
    persist_check_reload_schema(isettings)
    if instance_exists(isettings):
        return load(isettings.name, migrate=migrate)
    if isettings.cloud_storage and isettings._sqlite_file_local.exists():
        logger.error(ERROR_SQLITE_CACHE.format(settings.instance._sqlite_file_local))
        return None
    setup_schema(isettings, settings.user)
    register(isettings, settings.user)
    write_bionty_versions(isettings)
    return None
