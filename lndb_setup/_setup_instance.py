from pathlib import Path
from typing import Union

from cloudpathlib import CloudPath
from lamin_logger import logger
from sqlalchemy import text

from ._db import insert_if_not_exists
from ._docs import doc_args
from ._migrate import check_migrate
from ._settings_instance import InstanceSettings
from ._settings_instance import instance_description as description
from ._settings_load import (
    load_instance_settings,
    load_or_create_instance_settings,
    load_or_create_user_settings,
    setup_storage_dir,
)
from ._settings_save import save_instance_settings
from ._settings_store import settings_dir
from ._setup_schema import setup_schema
from ._setup_storage import get_storage_region


def update_db(isettings, usettings):
    insert_if_not_exists.user(usettings.email, usettings.id, usettings.handle)

    insert_if_not_exists.storage(isettings.storage_dir, isettings.storage_region)


def setup_instance_db():
    """Setup database.

    Contains:
    - Database creation.
    - Sign-up and/or log-in.
    """
    isettings = load_or_create_instance_settings()
    usettings = load_or_create_user_settings()
    if isettings.storage_dir is None:
        logger.warning("Instance is not configured. Call `lndb init` or `lndb load`.")
        return None

    if isettings._sqlite_file.exists():
        logger.info(f"Using instance: {isettings._sqlite_file}")
        check_migrate(usettings=usettings, isettings=isettings)
    else:
        if isettings.cloud_storage and isettings._sqlite_file_local.exists():
            logger.error(
                "Your cached local SQLite file still exists, while your cloud SQLite"
                " file was deleted.\nPlease delete"
                f" {isettings._sqlite_file_local} or add it to the cloud"
                " location."
            )
            return None
        if isettings._dbconfig != "sqlite":
            if schema_exists(isettings):
                return None
        setup_schema(isettings, usettings)

    update_db(isettings, usettings)


def schema_exists(isettings: InstanceSettings):
    with isettings.db_engine().connect() as conn:
        results = conn.execute(
            text(
                """
            SELECT EXISTS (
                SELECT FROM
                    information_schema.tables
                WHERE
                    table_schema LIKE 'public' AND
                    table_name = 'user'
            );
        """
            )
        ).first()
        return results[0]


def load(instance_name: str):
    """Load existing instance."""
    usettings = load_or_create_user_settings()
    isettings = load_instance_settings(settings_dir / f"instance-{instance_name}.env")
    assert isettings.name is not None
    save_instance_settings(isettings)

    from ._settings import settings

    settings._instance_settings = None

    message = check_migrate(usettings=usettings, isettings=isettings)
    if message == "migrate-failed":
        return message
    update_db(isettings, usettings)
    return message


@doc_args(
    description.storage_dir,
    description._dbconfig,
    description.schema_modules,
)
def init(
    *,
    storage: Union[str, Path, CloudPath],
    dbconfig: str = "sqlite",
    schema: Union[str, None] = None,
) -> Union[None, str]:
    """Setup LaminDB.

    Args:
        storage: {}
        dbconfig: {}
        schema: {}
    """
    usettings = load_or_create_user_settings()
    if usettings.id is None:
        logger.error("Login: lndb login user")
        return "need-to-login-first"

    # empty instance settings
    instance_settings = InstanceSettings()

    # setup storage
    instance_settings.storage_dir = setup_storage_dir(storage)
    instance_settings.storage_region = get_storage_region(instance_settings.storage_dir)

    # setup _config
    instance_settings._dbconfig = dbconfig

    # setup schema
    if schema is not None:
        from ._setup_schema import known_schema_names

        validated_schema = []
        for module in known_schema_names:
            if module in schema:
                validated_schema.append(module)
        if len(validated_schema) == 0:
            raise RuntimeError(
                f"Unknown schema modules. Only know {known_schema_names}."
            )
        instance_settings.schema_modules = ", ".join(validated_schema)
    else:
        instance_settings.schema_modules = None

    save_instance_settings(instance_settings)

    setup_instance_db()

    from ._settings import settings

    settings._instance_settings = None
    return None
