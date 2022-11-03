from pathlib import Path
from typing import Union

from cloudpathlib import CloudPath
from lamin_logger import logger
from sqlalchemy import text

from ._db import insert_if_not_exists, upsert
from ._docs import doc_args
from ._hub import push_instance_if_not_exists
from ._migrate import check_migrate
from ._settings_instance import InstanceSettings
from ._settings_instance import instance_description as description
from ._settings_load import (
    load_instance_settings,
    load_or_create_instance_settings,
    load_or_create_user_settings,
    setup_storage_root,
)
from ._settings_save import save_instance_settings
from ._settings_store import settings_dir
from ._setup_schema import setup_schema
from ._setup_storage import get_storage_region


def update_db(isettings, usettings):
    # we should also think about updating the user name here at some point!
    # (passing user.name from cloud to the upsert as is done in setup_user.py)
    upsert.user(usettings.email, usettings.id, usettings.handle, usettings.name)

    storage = insert_if_not_exists.storage(
        isettings.storage_root, isettings.storage_region
    )
    push_instance_if_not_exists(storage)


def setup_instance_db():
    """Setup database.

    Contains:
    - Database creation.
    - Sign-up and/or log-in.
    """
    isettings = load_or_create_instance_settings()
    usettings = load_or_create_user_settings()
    if isettings.storage_root is None:
        logger.warning("Instance is not configured. Call `lndb init` or `lndb load`.")
        return None

    if instance_exists(isettings):
        logger.info(f"Loading instance: {isettings.name}")
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
        setup_schema(isettings, usettings)

    update_db(isettings, usettings)


def instance_exists(isettings: InstanceSettings):
    if isettings._sqlite_file.exists():
        return True
    elif isettings._dbconfig != "sqlite":
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
            ).first()
            return results[0]
    else:
        return False


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
    description.storage_root,
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
    instance_settings.storage_root = setup_storage_root(storage)
    instance_settings.storage_region = get_storage_region(
        instance_settings.storage_root
    )

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
