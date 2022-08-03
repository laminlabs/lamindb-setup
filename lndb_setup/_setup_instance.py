from pathlib import Path
from typing import Union

import sqlmodel as sqm
from cloudpathlib import CloudPath
from lamin_logger import logger
from sqlmodel import SQLModel

from ._db import insert, insert_if_not_exists
from ._docs import doc_args
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


def setup_instance_db():
    """Setup database.

    Contains:
    - Database creation.
    - Sign-up and/or log-in.
    """
    instance_settings = load_or_create_instance_settings()
    user_settings = load_or_create_user_settings()
    if instance_settings.storage_dir is None:
        logger.warning("Instance is not configured. Call `lndb init` or `lndb load`.")
        return None
    instance_name = instance_settings.name
    sqlite_file = instance_settings._sqlite_file
    schema_modules = instance_settings.schema_modules

    if sqlite_file.exists():
        logger.info(f"Using instance: {sqlite_file}")
        import lndb_schema_core  # noqa

        with sqm.Session(instance_settings.db_engine()) as session:
            version_table = session.exec(
                sqm.select(lndb_schema_core.version_yvzi)
            ).all()

        versions = [row.v for row in version_table]

        current_version = lndb_schema_core.__version__

        if current_version not in versions:
            logger.info(
                "Did you already migrate your db to core schema v{current_version}?"
                " (y/n)"
            )
            logger.info(
                f"If yes, run `lndb_setup._db.insert.version_yvzi({current_version},"
                " user_settings.id)`"
            )
            logger.warning(
                "If no, either migrate your instance db schema to version"
                f" {current_version}.\nOr install the latest version {versions}."
            )
    else:
        msg = "Loading schema modules: core"
        import lndb_schema_core  # noqa

        if schema_modules is not None and "bionty" in schema_modules:
            import lndb_schema_bionty  # noqa

            msg += ", bionty"
        if schema_modules is not None and "wetlab" in schema_modules:
            import lndb_schema_wetlab  # noqa

            msg += ", wetlab"
        logger.info(f"{msg}.")
        SQLModel.metadata.create_all(instance_settings.db_engine())
        instance_settings._update_cloud_sqlite_file()
        insert.version_yvzi(lndb_schema_core.__version__, user_settings.id)
        logger.info(
            f"Created instance {instance_name} with core schema"
            f" v{lndb_schema_core.__version__}: {sqlite_file}"
        )

    insert_if_not_exists.user(
        user_settings.email, user_settings.id, user_settings.handle
    )


def load(instance_name: str):
    """Load existing instance."""
    InstanceSettings.name
    instance_settings = load_instance_settings(
        settings_dir / f"instance-{instance_name}.env"
    )
    assert instance_settings.name is not None
    save_instance_settings(instance_settings)

    from ._settings import settings

    settings._instance_settings = None


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
    user_settings = load_or_create_user_settings()
    if user_settings.id is None:
        logger.error("Login: lndb login user")
        return "need-to-login-first"

    # empty instance settings
    instance_settings = InstanceSettings()

    # setup storage
    instance_settings.storage_dir = setup_storage_dir(storage)

    # setup _config
    instance_settings._dbconfig = dbconfig
    if dbconfig != "sqlite":
        raise NotImplementedError()

    # setup schema
    if schema is not None:
        known_modules = ["bionty", "wetlab"]
        validated_schema = []
        for module in known_modules:
            if module in schema:
                validated_schema.append(module)
        if len(validated_schema) == 0:
            raise RuntimeError(f"Unknown schema modules. Only know {known_modules}.")
        instance_settings.schema_modules = ", ".join(validated_schema)
    save_instance_settings(instance_settings)

    setup_instance_db()

    from ._settings import settings

    settings._instance_settings = None
    return None
