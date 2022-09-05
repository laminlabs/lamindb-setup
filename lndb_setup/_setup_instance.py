from pathlib import Path
from typing import Union

import lnschema_core
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


def configure_schema_wetlab(schema_modules):
    if "retro" in schema_modules:
        file_loc = lnschema_core.__file__.replace("core", "wetlab")
        with open(file_loc, "r") as f:
            content = f.read()
        with open(file_loc, "w") as f:
            content = content.replace(
                '_tables = ["biosample", "techsample"]', "_tables = []"
            )
            f.write(content)


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
    user_settings = load_or_create_user_settings()
    if isettings.storage_dir is None:
        logger.warning("Instance is not configured. Call `lndb init` or `lndb load`.")
        return None
    schema_modules = isettings.schema_modules

    if isettings._sqlite_file.exists():
        logger.info(f"Using instance: {isettings._sqlite_file}")

        with sqm.Session(isettings.db_engine()) as session:
            version_table = session.exec(sqm.select(lnschema_core.version_yvzi)).all()

        versions = [row.v for row in version_table]

        current_version = lnschema_core.__version__

        if current_version not in versions:
            logger.error(
                f"Your database does not seem up-to-date with installed core schema module v{current_version}.\n"  # noqa
                f"If you already migrated, run `lndb_setup._db.insert.version_yvzi({current_version}, db.settings.user.id)`\n"  # noqa
                f"If not, migrate to core schema version {current_version} or install {versions}."  # noqa
            )
            return None
    else:
        if isettings.cloud_storage and isettings._sqlite_file_local.exists():
            logger.error(
                "Your cached local SQLite file still exists, while your cloud SQLite"
                " file was deleted.\nPlease delete"
                f" {isettings._sqlite_file_local} or add it to the cloud"
                " location."
            )
            return None

        msg = "Loading schema modules: core"

        if schema_modules is not None and "bionty" in schema_modules:
            import lnschema_bionty  # noqa

            msg += ", bionty"
        if schema_modules is not None and "wetlab" in schema_modules:
            configure_schema_wetlab(schema_modules)

            import lnschema_wetlab  # noqa

            msg += ", wetlab"
        if schema_modules is not None and "bfx" in schema_modules:
            import lnbfx.schema  # noqa

            msg += ", bfx"

        if schema_modules is not None and "retro" in schema_modules:
            import lnschema_retro  # noqa

            msg += ", retro"
        logger.info(f"{msg}.")
        SQLModel.metadata.create_all(isettings.db_engine())
        isettings._update_cloud_sqlite_file()
        insert.version_yvzi(lnschema_core.__version__, user_settings.id)
        logger.info(
            f"Created instance {isettings.name} with core schema"
            f" v{lnschema_core.__version__}: {isettings._sqlite_file}"
        )

    update_db(isettings, user_settings)


def load(instance_name: str):
    """Load existing instance."""
    user_settings = load_or_create_user_settings()
    isettings = load_instance_settings(settings_dir / f"instance-{instance_name}.env")
    assert isettings.name is not None
    save_instance_settings(isettings)

    from ._settings import settings

    settings._instance_settings = None

    update_db(isettings, user_settings)


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
    storage_root_str = str(instance_settings.storage_dir)
    storage_region = None
    if storage_root_str.startswith("s3://"):
        import boto3

        response = boto3.client("s3").get_bucket_location(
            Bucket=storage_root_str.replace("s3://", "")
        )
        # returns `None` for us-east-1
        # returns a string like "eu-central-1" etc. for all other regions
        storage_region = response["LocationConstraint"]

    instance_settings.storage_region = storage_region

    # setup _config
    instance_settings._dbconfig = dbconfig
    if dbconfig != "sqlite":
        raise NotImplementedError()

    # setup schema
    if schema is not None:
        known_modules = ["bionty", "wetlab", "bfx", "retro"]
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
