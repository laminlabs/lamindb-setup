from pathlib import Path
from typing import Union

from cloudpathlib import CloudPath
from lamin_logger import logger
from sqlmodel import SQLModel

from ._db import insert_if_not_exists
from ._docs import doc_args
from ._settings_instance import InstanceSettings, description
from ._settings_load import (
    load_instance_settings,
    load_or_create_instance_settings,
    load_or_create_user_settings,
    setup_storage_dir,
)
from ._settings_save import save_instance_settings, save_user_settings
from ._settings_store import settings_dir
from ._setup_user import log_in_user


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
    instance_name = instance_settings.instance_name
    sqlite_file = instance_settings._sqlite_file
    schema_modules = instance_settings.schema_modules

    if sqlite_file.exists():
        logger.info(f"Using instance: {sqlite_file}")
    else:
        msg = "Loading schema modules: core"
        if schema_modules is not None and "bionty" in schema_modules:
            import lndb_schema_bionty  # noqa

            msg += ", bionty"
        if schema_modules is not None and "wetlab" in schema_modules:
            import lndb_schema_wetlab  # noqa

            msg += ", wetlab"
        logger.info(f"{msg}.")
        SQLModel.metadata.create_all(instance_settings.db_engine())
        instance_settings._update_cloud_sqlite_file()
        logger.info(f"Created instance {instance_name}: {sqlite_file}")

    insert_if_not_exists.user(user_settings.user_email, user_settings.user_id)


def load_instance(instance_name: str):
    """Load existing instance."""
    InstanceSettings.instance_name
    instance_settings = load_instance_settings(settings_dir / f"{instance_name}.env")
    assert instance_settings.instance_name is not None
    save_instance_settings(instance_settings)

    from ._settings import settings

    settings._instance_settings = None


@doc_args(
    description.storage_dir,
    description._dbconfig,
    description.schema_modules,
)
def init_instance(
    *,
    storage: Union[str, Path, CloudPath, None] = None,
    dbconfig: str = "sqlite",
    schema: Union[str, None] = None,
) -> None:
    """Setup LaminDB.

    Args:
        storage: {}
        dbconfig: {}
        schema: {}
    """
    # settings.user_email & settings.user_secret are set
    instance_settings = load_or_create_instance_settings()
    user_settings = load_or_create_user_settings()
    if user_settings.user_id is None:
        if (
            user_settings.user_email is not None
            and user_settings.user_secret is not None  # noqa
        ):
            # complete user setup, this *only* happens after *sign_up_first_time*
            logger.info("Completing user sign up. Only happens once!")
            log_in_user(
                email=user_settings.user_email, secret=user_settings.user_secret
            )
            user_settings = (
                load_or_create_user_settings()
            )  # need to reload, here, to get user_id
        else:
            raise RuntimeError("Login user: lndb login --email")
    save_user_settings(user_settings)

    # setup storage
    if storage is None:
        if instance_settings.storage_dir is None:
            raise RuntimeError(
                "No storage in .env, please call: lndb init --storage <location>"
            )
        else:
            storage = instance_settings.storage_dir
    else:
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
