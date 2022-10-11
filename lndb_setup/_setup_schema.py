import importlib

import sqlmodel as sqm
from lamin_logger import logger

from ._db import insert
from ._settings_instance import InstanceSettings
from ._settings_load import load_or_create_instance_settings
from ._settings_user import UserSettings

known_schema_names = [
    "bionty",
    "wetlab",
    "drylab",
    "bfx",
    "retro",
    "swarm",
    "harmonic-docking",
]


def get_schema_module_name(schema_name):
    if schema_name == "bfx":
        return "lnbfx.schema"
    else:
        return f"lnschema_{schema_name.replace('-', '_')}"


def setup_schema(isettings: InstanceSettings, usettings: UserSettings):
    if isettings.schema_modules is not None:
        schema_names = isettings.schema_modules.split(", ")
    else:
        schema_names = []

    msg = "Loading schema modules: "
    for schema_name in ["core"] + schema_names:
        schema_module = importlib.import_module(get_schema_module_name(schema_name))
        msg += f"{schema_name}=={schema_module.__version__} "
    logger.info(f"{msg}")

    sqm.SQLModel.metadata.create_all(isettings.db_engine())

    # we could try to also retrieve the user name here at some point
    insert.user(
        email=usettings.email,
        user_id=usettings.id,
        handle=usettings.handle,
        name=usettings.name,
    )

    for schema_name in ["core"] + schema_names:
        schema_module = importlib.import_module(get_schema_module_name(schema_name))
        insert.version(
            schema_module=schema_module,
            user_id=usettings.id,  # type: ignore
            cloud_sqlite=False,
        )
        # this is the only time we need manipulate the migration table
        # in all other cases alembic is going to to do this for us
        schema_id, migration = (
            schema_module._schema_id
            if hasattr(schema_module, "_schema_id")
            else schema_module._schema,  # backward compat
            schema_module._migration,
        )
        if migration is not None:
            migration_table = getattr(schema_module, f"migration_{schema_id}")
            settings = load_or_create_instance_settings()
            engine = settings.db_engine()
            with sqm.Session(engine) as session:
                session.add(migration_table(version_num=migration))
                session.commit()
    isettings._update_cloud_sqlite_file()

    logger.info(f"Created instance {isettings.name}: {isettings._sqlite_file}")
