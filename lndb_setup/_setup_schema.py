import importlib

import sqlalchemy as sa
import sqlmodel as sqm
from lamin_logger import logger

from ._db import insert
from ._settings_instance import InstanceSettings
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


def create_schema_if_not_exists(schema_name: str, isettings: InstanceSettings):
    # create the schema module in case it doesn't exist
    if isettings._dbconfig != "sqlite":
        with isettings.db_engine().connect() as conn:
            if not conn.dialect.has_schema(conn, schema_name):
                conn.execute(sa.schema.CreateSchema(schema_name))
            conn.commit()


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
        create_schema_if_not_exists(schema_name, isettings)
        schema_module = importlib.import_module(get_schema_module_name(schema_name))
        msg += f"{schema_name}=={schema_module.__version__} "
    logger.info(f"{msg}")

    # add naming convention for alembic
    sqm.SQLModel.metadata.naming_convention = {
        "ix": "ix_%(column_0_label)s",
        "uq": "uq_%(table_name)s_%(column_0_name)s",
        "ck": "ck_%(table_name)s_`%(constraint_name)s`",
        "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
        "pk": "pk_%(table_name)s",
    }

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
        schema_id, migration = schema_module._schema_id, schema_module._migration
        if migration is not None:
            table_loc = (
                schema_module.dev if hasattr(schema_module, "dev") else schema_module
            )
            migration_table = getattr(table_loc, f"migration_{schema_id}")
            with isettings.session() as session:
                session.add(migration_table(version_num=migration))
                session.commit()
    isettings._update_cloud_sqlite_file()

    logger.info(f"Created instance {isettings.name}.")
