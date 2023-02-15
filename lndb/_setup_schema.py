import importlib

import sqlalchemy as sa
import sqlmodel as sqm
from lamin_logger import logger
from lnhub_rest._assets._schemas import get_schema_lookup_name, get_schema_module_name

from ._db import insert
from ._settings import settings
from ._settings_instance import InstanceSettings
from ._settings_user import UserSettings


def create_schema_if_not_exists(schema_name: str, isettings: InstanceSettings):
    # create the SQL-level schema module in case it doesn't exist
    schema_lookup_name = get_schema_lookup_name(schema_name)
    if isettings.dialect != "sqlite":
        with isettings.engine.connect() as conn:
            if not conn.dialect.has_schema(conn, schema_lookup_name):
                conn.execute(sa.schema.CreateSchema(schema_lookup_name))
            conn.commit()


def reload_orms(schema_name, module, isettings):
    # root-level ORMs
    orms = [cls for cls in module.__dict__.values() if hasattr(cls, "__table__")]
    # dev-level ORMs
    if hasattr(module, "dev"):
        orms += [
            cls
            for cls in module.dev.__dict__.values()
            if hasattr(cls, "__table__")
            # exclude the version_* and migration_* tables in the root namespace
            and not cls.__name__.startswith("version_")
            and not cls.__name__.startswith("migration_")
        ]
    # link tables
    if hasattr(module, "link"):
        orms += [
            cls for cls in module.link.__dict__.values() if hasattr(cls, "__table__")
        ]
    if isettings.dialect == "sqlite":
        # only those orms that are actually in a schema
        orms = [
            orm
            for orm in orms
            if hasattr(orm.__table__, "schema") and orm.__table__.schema is not None
        ]
        for orm in orms:
            orm.__table__.schema = None
            # I don't know why the following is needed... it shouldn't
            if not orm.__table__.name.startswith(f"{schema_name}."):
                orm.__table__.name = f"{schema_name}.{orm.__table__.name}"
    else:  # postgres
        orms = [
            orm
            for orm in orms
            if (hasattr(orm.__table__, "schema") and orm.__table__.schema is None)
        ]
        for orm in orms:
            orm.__table__.schema = schema_name
            orm.__table__.name = orm.__table__.name.replace(f"{schema_name}.", "")


def load_schema(isettings: InstanceSettings, reload: bool = False):
    schema_names = ["core"] + list(isettings.schema)
    msg = "Loading schema modules: "
    for schema_name in schema_names:
        create_schema_if_not_exists(schema_name, isettings)
        module = importlib.import_module(get_schema_module_name(schema_name))
        if reload:  # importlib.reload doesn't do the job! hence, manual below
            reload_orms(schema_name, module, isettings)
        msg += f"{schema_name}=={module.__version__} "
    return msg, schema_names


def setup_schema(isettings: InstanceSettings, usettings: UserSettings):
    msg, schema_names = load_schema(isettings)
    logger.info(f"{msg}")

    sqm.SQLModel.metadata.create_all(isettings.engine)

    # we could try to also retrieve the user name here at some point
    insert.user(
        email=usettings.email,
        user_id=usettings.id,
        handle=usettings.handle,
        name=usettings.name,
    )

    for schema_name in schema_names:
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
            # we purposefully do not use isettings.session(), here, as we do *not*
            # want to update the local sqlite file from the cloud while looping over
            # schema modules
            # in fact, a synchronization issue led to loss of version information,
            # because the old cloud sqlite file overwrote the newer local file
            with sqm.Session(isettings.engine) as session:
                session.add(migration_table(version_num=migration))
                session.commit()
    isettings._update_cloud_sqlite_file()

    logger.info(f"Created instance {settings.user.handle}/{isettings.name}")
