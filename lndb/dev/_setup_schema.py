import importlib
from types import ModuleType

import sqlalchemy as sa
import sqlmodel as sqm
from importlib_metadata import requires as importlib_requires
from importlib_metadata import version as get_pip_version
from lamin_logger import logger
from lnhub_rest._assets._schemas import get_schema_lookup_name, get_schema_module_name
from packaging.requirements import Requirement

from ._db import insert
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


def check_schema_version_and_import(schema_name) -> ModuleType:
    def check_version(module_version):
        schema_module_name = get_schema_module_name(schema_name)
        lamindb_version = get_pip_version("lamindb")
        for req in importlib_requires("lamindb"):
            req = Requirement(req)
            if schema_module_name == req.name:
                if not req.specifier.contains(module_version):
                    # it's currently important that we only import lamindb in
                    # case of an error being raised
                    import lamindb

                    if lamindb.__version__ != lamindb_version:
                        warning = (
                            "\nWARNING: importlib_metadata.version('lamindb') gives"
                            f" v{lamindb_version}, whereas `import lamindb` gives"
                            f" v{lamindb.__version__}"
                            f"\nConsider `pip install lamindb=={lamindb_version}`"
                        )
                    else:
                        warning = ""
                    raise RuntimeError(
                        f"lamindb v{lamindb_version} needs"
                        f" lnschema_{schema_name}{req.specifier}, you have"
                        f" {module_version} {warning}"
                    )

    try:
        # use live version if we can import
        module = importlib.import_module(get_schema_module_name(schema_name))
        module_version = module.__version__
    except Exception as import_error:
        # use pypi version instead
        module_version = get_pip_version(get_schema_module_name(schema_name))
        check_version(module_version)
        raise import_error

    check_version(module_version)
    return module


def load_schema(isettings: InstanceSettings):
    reload = False  # see comments below
    schema_names = ["core"] + list(isettings.schema)
    msg = "Loading schema modules: "
    for schema_name in schema_names:
        create_schema_if_not_exists(schema_name, isettings)
        module = check_schema_version_and_import(schema_name)
        if schema_name == "core":
            # here, we determine whether we have to reload the ORMs
            # it's necessary if switching from or to sqlite
            user_table = module.User.__table__
            if isettings.dialect != "sqlite" and user_table.schema is None:
                reload = True
            if isettings.dialect == "sqlite" and user_table.schema is not None:
                reload = True
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
