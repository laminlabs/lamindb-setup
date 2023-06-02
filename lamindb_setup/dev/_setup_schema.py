import importlib
from types import ModuleType

import sqlmodel as sqm
from importlib_metadata import requires as importlib_requires
from importlib_metadata import version as get_pip_version
from lamin_logger import logger
from lnhub_rest._assets._schemas import get_schema_module_name
from packaging.requirements import Requirement

from .. import _USE_DJANGO
from ._db import insert
from ._django import setup_django
from ._settings_instance import InstanceSettings
from ._settings_user import UserSettings


def check_schema_version_and_import(schema_name) -> ModuleType:
    lamindb_installed = True
    try:
        get_pip_version("lamindb")  # noqa
    except Exception:
        lamindb_installed = False

    def check_version(module_version):
        if not lamindb_installed:
            return None
        schema_module_name = get_schema_module_name(schema_name)
        lamindb_version = get_pip_version("lamindb")
        for req in importlib_requires("lamindb"):
            req = Requirement(req)
            if schema_module_name == req.name:
                if not req.specifier.contains(module_version):
                    raise RuntimeError(
                        f"lamindb v{lamindb_version} needs"
                        f" lnschema_{schema_name}{req.specifier}, you have"
                        f" {module_version}"
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
    if _USE_DJANGO:
        setup_django(isettings)

    schema_names = ["core"] + list(isettings.schema)
    msg = "Loading schema modules: "
    for schema_name in schema_names:
        module = check_schema_version_and_import(schema_name)
        msg += f"{schema_name}=={module.__version__} "
    return msg, schema_names


def setup_schema(isettings: InstanceSettings, usettings: UserSettings):
    msg, schema_names = load_schema(isettings)
    logger.info(f"{msg}")

    if not _USE_DJANGO:
        sqm.SQLModel.metadata.create_all(isettings.engine)

        # we could try to also retrieve the user name here at some point
        insert.user(
            email=usettings.email,
            user_id=usettings.id,
            handle=usettings.handle,
            name=usettings.name,
        )

    if not _USE_DJANGO:
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
                    schema_module.dev
                    if hasattr(schema_module, "dev")
                    else schema_module
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
