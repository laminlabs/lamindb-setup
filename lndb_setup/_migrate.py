import importlib
import os
from pathlib import Path
from subprocess import run
from typing import Any

import sqlmodel as sqm
from lamin_logger import logger

from ._db import insert
from ._settings_instance import InstanceSettings
from ._settings_user import UserSettings
from ._setup_schema import create_schema_if_not_exists, get_schema_module_name


def check_migrate(
    *,
    usettings: UserSettings,
    isettings: InstanceSettings,
):
    if "LAMIN_SKIP_MIGRATION" in os.environ:
        if os.environ["LAMIN_SKIP_MIGRATION"] == "true":
            return "migrate-failed"

    status = []

    if isettings.schema_modules is not None:
        schema_names = isettings.schema_modules.split(", ")
    else:
        schema_names = []

    for schema_name in ["core"] + schema_names:
        create_schema_if_not_exists(schema_name, isettings)
        schema_module = importlib.import_module(get_schema_module_name(schema_name))
        if schema_module._migration is None:
            status.append("migrate-unnecessary")
            continue

        schema_id = schema_module._schema_id

        with sqm.Session(isettings.db_engine()) as session:
            table_loc = (
                schema_module.dev if hasattr(schema_module, "dev") else schema_module
            )
            version_table = getattr(table_loc, f"version_{schema_id}")
            versions = session.exec(sqm.select(version_table.v)).all()

        current_version = schema_module.__version__

        if current_version not in versions and len(versions) > 0:
            # run a confirmation dialogue outside a pytest run
            if "PYTEST_CURRENT_TEST" not in os.environ:
                logger.warning(
                    "Run the command in the shell to respond to the following dialogue."
                )

                response = input(
                    f"Do you want to migrate {schema_name} from {versions[-1]} to"
                    f" {current_version} (y/n)?"
                )

                if response != "y":
                    logger.warning(
                        f"Your db does not match the latest versio of schema {schema_name}."  # noqa
                        "Either install a previous API version or migrate the database."
                    )
                    return None
            else:
                logger.info(
                    f"Migrating {schema_name} from {versions[-1]} to {current_version}."
                )

            status.append(
                migrate(
                    version=current_version,
                    usettings=usettings,
                    isettings=isettings,
                    schema_name=schema_name,
                    schema_id=schema_id,
                    schema_module=schema_module,
                )
            )
        else:
            status.append("migrate-unnecessary")

    if "migrate-failed" in status:
        return "migrate-failed"
    elif "migrate-success" in status:
        return "migrate-success"
    else:
        return "migrate-unnecessary"


def modify_alembic_ini(
    filepath: Path, isettings: InstanceSettings, schema_name: str, revert: bool = False
):
    schema_module_path = (
        get_schema_module_name(schema_name).replace(".", "/") + "/migrations"
    )
    sl_from, sl_to = schema_module_path, "migrations"
    url_from = "sqlite:///tests/testdb.lndb"
    url_to_sqlite = f"sqlite:///{isettings._sqlite_file_local}"
    url_to = url_to_sqlite if isettings._dbconfig == "sqlite" else isettings._dbconfig

    if revert:
        sl_from, sl_to = sl_to, sl_from
        url_from, url_to = url_to, url_from

    with open(filepath) as f:
        content = f.read()

    content = content.replace(
        f"script_location = {sl_from}",
        f"script_location = {sl_to}",
    ).replace(
        f"sqlalchemy.url = {url_from}",
        f"sqlalchemy.url = {url_to}",
    )

    with open(filepath, "w") as f:
        f.write(content)


def migrate(
    *,
    version: str,
    usettings: UserSettings,
    isettings: InstanceSettings,
    schema_name: str,
    schema_id: str,
    schema_module: Any,
):
    """Migrate database to latest version."""
    schema_root = Path(schema_module.__file__).parent
    filepath = schema_root / "alembic.ini"

    modify_alembic_ini(filepath, isettings, schema_name)

    process = run(
        f"python -m alembic --name {schema_id} upgrade head",
        cwd=f"{schema_root}",
        shell=True,
    )

    if process.returncode == 0:
        logger.success(f"Successfully migrated schema {schema_name} to v{version}.")
        # The following call will also update the sqlite file in the cloud.
        insert.version(
            schema_module=schema_module,
            user_id=usettings.id,  # type: ignore
        )
    else:
        logger.error("Automatic migration failed.")

    modify_alembic_ini(filepath, isettings, schema_name, revert=True)

    if process.returncode == 0:
        return "migrate-success"
    else:
        return "migrate-failed"
