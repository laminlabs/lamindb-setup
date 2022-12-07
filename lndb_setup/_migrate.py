import importlib
import os
from pathlib import Path
from subprocess import run
from typing import Any, Optional

import sqlmodel as sqm
from lamin_logger import logger
from natsort import natsorted

from ._db import insert
from ._settings_instance import InstanceSettings
from ._settings_user import UserSettings
from ._setup_schema import create_schema_if_not_exists, get_schema_module_name


def check_migrate(
    *,
    usettings: UserSettings,
    isettings: InstanceSettings,
    migrate_confirmed: Optional[bool] = None,
):
    if "LAMIN_SKIP_MIGRATION" in os.environ:
        if os.environ["LAMIN_SKIP_MIGRATION"] == "true":
            return "migrate-failed"

    status = []
    schema_names = ["core"] + list(isettings.schema)

    for schema_name in schema_names:
        create_schema_if_not_exists(schema_name, isettings)
        schema_module_name = get_schema_module_name(schema_name)
        schema_module = importlib.import_module(schema_module_name)
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
            versions = natsorted(versions)

        current_version = schema_module.__version__

        if current_version not in versions and len(versions) > 0:
            logger.info(
                f"Schema {schema_name} v{versions[-1]} is not up to date"
                f" with {current_version}."
            )
            # if migration is confirmed, continue
            if migrate_confirmed:
                pass
            # run a confirmation dialogue outside a pytest run
            elif "PYTEST_CURRENT_TEST" not in os.environ:
                logger.warning(
                    "Run the command in the shell to respond to the following dialogue."
                )

                response = input(
                    f"Do you want to migrate {schema_name} from {versions[-1]} to"
                    f" {current_version} (y/n)?"
                )

                if response != "y":
                    logger.warning(
                        f"Your db does not match the latest version of schema {schema_name}.\n"  # noqa
                        "For production use, either install"
                        f" {schema_module_name} {versions[-1]} "
                        f"or migrate the database to {current_version}."
                    )
                    return None
            else:
                logger.warning(
                    f"Migrate instance {isettings.name} outside a test (CI) run. "
                    "Unexpected errors might happen."
                )
                return "migrate-failed"

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
    url_from = "sqlite:///testdb/testdb.lndb"
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
