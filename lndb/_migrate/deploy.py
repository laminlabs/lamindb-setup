"""Check and auto-deploy migrations."""

import importlib
import os
from pathlib import Path
from subprocess import run
from typing import Any, Optional

import sqlmodel as sqm
from lamin_logger import logger
from natsort import natsorted
from packaging.version import parse as vparse

from lndb._migrate.utils import generate_module_files, modify_alembic_ini
from lndb.dev._db import insert
from lndb.dev._settings_instance import InstanceSettings
from lndb.dev._settings_user import UserSettings
from lndb.dev._setup_schema import create_schema_if_not_exists, get_schema_module_name


def check_deploy_migration(
    *,
    usettings: UserSettings,
    isettings: InstanceSettings,
    attempt_deploy: Optional[bool] = None,
):
    if "LAMIN_SKIP_MIGRATION" in os.environ:
        if os.environ["LAMIN_SKIP_MIGRATION"] == "true":
            return "migrate-skipped"

    # lock the whole migration
    locker = isettings._cloud_sqlite_locker
    locker.lock()
    # synchronize the sqlite file before proceeding
    isettings._update_local_sqlite_file()

    status = []
    schema_names = ["core"] + list(isettings.schema)
    # enforce order (if bionty is part of schema_names, it needs to come 2nd)
    if "bionty" in schema_names:
        schema_names.remove("bionty")
        schema_names.insert(1, "bionty")

    for schema_name in schema_names:
        create_schema_if_not_exists(schema_name, isettings)
        schema_module_name = get_schema_module_name(schema_name)
        schema_module = importlib.import_module(schema_module_name)
        # if _migration is None, there hasn't yet been any
        if schema_module._migration is None:
            status.append("migrate-unnecessary")
            continue

        schema_id = schema_module._schema_id

        with sqm.Session(isettings.engine) as session:
            table_loc = (
                schema_module.dev if hasattr(schema_module, "dev") else schema_module
            )
            version_table = getattr(table_loc, f"version_{schema_id}")
            versions = session.exec(sqm.select(version_table.v)).all()
            versions = natsorted(versions)  # latest version is last

        current_version = schema_module.__version__

        # attempt deploy as we want to test migrating the database
        if attempt_deploy:
            deploy_migration = True
        # check whether we need to deploy based on version comparison
        elif current_version not in versions and len(versions) > 0:
            if vparse(current_version) < vparse(versions[-1]):  # type: ignore
                raise RuntimeError(
                    f"You are trying to connect to a DB ({isettings.identifier}) that"
                    f" runs v{versions[-1]} of {schema_module_name} but you only have"
                    f" v{current_version} installed.\nPlease run `pip install"
                    f" {schema_module_name}=={versions[-1]}`, or install the latest"
                    " schema module version from GitHub."
                )
            logger.warning(
                f"Schema {schema_name} v{versions[-1]} is not up to date"
                f" with {current_version}"
            )
            # if migration is confirmed, continue
            if "PYTEST_CURRENT_TEST" not in os.environ:
                logger.warning(
                    "Run the command in the shell to respond to the following dialogue"
                )

                response = input(
                    f"Do you want to migrate {schema_name} from {versions[-1]} to"
                    f" {current_version} (y/n)?"
                )

                if response != "y":
                    logger.warning(
                        f"Instance {isettings.identifier} does not match the latest version of schema {schema_name}.\n"  # noqa
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

            deploy_migration = True

        else:
            deploy_migration = False
            status.append("migrate-unnecessary")

        if deploy_migration:
            generate_module_files(
                package_name=schema_module_name,
                migrations_path=Path(schema_module.__file__).parent / "migrations",  # type: ignore  # noqa
                schema_id=schema_id,
            )
            migrate_status = deploy(
                version=current_version,
                usettings=usettings,
                isettings=isettings,
                schema_name=schema_name,
                schema_id=schema_id,
                schema_module=schema_module,
            )
            status.append(migrate_status)

    locker.unlock()

    if "migrate-failed" in status:
        return "migrate-failed"
    elif "migrate-success" in status:
        return "migrate-success"
    else:
        return "migrate-unnecessary"


def deploy(
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
        logger.success(f"Migrated schema {schema_name} to v{version}")
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
