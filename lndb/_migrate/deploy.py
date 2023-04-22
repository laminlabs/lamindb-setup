"""Check and auto-deploy migrations."""

import importlib
import os
from pathlib import Path
from subprocess import run
from types import ModuleType
from typing import Any, Optional

import sqlmodel as sqm
from lamin_logger import logger
from packaging.version import parse as vparse

from lndb._migrate.utils import generate_module_files, modify_alembic_ini
from lndb.dev._db import insert
from lndb.dev._settings_instance import InstanceSettings
from lndb.dev._settings_user import UserSettings
from lndb.dev._setup_schema import create_schema_if_not_exists, get_schema_module_name


def decide_deploy_migration(
    schema_name: str, isettings: InstanceSettings, schema_module: ModuleType
) -> bool:
    schema_id = schema_module._schema_id
    schema_module_name = schema_module.__name__
    current_version = schema_module.__version__
    current_migration = schema_module._migration

    # query the versions table of the schema in the database
    with sqm.Session(isettings.engine) as session:
        table_loc = (
            schema_module.dev if hasattr(schema_module, "dev") else schema_module
        )
        version_table = getattr(table_loc, f"version_{schema_id}")
        result = session.exec(
            sqm.select(version_table.v, version_table.migration).order_by(
                version_table.created_at.desc()
            )
        ).first()
        if result is None:
            logger.warning("No row in the versions table")
            return False
        deployed_version, deployed_migration = result["v"], result["migration"]

    # if there is no migration, yet, we don't need to deploy it  # noqa
    if deployed_migration is None and current_migration is None:
        deploy_migration = False
    # if the current version is smaller than the deployed version  # noqa
    elif vparse(current_version) < vparse(deployed_version):
        # no need to worry if current migration is same as deployed migration
        if current_migration == deployed_migration:
            deploy_migration = False
        else:  # otherwise, raise an error
            raise RuntimeError(
                f"You are trying to connect to an instance ({isettings.identifier})"
                f" that runs v{deployed_version} (migration {deployed_migration}) of"
                f" {schema_module_name} but you only have v{current_version} (migration"
                f" {current_migration}) installed.\nPlease run `pip install"
                f" {schema_module_name}=={deployed_version}`, or install the latest"
                " schema module version from GitHub."
            )
    else:  # if the current version is higher or equal to the latest deployed version
        # no need to worry if current migration is same as deployed migration
        if current_migration == deployed_migration:
            deploy_migration = False
        else:
            logger.warning(
                f"Deployed schema {schema_name} v{deployed_version} (migration"
                f" {deployed_migration}) is not up to date with installed"
                f" v{current_version} (migration {current_migration})"
            )
            # if migration is confirmed, continue
            if "PYTEST_CURRENT_TEST" not in os.environ:
                logger.warning(
                    "You might need to run the command in a shell if you can't respond"
                    " to the following dialogue"
                )
                response = input(
                    f"Do you want to migrate {schema_name} from"
                    f" {deployed_version}/{deployed_migration} to"
                    f" {current_version}/{current_migration} (y/n)?"
                )
                if response != "y":
                    logger.warning(
                        f"Instance {isettings.identifier} does not match the latest version of schema {schema_name}.\n"  # noqa
                        f"For production use, either install {schema_module_name} {deployed_version} "  # noqa
                        f"or migrate the database to {current_version}."
                    )
                    deploy_migration = False
                else:
                    deploy_migration = True
            else:
                logger.warning(
                    f"Migrate instance {isettings.name} outside a test (CI) run. "
                    "Unexpected errors might happen."
                )
                deploy_migration = False
    return deploy_migration


def check_deploy_migration(
    *,
    usettings: UserSettings,
    isettings: InstanceSettings,
    attempt_deploy: Optional[bool] = None,
):
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
        schema_id = schema_module._schema_id
        current_version = schema_module.__version__

        if attempt_deploy:  # attempt deploy as we want to test migrating the database
            deploy_migration = True
        else:  # check whether we need to deploy based on version comparison
            deploy_migration = decide_deploy_migration(
                schema_name, isettings, schema_module
            )

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
        else:
            status.append("migrate-unnecessary")

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
