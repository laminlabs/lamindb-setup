import os
from pathlib import Path
from subprocess import run

import sqlmodel as sqm
from lamin_logger import logger

from ._db import insert
from ._settings_instance import InstanceSettings
from ._settings_user import UserSettings


def check_migrate(
    *,
    usettings: UserSettings,
    isettings: InstanceSettings,
):
    status = []

    schema_names = []
    if isettings.schema_modules is not None:
        schema_names = isettings.schema_modules.split(", ")

    for schema_name in ["core"] + schema_names:
        if schema_name == "core":
            schema_id = "yvzi"
            import lnschema_core as schema_module
        elif schema_name == "bionty":
            schema_id = "zdno"
            import lnschema_bionty as schema_module
        else:
            logger.info(f"Migration for {schema_name} not yet implemented.")
            continue

        with sqm.Session(isettings.db_engine()) as session:
            version_table = getattr(schema_module, f"version_{schema_id}")
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
    sl_from, sl_to = f"lnschema_{schema_name}/migrations", "migrations"
    url_from, url_to = (
        "sqlite:///tests/testdb.lndb",
        f"sqlite:///{isettings._sqlite_file_local}",
    )

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
    schema_name: str = "core",
    schema_id: str = "yvzi",
):
    """Migrate database to latest version."""
    if schema_name == "core":
        import lnschema_core as schema_module
    elif schema_name == "bionty":
        import lnschema_bionty as schema_module
    else:
        raise NotImplementedError

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
            schema_id,
            schema_module.__version__,
            schema_module._migration,
            usettings.id,  # type: ignore
        )
    else:
        logger.error("Automatic migration failed.")

    modify_alembic_ini(filepath, isettings, schema_name, revert=True)

    if process.returncode == 0:
        return "migrate-success"
    else:
        return "migrate-failed"
