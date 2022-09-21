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
    schema: str = "lnschema_core",
):
    if schema == "lnschema_core":
        import lnschema_core
    else:
        raise NotImplementedError

    with sqm.Session(isettings.db_engine()) as session:
        version_table = session.exec(sqm.select(lnschema_core.version_yvzi)).all()

    versions = [row.v for row in version_table]

    current_version = lnschema_core.__version__

    if current_version not in versions:
        # run a confirmation dialogue outside a pytest run
        if "PYTEST_CURRENT_TEST" not in os.environ:
            logger.warning(
                "Run the command in the shell to respond to the following dialogue."
            )

            response = input(
                f"Do you want to migrate from {versions[-1]} to"
                f" {current_version} (y/n)?"
            )

            if response != "y":
                logger.warning(
                    "Your database does not seem up to date with the latest schema."
                    "Either install a previous API version or migrate the database."
                )
                return None
        else:
            logger.info(f"Migrating from {versions[-1]} to {current_version}.")

        return migrate(
            version=current_version,
            usettings=usettings,
            isettings=isettings,
            schema="lnschema_core",
        )
    else:
        return "migrate-unnecessary"


def modify_alembic_ini(
    filepath: Path, isettings: InstanceSettings, revert: bool = False
):
    sl_from, sl_to = "lnschema_core/migrations", "migrations"
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
    schema: str = "lnschema_core",
):
    """Migrate database to latest version."""
    if schema == "lnschema_core":
        import lnschema_core
    else:
        raise NotImplementedError

    schema_root = Path(lnschema_core.__file__).parent
    filepath = schema_root / "alembic.ini"

    modify_alembic_ini(filepath, isettings)

    process = run(
        "python -m alembic --name yvzi upgrade head",
        cwd=f"{schema_root}",
        shell=True,
    )

    if process.returncode == 0:
        logger.success(f"Successfully migrated {schema} to v{version}.")
        # The following call will also update the sqlite file in the cloud.
        insert.version_yvzi(
            lnschema_core.__version__, lnschema_core._migration, usettings.id
        )
    else:
        logger.error("Automatic migration failed.")

    modify_alembic_ini(filepath, isettings, revert=True)

    if process.returncode == 0:
        return "migrate-success"
    else:
        return "migrate-failed"
