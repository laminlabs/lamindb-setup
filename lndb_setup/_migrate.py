from pathlib import Path
from subprocess import call

import lamin_logger as logger
import sqlmodel as sqm

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
        response = input("Do you want to migrate (y/n)?")

        if response != "y":
            logger.warning(
                "Your database does not seem up to date with the latest schema."
                "Either install a previous API version or migrate the database."
            )
            return None

        migrate(
            version=current_version,
            usettings=usettings,
            isettings=isettings,
            schema="lnschema_core",
        )


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
    alembic_ini = schema_root / "alembic.ini"

    with open(alembic_ini) as f:
        content = f.read()

    content = content.replace(
        "sqlalchemy.url = sqlite:///tests/testdb.lndb",
        "sqlalchemy.url = {isettings._sqlite_file_local}",
    )

    with open(alembic_ini, "w") as f:
        f.write(content)

    migration_status = call(
        f"cd {schema_root}; python -m alembic --name yvzi upgrade head"
    )

    if migration_status == 0:
        logger.success(f"Successfully migrated {schema} to v{version}.")
        isettings._update_cloud_sqlite_file()

        insert.version_yvzi(
            lnschema_core.__version__, lnschema_core._migration, usettings.id
        )
    else:
        logger.error("Automatic migration failed.")
