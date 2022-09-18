from subprocess import call

import lamin_logger as logger

from ._db import insert
from ._settings_instance import InstanceSettings
from ._settings_user import UserSettings


def migrate(
    *,
    version: str,
    usettings: UserSettings,
    isettings: InstanceSettings,
    schema_module: str = "lnschema_core",
):
    """Migrate database to latest version."""
    if schema_module == "lnschema_core":
        import lnschema_core
    else:
        raise NotImplementedError

    migration_status = call("python -m alembic --name yvzi upgrade head")

    if migration_status == 0:
        logger.success(f"Successfully migrated {schema_module} to v{version}.")
        isettings._update_cloud_sqlite_file()

        insert.version_yvzi(
            lnschema_core.__version__, lnschema_core._migration, usettings.id
        )
