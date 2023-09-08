# flake8: noqa
import builtins
import os

from lamin_utils import logger

from ._settings_instance import InstanceSettings

IS_RUN_FROM_IPYTHON = getattr(builtins, "__IPYTHON__", False)
IS_SETUP = False
MISSING_MIGRATIONS_WARNING = """

Your database is not up to date with your installed Python library.

The database misses the following migrations:
{missing_migrations}

Only if you are an admin and manage migrations manually, deploy them to the database: lamin migrate deploy

Otherwise, downgrade your Python library to match the database!
"""


def get_migrations_to_sync():
    from django.db import DEFAULT_DB_ALIAS, connections
    from django.db.migrations.executor import MigrationExecutor

    connection = connections[DEFAULT_DB_ALIAS]
    connection.prepare_database()
    executor = MigrationExecutor(connection)
    targets = executor.loader.graph.leaf_nodes()
    missing_migrations = [mig[0] for mig in executor.migration_plan(targets)]
    return missing_migrations


# this bundles set up and migration management
def setup_django(
    isettings: InstanceSettings,
    deploy_migrations: bool = False,
    create_migrations: bool = False,
    configure_only: bool = False,
    init: bool = False,
):
    if IS_RUN_FROM_IPYTHON:
        os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"

    import dj_database_url
    import django
    from django.conf import settings
    from django.core.management import call_command

    # configuration
    if not settings.configured:
        default_db = dj_database_url.config(
            default=isettings.db,
            conn_max_age=600,
            conn_health_checks=True,
        )
        DATABASES = {
            "default": default_db,
        }
        from ._setup_schema import get_schema_module_name

        schema_names = ["core"] + list(isettings.schema)
        schema_module_names = [get_schema_module_name(n) for n in schema_names]

        settings.configure(
            INSTALLED_APPS=schema_module_names,
            DATABASES=DATABASES,
            DEFAULT_AUTO_FIELD="django.db.models.BigAutoField",
            TIME_ZONE="UTC",
            USE_TZ=True,
        )
        django.setup(set_prefix=False)

    if configure_only:
        return None

    # migrations management
    if create_migrations:
        call_command("makemigrations")
        return None

    missing_migrations = get_migrations_to_sync()
    if len(missing_migrations) > 0:
        if deploy_migrations:
            verbosity = 0 if init else 2
            call_command("migrate", verbosity=verbosity)
            if not init:
                # only update if called from lamin migrate deploy
                # if called from load_schema(..., init=True)
                # no need to update the remote sqlite
                isettings._update_cloud_sqlite_file()
        else:
            logger.warning(
                MISSING_MIGRATIONS_WARNING.format(missing_migrations=missing_migrations)
            )
    else:
        if deploy_migrations:
            logger.success("database already up-to-date with migrations!")

    global IS_SETUP
    IS_SETUP = True
