import builtins
import os

from lamin_logger import logger

from ._settings_instance import InstanceSettings

IS_RUN_FROM_IPYTHON = getattr(builtins, "__IPYTHON__", False)
IS_SETUP = False


def get_migrations_to_sync():
    from django.db import DEFAULT_DB_ALIAS, connections
    from django.db.migrations.executor import MigrationExecutor

    connection = connections[DEFAULT_DB_ALIAS]
    connection.prepare_database()
    executor = MigrationExecutor(connection)
    targets = executor.loader.graph.leaf_nodes()
    planned_migrations = [mig[0] for mig in executor.migration_plan(targets)]
    return planned_migrations


def setup_django(
    isettings: InstanceSettings,
    deploy_migrations: bool = False,
    create_migrations: bool = False,
    init: bool = False,
):
    if IS_RUN_FROM_IPYTHON:
        os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
    import dj_database_url
    import django
    from django.conf import settings
    from django.core.management import call_command

    default_db = dj_database_url.config(
        default=isettings.db,
        conn_max_age=600,
        conn_health_checks=True,
    )
    DATABASES = {
        "default": default_db,
    }
    from lnhub_rest._assets._schemas import get_schema_module_name

    schema_names = ["core"] + list(isettings.schema)
    schema_module_names = [get_schema_module_name(n) for n in schema_names]

    if not settings.configured:
        settings.configure(
            INSTALLED_APPS=schema_module_names,
            DATABASES=DATABASES,
            DEFAULT_AUTO_FIELD="django.db.models.BigAutoField",
        )
        django.setup(set_prefix=False)

        if create_migrations:
            call_command("makemigrations")
            return None

        planned_migrations = get_migrations_to_sync()
        if len(planned_migrations) > 0:
            if deploy_migrations:
                call_command("migrate")
                if not init:  # delay sync
                    isettings._update_cloud_sqlite_file()
            else:
                logger.warning(
                    f"Your database is not up to date:\n{planned_migrations}\nConsider"
                    " migrating it: lamin migrate deploy\nIf you can't yet migrate,"
                    " consider installing an older schema module version to avoid"
                    " potential errors"
                )
        else:
            if deploy_migrations:
                logger.info("Database already up-to-date with migrations!")
        global IS_SETUP
        IS_SETUP = True
