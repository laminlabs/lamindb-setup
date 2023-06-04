import builtins
import os

from lamin_logger import logger
from lnhub_rest._assets._schemas import get_schema_module_name

try:
    from django.db import connections
    from django.db.migrations.executor import MigrationExecutor
except ImportError:
    pass

from ._settings_instance import InstanceSettings

IS_RUN_FROM_IPYTHON = getattr(builtins, "__IPYTHON__", False)
IS_SETUP = False


def get_migrations_to_sync():
    from django.db import DEFAULT_DB_ALIAS

    connection = connections[DEFAULT_DB_ALIAS]
    connection.prepare_database()
    executor = MigrationExecutor(connection)
    targets = executor.loader.graph.leaf_nodes()
    planned_migrations = [mig[0] for mig in executor.migration_plan(targets)]
    return planned_migrations


def setup_django(isettings: InstanceSettings, init: bool = False):
    print("setup django")
    if IS_RUN_FROM_IPYTHON:
        os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
    import dj_database_url
    import django
    from django.conf import settings

    default_db = dj_database_url.config(
        default=isettings.db,
        conn_max_age=600,
        conn_health_checks=True,
    )
    DATABASES = {
        "default": default_db,
    }

    schema_names = ["core"] + list(isettings.schema)
    schema_module_names = [get_schema_module_name(n) for n in schema_names]

    if not settings.configured:
        settings.configure(
            INSTALLED_APPS=schema_module_names,
            DATABASES=DATABASES,
        )
        django.setup(set_prefix=False)

        planned_migrations = get_migrations_to_sync()
        print(init)
        if len(planned_migrations) > 0:
            if init:
                from django.core.management import call_command

                call_command("migrate")
                isettings._update_cloud_sqlite_file()
            else:
                logger.warning(
                    "Your database is not up to date, consider deploying migrations"
                    f" via: lamin migrate deploy:\n{planned_migrations}"
                )
        global IS_SETUP
        IS_SETUP = True
    # else:
    #     raise RuntimeError(
    #         "Please restart Python session, django doesn't currently support "
    #         "switching among instances in one session"
    #     )
