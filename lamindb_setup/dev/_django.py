import builtins
import os

try:
    from django.db import connections
    from django.db.migrations.executor import MigrationExecutor
except ImportError:
    pass

from ._settings_instance import InstanceSettings

IS_RUN_FROM_IPYTHON = getattr(builtins, "__IPYTHON__", False)
IS_SETUP = False


def is_database_synchronized():
    from django.db import DEFAULT_DB_ALIAS

    connection = connections[DEFAULT_DB_ALIAS]
    connection.prepare_database()
    executor = MigrationExecutor(connection)
    targets = executor.loader.graph.leaf_nodes()
    return not executor.migration_plan(targets)


def setup_django(isettings: InstanceSettings):
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

    if not settings.configured:
        settings.configure(
            INSTALLED_APPS=[
                "lnschema_core",
            ],
            DATABASES=DATABASES,
        )
        django.setup(set_prefix=False)
        if not is_database_synchronized():
            from django.core.management import call_command

            print("applying migrations")
            call_command("migrate")
            isettings._update_cloud_sqlite_file()
        global IS_SETUP
        IS_SETUP = True
    # else:
    #     raise RuntimeError(
    #         "Please restart Python session, django doesn't currently support "
    #         "switching among instances in one session"
    #     )
