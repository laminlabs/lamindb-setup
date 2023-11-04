# flake8: noqa
import builtins
import os
from pathlib import Path
import shutil
from lamin_utils import logger
from ._settings_store import current_instance_settings_file
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
    view_schema: bool = False,
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
            # https://laminlabs.slack.com/archives/C04FPE8V01W/p1698239551460289
            # assuming postgres terminates a connection after 299s
            conn_max_age=299,
            conn_health_checks=True,
        )
        DATABASES = {
            "default": default_db,
        }
        from .._init_instance import get_schema_module_name

        schema_names = ["core"] + list(isettings.schema)
        installed_apps = [get_schema_module_name(n) for n in schema_names]
        if view_schema:
            installed_apps = installed_apps[::-1]  # to fix how apps appear
            installed_apps += ["schema_graph", "django.contrib.staticfiles"]

        kwargs = dict(
            INSTALLED_APPS=installed_apps,
            DATABASES=DATABASES,
            DEFAULT_AUTO_FIELD="django.db.models.BigAutoField",
            TIME_ZONE="UTC",
            USE_TZ=True,
        )
        if view_schema:
            kwargs.update(
                DEBUG=True,
                ROOT_URLCONF="lamindb_setup._schema",
                SECRET_KEY="dummy",
                TEMPLATES=[
                    {
                        "BACKEND": "django.template.backends.django.DjangoTemplates",
                        "APP_DIRS": True,
                    },
                ],
                STATIC_ROOT=f"{Path.home().as_posix()}/.lamin/",
                STATICFILES_FINDERS=[
                    "django.contrib.staticfiles.finders.AppDirectoriesFinder",
                ],
                STATIC_URL="static/",
            )
        settings.configure(**kwargs)
        django.setup(set_prefix=False)

    if configure_only:
        return None

    # migrations management
    if create_migrations:
        call_command("makemigrations")
        return None

    # check that migrations have been deployed
    settings_file_existed = isettings._get_settings_file().exists()
    # make a temporary copy of the current settings file
    current_settings_file = current_instance_settings_file()
    current_settings_file_existed = current_settings_file.exists()
    if current_settings_file_existed:
        shutil.copy(current_settings_file, current_settings_file.with_name("_tmp.env"))
    isettings._persist()  # temporarily make settings available to migrations, should probably if fails
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
    # clean up temporary settings files
    if not settings_file_existed:
        isettings._get_settings_file().unlink()
    current_instance_settings_file().unlink()
    if current_settings_file_existed:
        shutil.copy(current_settings_file.with_name("_tmp.env"), current_settings_file)

    global IS_SETUP
    IS_SETUP = True
