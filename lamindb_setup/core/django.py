from __future__ import annotations

# flake8: noqa
import builtins
import os
from pathlib import Path
import time
from ._settings_instance import InstanceSettings


IS_RUN_FROM_IPYTHON = getattr(builtins, "__IPYTHON__", False)
IS_SETUP = False
IS_MIGRATING = False
CONN_MAX_AGE = 299


class DBTokenManager:
    def __init__(self):
        from django.db.transaction import Atomic

        self.original_atomic_enter = Atomic.__enter__

    def set_token(self, token: str | None, connection_name: str = "default"):
        # None to reset
        from django.db import connections
        from django.db.transaction import Atomic

        connection = connections[connection_name]
        assert connection.vendor == "postgresql"

        if token is not None:
            from psycopg2.extensions import adapt

            # escape correctly to avoid wrangling with params
            set_token_query = (
                f"SELECT set_token({adapt(token).getquoted().decode()}, true); "
            )

            # this relies on psycopg2 specific behaviour
            # won't work with psycopg3
            def set_token_wrapper(execute, sql, params, many, context):
                in_atomic_block = (
                    context is not None
                    and "connection" in context
                    and context["connection"].in_atomic_block
                )
                # ignore atomic blocks
                if not in_atomic_block:
                    sql = set_token_query + sql
                return execute(sql, params, many, context)

            connection.execute_wrappers.append(set_token_wrapper)

            # ensure we set the token only once for an outer atomic block
            def __enter__(atomic):
                self.original_atomic_enter(atomic)
                is_same_connection = (
                    "default" if atomic.using is None else atomic.using
                ) == connection_name
                if is_same_connection and len(connection.atomic_blocks) == 1:
                    # use raw psycopg2 connection here
                    # atomic block ensures connection
                    connection.connection.cursor().execute(set_token_query)

            Atomic.__enter__ = __enter__
        else:
            connection.execute_wrappers = [
                w
                for w in connection.execute_wrappers
                if getattr(w, "__name__", None) != "set_token_wrapper"
            ]
            Atomic.__enter__ = self.original_atomic_enter


db_token_manager = DBTokenManager()


def close_if_health_check_failed(self) -> None:
    if self.close_at is not None:
        if time.monotonic() >= self.close_at:
            self.close()
        self.close_at = time.monotonic() + CONN_MAX_AGE


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
            env="LAMINDB_DJANGO_DATABASE_URL",
            default=isettings.db,
            # see comment next to patching BaseDatabaseWrapper below
            conn_max_age=CONN_MAX_AGE,
            conn_health_checks=True,
        )
        DATABASES = {
            "default": default_db,
        }
        from .._init_instance import get_schema_module_name

        module_names = ["core"] + list(isettings.modules)
        raise_import_error = True if init else False
        installed_apps = ["django.contrib.contenttypes"]
        installed_apps += [
            package_name
            for name in module_names
            if (
                package_name := get_schema_module_name(
                    name, raise_import_error=raise_import_error
                )
            )
            is not None
        ]
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
        # https://laminlabs.slack.com/archives/C04FPE8V01W/p1698239551460289
        from django.db.backends.base.base import BaseDatabaseWrapper

        BaseDatabaseWrapper.close_if_health_check_failed = close_if_health_check_failed

        if isettings._fine_grained_access and isettings._db_permissions == "jwt":
            from ._hub_core import access_db

            db_token_manager.set_token(access_db(isettings))

    if configure_only:
        return None

    # migrations management
    if create_migrations:
        call_command("makemigrations")
        return None

    if deploy_migrations:
        call_command("migrate", verbosity=2)
        isettings._update_cloud_sqlite_file(unlock_cloud_sqlite=False)
    elif init:
        global IS_MIGRATING
        IS_MIGRATING = True
        call_command("migrate", verbosity=0)
        IS_MIGRATING = False

    global IS_SETUP
    IS_SETUP = True

    if isettings.keep_artifacts_local:
        isettings._search_local_root()
