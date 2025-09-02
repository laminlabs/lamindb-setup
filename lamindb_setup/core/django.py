from __future__ import annotations

# flake8: noqa
import builtins
import os
import sys
import importlib as il
import jwt
import time
from pathlib import Path
import time
from ._settings_instance import InstanceSettings, is_local_db_url

from lamin_utils import logger


IS_RUN_FROM_IPYTHON = getattr(builtins, "__IPYTHON__", False)
IS_SETUP = False
IS_MIGRATING = False
CONN_MAX_AGE = 299


# db token that refreshes on access if needed
class DBToken:
    def __init__(
        self, instance: InstanceSettings | dict, access_token: str | None = None
    ):
        self.instance = instance
        self.access_token = access_token
        # initialized in token_query
        self._token: str | None = None
        self._token_query: str | None = None
        self._expiration: float

    def _refresh_token(self):
        from ._hub_core import access_db
        from psycopg2.extensions import adapt

        self._token = access_db(self.instance, self.access_token)
        self._token_query = (
            f"SELECT set_token({adapt(self._token).getquoted().decode()}, true);"
        )
        self._expiration = jwt.decode(self._token, options={"verify_signature": False})[
            "exp"
        ]

    @property
    def token_query(self) -> str:
        # refresh token if needed
        if self._token is None or time.time() >= self._expiration:
            self._refresh_token()

        return self._token_query  # type: ignore


# a class to manage jwt in dbs
class DBTokenManager:
    def __init__(self):
        from django.db.transaction import Atomic

        self.original_atomic_enter = Atomic.__enter__

        self.tokens: dict[str, DBToken] = {}

    def get_connection(self, connection_name: str):
        from django.db import connections

        connection = connections[connection_name]
        assert connection.vendor == "postgresql"

        return connection

    def set(self, token: DBToken, connection_name: str = "default"):
        from django.db.transaction import Atomic

        connection = self.get_connection(connection_name)

        def set_token_wrapper(execute, sql, params, many, context):
            not_in_atomic_block = (
                context is None
                or "connection" not in context
                or not context["connection"].in_atomic_block
            )
            # ignore atomic blocks
            if not_in_atomic_block:
                sql = token.token_query + sql
            result = execute(sql, params, many, context)
            # this ensures that psycopg3 in the current env doesn't break this wrapper
            # psycopg3 returns a cursor
            # psycopg3 fetching differs from psycopg2, it returns the output of all sql statements
            # not only the last one as psycopg2 does. So we shift the cursor from set_token
            if (
                not_in_atomic_block
                and result is not None
                and hasattr(result, "nextset")
            ):
                result.nextset()
            return result

        connection.execute_wrappers.append(set_token_wrapper)

        self.tokens[connection_name] = token

        # ensure we set the token only once for an outer atomic block
        def __enter__(atomic):
            self.original_atomic_enter(atomic)
            connection_name = "default" if atomic.using is None else atomic.using
            if connection_name in self.tokens:
                # here we don't use the connection from the closure
                # because Atomic is a single class to manage transactions for all connections
                connection = self.get_connection(connection_name)
                if len(connection.atomic_blocks) == 1:
                    token = self.tokens[connection_name]
                    # use raw psycopg2 connection here
                    # atomic block ensures connection
                    connection.connection.cursor().execute(token.token_query)

        Atomic.__enter__ = __enter__
        logger.debug("django.db.transaction.Atomic.__enter__ has been patched")

    def reset(self, connection_name: str = "default"):
        connection = self.get_connection(connection_name)

        connection.execute_wrappers = [
            w
            for w in connection.execute_wrappers
            if getattr(w, "__name__", None) != "set_token_wrapper"
        ]

        self.tokens.pop(connection_name, None)


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
    appname_number: tuple[str, int] | None = None,
):
    if IS_RUN_FROM_IPYTHON:
        os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
        logger.debug("DJANGO_ALLOW_ASYNC_UNSAFE env variable has been set to 'true'")

    import dj_database_url
    import django
    from django.apps import apps
    from django.conf import settings
    from django.core.management import call_command

    # configuration
    if not settings.configured:
        instance_db = isettings.db
        if isettings.dialect == "postgresql":
            if os.getenv("LAMIN_DB_SSL_REQUIRE") == "false":
                ssl_require = False
            else:
                ssl_require = not is_local_db_url(instance_db)
            options = {"connect_timeout": os.getenv("PGCONNECT_TIMEOUT", 20)}
        else:
            ssl_require = False
            options = {}
        default_db = dj_database_url.config(
            env="LAMINDB_DJANGO_DATABASE_URL",
            default=instance_db,
            # see comment next to patching BaseDatabaseWrapper below
            conn_max_age=CONN_MAX_AGE,
            conn_health_checks=True,
            ssl_require=ssl_require,
        )
        if options:
            # do not overwrite keys in options if set
            default_db["OPTIONS"] = {**options, **default_db.get("OPTIONS", {})}
        DATABASES = {
            "default": default_db,
        }
        from .._init_instance import get_schema_module_name

        module_names = ["core"] + list(isettings.modules)
        raise_import_error = True if init else False
        installed_apps = [
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
        # this isn't needed the first time django.setup() is called, but for unknown reason it's needed the second time
        # the first time, it already defaults to true
        apps.apps_ready = True
        django.setup(set_prefix=False)
        # https://laminlabs.slack.com/archives/C04FPE8V01W/p1698239551460289
        from django.db.backends.base.base import BaseDatabaseWrapper

        BaseDatabaseWrapper.close_if_health_check_failed = close_if_health_check_failed
        logger.debug(
            "django.db.backends.base.base.BaseDatabaseWrapper.close_if_health_check_failed has been patched"
        )

        if isettings._fine_grained_access and isettings._db_permissions == "jwt":
            db_token = DBToken(isettings)
            db_token_manager.set(db_token)  # sets for the default connection

    if configure_only:
        return None

    # migrations management
    if create_migrations:
        call_command("makemigrations")
        return None

    if deploy_migrations:
        if appname_number is None:
            call_command("migrate", verbosity=2)
        else:
            app_name, app_number = appname_number
            call_command("migrate", app_name, app_number, verbosity=2)
        isettings._update_cloud_sqlite_file(unlock_cloud_sqlite=False)
    elif init:
        global IS_MIGRATING
        IS_MIGRATING = True
        call_command("migrate", verbosity=0)
        IS_MIGRATING = False

    global IS_SETUP
    IS_SETUP = True

    if isettings.keep_artifacts_local:
        isettings._local_storage = isettings._search_local_root()


# THIS IS NOT SAFE
# especially if lamindb is imported already
# django.setup fails if called for the second time
# reset_django() allows to call setup again,
# needed to connect to a different instance in the same process if connected already
# there could be problems if models are already imported from lamindb or other modules
# these 'old' models can have any number of problems
def reset_django():
    from django.conf import settings
    from django.apps import apps
    from django.db import connections

    if not settings.configured:
        return

    connections.close_all()

    if getattr(settings, "_wrapped", None) is not None:
        settings._wrapped = None

    app_names = {"django"} | {app.name for app in apps.get_app_configs()}

    apps.app_configs.clear()
    apps.all_models.clear()
    apps.apps_ready = apps.models_ready = apps.ready = apps.loading = False
    apps.clear_cache()

    # i suspect it is enough to just drop django and all the apps from sys.modules
    # the code above is just a precaution
    for module_name in list(sys.modules):
        if module_name.partition(".")[0] in app_names:
            del sys.modules[module_name]

    il.invalidate_caches()

    global db_token_manager
    db_token_manager = DBTokenManager()

    global IS_SETUP
    IS_SETUP = False
