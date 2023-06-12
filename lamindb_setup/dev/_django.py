import builtins
import os
from pathlib import Path
from typing import Dict

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


def replace_content(filename: Path, mapped_content: Dict[str, str]) -> None:
    with open(filename) as f:
        content = f.read()
    with open(filename, "w") as f:
        for key, value in mapped_content.items():
            content = content.replace(key, value)
        f.write(content)


def update_lnschema_core_migration(backward=False):
    import lnschema_core

    mapper = {"MANAGED = True", "MANAGED = False"}
    if backward:
        mapper = {"MANAGED = False", "MANAGED = True"}
    replace_content(lnschema_core.__file__, mapper)


def check_is_legacy_instance_and_fix(isettings):
    import sqlalchemy as sa

    engine = sa.create_engine(isettings.db)
    # this checks whether django_migrations is already available
    try:
        with engine.connect() as conn:
            conn.execute(sa.text("select * from django_migrations")).first()
        return
    except Exception:
        pass

    # now let's proceed
    stmts = [
        "alter table lnschema_core_run add column run_at datetime",
        "drop index if exists ix_core_run_transform_v",
        "drop index if exists ix_lnschema_core_run_transform_version",
        "alter table lnschema_core_run drop column transform_version",
        "alter table lnschema_core_project add column external_id varchar(40)",
        "alter table lnschema_core_transform rename column name to short_name",
        "alter table lnschema_core_transform rename column title to name",
        "alter table lnschema_core_runinput add column id bigint",
        "update lnschema_core_transform set name = short_name where name is null",
        "alter table lnschema_core_transform add column stem_id varchar(12)",
        "update lnschema_core_transform set stem_id = id",
        "alter table lnschema_core_features rename to lnschema_core_featureset",
        "alter table lnschema_core_featureset add column updated_at datetime",
    ]
    with engine.connect() as conn:
        for stmt in stmts:
            try:
                conn.execute(sa.text(stmt))
            except Exception as e:
                logger.warning(f"Failed to execute: {stmt} because of {e}")

    update_lnschema_core_migration()
    return True


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
                verbosity = 0 if init else 2
                check = check_is_legacy_instance_and_fix(isettings)
                call_command("migrate", verbosity=verbosity)
                if check:
                    update_lnschema_core_migration(backward=True)
                # if not init:
                # only update if called from lamin migrate deploy
                # if called from load_schema(..., init=True)
                # no need to update the remote sqlite
                # isettings._update_cloud_sqlite_file()
            else:
                logger.warning(
                    "\n\nYour database is not up to date with your installed"
                    " schemas!\n\nIt misses the following"
                    f" migrations:\n{planned_migrations}\n\nIf you are an admin and"
                    " know what you're doing, deploy the migration:\nlamin migrate"
                    " deploy\n\nOtherwise, please install previouses release of the"
                    " above-mentioned schemas\n\nIn case you haven't yet migrated to"
                    " Django, use lamindb 0.41.2 & reach out to Lamin\n"
                )
        else:
            if deploy_migrations:
                logger.info("Database already up-to-date with migrations!")
        global IS_SETUP
        IS_SETUP = True
