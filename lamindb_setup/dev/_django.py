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


def check_is_legacy_instance_and_fix(isettings) -> bool:
    import sqlalchemy as sa

    engine = sa.create_engine(isettings.db)

    # this checks whether its a legacy instance before lamindb 0.41.0
    try:
        with engine.connect() as conn:
            conn.execute(sa.text("select * from core.user")).first()
        raise RuntimeError(
            "Please first load your instance with lamindb 0.41.2, after that, you can"
            " upgrade to lamindb >=0.42"
        )
    except Exception:
        pass
    # this checks whether django_migrations is already available
    try:
        with engine.connect() as conn:
            conn.execute(sa.text("select * from lnschema_core_user")).first()
        user_table_exists = True
    except Exception:
        user_table_exists = False
    try:
        with engine.connect() as conn:
            conn.execute(sa.text("select * from django_migrations")).first()
        django_table_exists = True
    except Exception:
        django_table_exists = False

    # neither user table nor django table there, it's an empty new instance
    if not user_table_exists and not django_table_exists:
        return False
    # the django table is there, it's a django instance
    if django_table_exists:
        return False
    # otherwise, it's a legacy instance: it has the user table but not the django table

    datetime_str = "datetime" if isettings.dialect == "sqlite" else "timestamp"

    # now let's proceed
    stmts = [
        f"alter table lnschema_core_run add column run_at {datetime_str}",
        "drop index if exists ix_core_run_transform_v",
        "drop index if exists ix_lnschema_core_run_transform_version",
        "drop index if exists ix_lnschema_core_file_transform_version",
        "alter table lnschema_core_run drop column transform_version",
        "alter table lnschema_core_file drop column transform_version",
        "alter table lnschema_core_project add column external_id varchar(40)",
        "alter table lnschema_core_transform rename column name to short_name",
        "alter table lnschema_core_transform rename column title to name",
        "alter table lnschema_core_runinput add column id bigint",
        "update lnschema_core_transform set name = short_name where name is null",
        "alter table lnschema_core_transform add column stem_id varchar(12)",
        "update lnschema_core_transform set stem_id = id",
        "alter table lnschema_core_features rename to lnschema_core_featureset",
        f"alter table lnschema_core_featureset add column updated_at {datetime_str}",
        # now rename
        "alter table lnschema_core_user rename to lnschema_core_legacy_user",
        "alter table lnschema_core_storage rename to lnschema_core_legacy_storage",
        "alter table lnschema_core_project rename to lnschema_core_legacy_project",
        "alter table lnschema_core_transform rename to lnschema_core_legacy_transform",
        "alter table lnschema_core_run rename to lnschema_core_legacy_run",
        "alter table lnschema_core_featureset rename to"
        " lnschema_core_legacy_featureset",
        "alter table lnschema_core_folder rename to lnschema_core_legacy_folder",
        "alter table lnschema_core_file rename to lnschema_core_legacy_file",
        "alter table lnschema_core_runinput rename to lnschema_core_legacy_runinput",
    ]
    if "bionty" in isettings.schema:
        # fmt: off
        stmts += [
            "alter table lnschema_bionty_species rename to lnschema_bionty_legacy_species",  # noqa
            "alter table lnschema_bionty_gene rename to lnschema_bionty_legacy_gene",  # noqa
            "alter table lnschema_bionty_protein rename to lnschema_bionty_legacy_protein",  # noqa
            "alter table lnschema_bionty_cellmarker rename to lnschema_bionty_legacy_cellmarker",  # noqa
            "alter table lnschema_bionty_tissue rename to lnschema_bionty_legacy_tissue",  # noqa
            "alter table lnschema_bionty_celltype rename to lnschema_bionty_legacy_celltype",  # noqa
            "alter table lnschema_bionty_disease rename to lnschema_bionty_legacy_disease",  # noqa
            "alter table lnschema_bionty_cellline rename to lnschema_bionty_legacy_cellline",  # noqa
            "alter table lnschema_bionty_phenotype rename to lnschema_bionty_legacy_phenotype",  # noqa
            "alter table lnschema_bionty_pathway rename to lnschema_bionty_legacy_pathway",  # noqa
            "alter table lnschema_bionty_readout rename to lnschema_bionty_legacy_readout",  # noqa
        ]
        # fmt: on
    with engine.connect() as conn:
        for stmt in stmts:
            try:
                conn.execute(sa.text(stmt))
            except Exception as e:
                logger.warning(f"Failed to execute: {stmt} because of {e}")
    logger.success("Created legacy migration preparations")
    return True


def insert_legacy_data(isettings: InstanceSettings):
    import sqlalchemy as sa

    datetime_str = "datetime" if isettings.dialect == "sqlite" else "timestamp"

    if isettings.dialect == "sqlite":
        stmts = [
            "alter table lnschema_core_user drop column created_at",
            f"alter table lnschema_core_user create column created_at {datetime_str}",
            "alter table lnschema_core_transform drop column created_at",
            "alter table lnschema_core_transform create column created_by_id"
            "alter table lnschema_core_run drop column created_by_id",
            "alter table lnschema_core_run create column created_at VARCHAR(8)",
            "alter table lnschema_core_file drop column updated_at",
            f"alter table lnschema_core_file create column updated_at {datetime_str}",
        ]
    else:
        stmts = []
        # f"alter table lnschema_core_user alter column created_at {datetime_str} drop not null",  # noqa
        # f"alter table lnschema_core_transform alter column created_at {datetime_str} drop not null",  # noqa
        # "alter table lnschema_core_run alter column created_by_id VARCHAR(8) drop not null",  # noqa
        # f"alter table lnschema_core_file alter column updated_at {datetime_str} drop not null",  # noqa

    engine = sa.create_engine(isettings.db)
    stmts += [
        "insert into lnschema_core_user select * from lnschema_core_legacy_user",
        "insert into lnschema_core_storage select * from lnschema_core_legacy_storage",
        "insert into lnschema_core_project select * from lnschema_core_legacy_project",
        "insert into lnschema_core_transform select * from"
        " lnschema_core_legacy_transform",
        "insert into lnschema_core_run select * from lnschema_core_legacy_run",
        "insert into lnschema_core_featureset select * from"
        " lnschema_core_legacy_featureset",
        "insert into lnschema_core_folder select * from lnschema_core_legacy_folder",
        "insert into lnschema_core_file select * from lnschema_core_legacy_file",
        "insert into lnschema_core_runinput select * from"
        " lnschema_core_legacy_runinput",
    ]
    with engine.connect() as conn:
        for stmt in stmts:
            try:
                conn.execute(sa.text(stmt))
            except Exception as e:
                logger.warning(f"Failed to execute: {stmt} because of {e}")


def setup_django(
    isettings: InstanceSettings,
    deploy_migrations: bool = False,
    create_migrations: bool = False,
    init: bool = False,
):
    if IS_RUN_FROM_IPYTHON:
        os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"

    if deploy_migrations:
        check_legacy = check_is_legacy_instance_and_fix(isettings)

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
                call_command("migrate", verbosity=verbosity)
                if check_legacy:
                    insert_legacy_data(isettings)
                if not init:
                    # only update if called from lamin migrate deploy
                    # if called from load_schema(..., init=True)
                    # no need to update the remote sqlite
                    isettings._update_cloud_sqlite_file()
            else:
                logger.warning(
                    "\n\nYour database is not up to date with your installed"
                    " schemas!\n\nIt misses the following"
                    f" migrations:\n{planned_migrations}\n\nIf you are an admin and"
                    " know what you're doing, deploy the migration:\nlamin migrate"
                    " deploy\n\nOtherwise, please install previouses release of the"
                    " above-mentioned schemas\n\nIn case you haven't yet migrated to"
                    " Django, please upgrade to lamindb 0.41.2 before deploying this"
                    " migration\n"
                )
        else:
            if deploy_migrations:
                logger.info("Database already up-to-date with migrations!")
        global IS_SETUP
        IS_SETUP = True
