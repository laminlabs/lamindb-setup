import importlib
import io
from pathlib import Path
from subprocess import run

import sqlalchemy as sa
from alembic import command
from alembic.autogenerate.api import AutogenContext
from alembic.autogenerate.render import _render_cmd_body
from lamin_logger import logger
from pytest_alembic.config import Config
from pytest_alembic.executor import CommandExecutor, ConnectionExecutor
from pytest_alembic.plugin.error import AlembicTestFailure
from pytest_alembic.runner import MigrationContext
from sqlmodel import SQLModel

from lndb._delete import delete
from lndb._init_instance import init
from lndb._migrate import generate_module_files, get_schema_package_info


def get_migration_config(package_name: str, *, target_metadata=None, **kwargs):
    schema_package_dir = str(get_schema_package_info(package_name)[0])
    if target_metadata is None:
        target_metadata = SQLModel.metadata
    target_metadata.naming_convention = {
        "ix": "ix_%(column_0_label)s",
        "uq": "uq_%(table_name)s_%(column_0_name)s",
        "ck": "ck_%(table_name)s_`%(constraint_name)s`",
        "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
        "pk": "pk_%(table_name)s",
    }
    raw_config = dict(
        config_file_name=f"{schema_package_dir}/alembic.ini",
        script_location=f"{schema_package_dir}/migrations",
        target_metadata=target_metadata,
        **kwargs,
    )
    config = Config.from_raw_config(raw_config)
    return config


def get_migration_context(package_name: str, db: str, include_schemas=None):
    engine = sa.create_engine(db)
    config = get_migration_config(package_name, include_schemas=include_schemas)
    command_executor = CommandExecutor.from_config(config)
    command_executor.configure(connection=engine)
    migration_context = MigrationContext.from_config(
        config, command_executor, ConnectionExecutor(), engine
    )
    return migration_context


def get_migration_id_from_scripts(package_name: str):
    config = get_migration_config(package_name)
    output_buffer = io.StringIO()
    schema_package_dir = str(get_schema_package_info(package_name)[0])
    # get the id of the latest migration script
    if Path(f"{schema_package_dir}/migrations/versions").exists():
        command.heads(config.make_alembic_config(stdout=output_buffer))
        output = output_buffer.getvalue()
        migration_id = output.split(" ")[0]
    else:  # there is no scripts directory
        logger.warning(f"'{schema_package_dir}/migrations/versions' does not exist")
        migration_id = ""
    return migration_id


def migration_id_is_consistent(package_name):
    # package_name is either a simple module (if schema is at the root)
    # or a relative path to the submodule that contains the schema,
    # e.g. ./lnhub_rest/schema
    migration_id_from_scripts = get_migration_id_from_scripts(package_name)
    if package_name.startswith("./"):
        _, root, relative = package_name.split("/")
        assert relative == "schema"
        package = importlib.import_module(f".{relative}", package=root.lstrip("./"))
    else:
        package = importlib.import_module(package_name)
    if package._migration is None:
        migration_id_from_import = ""
    else:
        migration_id_from_import = package._migration
    return migration_id_from_import == migration_id_from_scripts


def model_definitions_match_ddl(package_name, db=None, dialect_name="sqlite"):
    generate_module_files(package_name)
    if db is None and dialect_name == "sqlite":
        db = "sqlite:///testdb/testdb.lndb"
        # need to call init to reload schema
        init(storage="testdb", _migrate=False)
    elif db is None and dialect_name == "postgresql":
        # requires postgres has been set up through _nox_tools
        db = "postgresql://postgres:pwd@0.0.0.0:5432/pgtest"
        # need to call init to reload schema
        init(db=db, storage="pgtest", _migrate=False)
    elif db is None:
        raise NotImplementedError(
            "Only sqlite and postgres test databases are implemented."
        )
    # the below is for debugging purposes, something with indexes doesn't work 100%
    # e = sa.create_engine(db)
    # print(sa.inspect(e).get_indexes("core.dobject"))
    include_schemas = True if dialect_name == "postgresql" else False
    migration_context = get_migration_context(
        package_name, db, include_schemas=include_schemas
    )
    execute_model_definitions_match_ddl(migration_context)
    if dialect_name == "postgresql":
        run("docker stop pgtest && docker rm pgtest", shell=True)
        delete("pgtest")
    else:
        delete("testdb")


def execute_model_definitions_match_ddl(alembic_runner):
    # this function is largely copied from the
    # MIT licensed https://github.com/schireson/pytest-alembic
    """Assert that the state of the migrations matches the state of the models.

    In general, the set of migrations in the history should coalesce into DDL
    which is described by the current set of models. Therefore, a call to
    `revision --autogenerate` should always generate an empty migration (e.g.
    find no difference between your database (i.e. migrations history) and your
    models).
    """

    def verify_is_empty_revision(migration_context, __, directives):
        script = directives[0]

        migration_is_empty = script.upgrade_ops.is_empty()
        if not migration_is_empty:
            autogen_context = AutogenContext(migration_context)
            rendered_upgrade = _render_cmd_body(script.upgrade_ops, autogen_context)

            if not migration_is_empty:
                raise AlembicTestFailure(
                    "The models describing the DDL of your database are out of sync"
                    " with the set of steps described in the revision history. This"
                    " usually means that someone has made manual changes to the"
                    " database's DDL, or some model has been changed without also"
                    " generating a migration to describe that"
                    f" change.\n{rendered_upgrade}"
                )

    try:
        alembic_runner.migrate_up_to("heads")
    except RuntimeError as e:
        raise AlembicTestFailure(
            "Failed to upgrade to the head revision. This means the historical chain"
            f" from an empty database, to the current revision is not possible.\n{e}"
        )

    alembic_runner.generate_revision(
        message="test revision",
        autogenerate=True,
        prevent_file_generation=True,
        process_revision_directives=verify_is_empty_revision,
    )
