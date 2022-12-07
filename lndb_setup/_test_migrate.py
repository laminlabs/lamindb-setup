import importlib
from subprocess import run
from typing import Optional

from alembic.autogenerate.api import AutogenContext
from alembic.autogenerate.render import _render_cmd_body
from cloudpathlib import CloudPath
from lamin_logger import logger
from packaging import version
from pytest_alembic.plugin.error import AlembicTestFailure

from ._assets import instances as test_instances
from ._clone import clone_test, setup_local_test_sqlite_file
from ._settings_instance import InstanceSettings
from ._setup_instance import init


def migrate_test(
    schema_package: str, n_instances: Optional[int] = None, dialect_name="sqlite"
):
    # auto-bump version to simulate state after release
    schema_module = importlib.import_module(schema_package)
    v = version.parse(schema_module.__version__)
    schema_module.__version__ = f"{v.major}.{v.minor+1}.0"  # type: ignore  # noqa
    # get test instances
    instances = test_instances.loc[test_instances[1] == schema_package][0]
    if dialect_name == "sqlite":
        run_instances = instances.loc[
            instances.str.startswith("s3://") | instances.str.startswith("gc://")
        ]
    elif dialect_name == "postgresql":
        run_instances = instances.loc[instances.str.startswith(dialect_name)]
    else:
        raise ValueError("Pass either 'sqlite' or 'postgresql'.")
    display_list = [inst.split("/")[-1] for inst in run_instances.tolist()]
    logger.info(f"These instances need to be tested: {display_list}")
    results = []
    for instance in run_instances.iloc[:n_instances]:
        logger.info(f"Testing: {instance}")
        dbconfig, storage = None, None
        for prefix in ["s3://", "gc://"]:
            if instance.startswith(prefix):
                dbconfig = "sqlite"
                storage = CloudPath(prefix + instance.replace(prefix, "").split("/")[0])
        if dbconfig is None and instance.startswith("postgresql"):
            dbconfig = instance
            storage = "pgtest"
        # init test instance
        src_settings = InstanceSettings(storage_root=storage, _dbconfig=dbconfig)  # type: ignore  # noqa
        connection_string = clone_test(src_settings=src_settings)
        if dbconfig == "sqlite":
            storage_test = setup_local_test_sqlite_file(src_settings, return_dir=True)
            result = init(dbconfig=dbconfig, storage=storage_test, migrate=True)
        else:
            result = init(dbconfig=connection_string, storage=storage, migrate=True)
        logger.info(result)
        if dialect_name == "postgresql":
            run("docker stop pgtest && docker rm pgtest", shell=True)
        results.append(result)
    return results


def model_definitions_match_ddl(alembic_runner):
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
