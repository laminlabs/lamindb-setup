from subprocess import run
from typing import Optional

from lamin_logger import logger
from lnhub_rest._assets import instances as test_instances
from lnhub_rest.core.instance._init_instance import (
    validate_db_arg,
    validate_schema_arg,
    validate_storage_arg,
)

from lndb._init_instance import infer_instance_name, init
from lndb._settings import settings
from lndb.dev import setup_local_test_sqlite_file
from lndb.dev._clone import clone_test
from lndb.dev._settings_instance import InstanceSettings

# from ._migrations_unit import model_definitions_match_ddl


def migrate_clones(
    schema_package: str, n_instances: Optional[int] = None, dialect_name="sqlite"
):
    # get test instances
    # format is:
    # - "s3://lamin-site-assets/lamin-site-assets.lndb" (sqlite)
    # - "postgresql://batman:robin@host:5432/lamindata" (postgres)
    instances = test_instances.loc[test_instances[1] == schema_package][0]
    if dialect_name == "sqlite":
        run_instances = instances.loc[
            instances.str.startswith("s3://") | instances.str.startswith("gc://")
        ]
    elif dialect_name == "postgresql":
        run_instances = instances.loc[instances.str.startswith(dialect_name)]
    else:
        raise ValueError("Pass either 'sqlite' or 'postgresql'.")
    display_list = [
        inst.split("/")[-1] for inst in run_instances.tolist()[:n_instances]
    ]
    logger.info(f"These instances will be tested: {display_list}")
    results = []
    for instance in run_instances.iloc[:n_instances]:
        db, storage_root, name = None, None, None
        for prefix in {"s3://", "gc://"}:
            if instance.startswith(prefix):
                db = None
                name = instance.split("/")[-1].replace(".lndb", "")
                storage_root = prefix + name
        if instance.startswith("postgresql"):
            db, name, storage_root = instance, "pgtest", "pgtest"
        logger.info(f"Testing: {instance}")
        result = migrate_clone(db=db, storage=storage_root, name=name, kill_docker=True)
        results.append(result)
    return results


def migrate_clone(
    *,
    db: Optional[str] = None,
    storage: Optional[str] = None,
    name: Optional[str] = None,
    schema: Optional[str] = None,
    n_rows: int = 10000,
    kill_docker: bool = False,
):
    """Clone and migrate a single instance.

    Args:
        db: Connection string (None for non-SQLite).
        storage: Storage (for SQLite instances).
        name: Instance name.
        schema: The schema string.
        n_rows: Number of rows per table to clone.
        kill_docker: Kill the docker container.
    """
    schema = validate_schema_arg(schema)
    validate_storage_arg(str(storage))  # needs improvement!
    validate_db_arg(db)
    logger.info(f"Will attempt to migrate these schemas beyond core: {schema}")
    name_str = infer_instance_name(storage=storage, name=name, db=db)
    # get settings, but not from load, as we don't want to trigger loading the instance
    src_settings = InstanceSettings(
        storage_root=storage if storage is not None else "pgtest",
        db=db,
        name=name_str,  # type: ignore  # noqa
        owner=settings.user.handle,
        schema=schema,  # has no effect right now
    )
    connection_string = clone_test(src_settings=src_settings, n_rows=n_rows)
    if db is None:
        storage_test = setup_local_test_sqlite_file(src_settings, return_dir=True)
        result = init(storage=storage_test, schema=schema, _migrate=True)
    else:
        result = init(
            db=connection_string, storage="pgtest", schema=schema, _migrate=True
        )
        # model_definitions_match_ddl(schema_package, db=connection_string)
    logger.info(result)
    if kill_docker and db is not None:
        run("docker stop pgtest && docker rm pgtest", shell=True)
    return result
