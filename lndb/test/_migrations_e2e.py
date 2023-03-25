import importlib
from subprocess import run
from typing import Optional

from lamin_logger import logger
from lnhub_rest._assets import instances as test_instances
from packaging import version

from lndb._init_instance import init
from lndb._settings import settings
from lndb.dev import setup_local_test_sqlite_file
from lndb.dev._clone import clone_test
from lndb.dev._settings_instance import InstanceSettings


def migrate_clones(
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
    display_list = [
        inst.split("/")[-1] for inst in run_instances.tolist()[:n_instances]
    ]
    logger.info(f"These instances will be tested: {display_list}")
    results = []
    for instance in run_instances.iloc[:n_instances]:
        logger.info(f"Testing: {instance}")
        result = migrate_clone(instance)
        results.append(result)
    return results


def migrate_clone(instance: str, n_rows: int = 10000):
    """instance takes the weird format defined in lnhub_rest._instances."""
    db, storage_root, name = None, None, None
    for prefix in {"s3://", "gc://"}:
        if instance.startswith(prefix):
            db = None
            name = instance.replace(prefix, "").split("/")[0]
            storage_root = prefix + name
    if instance.startswith("postgresql"):
        db = instance
        storage_root = "pgtest"
        name = "pgtest"
    # init test instance
    src_settings = InstanceSettings(
        storage_root=storage_root,
        db=db,
        name=name,  # type: ignore  # noqa
        owner=settings.user.handle,
    )
    connection_string = clone_test(src_settings=src_settings, n_rows=n_rows)
    if db is None:
        storage_test = setup_local_test_sqlite_file(src_settings, return_dir=True)
        result = init(storage=storage_test, _migrate=True)
    else:
        result = init(db=connection_string, storage=storage_root, _migrate=True)
    logger.info(result)
    if instance.startswith("postgresql"):
        run("docker stop pgtest && docker rm pgtest", shell=True)
    return result
