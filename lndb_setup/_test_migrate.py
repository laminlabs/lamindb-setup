import importlib
from subprocess import run
from time import sleep
from typing import Optional

from cloudpathlib import CloudPath
from lamin_logger import logger
from packaging import version

from ._clone import clone_test, setup_local_test_postgres, setup_local_test_sqlite_file
from ._settings_instance import InstanceSettings
from ._setup_instance import init
from ._test_instances import test_instances


def migrate_test(
    schema_package: str, n_instances: Optional[int] = None, dialect_name="sqlite"
):
    # this is super hacky, we shouldn't need to set up these test instances here
    if dialect_name == "sqlite":
        init(storage="testdb")
    elif dialect_name == "postgresql":
        connection_string = setup_local_test_postgres()
        sleep(2)
        init(storage="testdb", dbconfig=connection_string)
        run("docker stop pgtest && docker rm pgtest", shell=True)
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
