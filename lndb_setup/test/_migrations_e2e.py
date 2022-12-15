import importlib
from subprocess import run
from typing import Optional

from cloudpathlib import CloudPath
from lamin_logger import logger
from packaging import version

from lndb_setup._assets import instances as test_instances
from lndb_setup._clone import clone_test, setup_local_test_sqlite_file
from lndb_setup._settings import settings
from lndb_setup._settings_instance import InstanceSettings
from lndb_setup._setup_instance import init


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
    display_list = [inst.split("/")[-1] for inst in run_instances.tolist()]
    logger.info(f"These instances need to be tested: {display_list}")
    results = []
    for instance in run_instances.iloc[:n_instances]:
        logger.info(f"Testing: {instance}")
        url, storage, name = None, None, None
        for prefix in ["s3://", "gc://"]:
            if instance.startswith(prefix):
                url = None
                name = instance.replace(prefix, "").split("/")[0]
                storage = CloudPath(prefix + name)
        if instance.startswith("postgresql"):
            url = instance
            storage = "pgtest"
            name = "pgtest"
        # init test instance
        src_settings = InstanceSettings(
            storage_root=storage,
            url=url,
            name=name,  # type: ignore  # noqa
            owner=settings.user.handle,
        )
        connection_string = clone_test(src_settings=src_settings)
        if url is None:
            storage_test = setup_local_test_sqlite_file(src_settings, return_dir=True)
            result = init(storage=storage_test, migrate=True)
        else:
            result = init(url=connection_string, storage=storage, migrate=True)
        logger.info(result)
        if dialect_name == "postgresql":
            run("docker stop pgtest && docker rm pgtest", shell=True)
        results.append(result)
    return results
