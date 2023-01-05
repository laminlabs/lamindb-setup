import importlib
from pathlib import Path
from subprocess import run
from typing import Optional

from lamin_logger import logger

from lndb_setup.test import get_package_name


class migrate:
    """Manage migrations."""

    @staticmethod
    def generate(version: str = "vX.X.X", schema_root: Optional[Path] = None):
        """Generate migration for current schema module.

        Needs to be executed at the root level of the python package that contains
        the schema module.

        Args:
            version: Version string to label migration with.
            schema_root: Optional. Root directory of schema module.
        """
        package_name = get_package_name(schema_root)
        package = importlib.import_module(package_name)
        if not hasattr(package, "_schema_id"):
            package_name = f"{package_name}.schema"
            package = importlib.import_module(package_name)
        schema_id = getattr(package, "_schema_id")
        logger.info("Generate migration with reference db: testdb/testdb.lndb")
        command = (
            f"alembic --config {package_name}/alembic.ini --name {schema_id} revision"
            f" --autogenerate -m '{version}'"
        )
        if schema_root is not None:
            cwd = f"{schema_root}"
        else:
            cwd = None
        process = run(command, shell=True, cwd=cwd)

        if process.returncode == 0:
            logger.success(f"Successfully generated migration {version}.")
            return None
        else:
            logger.error("Generating migration failed.")
            return "migrate-gen-failed"
