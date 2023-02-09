import importlib
from pathlib import Path
from subprocess import run
from typing import Optional

from lamin_logger import logger

from lndb_setup.test import get_package_name


def _get_package_info(schema_root: Optional[Path] = None):
    package_name = get_package_name(schema_root)
    package = importlib.import_module(package_name)
    if not hasattr(package, "_schema_id"):
        package_name = f"{package_name}.schema"
        package = importlib.import_module(package_name)
    schema_id = getattr(package, "_schema_id")
    package_path = Path(package.__file__).parent.parent  # type:ignore
    return package_name, package_path, schema_id


def _generate_module_files(package_name: str, package_path: Path, schema_id: str):
    def read(filename: Path):
        with open(filename, "r") as f:
            return f.read()

    def write(filename: Path, content: str):
        with open(filename, "w") as f:
            return f.write(content)

    migrations_path = package_path / package_name / "migrations"
    _migrations_path = Path(__file__).parent / "_migrations"

    # ensures migrations/versions folder exists
    (migrations_path / "versions").mkdir(exist_ok=True, parents=True)

    if not (migrations_path / "env.py").exists():
        content = (
            read(_migrations_path / "env.py")
            .replace("_schema_id = None\n", "")
            .replace("# from {package_name} import *", f"from {package_name} import *")
            .replace(
                "# from {package_name} import _schema_id",
                f"from {package_name} import _schema_id",
            )
        )
        write(migrations_path / "env.py", content)

    if not (migrations_path / "script.py.mako").exists():
        import shutil

        shutil.copyfile(
            _migrations_path / "script.py.mako", migrations_path / "script.py.mako"
        )

    if not (migrations_path.parent / "alembic.ini").exists():
        content = (
            read(_migrations_path / "alembic.ini")
            .replace("[{schema_id}]", f"[{schema_id}]")
            .replace("{package_name}/migrations", f"{package_name}/migrations")
        )
        write(migrations_path.parent / "alembic.ini", content)


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
        package_name, package_path, schema_id = _get_package_info(
            schema_root=schema_root
        )
        _generate_module_files(
            package_name=package_name, package_path=package_path, schema_id=schema_id
        )
        db_path = package_path / "testdb/testdb.lndb"  # type: ignore # noqa
        if db_path.exists():
            rm = False
            logger.info("Generate migration with reference db: testdb/testdb.lndb")
        else:
            rm = True
            logger.info("Generate empty migration script.")
        command = (
            f"alembic --config {package_name}/alembic.ini --name {schema_id} revision"
            f" --autogenerate -m '{version}'"
        )
        if schema_root is not None:
            cwd = f"{schema_root}"
        else:
            cwd = None
        process = run(command, shell=True, cwd=cwd)

        if rm:
            run(f"rm {db_path.as_posix()}", shell=True, cwd=cwd)
            logger.info(
                "Please commit and push your changes and add migration code from CI to"
                " the script."
            )

        if process.returncode == 0:
            logger.success(f"Successfully generated migration {version}.")
            return None
        else:
            logger.error("Generating migration failed.")
            return "migrate-gen-failed"
