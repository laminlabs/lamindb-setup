from subprocess import run

from lamin_logger import logger

from lndb_setup.test import get_package_name


class migrate:
    """Manage migrations."""

    @staticmethod
    def generate():
        """Generate migration for current schema module.

        Needs to be executed at the root level of the python package that contains
        the schema module.
        """
        package_name = get_package_name()
        if not hasattr(package_name, "schema_id"):
            package_name = f"{package_name}.schema"
        schema_id = getattr(package_name, "schema_id")
        command = (
            f"alembic --config {package_name}/alembic.ini --name {schema_id} revision"
            " --autogenerate -m 'vX.X.X'"
        )
        process = run(command, shell=True)

        if process.returncode == 0:
            logger.success("Successfully generated migration vX.X.X.")
        else:
            logger.error("Automatic migration failed.")
