from subprocess import run
from typing import Optional

from lamin_logger import logger
from laminci import get_package_name

from lndb._migrate.deploy import check_deploy_migration  # noqa
from lndb._migrate.utils import (
    generate_module_files,
    get_schema_package_info,
    modify_alembic_ini,
    modify_migration_id_in__init__,
    set_alembic_logging_level,
)

push_instruction = """\
Please push your changes to a new remote branch, open a PR, and wait for CI.

Inspect the bottom of the output of the CI step 'Build'
and add the code to the migration script.

It will look like the following:
    op.drop_column('biosample', 'description', schema='wetlab')
    op.add_column('experiment', sa.Column('donor_id', sqlmodel.sql.sqltypes.AutoString(), schema="wetlab")  # noqa

Beware to account for renaming columns and tables manually, e.g.:
    op.alter_column("mytable", column_name="oldname", new_column_name="newname", schema="myschema")  # noqa

"""


class migrate:
    """Manage migrations."""

    @staticmethod
    def generate(
        version: str = "vX.X.X",
        package_name: Optional[str] = None,
    ):
        """Generate migration for current schema module.

        Needs to be executed at the root level of the python package that contains
        the schema module.

        Args:
            version: Version string to label migration with.
            package_name: Optional. Name of schema module package.
        """
        if package_name is None:
            package_name = get_package_name()
        package_dir, migrations_dir, schema_id = get_schema_package_info(package_name)
        generate_module_files(package_name)
        testdb_path = package_dir.parent / "testdb/testdb.lndb"  # type: ignore # noqa
        if testdb_path.exists():
            rm = False
            set_alembic_logging_level(migrations_dir, level="INFO")
            logger.info(f"Generating based on {testdb_path}")
        else:
            rm = True
            from lndb._settings import settings

            if settings._instance_exists:
                response = input(
                    "Will generate migration for instance"
                    " f{settings.instance.identifier}. Continue? (y/n)"
                )
                if response != "y":
                    return None
            else:
                logger.error("Please load the instance you'd like to migrate!")
                return None

            modify_alembic_ini(
                filepath=package_dir / "alembic.ini",
                isettings=settings.instance,
                package_name=package_name,
                move_sl=False,
            )
            set_alembic_logging_level(migrations_dir, level="WARN")
        command = (
            f"alembic --config {str(package_dir)}/alembic.ini --name"
            f" {schema_id} revision --autogenerate -m '{version}'"
        )
        process = run(command, shell=True)

        modify_migration_id_in__init__(package_name)

        if process.returncode == 0:
            logger.success(f"Successfully generated migration {version}.")
            if rm:
                modify_alembic_ini(
                    filepath=migrations_dir.parent / "alembic.ini",
                    isettings=settings.instance,
                    package_name=package_name,
                    move_sl=False,
                    revert=True,
                )
                logger.info(push_instruction)
            return None
        else:
            print(process.stderr)
            logger.error("Generating migration failed.")
            logger.info(f"Check content of {str(package_dir)}/alembic.ini")
            return "migrate-gen-failed"
