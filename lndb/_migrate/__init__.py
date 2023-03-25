from pathlib import Path
from subprocess import run
from typing import Optional

from lamin_logger import logger

from ._deploy import check_deploy_migration  # noqa
from ._utils import (
    generate_module_files,
    get_package_info,
    modify_alembic_ini,
    modify_migration_id_in__init__,
    set_alembic_logging_level,
)

push_instruction = """Please push your changes to a new remote branch and open a PR.
Inspect CI step Build output and add migration code to the script.
It will look like the following, beware of the renaming:
    op.drop_column('biosample', 'description', schema='wetlab')
    op.add_column('experiment', sa.Column('donor_id', sqlmodel.sql.sqltypes.AutoString(), schema="wetlab")"""  # noqa


class migrate:
    """Manage migrations."""

    @staticmethod
    def generate(
        version: str = "vX.X.X",
        schema_root: Optional[Path] = None,
        package_name: Optional[str] = None,
    ):
        """Generate migration for current schema module.

        Needs to be executed at the root level of the python package that contains
        the schema module.

        Args:
            version: Version string to label migration with.
            schema_root: Optional. Root directory of schema module.
            package_name: Optional. Name of schema module package.
        """
        package_name, migrations_path, schema_id = get_package_info(
            schema_root=schema_root, package_name=package_name
        )
        generate_module_files(
            package_name=package_name,  # type:ignore
            migrations_path=migrations_path,
            schema_id=schema_id,
        )
        testdb_path = migrations_path.parent.parent / "testdb/testdb.lndb"  # type: ignore # noqa
        if testdb_path.exists():
            # runs dev mode to write migration scripts
            rm = False
            set_alembic_logging_level(migrations_path, level="INFO")
            logger.info("Generate migration with reference db: testdb/testdb.lndb")
        else:
            # runs CI-guided mode to generate empty migration scripts
            rm = True
            from lndb._settings import settings

            modify_alembic_ini(
                filepath=migrations_path.parent / "alembic.ini",
                isettings=settings.instance,
                package_name=package_name,
                move_sl=False,
            )
            set_alembic_logging_level(migrations_path, level="WARN")
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

        modify_migration_id_in__init__(package_name)

        if process.returncode == 0:
            logger.success(f"Successfully generated migration {version}.")
            if rm:
                modify_alembic_ini(
                    filepath=migrations_path.parent / "alembic.ini",
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
            return "migrate-gen-failed"
