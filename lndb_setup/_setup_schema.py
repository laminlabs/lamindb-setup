import lnschema_core
from lamin_logger import logger
from sqlmodel import SQLModel

from ._db import insert
from ._settings_instance import InstanceSettings
from ._settings_user import UserSettings


def configure_schema_wetlab(schema_modules):
    def _no_biosample_techsample():
        file_loc = lnschema_core.__file__.replace("core", "wetlab")
        with open(file_loc, "r") as f:
            content = f.read()
        with open(file_loc, "w") as f:
            content = content.replace(
                '_tables = ["biosample", "techsample"]', "_tables = []"
            )
            f.write(content)

    if any([i in schema_modules for i in {"retro", "swarm"}]):
        _no_biosample_techsample()


def setup_schema(isettings: InstanceSettings, usettings: UserSettings):
    schema_modules = isettings.schema_modules

    msg = "Loading schema modules: core"

    if schema_modules is not None and "bionty" in schema_modules:
        import lnschema_bionty  # noqa

        msg += ", bionty"
    if schema_modules is not None and "wetlab" in schema_modules:
        configure_schema_wetlab(schema_modules)

        import lnschema_wetlab  # noqa

        msg += ", wetlab"
    if schema_modules is not None and "bfx" in schema_modules:
        import lnbfx.schema  # noqa

        msg += ", bfx"

    if schema_modules is not None and "retro" in schema_modules:
        import lnschema_retro  # noqa

        msg += ", retro"
    if schema_modules is not None and "swarm" in schema_modules:
        import maren.schema  # noqa

        msg += ", swarm"

    logger.info(f"{msg}.")

    SQLModel.metadata.create_all(isettings.db_engine())

    insert.version(
        "yvzi",
        lnschema_core.__version__,
        lnschema_core._migration,
        usettings.id,  # type: ignore
        cloud_sqlite=False,
    )

    if schema_modules is not None and "bionty" in schema_modules:
        insert.version(
            "zdno",
            lnschema_bionty.__version__,
            lnschema_bionty._migration,
            usettings.id,  # type: ignore
            cloud_sqlite=False,
        )

    isettings._update_cloud_sqlite_file()
