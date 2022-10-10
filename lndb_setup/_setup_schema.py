import importlib

from lamin_logger import logger
from sqlmodel import SQLModel

from ._db import insert
from ._settings_instance import InstanceSettings
from ._settings_user import UserSettings

known_schema_names = [
    "bionty",
    "wetlab",
    "bfx",
    "retro",
    "swarm",
    "harmonic-docking",
]


def get_schema_module_name(schema_name):
    if schema_name == "bfx":
        return "lnbfx.schema"
    else:
        return f"lnschema_{schema_name.replace('-', '_')}"


def setup_schema(isettings: InstanceSettings, usettings: UserSettings):
    if isettings.schema_modules is not None:
        schema_names = isettings.schema_modules.split(", ")
    else:
        schema_names = []

    msg = "Loading schema modules: "
    for schema_name in ["core"] + schema_names:
        schema_module = importlib.import_module(get_schema_module_name(schema_name))
        msg += f"{schema_name}=={schema_module.__version__}, "
    logger.info(f"{msg[:-2]}.")  # exclude the last comma

    SQLModel.metadata.create_all(isettings.db_engine())

    # we could try to also retrieve the user name here at some point
    insert.user(
        email=usettings.email, user_id=usettings.id, handle=usettings.handle, name=None
    )

    for schema_name in ["core"] + schema_names:
        schema_module = importlib.import_module(get_schema_module_name(schema_name))
        insert.version(
            schema_module=schema_module,
            user_id=usettings.id,  # type: ignore
            cloud_sqlite=False,
        )

    isettings._update_cloud_sqlite_file()

    logger.info(f"Created instance {isettings.name}: {isettings._sqlite_file}")
