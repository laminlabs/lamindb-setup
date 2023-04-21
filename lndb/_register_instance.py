from lamin_logger import logger
from lnhub_rest.core.instance._init_instance import init_instance as init_instance_hub

from ._settings import settings


def register():
    """Register an instance on the hub."""
    isettings = settings.instance
    result = init_instance_hub(
        owner=isettings.owner,
        name=isettings.name,
        storage=isettings.storage.root_as_str,
        db=isettings.db,
        schema=isettings._schema_str,
    )
    if result == "instance-exists-already":
        logger.info("Instance already registered")
    elif isinstance(result, str):
        raise RuntimeError(f"Creating instance on hub failed:\n{result}")
    logger.success("Instance registered")
