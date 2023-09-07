from uuid import UUID

from lamin_utils import logger

from ._settings import settings


def register():
    """Register an instance on the hub."""
    from .dev._hub_core import init_instance as init_instance_hub

    isettings = settings.instance
    result = init_instance_hub(
        name=isettings.name,
        storage=isettings.storage.root_as_str,
        db=isettings.db if isettings.dialect != "sqlite" else None,
        schema=isettings._schema_str,
    )
    if not isinstance(result, UUID):
        if result == "error-db-already-exists":
            logger.warning("DB already exists")
        else:
            raise RuntimeError(f"Registering instance on hub failed:\n{result}")
    else:
        logger.save(f"instance registered: https://lamin.ai/{isettings.identifier}")
        isettings._id = result
        isettings._persist()
