from lamin_utils import logger

from ._settings import settings


def register():
    """Register an instance on the hub."""
    from .dev._hub_core import init_instance as init_instance_hub

    isettings = settings.instance
    result = init_instance_hub(
        owner=isettings.owner,
        name=isettings.name,
        storage=isettings.storage.root_as_str,
        db=isettings.db if isettings.dialect != "sqlite" else None,
        schema=isettings._schema_str,
    )
    if result == "error-instance-exists-already":
        logger.info("instance was already registered")
    elif result.startswith("error-"):
        raise RuntimeError(f"Registering instance on hub failed:\n{result}")
    else:
        logger.save(f"instance registered: https://lamin.ai/{isettings.identifier}")
        isettings._id = result
        isettings._persist()
