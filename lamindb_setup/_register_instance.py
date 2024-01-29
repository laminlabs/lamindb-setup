from uuid import UUID

from lamin_utils import logger
from .dev.django import setup_django
from ._settings import settings


def register(_test: bool = False):
    """Register an instance on the hub."""
    from .dev._hub_core import init_instance as init_instance_hub
    from ._check_instance_setup import check_instance_setup

    isettings = settings.instance

    if not check_instance_setup() and not _test:
        setup_django(isettings)

    result = init_instance_hub(
        id=isettings.id,
        name=isettings.name,
        storage=isettings.storage,
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
