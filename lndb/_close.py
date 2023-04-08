import os

from lamin_logger import logger

from ._settings import settings
from .dev._settings_store import current_instance_settings_file


def close() -> None:
    """Close existing instance.

    Returns `None` if succeeds, otherwise an exception is raised.
    """
    if current_instance_settings_file().exists():
        instance = settings.instance.identifier
        current_instance_settings_file().unlink()
        os.environ["LAMINDB_INSTANCE_LOADED"] = "0"
        logger.success(f"Closed {instance}")
    else:
        logger.info("No instance loaded")
