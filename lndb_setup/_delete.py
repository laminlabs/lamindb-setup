from lamin_logger import logger

from ._settings_load import load_instance_settings
from ._settings_store import instance_settings_file


def delete(instance_name: str, owner_handle: str, delete_in_hub=True):
    """Delete an instance."""
    settings_file = instance_settings_file(instance_name, owner_handle)
    isettings = load_instance_settings(settings_file)

    isettings.storage_root.rmdir()
    logger.info("Instance root directory deleted.")

    if delete_in_hub:
        logger.info("Instance deleted from the hub.")
        pass

    settings_file.unlink()
    logger.info("Instance settings deleted.")
