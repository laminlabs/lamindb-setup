import shutil

from lamin_logger import logger

from ._hub import connect_hub_with_auth, get_instance, get_user_by_handle
from ._settings_load import load_instance_settings
from ._settings_store import instance_settings_file


def delete(instance_name: str, owner_handle: str, delete_in_hub=True):
    """Delete an instance."""
    settings_file = instance_settings_file(instance_name, owner_handle)
    isettings = load_instance_settings(settings_file)

    shutil.rmtree(isettings.storage_root)
    logger.info("Instance root directory deleted.")

    hub = connect_hub_with_auth()
    owner = get_user_by_handle(hub, isettings.owner)
    instance = get_instance(hub, isettings.name, owner["id"])
    hub.table("instance_user").delete("*").eq("instance_id", instance["id"]).execute()
    hub.table("dtransform").delete("*").eq("instance_id", instance["id"]).execute()
    hub.table("dobject").delete("*").eq("instance_id", instance["id"]).execute()
    hub.table("usage").delete("*").eq("instance_id", instance["id"]).execute()
    hub.auth.sign_out()

    settings_file.unlink()
    logger.info("Instance settings deleted.")
