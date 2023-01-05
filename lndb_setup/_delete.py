import shutil
from pathlib import Path

from lamin_logger import logger

from ._hub import (
    connect_hub_with_auth,
    get_instance,
    get_instance_default_storage,
    get_instances_related_to_storage_by_id,
    get_user_by_handle,
)
from ._settings import settings
from ._settings_load import load_instance_settings
from ._settings_store import instance_settings_file


def delete(instance_name: str):
    """Delete an instance."""
    hub = connect_hub_with_auth()

    settings_file = instance_settings_file(instance_name, settings.user.handle)

    if not settings_file.exists():
        # TODO: trying to load settings from the hub
        logger.error("Cannot find instance settings locally.")
        return "instance-settings-not-found"

    isettings = load_instance_settings(settings_file)
    owner = get_user_by_handle(hub, isettings.owner)

    # 1. Delete storage

    # Delete default storage if it's a local one

    instance_default_storage = get_instance_default_storage(
        hub, isettings.name, owner["id"]
    )
    if instance_default_storage["type"] == "local":
        if Path(instance_default_storage["root"]).exists():
            shutil.rmtree(instance_default_storage["root"])
            logger.info("Instance default storage root deleted.")
    else:
        logger.info(
            "Instance default storage won't be deleted as it is a cloud storage."
        )

    # Other attached storage are not deleted

    # 2. Delete cache

    if isettings.cache_dir:
        if isettings.cache_dir.exists():
            shutil.rmtree(isettings.cache_dir)
            logger.info("Instance cache deleted.")

    # 3. Delete hub records

    instance = get_instance(hub, isettings.name, owner["id"])

    # Delete all instance metadata

    hub.table("user_instance").delete().eq("instance_id", instance["id"]).execute()
    hub.table("usage").delete().eq("instance_id", instance["id"]).execute()
    hub.table("instance").delete().eq("id", instance["id"]).execute()

    # Delete storage metadata unless it's a shared storage
    instances = get_instances_related_to_storage_by_id(
        hub, instance_default_storage["id"]
    )
    if instances is None:
        hub.table("storage").delete().eq("id", instance_default_storage["id"]).execute()

    logger.info("Instance metadata deleted.")

    # All tables related to instance data will soon be removed
    # Writing any logic to delete associated records would be useless

    # 4. Delete settings file

    settings_file.unlink()
    logger.info("Instance settings deleted.")

    hub.auth.sign_out()
