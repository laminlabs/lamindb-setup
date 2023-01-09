import shutil
from pathlib import Path

from lamin_logger import logger
from supabase.client import Client

from ._hub import (
    connect_hub_with_auth,
    get_instance,
    get_instance_default_storage,
    get_user_by_handle,
)
from ._settings import settings
from ._settings_load import load_instance_settings
from ._settings_store import instance_settings_file
from ._setup_instance import load_isettings_from_hub, persist_check_reload_schema


def delete(instance_name: str):
    """Delete an instance."""
    hub = connect_hub_with_auth()
    try:
        message = delete_helper(hub, instance_name)
        return message
    finally:
        hub.auth.sign_out()


def delete_helper(hub: Client, instance_name: str):
    settings_file = instance_settings_file(instance_name, settings.user.handle)

    if not settings_file.exists():
        isettings, message = load_isettings_from_hub(
            instance_name, settings.user.handle
        )
        if message is not None:
            return message
        persist_check_reload_schema(isettings)
        settings_file = instance_settings_file(instance_name, settings.user.handle)

    isettings = load_instance_settings(settings_file)

    owner = get_user_by_handle(hub, isettings.owner)
    instance_metadata = get_instance(hub, isettings.name, owner["id"])
    instance_default_storage_metadata = get_instance_default_storage(
        hub, isettings.name, owner["id"]
    )

    # 1. Delete default storage or cache

    if instance_default_storage_metadata["type"] == "local":
        delete_storage(Path(instance_default_storage_metadata["root"]))
    else:
        delete_cache(isettings.cache_dir)
        logger.info(
            "Instance default storage won't be deleted as it is a cloud storage."
        )

    # 2. Delete metadata

    if instance_metadata:
        delete_metadata(
            hub, instance_metadata["id"], instance_default_storage_metadata["id"]
        )

    # 3. Delete settings

    delete_settings(settings_file)


def delete_storage(storage_root: Path):
    if storage_root.exists():
        shutil.rmtree(storage_root)
        logger.info("Instance default storage root deleted.")


def delete_cache(cache_dir: Path):
    if cache_dir:
        if cache_dir.exists():
            shutil.rmtree(cache_dir)
            logger.info("Instance cache deleted.")


def delete_metadata(hub: Client, instance_id: str, instance_default_storage_id: str):
    hub.table("user_instance").delete().eq("instance_id", instance_id).execute()
    hub.table("usage").delete().eq("instance_id", instance_id).execute()
    hub.table("instance").delete().eq("id", instance_id).execute()
    hub.table("storage").delete().eq("id", instance_default_storage_id).execute()

    logger.info("Instance metadata deleted.")


def delete_settings(settings_file: Path):
    settings_file.unlink()
    logger.info("Instance settings deleted.")
