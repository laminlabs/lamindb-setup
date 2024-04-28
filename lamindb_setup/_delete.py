from __future__ import annotations

import shutil
from typing import TYPE_CHECKING, Optional
from uuid import UUID

from lamin_utils import logger

from ._connect_instance import INSTANCE_NOT_FOUND_MESSAGE
from .core._hub_core import connect_instance as load_instance_from_hub
from .core._hub_core import delete_instance as delete_instance_on_hub
from .core._hub_core import get_storage_records_for_instance
from .core._settings import settings
from .core._settings_instance import InstanceSettings
from .core._settings_load import load_instance_settings
from .core._settings_storage import StorageSettings
from .core._settings_store import instance_settings_file
from .core.upath import HOSTED_BUCKETS, check_storage_is_empty

if TYPE_CHECKING:
    from pathlib import Path


def delete_cache(cache_dir: Path):
    if cache_dir is not None and cache_dir.exists():
        shutil.rmtree(cache_dir)


def delete_exclusion_dir(isettings: InstanceSettings) -> None:
    exclusion_dir = isettings.storage.root / f".lamindb/_exclusion/{isettings._id.hex}"
    if exclusion_dir.exists():
        exclusion_dir.rmdir()


def delete_by_isettings(isettings: InstanceSettings) -> None:
    settings_file = isettings._get_settings_file()
    if settings_file.exists():
        settings_file.unlink()
    delete_cache(isettings.storage.cache_dir)
    if isettings.dialect == "sqlite":
        try:
            if isettings._sqlite_file.exists():
                isettings._sqlite_file.unlink()
        except PermissionError:
            logger.warning(
                "Did not have permission to delete SQLite file:"
                f" {isettings._sqlite_file}"
            )
            pass
    # unset the global instance settings
    if settings._instance_exists and isettings.slug == settings.instance.slug:
        if settings._instance_settings_path.exists():
            settings._instance_settings_path.unlink()
        settings._instance_settings = None


def delete(
    instance_name: str, force: bool = False, require_empty: bool = True
) -> int | None:
    """Delete a LaminDB instance.

    Args:
        instance_name (str): The name of the instance to delete.
        force (bool): Whether to skip the confirmation prompt.
        require_empty (bool): Whether to check if the instance is empty before deleting.
    """
    if "/" in instance_name:
        logger.warning(
            "Deleting the instance of another user is currently not supported with the"
            " CLI. Please provide only the instance name when deleting an instance ('/'"
            " delimiter not allowed)."
        )
        raise ValueError("Invalid instance name: '/' delimiter not allowed.")
    instance_slug = f"{settings.user.handle}/{instance_name}"
    if settings._instance_exists and settings.instance.name == instance_name:
        isettings = settings.instance
    else:
        settings_file = instance_settings_file(instance_name, settings.user.handle)
        if not settings_file.exists():
            hub_result = load_instance_from_hub(
                owner=settings.user.handle, name=instance_name
            )
            if isinstance(hub_result, str):
                message = INSTANCE_NOT_FOUND_MESSAGE.format(
                    owner=settings.user.handle,
                    name=instance_name,
                    hub_result=hub_result,
                )
                logger.warning(message)
                return None
            instance_result, storage_result = hub_result
            ssettings = StorageSettings(
                root=storage_result["root"],
                region=storage_result["region"],
                uid=storage_result["lnid"],
            )
            isettings = InstanceSettings(
                id=UUID(instance_result["id"]),
                owner=settings.user.handle,
                name=instance_name,
                storage=ssettings,
                keep_artifacts_local=bool(instance_result["keep_artifacts_local"]),
                db=instance_result["db"] if "db" in instance_result else None,
                schema=instance_result["schema_str"],
                git_repo=instance_result["git_repo"],
            )
        else:
            isettings = load_instance_settings(settings_file)
    if isettings.dialect != "sqlite":
        logger.warning(
            f"delete() does not yet affect your Postgres database at {isettings.db}"
        )
    if not force:
        valid_responses = ["y", "yes"]
        user_input = (
            input(f"Are you sure you want to delete instance {instance_slug}? (y/n) ")
            .strip()
            .lower()
        )
        if user_input not in valid_responses:
            return -1

    # the actual deletion process begins here
    if isettings.dialect == "sqlite" and isettings.is_remote:
        # delete the exlusion dir first because it's hard to count its objects
        delete_exclusion_dir(isettings)
    if isettings.storage.type_is_cloud and isettings.storage.root_as_str.startswith(
        HOSTED_BUCKETS
    ):
        if not require_empty:
            logger.warning(
                "hosted storage always has to be empty, ignoring `require_empty`"
            )
        require_empty = True
    storage_records = get_storage_records_for_instance(isettings._id)
    for storage_record in storage_records:
        account_for_sqlite_file = (
            isettings.dialect == "sqlite"
            and isettings.storage.root_as_str == storage_record["root"]
        )
        n_objects = check_storage_is_empty(
            storage_record["root"],  # type: ignore
            raise_error=require_empty,
            account_for_sqlite_file=account_for_sqlite_file,
        )
        ssettings = StorageSettings(storage_record["root"])  # type: ignore
        if ssettings._mark_storage_root.exists():
            ssettings._mark_storage_root.unlink(
                missing_ok=True  # this is totally weird, but needed on Py3.11
            )
    logger.info(f"deleting instance {instance_slug}")
    # below we can skip check_storage_is_empty() because we already called
    # it above
    if settings.user.handle != "anonymous":
        delete_instance_on_hub(isettings._id, require_empty=False)
    delete_by_isettings(isettings)
    if n_objects == 0 and isettings.storage.type == "local":
        # dir is only empty after sqlite file was delete via delete_by_isettings
        (isettings.storage.root / ".lamindb").rmdir()
        isettings.storage.root.rmdir()
    return None
