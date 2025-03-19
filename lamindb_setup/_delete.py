from __future__ import annotations

import shutil
from typing import TYPE_CHECKING
from uuid import UUID

from lamin_utils import logger

from ._connect_instance import _connect_instance, get_owner_name_from_identifier
from .core._aws_options import HOSTED_BUCKETS
from .core._hub_core import delete_instance as delete_instance_on_hub
from .core._hub_core import get_storage_records_for_instance
from .core._settings import settings
from .core._settings_storage import StorageSettings
from .core.upath import LocalPathClasses, check_storage_is_empty

if TYPE_CHECKING:
    from pathlib import Path

    from .core._settings_instance import InstanceSettings


def delete_cache(isettings: InstanceSettings):
    # avoid init of root
    root = isettings.storage._root_init
    if not isinstance(root, LocalPathClasses):
        cache_dir = settings.cache_dir / root.path
        if cache_dir.exists():
            shutil.rmtree(cache_dir)


def delete_exclusion_dir(isettings: InstanceSettings) -> None:
    exclusion_dir = isettings.storage.root / f".lamindb/_exclusion/{isettings._id.hex}"
    if exclusion_dir.exists():
        exclusion_dir.rmdir()


def delete_by_isettings(isettings: InstanceSettings) -> None:
    settings_file = isettings._get_settings_file()
    if settings_file.exists():
        settings_file.unlink()
    delete_cache(isettings)
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


def delete(slug: str, force: bool = False, require_empty: bool = True) -> int | None:
    """Delete a LaminDB instance.

    Args:
        slug: The instance slug `account_handle/instance_name` or URL.
            If the instance is owned by you, it suffices to pass the instance name.
        force: Whether to skip the confirmation prompt.
        require_empty: Whether to check if the instance is empty before deleting.
    """
    owner, name = get_owner_name_from_identifier(slug)
    isettings = _connect_instance(owner, name, raise_permission_error=False)
    if isettings.dialect != "sqlite":
        logger.warning(
            f"delete() does not yet affect your Postgres database at {isettings.db}"
        )
    if isettings.is_on_hub and settings.user.handle == "anonymous":
        logger.warning(
            "won't delete the hub component of this instance because you're not logged in"
        )
    if not force:
        valid_responses = ["y", "yes"]
        user_input = (
            input(f"Are you sure you want to delete instance {isettings.slug}? (y/n) ")
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
    # first the default storage
    n_files = check_storage_is_empty(
        isettings.storage.root,
        raise_error=require_empty,
        account_for_sqlite_file=isettings.dialect == "sqlite",
    )
    if isettings.storage._mark_storage_root.exists():
        isettings.storage._mark_storage_root.unlink(
            missing_ok=True  # this is totally weird, but needed on Py3.11
        )
    # now everything that's on the hub
    if settings.user.handle != "anonymous":
        storage_records = get_storage_records_for_instance(isettings._id)
        for storage_record in storage_records:
            if storage_record["root"] == isettings.storage.root_as_str:
                continue
            ssettings = StorageSettings(storage_record["root"])  # type: ignore
            check_storage_is_empty(
                ssettings.root,  # type: ignore
                raise_error=require_empty,
            )
            if ssettings._mark_storage_root.exists():
                ssettings._mark_storage_root.unlink(
                    missing_ok=True  # this is totally weird, but needed on Py3.11
                )
    logger.info(f"deleting instance {isettings.slug}")
    # below we can skip check_storage_is_empty() because we already called
    # it above
    if settings.user.handle != "anonymous" and isettings.is_on_hub:
        # start with deleting things on the hub
        # this will error if the user doesn't have permission
        delete_instance_on_hub(isettings._id, require_empty=False)
    delete_by_isettings(isettings)
    # if lamin.db file was delete, then we might count -1
    if n_files <= 0 and isettings.storage.type == "local":
        # dir is only empty after sqlite file was delete via delete_by_isettings
        if (isettings.storage.root / ".lamindb").exists():
            (isettings.storage.root / ".lamindb").rmdir()
        if isettings.storage.root.exists():
            isettings.storage.root.rmdir()
    return None
