from __future__ import annotations

from typing import TYPE_CHECKING

from lamin_utils import logger

from ._init_instance import register_storage_in_instance
from .core._hub_core import delete_storage_record
from .core._settings import settings
from .core._settings_storage import StorageSettings, init_storage

if TYPE_CHECKING:
    from lamindb_setup.types import UPathStr


def set_managed_storage(root: UPathStr, host: str | None = None, **fs_kwargs):
    """Add or switch to another managed storage location.

    Args:
        root: `UPathStr` - The new storage root, e.g., an S3 bucket.
        host: `str | None = None` For a shared local storage location, pass a globally unique host identifier, e.g. `"my-institute-cluster-1"`, `"my-server-abcd"`, ...
            Discuss the naming convention with an admin.
        **fs_kwargs: Additional fsspec arguments for cloud root, e.g., profile.

    """
    if settings.instance.dialect == "sqlite":
        raise ValueError(
            "Can't add additional managed storage locations for sqlite instances."
        )
    if not settings.instance.is_on_hub:
        raise ValueError(
            "Can't add additional managed storage locations for instances that aren't managed through the hub."
        )

    # we do not just query the instance storage table because
    # we might need some information from the hub
    if not StorageSettings(root).type_is_cloud and host is None:
        host = "unspecified-host"
        logger.warning(
            "setting local storage locations with a single path is deprecated, "
            "use a tuple of (local_root, host) instead"
        )

    # here the storage is registered in the hub
    # hub_record_status="hub-record-created" if a new record is created
    # "hub-record-retrieved" if the storage is in the hub already
    ssettings, hub_record_status = init_storage(
        root=root,
        instance_id=settings.instance._id,
        instance_slug=settings.instance.slug,
        register_hub=settings.instance.is_on_hub,
        prevent_register_hub=not settings.instance.is_on_hub,
        region=host,
    )
    if ssettings._instance_id is None:
        raise ValueError(
            f"Cannot manage storage without write access: {ssettings.root}"
        )
    # here the storage is saved in the instance
    # if any error happens the record in the hub is deleted
    # if it was created earlier and not retrieved
    try:
        register_storage_in_instance(ssettings)
    except Exception as e:
        if hub_record_status == "hub-record-created" and ssettings._uuid is not None:
            delete_storage_record(ssettings)
        raise e

    settings.instance._storage = ssettings
    settings.storage._set_fs_kwargs(**fs_kwargs)
