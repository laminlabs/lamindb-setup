from __future__ import annotations

from typing import TYPE_CHECKING, Literal

from lamin_utils import logger

from ._init_instance import register_storage_in_instance
from .core._hub_core import delete_storage_record
from .core._hub_core import init_storage as init_storage_hub
from .core._settings import settings
from .core._settings_storage import init_storage

if TYPE_CHECKING:
    from lamindb_setup.core.types import UPathStr


def set_managed_storage(root: UPathStr, **fs_kwargs):
    """Add or switch to another managed storage location.

    Args:
        root: `UPathStr` - The new storage root, e.g., an S3 bucket.
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

    ssettings = init_storage(
        root=root, instance_id=settings.instance._id, prevent_register_hub=True
    )
    if ssettings._instance_id is None:
        raise ValueError(
            f"Cannot manage storage without write access: {ssettings.root}"
        )

    # this stores the result of init_storage_hub
    hub_record_status: Literal["hub_record_retrieved", "hub_record_created"] | None = (
        None
    )
    if settings.instance.is_on_hub:
        hub_record_status = init_storage_hub(ssettings, auto_populate_instance=True)
    try:
        register_storage_in_instance(ssettings)
    except Exception as e:
        if hub_record_status == "hub_record_created":
            delete_storage_record(ssettings._uuid)  # type: ignore
        raise e

    settings.instance._storage = ssettings
    settings.instance._persist()  # this also updates the settings object
    settings.storage._set_fs_kwargs(**fs_kwargs)
