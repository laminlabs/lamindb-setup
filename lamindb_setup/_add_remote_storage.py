from lamin_utils import logger

from lamindb_setup.core.types import UPathStr

from ._init_instance import register_user_and_storage
from .core._settings import settings
from .core._settings_instance import InstanceSettings
from .core._settings_storage import StorageSettings


def switch_default_storage(root: UPathStr, **fs_kwargs):
    """Add a remote default storage location to a local instance.

    This can be used to selectively share data.

    Args:
        root: `UPathStr` - The new storage root, e.g., an S3 bucket.
        **fs_kwargs: Additional fsspec arguments for cloud root, e.g., profile.

    Example:
    >>> ln.setup.set.storage(
    >>>    "s3://some-bucket",
    >>>     profile="some_profile", # fsspec arg
    >>>     cache_regions=True # fsspec arg for s3
    >>> )

    """
    if settings.instance.dialect == "sqlite":
        logger.error("can't set storage for sqlite instances.")
        return "set-storage-failed"
    ssettings = StorageSettings(root=root)
    new_isettings = InstanceSettings(
        owner=settings.instance.owner,
        name=settings.instance.name,
        storage=ssettings,
        db=settings.instance.db,
        schema=settings.instance._schema_str,
        id=settings.instance.id,
    )

    new_isettings._persist()  # this also updates the settings object
    register_user_and_storage(new_isettings, settings.user)
    # we are not doing this for now because of difficulties to define the right RLS policy  # noqa
    # https://laminlabs.slack.com/archives/C04FPE8V01W/p1687948324601929?thread_ts=1687531921.394119&cid=C04FPE8V01W
    # if settings.instance.is_remote:
    #     init_storage_hub(
    #         root, account_handle=settings.instance.owner  # type: ignore
    #     )

    settings.storage._set_fs_kwargs(**fs_kwargs)
