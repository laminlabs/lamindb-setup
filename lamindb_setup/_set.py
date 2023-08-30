from pathlib import Path
from typing import Union

from lamin_utils import logger

from lamindb_setup.dev.upath import UPath

from ._init_instance import register_user_and_storage
from ._settings import settings
from .dev import deprecated
from .dev._settings_instance import InstanceSettings


class set:
    """Set properties of current instance."""

    @staticmethod
    def storage(root: Union[str, Path, UPath], **fs_kwargs):
        """Set storage.

        Args:
            root: `Union[str, Path, UPath]` - The new storage root, e.g., an S3 bucket.
            **fs_kwargs: Additional fsspec arguments for cloud root, e.g., profile.

        Example:

        >>> ln.setup.set.storage(
        >>>    "s3://some-bucket",
        >>>     profile="some_profile", # fsspec arg
        >>>     cache_regions=True # fsspec arg for s3
        >>> )
        """
        from .dev._hub_utils import get_storage_region

        if settings.instance.dialect == "sqlite":
            logger.error("can't set storage for sqlite instances.")
            return "set-storage-failed"

        new_isettings = InstanceSettings(
            owner=settings.instance.owner,
            name=settings.instance.name,
            storage_root=root,
            storage_region=get_storage_region(root),
            db=settings.instance.db,
            schema=settings.instance._schema_str,
            id=settings.instance._id,
        )

        new_isettings._persist()  # this also updates the settings object
        register_user_and_storage(new_isettings, settings.user)
        # we are not doing this for now because of difficulties to define the right RLS policy  # noqa
        # https://laminlabs.slack.com/archives/C04FPE8V01W/p1687948324601929?thread_ts=1687531921.394119&cid=C04FPE8V01W
        # if settings.instance.is_remote:
        #     add_storage_hub(
        #         root, account_handle=settings.instance.owner  # type: ignore
        #     )

        settings.storage._set_fs_kwargs(**fs_kwargs)

        logger.save(f"set storage {root}")


@deprecated("lamindb_setup.set.storage()")
def set_storage(storage: Union[str, Path, UPath]):
    """Deprecated in favor of `set.storage`."""
    set.storage(storage)
