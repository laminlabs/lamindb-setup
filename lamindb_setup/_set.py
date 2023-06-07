from pathlib import Path
from typing import Union

from lamin_logger import logger

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
        from lnhub_rest.core.storage._add_storage import add_storage as add_storage_hub

        if settings.instance.dialect == "sqlite":
            logger.error("Can't set storage for sqlite instances.")
            return "set-storage-failed"

        new_isettings = InstanceSettings(
            owner=settings.instance.owner,
            name=settings.instance.name,
            storage_root=root,
            db=settings.instance.db,
            schema=settings.instance._schema_str,
        )

        new_isettings._persist()  # this also updates the settings object
        register_user_and_storage(new_isettings, settings.user)
        if settings.instance.is_remote:
            add_storage_hub(root, account_handle=settings.instance.owner)

        settings.storage._set_fs_kwargs(**fs_kwargs)

        logger.success(f"Set storage {root}")


@deprecated("lamindb_setup.set.storage()")
def set_storage(storage: Union[str, Path, UPath]):
    """Deprecated in favor of `set.storage`."""
    set.storage(storage)
