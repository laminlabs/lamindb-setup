from pathlib import Path
from typing import Union

from lamin_logger import logger
from lnhub_rest.core.storage._add_storage import add_storage as add_storage_hub

from lndb.dev.upath import UPath

from ._init_instance import register
from ._settings import settings
from .dev import deprecated
from .dev._settings_instance import InstanceSettings


class set:
    """Set properties of current instance."""

    @staticmethod
    def storage(root: Union[str, Path, UPath]):
        """Set storage."""
        if settings.instance.owner != settings.user.handle:
            logger.error("Can only set storage if current user is instance owner.")
            return "only-owner-can-set-storage"

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

        new_isettings._persist()
        register(new_isettings, settings.user)
        if settings.instance.is_remote:
            add_storage_hub(root, account_handle=settings.instance.owner)

        logger.info(f"Set storage {root}")


@deprecated("lndb.set.storage()")
def set_storage(storage: Union[str, Path, UPath]):
    """Deprecated in favor of `set.storage`."""
    set.storage(storage)
