from __future__ import annotations

from lamin_utils import logger

from .core._settings import settings
from .core._settings_storage import base62
from .core.django import setup_django


def register(_test: bool = False):
    """Register an instance on the hub."""
    from ._check_setup import _check_instance_setup
    from .core._hub_core import init_instance_hub, init_storage_hub

    logger.warning("note that register() is only for testing purposes")

    isettings = settings.instance
    if not _check_instance_setup() and not _test:
        setup_django(isettings)

    ssettings = settings.instance.storage
    if ssettings._uid is None and _test:
        # because django isn't up, we can't get it from the database
        ssettings._uid = base62(12)
    init_instance_hub(isettings)
    init_storage_hub(ssettings, is_default=True)
    isettings._is_on_hub = True
    isettings._persist()
    if isettings.dialect != "sqlite" and not _test:
        from ._schema_metadata import update_schema_in_hub

        update_schema_in_hub()
