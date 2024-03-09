from .core.django import setup_django
from .core._settings import settings
from .core._settings_storage import base62


def register(_test: bool = False):
    """Register an instance on the hub."""
    from .core._hub_core import init_instance as init_instance_hub
    from .core._hub_core import init_storage as init_storage_hub
    from ._check_setup import _check_instance_setup

    isettings = settings.instance
    if not _check_instance_setup() and not _test:
        setup_django(isettings)

    ssettings = settings.instance.storage
    if ssettings._uid is None and _test:
        # because django isn't up, we can't get it from the database
        ssettings._uid = base62(8)
    ssettings._uuid = init_storage_hub(ssettings)
    init_instance_hub(isettings)
    isettings._persist()
