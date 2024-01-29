from .dev.django import setup_django
from ._settings import settings


def register(_test: bool = False):
    """Register an instance on the hub."""
    from .dev._hub_core import init_instance as init_instance_hub
    from ._check_instance_setup import check_instance_setup

    isettings = settings.instance

    if not check_instance_setup() and not _test:
        setup_django(isettings)

    isettings = settings.instance
    init_instance_hub(isettings)
    isettings._persist()
