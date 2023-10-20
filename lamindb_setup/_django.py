from typing import Optional
from ._settings import settings
from .dev.django import setup_django


def django(command: str, package_name: Optional[str] = None, **kwargs):
    """Manage migrations.

    Examples:

    >>> import lamindb as ln
    >>> ln.setup.django("sqlsequencereset lnschema_core")

    """
    from django.core.management import call_command

    setup_django(settings.instance)
    if package_name is not None:
        args = [package_name]
    else:
        args = []
    call_command(command, *args, **kwargs)
