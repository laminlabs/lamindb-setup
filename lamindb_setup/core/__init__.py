"""Setup core library.

Settings
--------

.. autoclass:: SetupSettings

.. autoclass:: UserSettings

.. autoclass:: InstanceSettings

.. autoclass:: StorageSettings

"""

from . import django
from ._deprecated import deprecated  # documented in lamindb.base
from ._docs import doc_args  # documented in lamindb.base
from ._settings import SetupSettings
from ._settings_user import UserSettings
from .canonical_suffix import CanonicalSuffix


def __getattr__(name: str):
    if name == "upath":
        from . import upath

        return upath
    if name == "InstanceSettings":
        from ._settings_instance import InstanceSettings

        return InstanceSettings
    if name == "StorageSettings":
        from ._settings_storage import StorageSettings

        return StorageSettings
    if name == "upload_sqlite_clone":
        from ._clone import upload_sqlite_clone

        return upload_sqlite_clone
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
