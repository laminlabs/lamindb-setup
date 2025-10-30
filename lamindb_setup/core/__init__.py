"""Core setup library.

.. autoclass:: SetupSettings
.. autoclass:: UserSettings
.. autoclass:: InstanceSettings
.. autoclass:: StorageSettings

"""

from . import django, upath
from ._clone import (
    connect_local_sqlite,
    connect_remote_sqlite,
    init_local_sqlite,
    upload_sqlite_clone,
)
from ._deprecated import deprecated  # documented in lamindb.base
from ._docs import doc_args  # documented in lamindb.base
from ._settings import SetupSettings
from ._settings_instance import InstanceSettings
from ._settings_storage import StorageSettings
from ._settings_user import UserSettings
