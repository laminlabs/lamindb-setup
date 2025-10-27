"""Core setup library.

Settings:

.. autosummary::
   :toctree:

   SetupSettings
   UserSettings
   InstanceSettings
   StorageSettings

"""

from . import django, upath
from ._clone import connect_local_sqlite, init_local_sqlite
from ._deprecated import deprecated
from ._docs import doc_args
from ._settings import SetupSettings
from ._settings_instance import InstanceSettings
from ._settings_storage import StorageSettings
from ._settings_user import UserSettings
