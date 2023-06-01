"""Developer API.

Paths and file system:

.. autosummary::
   :toctree:

   upath

Settings:

.. autosummary::
   :toctree:

   UserSettings
   InstanceSettings
   StorageSettings

"""
from . import upath
from ._deprecated import deprecated
from ._docs import doc_args
from ._settings_instance import InstanceSettings
from ._settings_user import UserSettings
from ._storage import StorageSettings

# below is for backward compat
Storage = StorageSettings
