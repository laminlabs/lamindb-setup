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
from ._testdb import setup_local_test_postgres, setup_local_test_sqlite_file

# below is for backward compat
Storage = StorageSettings
