from __future__ import annotations

"""Core library.

Settings:

.. autosummary::
   :toctree:

   UserSettings
   InstanceSettings
   StorageSettings

"""
from . import django, types, upath
from ._deprecated import deprecated
from ._docs import doc_args
from ._settings_instance import InstanceSettings
from ._settings_storage import StorageSettings
from ._settings_user import UserSettings
