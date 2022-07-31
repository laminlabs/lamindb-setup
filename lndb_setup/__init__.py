"""LaminDB setup.

User API
--------

Settings:

.. autosummary::
   :toctree:

   settings

Setup user account (`lndb signup`, `lndb login`):

.. autosummary::
   :toctree:

   sign_up_user
   log_in_user

Setup instance (`lndb init`, `lndb load`):

.. autosummary::
   :toctree:

   init_instance
   load_instance

Dev API
-------

.. autosummary::
   :toctree:

   UserSettings
   InstanceSettings
   Storage
"""

__version__ = "0.3.2"  # denote a pre-release for 0.1.0 with 0.1a1
from . import _check_versions  # noqa
from ._schema import schema  # noqa
from ._settings import settings  # noqa
from ._settings_instance import InstanceSettings, Storage  # noqa
from ._settings_user import UserSettings  # noqa
from ._setup_instance import init_instance, load_instance  # noqa
from ._setup_user import log_in_user, sign_up_user  # noqa
