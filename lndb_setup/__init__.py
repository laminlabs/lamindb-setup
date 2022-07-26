"""LaminDB setup.

User API
--------

Setup user account (`lndb signup`, `lndb login`):

.. autosummary::
   :toctree:

   sign_up_user
   log_in_user

Setup instance (`lndb init`, `lndb load`):

.. autosummary::
   :toctree:

   setup_instance
   load_instance

Dev API
-------

.. autosummary::
   :toctree:

   UserSettings
   InstanceSettings
"""

__version__ = "0.2.0"  # denote a pre-release for 0.1.0 with 0.1a1
from . import _check_versions  # noqa
from ._schema import schema  # noqa
from ._settings_instance import InstanceSettings  # noqa
from ._settings_load import (  # noqa
    load_or_create_instance_settings,
    load_or_create_user_settings,
)
from ._settings_user import UserSettings  # noqa
from ._setup_instance import load_instance, setup_instance  # noqa
from ._setup_user import log_in_user, sign_up_user  # noqa
