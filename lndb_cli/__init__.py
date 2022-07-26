"""LaminDB command line tool.

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
"""

__version__ = "0.2.0"  # denote a pre-release for 0.1.0 with 0.1a1
from . import _check_versions  # noqa
from ._schema import schema  # noqa
from ._settings import InstanceSettings, UserSettings  # noqa
from ._settings_load import (  # noqa
    load_or_create_instance_settings,
    load_or_create_user_settings,
)
from ._setup_instance import load_instance, setup_instance  # noqa
from ._setup_user import log_in_user, sign_up_user  # noqa
