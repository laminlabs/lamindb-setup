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

   signup
   login

Setup instance (`lndb init`, `lndb load`):

.. autosummary::
   :toctree:

   init
   load

Dev API
-------

.. autosummary::
   :toctree:

   UserSettings
   InstanceSettings
   Storage
"""

__version__ = "0.13.2"
# hide the supabase error in a thread on windows
import os

from . import _check_versions  # noqa
from ._schema import schema  # noqa
from ._settings import settings  # noqa
from ._settings_instance import InstanceSettings, Storage  # noqa
from ._settings_user import UserSettings  # noqa
from ._setup_instance import init, load  # noqa
from ._setup_user import login, signup  # noqa

if os.name == "nt":
    import threading

    original_excepthook = threading.excepthook

    def except_hook(args):
        is_overflow = args.exc_type is OverflowError
        for_timeout = str(args.exc_value) == "timeout value is too large"
        if not (is_overflow and for_timeout):
            original_excepthook(args)

    threading.excepthook = except_hook
