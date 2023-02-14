"""LaminDB setup.

Import the package::

   import lndb as lndb

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

More instance operations:

.. autosummary::
   :toctree:

   close
   delete
   info
   set_storage

Manage creating and testing migrations (deployment is automatic):

.. autosummary::
   :toctree:

   migrate


Dev API
-------

.. autosummary::
   :toctree:

   UserSettings
   InstanceSettings
   Storage
"""

__version__ = "0.32.4"  # denote a pre-release for 0.1.0 with 0.1rc1

import sys
from os import name as _os_name

from . import _check_versions  # noqa
from ._close import close  # noqa
from ._delete import delete  # noqa
from ._info import info  # noqa
from ._init_instance import init  # noqa
from ._load_instance import load  # noqa
from ._migrations import migrate
from ._schema import schema  # noqa
from ._set import set_storage  # noqa
from ._settings import settings  # noqa
from ._settings_instance import InstanceSettings, Storage  # noqa
from ._settings_user import UserSettings  # noqa
from ._setup_user import login, signup  # noqa


# unlock and clear even if an uncaught exception happens
def _clear_on_exception(typ, value, traceback):
    from ._exclusion import _locker

    if _locker is not None:
        try:
            _locker._clear()
        except:
            pass
    sys.__excepthook__(typ, value, traceback)


sys.excepthook = _clear_on_exception

# hide the supabase error in a thread on windows
if _os_name == "nt":
    if sys.version_info.minor > 7:
        import threading

        _original_excepthook = threading.excepthook

        def _except_hook(args):
            is_overflow = args.exc_type is OverflowError
            for_timeout = str(args.exc_value) == "timeout value is too large"
            if not (is_overflow and for_timeout):
                _original_excepthook(args)

        threading.excepthook = _except_hook
