"""LaminDB setup.

Import the package::

   import lndb_setup as lndb

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

__version__ = "0.30.7"  # denote a pre-release for 0.1.0 with 0.1a1

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

# hide the supabase error in a thread on windows
if _os_name == "nt":
    from sys import version_info as _python_version

    if _python_version.minor > 7:
        import threading

        _original_excepthook = threading.excepthook

        def _except_hook(args):
            is_overflow = args.exc_type is OverflowError
            for_timeout = str(args.exc_value) == "timeout value is too large"
            if not (is_overflow and for_timeout):
                _original_excepthook(args)

        threading.excepthook = _except_hook
