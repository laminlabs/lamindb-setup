"""LaminDB setup.

Every function in the API below matches a command in the `lamin` CLI.

Setup user account:

.. autosummary::
   :toctree:

   signup
   login

Setup instance:

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
   set
   register

Manage creating and testing migrations (deployment is automatic):

.. autosummary::
   :toctree:

   migrate

Settings:

.. autosummary::
   :toctree:

   settings

Developer API.

.. autosummary::
   :toctree:

   dev
"""

__version__ = "0.44.1"  # denote a release candidate for 0.1.0 with 0.1rc1

import sys
from os import name as _os_name

from . import _check_versions  # noqa
from . import dev, test
from ._close import close  # noqa
from ._delete import delete  # noqa
from ._info import info  # noqa
from ._init_instance import init  # noqa
from ._load_instance import load  # noqa
from ._migrate import migrate
from ._register_instance import register  # noqa
from ._schema import schema  # noqa
from ._set import set, set_storage  # noqa
from ._settings import settings  # noqa
from ._setup_user import login, signup  # noqa


# unlock and clear even if an uncaught exception happens
def _clear_on_exception(typ, value, traceback):
    from .dev._exclusion import _locker

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
