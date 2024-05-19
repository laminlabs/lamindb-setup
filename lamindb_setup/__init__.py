"""Setup & configure LaminDB.

Many functions in this "setup API" have a matching command in the :doc:`docs:cli` CLI.

Guide: :doc:`docs:setup`.

Basic operations:

.. autosummary::
   :toctree:

   login
   logout
   init
   close
   delete

Instance operations:

.. autosummary::
   :toctree:

   migrate
   register

Modules & settings:

.. autosummary::
   :toctree:

   settings
   core
   django

"""

__version__ = "0.72.0"  # denote a release candidate for 0.1.0 with 0.1rc1

import sys
from os import name as _os_name

from . import core
from ._check_setup import _check_instance_setup
from ._close import close
from ._connect_instance import connect, load
from ._delete import delete
from ._django import django
from ._init_instance import init
from ._migrate import migrate
from ._register_instance import register
from ._setup_user import login, logout
from .core._settings import settings

dev = core  # backward compat
_TESTING = False  # used in lamindb tests

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

settings.__doc__ = """Global :class:`~lamindb.setup.core.SetupSettings`."""
