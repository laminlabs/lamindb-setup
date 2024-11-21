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

Modules & settings:

.. autosummary::
   :toctree:

   settings
   core
   django

"""

__version__ = "0.81.1"  # denote a release candidate for 0.1.0 with 0.1rc1

import os as _os
import sys as _sys

from . import core
from ._check_setup import _check_instance_setup
from ._close import close
from ._connect_instance import connect, load
from ._delete import delete
from ._django import django
from ._entry_points import call_registered_entry_points as _call_registered_entry_points
from ._init_instance import init
from ._migrate import migrate
from ._register_instance import register
from ._setup_user import login, logout
from .core._settings import settings

_TESTING = _os.getenv("LAMIN_TESTING") is not None

# hide the supabase error in a thread on windows
if _os.name == "nt":
    if _sys.version_info.minor > 7:
        import threading

        _original_excepthook = threading.excepthook

        def _except_hook(args):
            is_overflow = args.exc_type is OverflowError
            for_timeout = str(args.exc_value) == "timeout value is too large"
            if not (is_overflow and for_timeout):
                _original_excepthook(args)

        threading.excepthook = _except_hook

# provide a way for other packages to run custom code on import
_call_registered_entry_points("lamindb_setup.on_import")

settings.__doc__ = """Global :class:`~lamindb.setup.core.SetupSettings`."""
