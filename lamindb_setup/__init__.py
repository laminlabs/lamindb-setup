"""Setup & configure LaminDB.

Every function in the API has a matching command in the `lamin` CLI.

Typically, you'll want to use the CLI rather than this API.

Guide: :doc:`docs:setup`.

Setup:

.. autosummary::
   :toctree:

   login
   init
   load
   close
   delete

More instance operations:

.. autosummary::
   :toctree:

   info
   set
   migrate
   register

Settings:

.. autosummary::
   :toctree:

   settings

Developer API.

.. autosummary::
   :toctree:

   dev
   django

"""

__version__ = "0.61.2"  # denote a release candidate for 0.1.0 with 0.1rc1

import sys
from os import name as _os_name

from . import dev
from ._check_instance_setup import check_instance_setup as _check_instance_setup  # noqa
from ._close import close  # noqa
from ._delete import delete  # noqa
from ._info import info  # noqa
from ._init_instance import init  # noqa
from ._load_instance import load  # noqa
from ._migrate import migrate
from ._register_instance import register  # noqa
from ._set import set, set_storage  # noqa
from ._settings import settings  # noqa
from ._setup_user import login  # noqa
from ._django import django

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
