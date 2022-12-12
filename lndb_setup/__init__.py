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

Close a loaded instance context:

.. autosummary::
   :toctree:

   close

Dev API
-------

.. autosummary::
   :toctree:

   UserSettings
   InstanceSettings
   Storage
"""

__version__ = "0.25.0"
import atexit
from os import name as _os_name

from ._schema import schema  # noqa
from ._settings import settings  # noqa
from ._settings_instance import InstanceSettings, Storage  # noqa
from ._settings_user import UserSettings  # noqa
from ._setup_instance import close, init, load  # noqa
from ._setup_user import login, signup  # noqa


# close the database session
@atexit.register
def cleanup_session():
    instance = settings._instance_settings
    if instance is not None and instance._session is not None:
        instance._session.close()


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
