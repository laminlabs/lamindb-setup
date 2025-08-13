"""Errors.

.. autosummary::
   :toctree: .

   CurrentInstanceNotConfigured
   InstanceNotSetupError
   ModuleWasntConfigured
   StorageAlreadyManaged
   StorageNotEmpty
   InstanceLockedException
   SettingsEnvFileOutdated
   CannotSwitchDefaultInstance

"""

from __future__ import annotations

import click


class DefaultMessageException(Exception):
    default_message: str | None = None

    def __init__(self, message: str | None = None):
        if message is None:
            message = self.default_message
        super().__init__(message)


# TODO: remove this exception sooner or later because we don't have a need for it anymore
class InstanceNotSetupError(DefaultMessageException):
    default_message = """\
To use lamindb, you need to connect to an instance.

Connect to an instance: `ln.connect()`. Init an instance: `ln.setup.init()`.

If you used the CLI to set up lamindb in a notebook, restart the Python session.
"""


class CurrentInstanceNotConfigured(DefaultMessageException):
    default_message = """\
No instance is connected! Call
- CLI:     lamin connect / lamin init
- Python:  ln.connect()  / ln.setup.init()
- R:       ln$connect()  / ln$setup$init()"""


MODULE_WASNT_CONFIGURED_MESSAGE_TEMPLATE = (
    "'{}' wasn't configured for this instance -- "
    "if you want it, go to your instance settings page and add it under 'schema modules' (or ask an admin to do so)"
)


class ModuleWasntConfigured(Exception):
    pass


class StorageAlreadyManaged(Exception):
    pass


class StorageNotEmpty(click.ClickException):
    def show(self, file=None):
        pass


# raise if a cloud SQLite instance is already locked
# ignored by unlock_cloud_sqlite_upon_exception
class InstanceLockedException(Exception):
    pass


class SettingsEnvFileOutdated(Exception):
    pass


class CannotSwitchDefaultInstance(Exception):
    pass
