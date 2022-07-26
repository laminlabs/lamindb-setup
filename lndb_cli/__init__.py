"""LaminDB command line tool."""

__version__ = "0.2.0"  # denote a pre-release for 0.1.0 with 0.1a1
from . import _check_versions  # noqa
from ._schema import schema  # noqa
from ._settings import InstanceSettings, UserSettings  # noqa
from ._settings_load import (  # noqa
    load_or_create_instance_settings,
    load_or_create_user_settings,
)
