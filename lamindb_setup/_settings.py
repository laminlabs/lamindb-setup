import os
from typing import Union

from lamindb_setup.dev import InstanceSettings, StorageSettings, UserSettings
from lamindb_setup.dev._settings_load import (
    load_instance_settings,
    load_or_create_user_settings,
)


# https://stackoverflow.com/questions/128573/using-property-on-classmethods/64738850#64738850
class classproperty(object):
    def __init__(self, fget):
        self.fget = fget

    def __get__(self, owner_self, owner_cls):
        """Get."""
        return self.fget(owner_cls)


class settings:
    """Settings.

    - :class:`~lamindb_setup.dev.InstanceSettings`
    - :class:`~lamindb_setup.dev.StorageSettings`
    - :class:`~lamindb_setup.dev.UserSettings`
    """

    _user_settings: Union[UserSettings, None] = None
    _instance_settings: Union[InstanceSettings, None] = None

    _user_settings_env: Union[str, None] = None
    _instance_settings_env: Union[str, None] = None

    @classproperty
    def user(cls) -> UserSettings:
        """User."""
        if (
            cls._user_settings is None
            or cls._user_settings_env != get_env_name()  # noqa
        ):
            cls._user_settings = load_or_create_user_settings()
            cls._user_settings_env = get_env_name()
            if cls._user_settings and cls._user_settings.uid is None:
                raise RuntimeError("Need to login, first: lamin login <email>")
        return cls._user_settings  # type: ignore

    @classproperty
    def instance(cls) -> InstanceSettings:
        """Instance."""
        if (
            cls._instance_settings is None
            or cls._instance_settings_env != get_env_name()  # noqa
        ):
            cls._instance_settings = load_instance_settings()
            cls._instance_settings_env = get_env_name()
        return cls._instance_settings  # type: ignore

    @classproperty
    def storage(cls) -> StorageSettings:
        """Storage."""
        return cls.instance.storage

    @classproperty
    def _instance_exists(cls):
        try:
            cls.instance
            return True
        except RuntimeError:
            return False


def get_env_name():
    if "LAMIN_ENV" in os.environ:
        return os.environ["LAMIN_ENV"]
    else:
        return "prod"
