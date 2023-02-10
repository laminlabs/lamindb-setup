import os
from typing import Union

from ._settings_instance import InstanceSettings
from ._settings_load import load_instance_settings, load_or_create_user_settings
from ._settings_user import UserSettings


# https://stackoverflow.com/questions/128573/using-property-on-classmethods/64738850#64738850
class classproperty(object):
    def __init__(self, fget):
        self.fget = fget

    def __get__(self, owner_self, owner_cls):
        """Get."""
        return self.fget(owner_cls)


class settings:
    """Settings access.

    - :class:`~lndb.InstanceSettings`
    - :class:`~lndb.UserSettings`
    """

    _user_settings: Union[UserSettings, None] = None
    _instance_settings: Union[InstanceSettings, None] = None

    _user_settings_env: Union[str, None] = None
    _instance_settings_env: Union[str, None] = None

    @classproperty
    def user(cls) -> UserSettings:
        """User-related settings."""
        if (
            cls._user_settings is None
            or cls._user_settings_env != get_env_name()  # noqa
        ):
            cls._user_settings = load_or_create_user_settings()
            cls._user_settings_env = get_env_name()
            if cls._user_settings and cls._user_settings.id is None:
                raise RuntimeError("Need to login, first: lndb login <email>.")
        return cls._user_settings  # type: ignore

    @classproperty
    def instance(cls) -> InstanceSettings:
        """Instance-related settings."""
        if (
            cls._instance_settings is None
            or cls._instance_settings_env != get_env_name()  # noqa
        ):
            cls._instance_settings = load_instance_settings()
            cls._instance_settings_env = get_env_name()
        return cls._instance_settings  # type: ignore

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
