import os
from typing import Union

from ._settings_instance import InstanceSettings
from ._settings_load import (load_or_create_instance_settings,
                             load_or_create_user_settings)
from ._settings_user import UserSettings


# https://stackoverflow.com/questions/128573/using-property-on-classmethods/64738850#64738850
class classproperty(object):
    def __init__(self, fget):
        self.fget = fget

    def __get__(self, owner_self, owner_cls):
        """Get."""
        return self.fget(owner_cls)


class settings_from_env:
    """Settings access.

    - :class:`~lndb_setup.InstanceSettings`
    - :class:`~lndb_setup.UserSettings`
    """

    _user_settings: Union[UserSettings, None] = None
    _instance_settings: Union[InstanceSettings, None] = None

    @classproperty
    def user(cls) -> UserSettings:
        """User-related settings."""
        if cls._user_settings is None:
            cls._user_settings = load_or_create_user_settings()
        return cls._user_settings  # type: ignore

    @classproperty
    def instance(cls) -> InstanceSettings:
        """Instance-related settings."""
        if cls._instance_settings is None:
            cls._instance_settings = load_or_create_instance_settings()
        return cls._instance_settings  # type: ignore


class settings_from_hub:

    @classproperty
    def user(cls) -> UserSettings:
        """User-related settings."""
        pass

    @classproperty
    def instance(cls, instance_id: str) -> InstanceSettings:
        """Instance-related settings."""
        pass


class SettingManager:

    def __init__(self, instance_id: str = None) -> None:
        self.setting_source = os.environ['LAMIN_SETTING_SOURCE']
        self.instance_id = instance_id

    @classproperty
    def user(self) -> UserSettings:
        if self.setting_source == 'ENV':
            return settings_from_env.user()
        elif self.setting_source == 'HUB':
            return settings_from_hub.user()
        else:
            raise Exception

    @classproperty
    def instance(self) -> InstanceSettings:
        if self.setting_source == 'ENV':
            return settings_from_env.instance()
        elif self.setting_source == 'HUB' and self.instance_id:
            return settings_from_hub.instance(self.instance_id)
        else:
            raise Exception
