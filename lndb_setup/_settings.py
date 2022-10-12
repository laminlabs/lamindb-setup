from typing import Union

from ._settings_instance import InstanceSettings
from ._settings_load import (
    load_instance_settings_from_parameters,
    load_or_create_instance_settings,
    load_or_create_user_settings,
)
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

    def instance_from_params(
        cls, storage_root: str, storage_region: str, dbconfig: str, schema_modules: str
    ) -> InstanceSettings:
        """Instance-related settings."""
        instance_settings = load_instance_settings_from_parameters(
            storage_root, storage_region, dbconfig, schema_modules
        )
        return instance_settings
