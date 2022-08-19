from pathlib import Path
from typing import Union

from cloudpathlib import CloudPath
from pydantic.error_wrappers import ValidationError

from ._settings_instance import InstanceSettings
from ._settings_store import (
    InstanceSettingsStore,
    UserSettingsStore,
    current_instance_settings_file,
    current_user_settings_file,
)
from ._settings_user import UserSettings


def load_or_create_instance_settings():
    """Return current user settings."""
    if not current_instance_settings_file.exists():
        global InstanceSettings
        return InstanceSettings()
    else:
        settings = load_instance_settings(current_instance_settings_file)
        return settings


def load_instance_settings(instance_settings_file: Path):
    try:
        settings_store = InstanceSettingsStore(_env_file=instance_settings_file)
    except ValidationError:
        raise RuntimeError(
            "Your instance settings file is invalid, please delete"
            f" {instance_settings_file} and init the instance again."
        )
    settings = setup_instance_from_store(settings_store)
    return settings


def load_or_create_user_settings():
    """Return current user settings."""
    if not current_user_settings_file.exists():
        global UserSettings
        return UserSettings()
    else:
        settings = load_user_settings(current_user_settings_file)
        return settings


def load_user_settings(user_settings_file: Path):
    try:
        settings_store = UserSettingsStore(_env_file=user_settings_file)
    except ValidationError:
        raise RuntimeError(
            "Your user settings file is invalid, please delete"
            f" {user_settings_file} and log in again."
        )
    settings = setup_user_from_store(settings_store)
    return settings


def setup_storage_dir(storage: Union[str, Path, CloudPath]) -> Union[Path, CloudPath]:
    if str(storage).startswith(("s3://", "gs://")):
        storage_dir = CloudPath(storage)
    elif str(storage) == "null":
        return None
    else:
        storage_dir = Path(storage).absolute()
        storage_dir.mkdir(parents=True, exist_ok=True)
    return storage_dir


def setup_instance_from_store(store: InstanceSettingsStore) -> InstanceSettings:
    settings = InstanceSettings()
    settings.storage_dir = setup_storage_dir(store.storage_dir)
    settings._dbconfig = store.dbconfig
    settings.schema_modules = store.schema_modules
    settings.storage_region = store.storage_region
    return settings


def setup_user_from_store(store: UserSettingsStore) -> UserSettings:
    settings = UserSettings()
    settings.email = store.email
    settings.password = store.password if store.password != "null" else None
    settings.id = store.id if store.id != "null" else None
    settings.handle = store.handle if store.handle != "null" else None
    return settings
