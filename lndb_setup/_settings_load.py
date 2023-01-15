from pathlib import Path
from typing import Optional, Union

from pydantic.error_wrappers import ValidationError

from ._settings_instance import InstanceSettings
from ._settings_store import (
    InstanceSettingsStore,
    UserSettingsStore,
    current_instance_settings_file,
    current_user_settings_file,
)
from ._settings_user import UserSettings
from ._upath_ext import UPath


def load_instance_settings(instance_settings_file: Optional[Path] = None):
    if instance_settings_file is None:
        instance_settings_file = current_instance_settings_file()
    if not instance_settings_file.exists():
        raise RuntimeError("Instance is not setup. Please call `lndb init`.")
    try:
        settings_store = InstanceSettingsStore(_env_file=instance_settings_file)
    except ValidationError:
        raise RuntimeError(
            "Your instance settings file is invalid, please delete"
            f" {instance_settings_file} and init the instance again."
        )
    isettings = setup_instance_from_store(settings_store)
    return isettings


load_or_create_instance_settings = load_instance_settings  # backward compat


def load_or_create_user_settings():
    """Return current user settings."""
    if not current_user_settings_file().exists():
        global UserSettings
        return UserSettings()
    else:
        settings = load_user_settings(current_user_settings_file())
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


def setup_storage_root(storage: Union[str, Path, UPath]) -> Union[Path, UPath]:
    storage_str = str(storage)
    if storage_str.startswith("s3://") or storage_str.startswith("gs://"):
        storage_root = UPath(storage)
    else:  # local path
        storage_root = Path(storage).absolute()
        storage_root.mkdir(parents=True, exist_ok=True)
    return storage_root


def setup_instance_from_store(store: InstanceSettingsStore) -> InstanceSettings:
    return InstanceSettings(
        owner=store.owner,
        name=store.name,
        storage_root=setup_storage_root(store.storage_root),
        url=store.url if store.url != "null" else None,
        _schema=store.schema_,
        storage_region=store.storage_region,
    )


def setup_user_from_store(store: UserSettingsStore) -> UserSettings:
    settings = UserSettings()
    settings.email = store.email
    settings.password = store.password if store.password != "null" else None
    settings.access_token = store.access_token
    settings.id = store.id if store.id != "null" else None
    settings.handle = store.handle if store.handle != "null" else None
    settings.name = store.name if store.name != "null" else None
    return settings
