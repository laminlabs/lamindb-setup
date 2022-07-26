from pathlib import Path
from typing import Union

from cloudpathlib import CloudPath

from ._settings import InstanceSettings, UserSettings
from ._settings_save import save_instance_settings, save_user_settings
from ._settings_store import (
    InstanceSettingsStore,
    UserSettingsStore,
    current_instance_settings_file,
    current_user_settings_file,
    settings_dir,
)


def load_or_create_instance_settings():
    """Return current user settings."""
    if not current_instance_settings_file.exists():
        global InstanceSettings
        return InstanceSettings()
    else:
        settings = load_instance_settings(current_instance_settings_file)
        return settings


def load_instance_settings(instance_settings_file: Path):
    settings_store = InstanceSettingsStore(_env_file=instance_settings_file)
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
    settings_store = UserSettingsStore(_env_file=user_settings_file)
    settings = setup_user_from_store(settings_store)
    return settings


def setup_storage_dir(storage: Union[str, Path, CloudPath]) -> Union[Path, CloudPath]:
    if str(storage).startswith(("s3://", "gs://")):
        storage_dir = CloudPath(storage)
    elif str(storage) == "null":
        return None
    else:
        storage_dir = Path(storage)
        storage_dir.mkdir(parents=True, exist_ok=True)
    return storage_dir


def setup_instance_from_store(store: InstanceSettingsStore) -> InstanceSettings:
    settings = InstanceSettings()
    settings.storage_dir = setup_storage_dir(store.storage_dir)
    settings._dbconfig = store.dbconfig
    settings.schema_modules = store.schema_modules
    return settings


def setup_user_from_store(store: UserSettingsStore) -> UserSettings:
    settings = UserSettings()
    settings.user_email = store.user_email
    settings.user_secret = store.user_secret if store.user_secret != "null" else None
    settings.user_id = store.user_id if store.user_id != "null" else None
    return settings


def switch_instance(instance_name: str):
    InstanceSettings.instance_name
    settings = load_instance_settings(settings_dir / f"{instance_name}.env")
    assert settings.instance_name is not None
    save_instance_settings(settings)


def switch_user(user_email: str):
    settings_file = settings_dir / f"{user_email}.env"
    if settings_file.exists():
        settings = load_user_settings(settings_file)
        assert settings.user_email is not None
    else:
        settings = load_or_create_user_settings()
        settings.user_email = user_email
    save_user_settings(settings)
