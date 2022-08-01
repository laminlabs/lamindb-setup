from pathlib import Path
from typing import Any, Dict, Union, get_type_hints

from ._settings_instance import InstanceSettings
from ._settings_store import (
    InstanceSettingsStore,
    UserSettingsStore,
    current_instance_settings_file,
    current_user_settings_file,
    settings_dir,
)
from ._settings_user import UserSettings


def save_instance_settings(settings: InstanceSettings):
    assert settings.name is not None
    type_hints = get_type_hints(InstanceSettingsStore)
    save_settings(settings, current_instance_settings_file, type_hints)
    save_settings(
        settings, settings_dir / f"instance-{settings.name}.env", type_hints
    )  # noqa


def save_user_settings(settings: UserSettings):
    assert settings.email is not None
    type_hints = get_type_hints(UserSettingsStore)
    save_settings(settings, current_user_settings_file, type_hints)
    save_settings(settings, settings_dir / f"user-{settings.email}.env", type_hints)
    if settings.handle is not None:
        save_settings(
            settings, settings_dir / f"user-{settings.handle}.env", type_hints
        )  # noqa


def save_settings(
    settings: Union[InstanceSettings, UserSettings],
    settings_file: Path,
    type_hints: Dict[str, Any],
):
    with open(settings_file, "w") as f:
        for key, type in type_hints.items():
            settings_key = f"_{key}" if key == "dbconfig" else key
            value = getattr(settings, settings_key)
            if value is None:
                value = "null"
            else:
                value = type(value)
            f.write(f"{key}={value}\n")
