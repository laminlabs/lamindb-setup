from pathlib import Path
from typing import Any, Dict, get_type_hints

from ._settings_store import (
    UserSettingsStore,
    get_current_user_settings_file,
    get_user_settings_file_from_email,
    get_user_settings_file_from_handle,
)
from ._settings_user import UserSettings


def save_user_settings(settings: UserSettings):
    assert settings.email is not None
    type_hints = get_type_hints(UserSettingsStore)
    save_settings(settings, get_current_user_settings_file(), type_hints)
    save_settings(
        settings, get_user_settings_file_from_email(settings.email), type_hints
    )
    if settings.handle is not None:
        save_settings(
            settings, get_user_settings_file_from_handle(settings.handle), type_hints
        )  # noqa


def save_settings(
    settings: Any,
    settings_file: Path,
    type_hints: Dict[str, Any],
):
    with open(settings_file, "w") as f:
        for store_key, type in type_hints.items():
            if "__" not in store_key:
                if store_key in {"dbconfig_", "schema_"}:
                    settings_key = f"_{store_key.rstrip('_')}"
                else:
                    settings_key = store_key
                value = getattr(settings, settings_key)
                if value is None:
                    value = "null"
                else:
                    value = type(value)
                f.write(f"{store_key}={value}\n")
