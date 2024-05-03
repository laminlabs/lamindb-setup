from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any, Optional, get_type_hints
from uuid import UUID

from ._settings_store import (
    InstanceSettingsStore,
    UserSettingsStore,
    current_user_settings_file,
    user_settings_file_email,
    user_settings_file_handle,
)

if TYPE_CHECKING:
    from ._settings_user import UserSettings
    from .upath import UPath


def save_user_settings(settings: UserSettings):
    type_hints = get_type_hints(UserSettingsStore)
    prefix = "lamin_user_"
    save_settings(settings, current_user_settings_file(), type_hints, prefix)
    if settings.email is not None:
        save_settings(
            settings, user_settings_file_email(settings.email), type_hints, prefix
        )
    if settings.handle is not None and settings.handle != "anonymous":
        save_settings(
            settings, user_settings_file_handle(settings.handle), type_hints, prefix
        )


def save_settings(
    settings: Any,
    settings_file: Path,
    type_hints: dict[str, Any],
    prefix: str,
):
    with open(settings_file, "w") as f:
        for store_key, type in type_hints.items():
            if type == Optional[str]:
                type = str
            if type == Optional[bool]:
                type = bool
            if "__" not in store_key:
                if store_key == "storage_root":
                    value = settings.storage.root_as_str
                elif store_key == "storage_region":
                    value = settings.storage.region
                else:
                    if store_key in {"db", "schema_str", "name_", "uuid", "id"}:
                        settings_key = f"_{store_key.rstrip('_')}"
                    else:
                        settings_key = store_key
                    value = getattr(settings, settings_key)
                if value is None:
                    value = "null"
                elif isinstance(value, UUID):
                    value = value.hex
                else:
                    value = type(value)
                f.write(f"{prefix}{store_key}={value}\n")


def save_instance_settings(settings: Any, settings_file: Path):
    type_hints = get_type_hints(InstanceSettingsStore)
    prefix = "lamindb_instance_"
    save_settings(settings, settings_file, type_hints, prefix)


def save_system_storage_settings(
    cache_path: str | Path | UPath | None, settings_file: Path
):
    cache_path = "null" if cache_path is None else cache_path
    if isinstance(cache_path, Path):  # also True for UPath
        cache_path = cache_path.as_posix()
    with open(settings_file, "w") as f:
        f.write(f"lamindb_cache_path={cache_path}")
