from pathlib import Path

from typing import Any, Dict, Union, get_type_hints
from uuid import UUID

from ._settings_store import (
    UserSettingsStore,
    current_user_settings_file,
    user_settings_file_email,
    user_settings_file_handle,
)
from ._settings_user import UserSettings
from .upath import UPath


def save_user_settings(settings: UserSettings):
    assert settings.email is not None
    type_hints = get_type_hints(UserSettingsStore)
    save_settings(settings, current_user_settings_file(), type_hints)
    save_settings(settings, user_settings_file_email(settings.email), type_hints)
    if settings.handle is not None:
        save_settings(
            settings, user_settings_file_handle(settings.handle), type_hints
        )  # noqa


def save_settings(
    settings: Any,
    settings_file: Path,
    type_hints: Dict[str, Any],
):
    with open(settings_file, "w") as f:
        for store_key, type in type_hints.items():
            if "__" not in store_key:
                if store_key in {"dbconfig_", "schema_", "name_"}:
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
                f.write(f"lamin_user_{store_key}={value}\n")


def save_instance_settings(settings: Any, settings_file: Path):
    with open(settings_file, "w") as f:
        f.write(f"lamindb_instance_owner={settings.owner}\n")
        f.write(f"lamindb_instance_name={settings.name}\n")
        f.write(f"lamindb_instance_storage_root={str(settings.storage.root)}\n")
        storage_region = (
            settings.storage.region if settings.storage.region is not None else "null"
        )
        f.write(f"lamindb_instance_storage_region={storage_region}\n")
        db = settings._db if settings._db is not None else "null"
        f.write(f"lamindb_instance_db={db}\n")
        schema_str = (
            settings._schema_str if settings._schema_str is not None else "null"
        )
        f.write(f"lamindb_instance_schema_str={schema_str}\n")
        id = settings.id.hex if settings._id is not None else "null"
        f.write(f"lamindb_instance_id={id}\n")


def save_system_storage_settings(
    cache_path: Union[str, Path, UPath, None], settings_file: Path
):
    cache_path = "null" if cache_path is None else cache_path
    if isinstance(cache_path, Path):  # also True for UPath
        cache_path = cache_path.as_posix()
    with open(settings_file, "w") as f:
        f.write(f"lamindb_cache_path={cache_path}")
