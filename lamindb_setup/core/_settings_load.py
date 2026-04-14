from __future__ import annotations

import os
from pathlib import Path
from typing import TYPE_CHECKING
from uuid import UUID

from dotenv import dotenv_values
from lamin_utils import logger

from lamindb_setup.errors import CurrentInstanceNotConfigured, SettingsEnvFileOutdated

from ._settings_instance import InstanceSettings
from ._settings_storage import StorageSettings
from ._settings_store import (
    InstanceSettingsStore,
    UserSettingsStore,
    current_instance_settings_file,
    current_user_settings_file,
    find_local_current_instance_file,
    instance_settings_file,
    platform_user_storage_settings_file,
    system_settings_file,
)
from ._settings_user import UserSettings


def load_cache_path_from_settings(storage_settings: Path | None = None) -> Path | None:
    if storage_settings is None:
        paltform_user_storage_settings = platform_user_storage_settings_file()
        if paltform_user_storage_settings.exists():
            cache_path = dotenv_values(paltform_user_storage_settings).get(
                "lamindb_cache_path", None
            )
        else:
            cache_path = None

        if cache_path in {None, "null", ""}:
            storage_settings = system_settings_file()
        else:
            return Path(cache_path)

    if storage_settings.exists():
        cache_path = dotenv_values(storage_settings).get("lamindb_cache_path", None)
        return Path(cache_path) if cache_path not in {None, "null", ""} else None
    else:
        return None


def _instance_settings_file_from_identifier(identifier: str) -> Path | None:
    from lamindb_setup._connect_instance import get_owner_name_from_identifier

    try:
        owner, name = get_owner_name_from_identifier(identifier.strip())
    except ValueError:
        return None
    settings_file = instance_settings_file(name, owner)
    return settings_file if settings_file.exists() else None


def _resolve_default_instance_file() -> Path | None:
    env_identifier = os.environ.get("LAMIN_CURRENT_INSTANCE")
    if env_identifier is not None:
        env_file = _instance_settings_file_from_identifier(env_identifier)
        if env_file is not None:
            return env_file
    marker_file = find_local_current_instance_file()
    if marker_file is not None:
        marker_settings = _instance_settings_file_from_identifier(
            marker_file.read_text()
        )
        if marker_settings is not None:
            return marker_settings
    fallback = current_instance_settings_file()
    return fallback if fallback.exists() else None


def load_instance_settings(instance_settings_file: Path | None = None):
    if instance_settings_file is None:
        isettings_file = _resolve_default_instance_file()
        if isettings_file is None:
            from ._settings import settings

            isettings = InstanceSettings(
                id=UUID("00000000-0000-0000-0000-000000000000"),
                owner="none",
                name="none",
                storage=None,
                modules=",".join(sorted(settings.modules)),
            )
            return isettings
    else:
        isettings_file = instance_settings_file

    if not isettings_file.exists():
        # this errors only if the file was explicitly provided
        raise CurrentInstanceNotConfigured
    try:
        settings_store = InstanceSettingsStore.from_env_file(
            isettings_file, "lamindb_instance_"
        )
    except (ValueError, KeyError, TypeError) as error:
        raise SettingsEnvFileOutdated(
            f"\n\n{error}\n\nYour instance settings file {isettings_file} is invalid"
            f" (likely outdated), see validation error. Please delete {isettings_file} &"
            " reload (remote) or re-initialize (local) the instance with the same name & storage location."
        ) from error
    isettings = setup_instance_from_store(settings_store)
    return isettings


def load_or_create_user_settings(api_key: str | None = None) -> UserSettings:
    """Return current user settings.

    Args:
        api_key: if provided and there is no current user, perform login and return the user settings.
    """
    current_user_settings = current_user_settings_file()
    if not current_user_settings.exists():
        if api_key is not None:
            from lamindb_setup._setup_user import login

            return login(api_key=api_key)
        else:
            logger.warning("using anonymous user (to identify, call: lamin login)")
        usettings = UserSettings(handle="anonymous", uid="00000000")
        from ._settings_save import save_user_settings

        save_user_settings(usettings)
    else:
        usettings = load_user_settings(current_user_settings)
    return usettings


def load_user_settings(user_settings_file: Path):
    try:
        settings_store = UserSettingsStore.from_env_file(
            user_settings_file, "lamin_user_"
        )
    except (ValueError, KeyError, TypeError) as error:
        msg = (
            "Your user settings file is invalid, please delete"
            f" {user_settings_file} and log in again."
        )
        raise SettingsEnvFileOutdated(msg) from error
    settings = setup_user_from_store(settings_store)
    return settings


def _null_to_value(field, value=None):
    return value if field in (None, "null") else field


def setup_instance_from_store(store: InstanceSettingsStore) -> InstanceSettings:
    ssettings = StorageSettings(
        root=store.storage_root,
        region=_null_to_value(store.storage_region),
    )
    return InstanceSettings(
        id=UUID(store.id),
        owner=store.owner,
        name=store.name,
        storage=ssettings,
        db=_null_to_value(store.db),
        modules=_null_to_value(store.schema_str),
        git_repo=_null_to_value(store.git_repo),
        keep_artifacts_local=store.keep_artifacts_local,  # type: ignore
        api_url=_null_to_value(store.api_url),
        schema_id=None if store.schema_id in {None, "null"} else UUID(store.schema_id),
        fine_grained_access=store.fine_grained_access,
        db_permissions=_null_to_value(store.db_permissions),
        _is_clone=store.is_clone,
    )


def setup_user_from_store(store: UserSettingsStore) -> UserSettings:
    settings = UserSettings()
    settings.email = _null_to_value(store.email)
    settings.password = _null_to_value(store.password)
    settings.access_token = store.access_token
    settings.api_key = _null_to_value(store.api_key)
    settings.uid = store.uid
    settings.handle = _null_to_value(store.handle, value="anonymous")
    settings.name = _null_to_value(store.name)
    settings._uuid = UUID(store.uuid) if store.uuid not in (None, "null") else None
    return settings
