from __future__ import annotations

import os
from typing import TYPE_CHECKING
from uuid import UUID, uuid4

from dotenv import dotenv_values
from lamin_utils import logger
from pydantic import ValidationError

from ._settings_instance import InstanceSettings
from ._settings_storage import StorageSettings
from ._settings_store import (
    InstanceSettingsStore,
    UserSettingsStore,
    current_instance_settings_file,
    current_user_settings_file,
    system_storage_settings_file,
)
from ._settings_user import UserSettings

if TYPE_CHECKING:
    from pathlib import Path


class SettingsEnvFileOutdated(Exception):
    pass


def load_system_storage_settings(system_storage_settings: Path | None = None) -> dict:
    if system_storage_settings is None:
        system_storage_settings = system_storage_settings_file()

    if system_storage_settings.exists():
        return dotenv_values(system_storage_settings)
    else:
        return {}


def load_instance_settings(instance_settings_file: Path | None = None):
    if instance_settings_file is None:
        instance_settings_file = current_instance_settings_file()
    if not instance_settings_file.exists():
        raise SystemExit("No instance connected! Call `lamin connect` or `lamin init`")
    try:
        settings_store = InstanceSettingsStore(_env_file=instance_settings_file)
    except (ValidationError, TypeError) as error:
        with open(instance_settings_file) as f:
            content = f.read()
        raise SettingsEnvFileOutdated(
            f"\n\n{error}\n\nYour instance settings file with\n\n{content}\nis invalid"
            f" (likely outdated), see validation error. Please delete {instance_settings_file} &"
            " reload (remote) or re-initialize (local) the instance with the same name & storage location."
        ) from error
    isettings = setup_instance_from_store(settings_store)
    return isettings


def load_or_create_user_settings(api_key: str | None = None) -> UserSettings:
    """Return current user settings.

    Args:
        api_key: if provided and there is no current user,
            perform login and return the user settings.
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
        settings_store = UserSettingsStore(_env_file=user_settings_file)
    except (ValidationError, TypeError) as error:
        msg = (
            "Your user settings file is invalid, please delete"
            f" {user_settings_file} and log in again."
        )
        print(msg)
        raise SettingsEnvFileOutdated(msg) from error
    settings = setup_user_from_store(settings_store)
    return settings


def _null_to_value(field, value=None):
    return field if field != "null" else value


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
        schema_id=None if store.schema_id == "null" else UUID(store.schema_id),
        fine_grained_access=store.fine_grained_access,
        db_permissions=_null_to_value(store.db_permissions),
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
    settings._uuid = UUID(store.uuid) if store.uuid != "null" else None
    return settings
