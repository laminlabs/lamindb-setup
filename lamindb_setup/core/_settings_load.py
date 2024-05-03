from __future__ import annotations

from typing import TYPE_CHECKING, Optional
from uuid import UUID, uuid4

from lamin_utils import logger
from pydantic.error_wrappers import ValidationError

from ._settings_instance import InstanceSettings
from ._settings_storage import StorageSettings
from ._settings_store import (
    InstanceSettingsStore,
    UserSettingsStore,
    current_instance_settings_file,
    current_user_settings_file,
)
from ._settings_user import UserSettings

if TYPE_CHECKING:
    from pathlib import Path


class SettingsEnvFileOutdated(Exception):
    pass


def load_instance_settings(instance_settings_file: Path | None = None):
    if instance_settings_file is None:
        instance_settings_file = current_instance_settings_file()
    if not instance_settings_file.exists():
        raise SystemExit("No instance is loaded! Call `lamin init` or `lamin load`")
    try:
        settings_store = InstanceSettingsStore(_env_file=instance_settings_file)
    except (ValidationError, TypeError) as error:
        with open(instance_settings_file) as f:
            content = f.read()
        raise SettingsEnvFileOutdated(
            f"\n\n{error}\n\nYour instance settings file with\n\n{content}\nis invalid"
            f" (likely outdated), please delete {instance_settings_file} &"
            " re-initialize (local) or re-connect to the instance (remote)"
        ) from error
    if settings_store.id == "null":
        raise ValueError(
            "Your instance._id is undefined, please either load your instance from the"
            f" hub or update {instance_settings_file} with a new id: {uuid4().hex}"
        )
    isettings = setup_instance_from_store(settings_store)
    return isettings


def load_or_create_user_settings() -> UserSettings:
    """Return current user settings."""
    current_user_settings = current_user_settings_file()
    if not current_user_settings.exists():
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


def setup_instance_from_store(store: InstanceSettingsStore) -> InstanceSettings:
    ssettings = StorageSettings(
        root=store.storage_root,
        region=store.storage_region if store.storage_region != "null" else None,
    )
    return InstanceSettings(
        id=UUID(store.id),
        owner=store.owner,
        name=store.name,
        storage=ssettings,
        db=store.db if store.db != "null" else None,  # type: ignore
        schema=store.schema_str if store.schema_str != "null" else None,
        git_repo=store.git_repo if store.git_repo != "null" else None,
        keep_artifacts_local=store.keep_artifacts_local,  # type: ignore
    )


def setup_user_from_store(store: UserSettingsStore) -> UserSettings:
    settings = UserSettings()
    settings.email = store.email
    settings.password = store.password if store.password != "null" else None
    settings.access_token = store.access_token
    settings.uid = store.uid
    settings.handle = store.handle if store.handle != "null" else "anonymous"
    settings.name = store.name if store.name != "null" else None
    settings._uuid = UUID(store.uuid) if store.uuid != "null" else None
    return settings
