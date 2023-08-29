import os
import shutil
from pathlib import Path

from lamin_utils import logger
from pydantic import BaseSettings


def get_settings_dir():
    settings_dir = Path.home() / ".lamin"
    settings_dir.mkdir(parents=True, exist_ok=True)
    # deal with legacy settings directory
    legacy_dir = settings_dir.with_name(".lndb")
    if legacy_dir.exists():
        if not settings_dir.exists():
            legacy_dir.rename(settings_dir)
            logger.info(f"renamed legacy settings dir {legacy_dir} to {settings_dir}")
        else:
            for path in legacy_dir.glob("*"):
                shutil.copy(path, settings_dir)
            logger.info(
                f"copied content of legacy settings dir {legacy_dir} to {settings_dir}."
                f" you can delete {legacy_dir}!"
            )
    return settings_dir


# user_config_dir in appdirs is weird on MacOS!
# hence, let's take home/.lndb
settings_dir = get_settings_dir()


def get_settings_file_name_prefix():
    if "LAMIN_ENV" in os.environ:
        if os.environ["LAMIN_ENV"] == "staging":
            return "staging-"
    return ""


def current_instance_settings_file():
    return settings_dir / f"{get_settings_file_name_prefix()}current_instance.env"


def current_user_settings_file():
    return settings_dir / f"{get_settings_file_name_prefix()}current_user.env"


def instance_settings_file(name: str, owner: str):
    return (
        settings_dir / f"{get_settings_file_name_prefix()}instance--{owner}--{name}.env"
    )


def user_settings_file_email(email: str):
    return settings_dir / f"{get_settings_file_name_prefix()}user--{email}.env"


def user_settings_file_handle(handle: str):
    return settings_dir / f"{get_settings_file_name_prefix()}user--{handle}.env"


class InstanceSettingsStore(BaseSettings):
    owner: str
    name: str
    storage_root: str
    storage_region: str  # should be Optional, but we use types for instantiating
    db: str  # should be Optional, but we use types for instantiating
    schema_str: str  # should be Optional, but we use types for instantiating
    id: str  # should be Optional, but we use types for instantiating

    class Config:
        env_file = ".env"


class UserSettingsStore(BaseSettings):
    email: str
    password: str
    access_token: str
    id: str
    handle: str
    name: str

    class Config:
        env_file = ".env"
