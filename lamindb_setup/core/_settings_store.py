import os
from pathlib import Path
from typing import Optional

from pydantic_settings import BaseSettings, SettingsConfigDict

if "LAMIN_SETTINGS_DIR" in os.environ:
    # Needed when running with AWS Lambda, as only tmp/ directory has a write access
    settings_dir = Path(f"{os.environ['LAMIN_SETTINGS_DIR']}/.lamin")
else:
    # user_config_dir in appdirs is weird on MacOS!
    # hence, let's take home/.lamin
    settings_dir = Path.home() / ".lamin"

settings_dir.mkdir(parents=True, exist_ok=True)


def get_settings_file_name_prefix():
    if "LAMIN_ENV" in os.environ:
        if os.environ["LAMIN_ENV"] != "prod":
            return f"{os.environ['LAMIN_ENV']}--"
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


def system_storage_settings_file():
    return settings_dir / "storage.env"


class InstanceSettingsStore(BaseSettings):
    api_url: Optional[str] = None
    owner: str
    name: str
    storage_root: str
    storage_region: Optional[str]  # take old type annotations here because pydantic
    db: Optional[str]  # doesn't like new types on 3.9 even with future annotations
    schema_str: Optional[str]
    schema_id: Optional[str] = None
    fine_grained_access: bool = False
    db_permissions: Optional[str] = None
    id: str
    git_repo: Optional[str]
    keep_artifacts_local: Optional[bool]
    model_config = SettingsConfigDict(env_prefix="lamindb_instance_", env_file=".env")


class UserSettingsStore(BaseSettings):
    email: str
    password: str
    access_token: str
    api_key: Optional[str] = None
    uid: str
    uuid: str
    handle: str
    name: str
    model_config = SettingsConfigDict(env_prefix="lamin_user_", env_file=".env")
