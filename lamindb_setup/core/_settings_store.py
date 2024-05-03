import os
from pathlib import Path
from typing import Optional

from pydantic import BaseSettings

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
    owner: str
    name: str
    storage_root: str
    storage_region: Optional[str]  # take old type annotations here because pydantic
    db: Optional[str]  # doesn't like new types on 3.9 even with future annotations
    schema_str: Optional[str]
    id: str
    git_repo: Optional[str]
    keep_artifacts_local: Optional[bool]

    class Config:
        env_prefix = "lamindb_instance_"
        env_file = ".env"


class UserSettingsStore(BaseSettings):
    email: str
    password: str
    access_token: str
    uid: str
    uuid: str
    handle: str
    name: str

    class Config:
        env_prefix = "lamin_user_"
        env_file = ".env"
