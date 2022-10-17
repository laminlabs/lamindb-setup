import os
from pathlib import Path

from pydantic import BaseSettings


def get_settings_dir():
    if "LAMIN_BASE_SETTINGS_DIR" in os.environ:
        return Path(os.environ["LAMIN_BASE_SETTINGS_DIR"]) / ".lndb"
    else:
        return Path.home() / ".lndb"


# user_config_dir in appdirs is weird on MacOS!
# hence, let's take home/.lndb
settings_dir = get_settings_dir()
settings_dir.mkdir(parents=True, exist_ok=True)
current_instance_settings_file = settings_dir / "current_instance.env"
current_user_settings_file = settings_dir / "current_user.env"


class InstanceSettingsStore(BaseSettings):
    storage_root: str
    storage_region: str
    dbconfig: str
    schema_modules: str

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


class Connector(BaseSettings):
    url: str
    key: str
