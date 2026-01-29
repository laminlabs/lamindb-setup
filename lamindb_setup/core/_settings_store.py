from __future__ import annotations

import os
from dataclasses import MISSING, dataclass, fields
from pathlib import Path
from typing import Any, get_args, get_type_hints

from dotenv import dotenv_values
from lamin_utils import logger
from platformdirs import site_config_dir

if "LAMIN_SETTINGS_DIR" in os.environ:
    # Needed for AWS Lambda, as only tmp/ has write access
    settings_dir = Path(f"{os.environ['LAMIN_SETTINGS_DIR']}/.lamin")
else:
    settings_dir = Path.home() / ".lamin"

try:
    settings_dir.mkdir(parents=True, exist_ok=True)
except Exception as e:
    logger.warning(f"Failed to create lamin settings directory at {settings_dir}: {e}")

system_settings_dir = Path(site_config_dir(appname="lamindb", appauthor="laminlabs"))


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


def platform_user_storage_settings_file():
    return settings_dir / "storage.env"


def system_settings_file():
    return system_settings_dir / "system.env"


def _load_env_to_kwargs(
    cls: type,
    path: Path | str,
    prefix: str,
) -> dict[str, Any]:
    path = Path(path) if isinstance(path, str) else path
    raw = dotenv_values(path)
    type_hints = get_type_hints(cls)
    flds = {f.name: f for f in fields(cls)}
    optional = {
        n
        for n, f in flds.items()
        if f.default is not MISSING or f.default_factory is not MISSING
    }

    kwargs: dict[str, Any] = {}
    for store_key in type_hints:
        if store_key.startswith("__"):
            continue
        env_key = f"{prefix}{store_key}"
        has_key = env_key in raw
        raw_val = raw.get(env_key) if has_key else None
        is_opt = store_key in optional

        if not has_key:
            if not is_opt:
                raise ValueError(f"Missing required key {env_key!r} in env file {path}")
            kwargs[store_key] = None
            continue
        if raw_val is None or raw_val == "" or raw_val == "null":
            kwargs[store_key] = None
            continue
        type_ = type_hints[store_key]
        args = get_args(type_) or ()
        if type_ is bool:
            kwargs[store_key] = raw_val.lower() in ("true", "1", "yes")
        elif type(None) in args:
            non_none = next((a for a in args if a is not type(None)), type_)
            if non_none is bool:
                kwargs[store_key] = raw_val.lower() in ("true", "1", "yes")
            else:
                kwargs[store_key] = raw_val
        else:
            kwargs[store_key] = raw_val
    return kwargs


@dataclass
class InstanceSettingsStore:
    # Required (no default) — must come first
    owner: str
    name: str
    storage_root: str
    storage_region: str | None
    db: str | None
    schema_str: str | None
    id: str
    git_repo: str | None
    keep_artifacts_local: bool | None
    # Optional
    api_url: str | None = None
    schema_id: str | None = None
    fine_grained_access: bool = False
    db_permissions: str | None = None
    is_clone: bool = False

    @classmethod
    def from_env_file(cls, path: Path | str, prefix: str) -> InstanceSettingsStore:
        kwargs = _load_env_to_kwargs(cls, path, prefix)
        return cls(**kwargs)


@dataclass
class UserSettingsStore:
    # Required (no default)
    email: str
    password: str
    access_token: str
    uid: str
    uuid: str
    handle: str
    name: str
    # Optional
    api_key: str | None = None

    @classmethod
    def from_env_file(cls, path: Path | str, prefix: str) -> UserSettingsStore:
        kwargs = _load_env_to_kwargs(cls, path, prefix)
        return cls(**kwargs)


@dataclass
class Connector:
    url: str
    key: str

    @classmethod
    def from_env_file(cls, path: Path | str, prefix: str) -> Connector:
        kwargs = _load_env_to_kwargs(cls, path, prefix)
        return cls(**kwargs)
