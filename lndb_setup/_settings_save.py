import os
from pathlib import Path
from typing import Any, Dict, get_type_hints

from ._settings_store import UserSettingsStore, current_user_settings_file, settings_dir
from ._settings_user import UserSettings


def get_settings_file_name_prefix():
    if "LAMIN_ENV" in os.environ:
        if os.environ["LAMIN_ENV"] == "dev":
            return "dev-"
        elif os.environ["LAMIN_ENV"] == "test":
            return "test-"
        elif os.environ["LAMIN_ENV"] == "staging":
            return "staging-"
    return ""


def save_user_settings(settings: UserSettings):
    assert settings.email is not None
    type_hints = get_type_hints(UserSettingsStore)
    prefix = get_settings_file_name_prefix()
    save_settings(settings, current_user_settings_file, type_hints)
    save_settings(
        settings,
        settings_dir / f"{prefix}user-{settings.email}.env",
        type_hints,
    )
    if settings.handle is not None:
        save_settings(
            settings,
            settings_dir / f"{prefix}user-{settings.handle}.env",
            type_hints,
        )  # noqa


def save_settings(
    settings: Any,
    settings_file: Path,
    type_hints: Dict[str, Any],
):
    with open(settings_file, "w") as f:
        for store_key, type in type_hints.items():
            if "__" not in store_key:
                if store_key in {"dbconfig_", "schema_"}:
                    settings_key = f"_{store_key.rstrip('_')}"
                else:
                    settings_key = store_key
                value = getattr(settings, settings_key)
                if value is None:
                    value = "null"
                else:
                    value = type(value)
                f.write(f"{store_key}={value}\n")
