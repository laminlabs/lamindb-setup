from __future__ import annotations

import os
import sys
from typing import TYPE_CHECKING

from appdirs import AppDirs

from ._settings_load import (
    load_instance_settings,
    load_or_create_user_settings,
    load_system_storage_settings,
)
from ._settings_store import current_instance_settings_file, settings_dir
from .upath import LocalPathClasses, UPath

if TYPE_CHECKING:
    from pathlib import Path

    from lamindb_setup.core import InstanceSettings, StorageSettings, UserSettings

    from .types import UPathStr


DEFAULT_CACHE_DIR = UPath(AppDirs("lamindb", "laminlabs").user_cache_dir)


def _process_cache_path(cache_path: UPathStr | None):
    if cache_path is None or cache_path == "null":
        return None
    cache_dir = UPath(cache_path)
    if not isinstance(cache_dir, LocalPathClasses):
        raise ValueError("cache dir should be a local path.")
    if cache_dir.exists() and not cache_dir.is_dir():
        raise ValueError("cache dir should be a directory.")
    return cache_dir


class SetupSettings:
    """Setup settings."""

    _using_key: str | None = None  # set through lamindb.settings

    _user_settings: UserSettings | None = None
    _instance_settings: InstanceSettings | None = None

    _user_settings_env: str | None = None
    _instance_settings_env: str | None = None

    _auto_connect_path: Path = settings_dir / "auto_connect"
    _private_django_api_path: Path = settings_dir / "private_django_api"

    _cache_dir: Path | None = None

    @property
    def _instance_settings_path(self) -> Path:
        return current_instance_settings_file()

    @property
    def settings_dir(self) -> Path:
        """The directory that holds locally persisted settings."""
        return settings_dir

    @property
    def auto_connect(self) -> bool:
        """Auto-connect to current instance upon `import lamindb`.

        Upon installing `lamindb`, this setting is `False`.

        Upon calling `lamin init` or `lamin connect` on the CLI, this setting is switched to `True`.

        `ln.connect()` doesn't change the value of this setting.

        You can manually change this setting

        - in Python: `ln.setup.settings.auto_connect = True/False`
        - via the CLI: `lamin settings set auto-connect true/false`
        """
        return self._auto_connect_path.exists()

    @auto_connect.setter
    def auto_connect(self, value: bool) -> None:
        if value:
            self._auto_connect_path.touch()
        else:
            self._auto_connect_path.unlink(missing_ok=True)

    @property
    def private_django_api(self) -> bool:
        """Turn internal Django API private to clean up the API (default `False`).

        This patches your local pip-installed django installation. You can undo
        the patch by setting this back to `False`.
        """
        return self._private_django_api_path.exists()

    @private_django_api.setter
    def private_django_api(self, value: bool) -> None:
        from ._private_django_api import private_django_api

        # we don't want to call private_django_api() twice
        if value and not self.private_django_api:
            private_django_api()
            self._private_django_api_path.touch()
        elif not value and self.private_django_api:
            private_django_api(reverse=True)
            self._private_django_api_path.unlink(missing_ok=True)

    @property
    def user(self) -> UserSettings:
        """Settings of current user."""
        env_changed = (
            self._user_settings_env is not None
            and self._user_settings_env != get_env_name()
        )
        if self._user_settings is None or env_changed:
            # only uses LAMIN_API_KEY if there is no current_user.env
            self._user_settings = load_or_create_user_settings(
                api_key=os.environ.get("LAMIN_API_KEY")
            )
            self._user_settings_env = get_env_name()
            if self._user_settings and self._user_settings.uid is None:
                raise RuntimeError("Need to login, first: lamin login")
        return self._user_settings  # type: ignore

    @property
    def instance(self) -> InstanceSettings:
        """Settings of current LaminDB instance."""
        env_changed = (
            self._instance_settings_env is not None
            and self._instance_settings_env != get_env_name()
        )
        if self._instance_settings is None or env_changed:
            self._instance_settings = load_instance_settings()
            self._instance_settings_env = get_env_name()
        return self._instance_settings  # type: ignore

    @property
    def storage(self) -> StorageSettings:
        """Settings of default storage."""
        return self.instance.storage

    @property
    def _instance_exists(self):
        try:
            self.instance  # noqa
            return True
        # this is implicit logic that catches if no instance is loaded
        except SystemExit:
            return False

    @property
    def cache_dir(self) -> UPath:
        """Cache root, a local directory to cache cloud files."""
        if "LAMIN_CACHE_DIR" in os.environ:
            cache_dir = UPath(os.environ["LAMIN_CACHE_DIR"])
        elif self._cache_dir is None:
            cache_path = load_system_storage_settings().get("lamindb_cache_path", None)
            cache_dir = _process_cache_path(cache_path)
            if cache_dir is None:
                cache_dir = DEFAULT_CACHE_DIR
            self._cache_dir = cache_dir
        else:
            cache_dir = self._cache_dir
        cache_dir.mkdir(parents=True, exist_ok=True)
        return cache_dir

    @property
    def paths(self) -> type[SetupPaths]:
        """Convert cloud paths to lamindb local paths.

        Use `settings.paths.cloud_to_local_no_update`
        or `settings.paths.cloud_to_local`.
        """
        return SetupPaths

    def __repr__(self) -> str:
        """Rich string representation."""
        # do not show current setting representation when building docs
        if "sphinx" in sys.modules:
            return object.__repr__(self)
        repr = self.user.__repr__()
        repr += f"\nAuto-connect in Python: {self.auto_connect}\n"
        repr += f"Private Django API: {self.private_django_api}\n"
        repr += f"Cache directory: {self.cache_dir.as_posix()}\n"
        if self._instance_exists:
            repr += self.instance.__repr__()
        else:
            repr += "\nNo instance connected"
        return repr


class SetupPaths:
    """A static class for conversion of cloud paths to lamindb local paths."""

    @staticmethod
    def cloud_to_local_no_update(
        filepath: UPathStr, cache_key: str | None = None
    ) -> UPath:
        """Local (or local cache) filepath from filepath without synchronization."""
        if not isinstance(filepath, UPath):
            filepath = UPath(filepath)
        # cache_key is ignored if filepath is a local path
        if not isinstance(filepath, LocalPathClasses):
            # settings is defined further in this file
            if cache_key is None:
                local_key = filepath.path  # type: ignore
                protocol = filepath.protocol  # type: ignore
                if protocol in {"http", "https"}:
                    local_key = local_key.removeprefix(protocol + "://")
            else:
                local_key = cache_key
            local_filepath = settings.cache_dir / local_key
        else:
            local_filepath = filepath
        return local_filepath

    @staticmethod
    def cloud_to_local(
        filepath: UPathStr, cache_key: str | None = None, **kwargs
    ) -> UPath:
        """Local (or local cache) filepath from filepath."""
        if not isinstance(filepath, UPath):
            filepath = UPath(filepath)
        # cache_key is ignored in cloud_to_local_no_update if filepath is local
        local_filepath = SetupPaths.cloud_to_local_no_update(filepath, cache_key)
        if not isinstance(filepath, LocalPathClasses):
            local_filepath.parent.mkdir(parents=True, exist_ok=True)
            filepath.synchronize(local_filepath, **kwargs)  # type: ignore
        return local_filepath


def get_env_name():
    if "LAMIN_ENV" in os.environ:
        return os.environ["LAMIN_ENV"]
    else:
        return "prod"


settings = SetupSettings()
