from __future__ import annotations

import os
import sys
import warnings
from typing import TYPE_CHECKING

from lamin_utils import logger
from platformdirs import user_cache_dir

from lamindb_setup.errors import CurrentInstanceNotConfigured

from ._settings_load import (
    load_cache_path_from_settings,
    load_instance_settings,
    load_or_create_user_settings,
)
from ._settings_store import (
    current_instance_settings_file,
    settings_dir,
    system_settings_dir,
)
from .upath import LocalPathClasses, UPath

if TYPE_CHECKING:
    from pathlib import Path

    from lamindb.models import Branch, Space

    from lamindb_setup.core import InstanceSettings, StorageSettings, UserSettings
    from lamindb_setup.types import UPathStr


DEFAULT_CACHE_DIR = UPath(user_cache_dir(appname="lamindb", appauthor="laminlabs"))


def _process_cache_path(cache_path: UPathStr | None) -> UPath | None:
    if cache_path is None or cache_path == "null":
        return None
    cache_dir = UPath(cache_path)
    if not isinstance(cache_dir, LocalPathClasses):
        raise ValueError("cache dir should be a local path.")
    if cache_dir.exists() and not cache_dir.is_dir():
        raise ValueError("cache dir should be a directory.")
    if not cache_dir.is_absolute():
        raise ValueError("A path to the cache dir should be absolute.")
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

    _branch = None  # do not have types here
    _space = None  # do not have types here

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
        return True

    @auto_connect.setter
    def auto_connect(self, value: bool) -> None:
        # logger.warning(
        #     "setting auto_connect to `False` no longer has an effect and the setting will likely be removed in the future; since lamindb 1.7, auto_connect `True` no longer clashes with connecting in a Python session",
        # )
        if value:
            self._auto_connect_path.touch()
        else:
            self._auto_connect_path.unlink(missing_ok=True)

    @property
    def _branch_path(self) -> Path:
        return (
            settings_dir
            / f"current-branch--{self.instance.owner}--{self.instance.name}.txt"
        )

    def _read_branch_idlike_name(self) -> tuple[int | str, str]:
        idlike: str | int = 1
        name: str = "main"
        try:
            branch_path = self._branch_path
        except SystemExit:  # in case no instance setup
            return idlike, name
        if branch_path.exists():
            idlike, name = branch_path.read_text().split("\n")
        return idlike, name

    @property
    # TODO: refactor so that it returns a BranchMock object
    # and we never need a DB request
    def branch(self) -> Branch:
        """Default branch."""
        if self._branch is None:
            from lamindb import Branch

            idlike, _ = self._read_branch_idlike_name()
            self._branch = Branch.get(idlike)
        return self._branch

    @branch.setter
    def branch(self, value: str | Branch) -> None:
        from lamindb import Branch, Q
        from lamindb.errors import DoesNotExist

        if isinstance(value, Branch):
            assert value._state.adding is False, "Branch must be saved"
            branch_record = value
        else:
            branch_record = Branch.filter(Q(name=value) | Q(uid=value)).one_or_none()
            if branch_record is None:
                raise DoesNotExist(
                    f"Branch '{value}', please check on the hub UI whether you have the correct `uid` or `name`."
                )
        # we are sure that the current instance is setup because
        # it will error on lamindb import otherwise
        self._branch_path.write_text(f"{branch_record.uid}\n{branch_record.name}")
        self._branch = branch_record

    @property
    def _space_path(self) -> Path:
        return (
            settings_dir
            / f"current-space--{self.instance.owner}--{self.instance.name}.txt"
        )

    def _read_space_idlike_name(self) -> tuple[int | str, str]:
        idlike: str | int = 1
        name: str = "all"
        try:
            space_path = self._space_path
        except SystemExit:  # in case no instance setup
            return idlike, name
        if space_path.exists():
            idlike, name = space_path.read_text().split("\n")
        return idlike, name

    @property
    # TODO: refactor so that it returns a BranchMock object
    # and we never need a DB request
    def space(self) -> Space:
        """Default space."""
        if self._space is None:
            from lamindb import Space

            idlike, _ = self._read_space_idlike_name()
            self._space = Space.get(idlike)
        return self._space

    @space.setter
    def space(self, value: str | Space) -> None:
        from lamindb import Q, Space
        from lamindb.errors import DoesNotExist

        if isinstance(value, Space):
            assert value._state.adding is False, "Space must be saved"
            space_record = value
        else:
            space_record = Space.filter(Q(name=value) | Q(uid=value)).one_or_none()
            if space_record is None:
                raise DoesNotExist(
                    f"Space '{value}', please check on the hub UI whether you have the correct `uid` or `name`."
                )
        # we are sure that the current instance is setup because
        # it will error on lamindb import otherwise
        self._space_path.write_text(f"{space_record.uid}\n{space_record.name}")
        self._space = space_record

    @property
    def is_connected(self) -> bool:
        """Determine whether the current instance is fully connected and ready to use.

        If `True`, the current instance is connected, meaning that the db and other settings
        are properly configured for use.
        """
        if self._instance_exists:
            return self.instance.slug != "none/none"
        else:
            return False

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
        except CurrentInstanceNotConfigured:
            return False

    @property
    def cache_dir(self) -> UPath:
        """Cache root, a local directory to cache cloud files."""
        if "LAMIN_CACHE_DIR" in os.environ:
            cache_dir = UPath(os.environ["LAMIN_CACHE_DIR"])
        elif self._cache_dir is None:
            cache_path = load_cache_path_from_settings()
            cache_dir = _process_cache_path(cache_path)
            if cache_dir is None:
                cache_dir = DEFAULT_CACHE_DIR
            self._cache_dir = cache_dir
        else:
            cache_dir = self._cache_dir
        try:
            cache_dir.mkdir(parents=True, exist_ok=True)
        # we don not want this to error
        # beause no actual writing happens on just getting the cache dir
        # in cloud_to_local_no_update for example
        # so it should not fail on read-only systems
        except Exception as e:
            logger.warning(
                f"Failed to create lamin cache directory at {cache_dir}: {e}"
            )
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
        repr = ""
        if self._instance_exists:
            repr += "Current branch & space:\n"
            repr += f" - branch: {self._read_branch_idlike_name()[1]}\n"
            repr += f" - space: {self._read_space_idlike_name()[1]}\n"
            repr += self.instance.__repr__()
        else:
            repr += "Current instance: None"
        repr += "\nConfig:\n"
        repr += f" - auto-connect in Python: {self.auto_connect}\n"
        repr += f" - private Django API: {self.private_django_api}\n"
        repr += "Local directories:\n"
        repr += f" - cache: {self.cache_dir.as_posix()}\n"
        repr += f" - user settings: {settings_dir.as_posix()}\n"
        repr += f" - system settings: {system_settings_dir.as_posix()}\n"
        repr += self.user.__repr__()
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
            filepath.synchronize_to(local_filepath, **kwargs)  # type: ignore
        return local_filepath


def get_env_name():
    if "LAMIN_ENV" in os.environ:
        return os.environ["LAMIN_ENV"]
    else:
        return "prod"


settings = SetupSettings()
