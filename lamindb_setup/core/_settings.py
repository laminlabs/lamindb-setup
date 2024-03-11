import os
from typing import Union, Optional
from lamindb_setup.core import InstanceSettings, StorageSettings, UserSettings
from lamindb_setup.core._settings_load import (
    load_instance_settings,
    load_or_create_user_settings,
)
from ._settings_store import current_instance_settings_file
from pathlib import Path
from ._settings_store import settings_dir


class SetupSettings:
    """Setup settings.

    - :class:`~lamindb_setup.core.InstanceSettings`
    - :class:`~lamindb_setup.core.StorageSettings`
    - :class:`~lamindb_setup.core.UserSettings`
    """

    _using_key: Optional[str] = None  # set through lamindb.settings

    _user_settings: Union[UserSettings, None] = None
    _instance_settings: Union[InstanceSettings, None] = None

    _user_settings_env: Union[str, None] = None
    _instance_settings_env: Union[str, None] = None

    _auto_connect_path: Path = settings_dir / "auto_connect"

    @property
    def _instance_settings_path(self) -> Path:
        return current_instance_settings_file()

    @property
    def settings_dir(self) -> Path:
        return settings_dir

    @property
    def auto_connect(self) -> bool:
        """Auto-connect to loaded instance upon lamindb import."""
        return self._auto_connect_path.exists()

    @auto_connect.setter
    def auto_connect(self, value: bool) -> None:
        if value:
            self._auto_connect_path.touch()
        else:
            self._auto_connect_path.unlink(missing_ok=True)

    @property
    def user(self) -> UserSettings:
        """User."""
        if (
            self._user_settings is None
            or self._user_settings_env != get_env_name()  # noqa
        ):
            self._user_settings = load_or_create_user_settings()
            self._user_settings_env = get_env_name()
            if self._user_settings and self._user_settings.uid is None:
                raise RuntimeError("Need to login, first: lamin login <email>")
        return self._user_settings  # type: ignore

    @property
    def instance(self) -> InstanceSettings:
        """Instance."""
        if (
            self._instance_settings is None
            or self._instance_settings_env != get_env_name()  # noqa
        ):
            self._instance_settings = load_instance_settings()
            self._instance_settings_env = get_env_name()
        return self._instance_settings  # type: ignore

    @property
    def storage(self) -> StorageSettings:
        """Storage."""
        return self.instance.storage

    @property
    def _instance_exists(self):
        try:
            self.instance
            return True
        # this is implicit logic that catches if no instance is loaded
        except SystemExit:
            return False

    def __repr__(self) -> str:
        """Rich string representation."""
        repr = self.user.__repr__()
        repr += f"\nAuto-connect in Python: {self.auto_connect}\n"
        if self._instance_exists:
            repr += self.instance.__repr__()
        else:
            repr += "\nNo instance connected"
        return repr


def get_env_name():
    if "LAMIN_ENV" in os.environ:
        return os.environ["LAMIN_ENV"]
    else:
        return "prod"


settings = SetupSettings()
