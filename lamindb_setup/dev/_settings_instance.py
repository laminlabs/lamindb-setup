import os
import shutil
from pathlib import Path
from typing import Literal, Optional, Set, Tuple, Union
from uuid import UUID

from lamin_utils import logger

from ._settings_save import save_instance_settings
from ._settings_storage import StorageSettings
from ._settings_store import current_instance_settings_file, instance_settings_file
from .upath import UPath


class InstanceSettings:
    """Instance settings."""

    def __init__(
        self,
        owner: str,  # owner handle
        name: str,  # instance name
        storage_root: Union[str, Path, UPath],  # storage location on cloud
        storage_region: Optional[str] = None,
        db: Optional[str] = None,  # DB URI
        schema: Optional[str] = None,  # comma-separated string of schema names
        id: Optional[UUID] = None,  # instance id
    ):
        self._owner: str = owner
        self._name: str = name
        self._storage: StorageSettings = StorageSettings(
            storage_root, region=storage_region
        )
        self._db: Optional[str] = db
        self._schema_str: Optional[str] = schema
        self._id: Optional[UUID] = id

    def __repr__(self):
        """Rich string representation."""
        representation = f"Current instance: {self.identifier}"
        attrs = ["owner", "name", "storage", "db", "schema"]
        for attr in attrs:
            value = getattr(self, attr)
            if attr == "storage":
                representation += f"\n- storage root: {value.root_as_str}"
                representation += f"\n- storage region: {value.region}"
            else:
                representation += f"\n- {attr}: {value}"
        return representation

    @property
    def owner(self) -> str:
        """Instance owner. A user or organization account handle."""
        return self._owner

    @property
    def name(self) -> str:
        """Instance name."""
        return self._name

    @property
    def identifier(self) -> str:
        """Unique semantic identifier.

        See remote instances at https://lamin.ai/owner/name.
        """
        return f"{self.owner}/{self.name}"

    @property
    def id(self) -> Optional[UUID]:
        """The instance id."""
        return self._id

    @property
    def schema(self) -> Set[str]:
        """Schema modules in addition to core schema."""
        if self._schema_str is None:
            return {}  # type: ignore
        else:
            return {schema for schema in self._schema_str.split(",") if schema != ""}

    @property
    def _sqlite_file(self) -> UPath:
        """SQLite file."""
        return self.storage.key_to_filepath(f"{self.name}.lndb")

    @property
    def _sqlite_file_local(self) -> Path:
        """Local SQLite file."""
        return self.storage.cloud_to_local_no_update(self._sqlite_file)

    def _update_cloud_sqlite_file(self) -> None:
        """Upload the local sqlite file to the cloud file."""
        if self._is_cloud_sqlite:
            sqlite_file = self._sqlite_file
            logger.warning(
                f"updating & unlocking cloud SQLite '{sqlite_file}' of instance"
                f" '{self.identifier}'"
            )
            cache_file = self.storage.cloud_to_local_no_update(sqlite_file)
            sqlite_file.upload_from(cache_file)  # type: ignore
            cloud_mtime = sqlite_file.modified.timestamp()  # type: ignore
            # this seems to work even if there is an open connection
            # to the cache file
            os.utime(cache_file, times=(cloud_mtime, cloud_mtime))
            self._cloud_sqlite_locker.unlock()

    def _update_local_sqlite_file(self) -> None:
        """Download the cloud sqlite file if it is newer than local."""
        if self._is_cloud_sqlite:
            logger.warning(
                "updating local SQLite & locking cloud SQLite (sync back & unlock:"
                " lamin close)"
            )
            sqlite_file = self._sqlite_file
            cache_file = self.storage.cloud_to_local_no_update(sqlite_file)
            sqlite_file.synchronize(cache_file)  # type: ignore
            self._cloud_sqlite_locker.lock()

    @property
    def db(self) -> str:
        """Database connection string (URI)."""
        if self._db is None:
            # here, we want the updated sqlite file
            # hence, we don't use self._sqlite_file_local()
            return f"sqlite:///{self.storage.cloud_to_local(self._sqlite_file)}"
        else:
            return self._db

    @property
    def dialect(self) -> Literal["sqlite", "postgresql"]:
        """SQL dialect."""
        if self._db is None or self._db.startswith("sqlite://"):
            return "sqlite"
        else:
            assert self._db.startswith("postgresql")
            return "postgresql"

    @property
    def session(self):
        raise NotImplementedError

    @property
    def _is_cloud_sqlite(self) -> bool:
        # can we make this a private property, Sergei?
        # as it's not relevant to the user
        """Is this a cloud instance with sqlite db."""
        return self.dialect == "sqlite" and self.storage.is_cloud

    @property
    def _cloud_sqlite_locker(self):
        # avoid circular import
        from .cloud_sqlite_locker import empty_locker, get_locker

        if self._is_cloud_sqlite:
            try:
                return get_locker(self)
            except PermissionError:
                logger.warning("read-only access - did not access locker")
                return empty_locker
        else:
            return empty_locker

    @property
    def storage(self) -> StorageSettings:
        """Low-level access to storage location."""
        return self._storage

    @property
    def is_remote(self) -> bool:
        """Boolean indicating if an instance has no local component."""
        if not self.storage.is_cloud:
            return False

        def is_local_uri(uri: str):
            if "@localhost:" in uri:
                return True
            if "@0.0.0.0:" in uri:
                return True
            if "@127.0.0.1" in uri:
                return True
            return False

        if self.dialect == "postgresql":
            if is_local_uri(self.db):
                return False
        # returns True for cloud SQLite
        # and remote postgres
        return True

    def _get_settings_file(self) -> Path:
        return instance_settings_file(self.name, self.owner)

    def _persist(self) -> None:
        assert self.name is not None
        if self.storage.type == "local":
            self.storage.root.mkdir(parents=True, exist_ok=True)

        filepath = self._get_settings_file()
        # persist under filepath for later reference
        save_instance_settings(self, filepath)
        # persist under current file for auto load
        shutil.copy2(filepath, current_instance_settings_file())
        # persist under settings class for same session reference
        # need to import here to avoid circular import
        from .._settings import settings

        settings._instance_settings = self

    def _init_db(self):
        from .django import setup_django

        setup_django(self, deploy_migrations=True, init=True)

    def _load_db(self) -> Tuple[bool, str]:
        # Is the database available and initialized as LaminDB?
        # returns a tuple of status code and message
        if self.dialect == "sqlite" and not self._sqlite_file.exists():
            return False, "SQLite file does not exist"
        from .django import setup_django

        # we need the local sqlite to setup django
        self._update_local_sqlite_file()
        # setting up django also performs a check for migrations & prints them
        # as warnings
        # this should fail, e.g., if the db is not reachable
        setup_django(self)
        return True, ""
